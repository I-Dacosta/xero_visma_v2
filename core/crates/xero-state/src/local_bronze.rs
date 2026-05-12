use crate::bq_sink::{self, BqRow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use tracing::warn;
use uuid::Uuid;
use xero_common::{EntityType, Error, Result};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct BronzeUpsertStats {
    pub inserted: i64,
    pub updated: i64,
    pub unchanged: i64,
    pub skipped_invalid: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BronzeSummary {
    pub entity_type: String,
    pub row_count: i64,
    pub distinct_runs_touching: i64,
    pub most_recent: Option<DateTime<Utc>>,
}

pub async fn summary_for_tenant(pg: &PgPool, tenant_id: &str) -> Result<Vec<BronzeSummary>> {
    let rows = sqlx::query(
        r#"
        SELECT entity_type,
               COUNT(*)                            AS row_count,
               COUNT(DISTINCT last_run_id)        AS distinct_runs_touching,
               MAX(last_seen_at)                  AS most_recent
        FROM   xero.local_bronze_record
        WHERE  tenant_id = $1
        GROUP  BY entity_type
        ORDER  BY entity_type
        "#,
    )
    .bind(tenant_id)
    .fetch_all(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(rows
        .into_iter()
        .map(|r| BronzeSummary {
            entity_type: r.get("entity_type"),
            row_count: r.get("row_count"),
            distinct_runs_touching: r.get("distinct_runs_touching"),
            most_recent: r.get("most_recent"),
        })
        .collect())
}

pub async fn upsert_records(
    pg: &PgPool,
    tenant_id: &str,
    entity: &EntityType,
    run_id: Uuid,
    records: &[Value],
) -> Result<BronzeUpsertStats> {
    let mut stats = BronzeUpsertStats::default();
    let now = Utc::now();
    // Collect rows that were either inserted or updated so we can stream them
    // to BigQuery (best-effort) after the pg commits.
    let mut accepted: Vec<BqRow> = Vec::new();

    for record in records {
        let Some(record_id) = record_id_for_entity(entity, record) else {
            stats.skipped_invalid += 1;
            continue;
        };

        let existing = sqlx::query(
            r#"
            SELECT payload
            FROM   xero.local_bronze_record
            WHERE  tenant_id = $1
              AND  entity_type = $2
              AND  record_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(entity.as_str())
        .bind(&record_id)
        .fetch_optional(pg)
        .await
        .map_err(|e| Error::StateStore(e.to_string()))?;

        if let Some(row) = existing {
            let existing_payload: Value = row.get("payload");
            if existing_payload == *record {
                stats.unchanged += 1;
                continue;
            }

            // Read `first_seen_at` so we can pass it to BQ unchanged.
            let first_seen_at: DateTime<Utc> = sqlx::query_scalar(
                r#"
                SELECT first_seen_at FROM xero.local_bronze_record
                WHERE  tenant_id=$1 AND entity_type=$2 AND record_id=$3
                "#,
            )
            .bind(tenant_id)
            .bind(entity.as_str())
            .bind(&record_id)
            .fetch_one(pg)
            .await
            .map_err(|e| Error::StateStore(e.to_string()))?;

            sqlx::query(
                r#"
                UPDATE xero.local_bronze_record
                SET    payload      = $4,
                       record_id_json = $5,
                       last_seen_at = $6,
                       last_run_id  = $7,
                       updated_at   = NOW()
                WHERE  tenant_id = $1
                  AND  entity_type = $2
                  AND  record_id = $3
                "#,
            )
            .bind(tenant_id)
            .bind(entity.as_str())
            .bind(&record_id)
            .bind(record)
            .bind(serde_json::json!({"record_id": record_id}))
            .bind(now)
            .bind(run_id)
            .execute(pg)
            .await
            .map_err(|e| Error::StateStore(e.to_string()))?;

            stats.updated += 1;
            accepted.push(BqRow {
                tenant_id: tenant_id.to_owned(),
                entity_type: entity.as_str().to_owned(),
                record_id: record_id.clone(),
                payload: record.clone(),
                first_seen_at,
                last_seen_at: now,
                last_run_id: run_id,
            });
        } else {
            sqlx::query(
                r#"
                INSERT INTO xero.local_bronze_record
                    (tenant_id, entity_type, record_id, record_id_json, payload,
                     first_seen_at, last_seen_at, last_run_id)
                VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
                "#,
            )
            .bind(tenant_id)
            .bind(entity.as_str())
            .bind(&record_id)
            .bind(serde_json::json!({"record_id": record_id}))
            .bind(record)
            .bind(now)
            .bind(run_id)
            .execute(pg)
            .await
            .map_err(|e| Error::StateStore(e.to_string()))?;

            stats.inserted += 1;
            accepted.push(BqRow {
                tenant_id: tenant_id.to_owned(),
                entity_type: entity.as_str().to_owned(),
                record_id: record_id.clone(),
                payload: record.clone(),
                first_seen_at: now,
                last_seen_at: now,
                last_run_id: run_id,
            });
        }
    }

    // Best-effort BQ stream. Failure here never bubbles — bronze is the source
    // of truth and the replay endpoint can push un-synced rows later.
    if !accepted.is_empty() {
        let sink = bq_sink::sink();
        if sink.is_active() {
            match sink.insert(&accepted).await {
                Ok(ok_count) if ok_count > 0 => {
                    let ids: Vec<String> =
                        accepted.iter().take(ok_count).map(|r| r.record_id.clone()).collect();
                    if let Err(e) = sqlx::query(
                        r#"
                        UPDATE xero.local_bronze_record
                        SET    bq_synced_at = NOW()
                        WHERE  tenant_id   = $1
                          AND  entity_type = $2
                          AND  record_id   = ANY($3::text[])
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(entity.as_str())
                    .bind(&ids)
                    .execute(pg)
                    .await
                    {
                        warn!(error = %e, "bq_synced_at update failed");
                    }
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "bq sink insert returned error"),
            }
        }
    }

    Ok(stats)
}

/// Push bronze rows that have `bq_synced_at IS NULL` to BigQuery.
/// Returns (candidates, accepted) counts. Best-effort: failures stay NULL
/// so the next replay picks them up again.
pub async fn replay_bq_pending(
    pg: &PgPool,
    tenant_id: &str,
    entity_filter: Option<&str>,
    limit: i64,
) -> Result<(i64, i64)> {
    let rows = if let Some(e) = entity_filter {
        sqlx::query(
            r#"
            SELECT tenant_id, entity_type, record_id, payload,
                   first_seen_at, last_seen_at, last_run_id
            FROM   xero.local_bronze_record
            WHERE  tenant_id = $1 AND entity_type = $2 AND bq_synced_at IS NULL
            ORDER  BY first_seen_at
            LIMIT  $3
            "#,
        )
        .bind(tenant_id)
        .bind(e)
        .bind(limit)
        .fetch_all(pg)
        .await
    } else {
        sqlx::query(
            r#"
            SELECT tenant_id, entity_type, record_id, payload,
                   first_seen_at, last_seen_at, last_run_id
            FROM   xero.local_bronze_record
            WHERE  tenant_id = $1 AND bq_synced_at IS NULL
            ORDER  BY first_seen_at
            LIMIT  $2
            "#,
        )
        .bind(tenant_id)
        .bind(limit)
        .fetch_all(pg)
        .await
    }
    .map_err(|e| Error::StateStore(e.to_string()))?;

    let candidates = rows.len() as i64;
    if candidates == 0 {
        return Ok((0, 0));
    }

    let batch: Vec<BqRow> = rows
        .into_iter()
        .map(|r| BqRow {
            tenant_id: r.get("tenant_id"),
            entity_type: r.get("entity_type"),
            record_id: r.get("record_id"),
            payload: r.get("payload"),
            first_seen_at: r.get("first_seen_at"),
            last_seen_at: r.get("last_seen_at"),
            last_run_id: r.get("last_run_id"),
        })
        .collect();

    let sink = bq_sink::sink();
    if !sink.is_active() {
        return Ok((candidates, 0));
    }
    let accepted = sink
        .insert(&batch)
        .await
        .map_err(|e| Error::StateStore(format!("bq sink: {e}")))?;

    if accepted > 0 {
        // Update by record-id-per-entity (group rows by entity for the IN list).
        let mut by_entity: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for r in batch.iter().take(accepted) {
            by_entity
                .entry(r.entity_type.clone())
                .or_default()
                .push(r.record_id.clone());
        }
        for (entity, ids) in by_entity {
            sqlx::query(
                r#"
                UPDATE xero.local_bronze_record
                SET    bq_synced_at = NOW()
                WHERE  tenant_id   = $1
                  AND  entity_type = $2
                  AND  record_id   = ANY($3::text[])
                "#,
            )
            .bind(tenant_id)
            .bind(entity)
            .bind(&ids)
            .execute(pg)
            .await
            .map_err(|e| Error::StateStore(e.to_string()))?;
        }
    }

    Ok((candidates, accepted as i64))
}

fn record_id_for_entity(entity: &EntityType, record: &Value) -> Option<String> {
    let id_field = entity.id_field();

    if let Some(s) = record.get(id_field).and_then(Value::as_str) {
        if !s.is_empty() {
            return Some(s.to_owned());
        }
    }

    if let Some(n) = record.get(id_field).and_then(Value::as_i64) {
        return Some(n.to_string());
    }

    if entity.as_str() == "tax_rates" {
        if let Some(s) = record.get("TaxType").and_then(Value::as_str) {
            if !s.is_empty() {
                return Some(s.to_owned());
            }
        }
    }

    None
}
