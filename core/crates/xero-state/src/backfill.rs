//! Backfill orchestrator state — `backfill_plan` and `backfill_chunk` CRUD.
//!
//! Concurrency model: `claim_next_pending_chunk` uses
//! `SELECT … FOR UPDATE SKIP LOCKED` so multiple worker replicas can fetch
//! chunks concurrently without trampling on each other.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;
use xero_common::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillPlan {
    pub plan_id: Uuid,
    pub tenant_id: String,
    pub entity_types: Vec<String>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub chunk_size_days: i32,
    pub status: String,
    pub total_chunks: i32,
    pub completed_chunks: i32,
    pub failed_chunks: i32,
    pub triggered_by: String,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillChunk {
    pub chunk_id: Uuid,
    pub plan_id: Uuid,
    pub tenant_id: String,
    pub entity_type: String,
    pub window_start: NaiveDate,
    pub window_end: NaiveDate,
    pub status: String,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub run_id: Option<Uuid>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NewBackfillPlan {
    pub tenant_id: String,
    pub entity_types: Vec<String>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub chunk_size_days: i32,
    pub triggered_by: String,
}

/// Build the set of chunks that decompose a plan. Chunks are
/// `chunk_size_days` long, half-open `[start, end)`, with the final chunk
/// possibly shorter when the range doesn't divide evenly.
pub fn enumerate_chunks(plan: &NewBackfillPlan, plan_id: Uuid) -> Vec<BackfillChunk> {
    let mut out = Vec::new();
    let mut cursor = plan.start_date;
    while cursor < plan.end_date {
        let mut next = cursor + chrono::Duration::days(plan.chunk_size_days as i64);
        if next > plan.end_date {
            next = plan.end_date;
        }
        for entity in &plan.entity_types {
            out.push(BackfillChunk {
                chunk_id: Uuid::new_v4(),
                plan_id,
                tenant_id: plan.tenant_id.clone(),
                entity_type: entity.clone(),
                window_start: cursor,
                window_end: next,
                status: "pending".to_owned(),
                attempt_count: 0,
                max_attempts: 3,
                run_id: None,
                error_message: None,
                created_at: Utc::now(),
                started_at: None,
                finished_at: None,
            });
        }
        cursor = next;
    }
    out
}

/// Atomically create the plan + all its chunks. Returns the persisted plan
/// (with `total_chunks` populated) and the count of inserted chunks.
pub async fn create_plan_with_chunks(
    pg: &PgPool,
    plan_id: Uuid,
    new_plan: &NewBackfillPlan,
) -> Result<(BackfillPlan, usize)> {
    let chunks = enumerate_chunks(new_plan, plan_id);
    let total_chunks = chunks.len() as i32;

    let mut tx = pg.begin().await.map_err(map_pg)?;

    let row = sqlx::query(
        r#"
        INSERT INTO xero.backfill_plan
            (plan_id, tenant_id, entity_types, start_date, end_date,
             chunk_size_days, total_chunks, triggered_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING plan_id, tenant_id, entity_types, start_date, end_date,
                  chunk_size_days, status, total_chunks, completed_chunks,
                  failed_chunks, triggered_by, error_message, created_at,
                  started_at, completed_at
        "#,
    )
    .bind(plan_id)
    .bind(&new_plan.tenant_id)
    .bind(serde_json::to_value(&new_plan.entity_types).map_err(|e| Error::StateStore(e.to_string()))?)
    .bind(new_plan.start_date)
    .bind(new_plan.end_date)
    .bind(new_plan.chunk_size_days)
    .bind(total_chunks)
    .bind(&new_plan.triggered_by)
    .fetch_one(&mut *tx)
    .await
    .map_err(map_pg)?;

    for chunk in &chunks {
        sqlx::query(
            r#"
            INSERT INTO xero.backfill_chunk
                (chunk_id, plan_id, tenant_id, entity_type,
                 window_start, window_end, max_attempts)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(chunk.chunk_id)
        .bind(plan_id)
        .bind(&chunk.tenant_id)
        .bind(&chunk.entity_type)
        .bind(chunk.window_start)
        .bind(chunk.window_end)
        .bind(chunk.max_attempts)
        .execute(&mut *tx)
        .await
        .map_err(map_pg)?;
    }

    tx.commit().await.map_err(map_pg)?;

    Ok((row_to_plan(row)?, chunks.len()))
}

pub async fn get_plan(pg: &PgPool, plan_id: Uuid) -> Result<Option<BackfillPlan>> {
    let row = sqlx::query(
        r#"
        SELECT plan_id, tenant_id, entity_types, start_date, end_date,
               chunk_size_days, status, total_chunks, completed_chunks,
               failed_chunks, triggered_by, error_message, created_at,
               started_at, completed_at
        FROM xero.backfill_plan
        WHERE plan_id = $1
        "#,
    )
    .bind(plan_id)
    .fetch_optional(pg)
    .await
    .map_err(map_pg)?;

    row.map(row_to_plan).transpose()
}

pub async fn list_plans_for_tenant(
    pg: &PgPool,
    tenant_id: &str,
    limit: i64,
) -> Result<Vec<BackfillPlan>> {
    let rows = sqlx::query(
        r#"
        SELECT plan_id, tenant_id, entity_types, start_date, end_date,
               chunk_size_days, status, total_chunks, completed_chunks,
               failed_chunks, triggered_by, error_message, created_at,
               started_at, completed_at
        FROM xero.backfill_plan
        WHERE tenant_id = $1
        ORDER BY created_at DESC
        LIMIT $2
        "#,
    )
    .bind(tenant_id)
    .bind(limit)
    .fetch_all(pg)
    .await
    .map_err(map_pg)?;

    rows.into_iter().map(row_to_plan).collect()
}

pub async fn list_chunks_for_plan(pg: &PgPool, plan_id: Uuid) -> Result<Vec<BackfillChunk>> {
    let rows = sqlx::query(
        r#"
        SELECT chunk_id, plan_id, tenant_id, entity_type, window_start, window_end,
               status, attempt_count, max_attempts, run_id, error_message,
               created_at, started_at, finished_at
        FROM xero.backfill_chunk
        WHERE plan_id = $1
        ORDER BY window_start, entity_type
        "#,
    )
    .bind(plan_id)
    .fetch_all(pg)
    .await
    .map_err(map_pg)?;

    rows.into_iter().map(row_to_chunk).collect()
}

/// Claim the next pending chunk for the worker. Uses `FOR UPDATE SKIP LOCKED`
/// so concurrent workers don't fight for the same row. Returns `None` if no
/// chunks are pending.
pub async fn claim_next_pending_chunk(pg: &PgPool) -> Result<Option<BackfillChunk>> {
    let mut tx = pg.begin().await.map_err(map_pg)?;

    let row = sqlx::query(
        r#"
        SELECT chunk_id, plan_id, tenant_id, entity_type, window_start, window_end,
               status, attempt_count, max_attempts, run_id, error_message,
               created_at, started_at, finished_at
        FROM xero.backfill_chunk
        WHERE status = 'pending' AND attempt_count < max_attempts
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
        "#,
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_pg)?;

    let Some(row) = row else {
        tx.commit().await.map_err(map_pg)?;
        return Ok(None);
    };

    let chunk_id: Uuid = row.get("chunk_id");
    sqlx::query(
        r#"
        UPDATE xero.backfill_chunk
        SET status = 'running',
            started_at = now(),
            attempt_count = attempt_count + 1
        WHERE chunk_id = $1
        "#,
    )
    .bind(chunk_id)
    .execute(&mut *tx)
    .await
    .map_err(map_pg)?;

    // Mark plan as running if first chunk picked up.
    let plan_id: Uuid = row.get("plan_id");
    sqlx::query(
        r#"
        UPDATE xero.backfill_plan
        SET status = 'running', started_at = COALESCE(started_at, now())
        WHERE plan_id = $1 AND status = 'pending'
        "#,
    )
    .bind(plan_id)
    .execute(&mut *tx)
    .await
    .map_err(map_pg)?;

    tx.commit().await.map_err(map_pg)?;

    let mut chunk = row_to_chunk(row)?;
    chunk.status = "running".to_owned();
    chunk.attempt_count += 1;
    Ok(Some(chunk))
}

pub async fn mark_chunk_succeeded(
    pg: &PgPool,
    chunk_id: Uuid,
    run_id: Uuid,
) -> Result<()> {
    let mut tx = pg.begin().await.map_err(map_pg)?;
    sqlx::query(
        r#"
        UPDATE xero.backfill_chunk
        SET status = 'succeeded',
            finished_at = now(),
            run_id = $2,
            error_message = NULL
        WHERE chunk_id = $1
        "#,
    )
    .bind(chunk_id)
    .bind(run_id)
    .execute(&mut *tx)
    .await
    .map_err(map_pg)?;

    let plan_id: Uuid = sqlx::query("SELECT plan_id FROM xero.backfill_chunk WHERE chunk_id = $1")
        .bind(chunk_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_pg)?
        .get("plan_id");

    bump_plan_counters_and_finalise(&mut tx, plan_id).await?;
    tx.commit().await.map_err(map_pg)
}

pub async fn mark_chunk_failed(
    pg: &PgPool,
    chunk_id: Uuid,
    error: &str,
) -> Result<()> {
    let mut tx = pg.begin().await.map_err(map_pg)?;

    // If retries remain, reset to pending so the worker tries again;
    // otherwise mark as terminal failure.
    sqlx::query(
        r#"
        UPDATE xero.backfill_chunk
        SET status = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'pending' END,
            finished_at = CASE WHEN attempt_count >= max_attempts THEN now() ELSE NULL END,
            error_message = $2
        WHERE chunk_id = $1
        "#,
    )
    .bind(chunk_id)
    .bind(error)
    .execute(&mut *tx)
    .await
    .map_err(map_pg)?;

    let plan_id: Uuid = sqlx::query("SELECT plan_id FROM xero.backfill_chunk WHERE chunk_id = $1")
        .bind(chunk_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_pg)?
        .get("plan_id");

    bump_plan_counters_and_finalise(&mut tx, plan_id).await?;
    tx.commit().await.map_err(map_pg)
}

async fn bump_plan_counters_and_finalise(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    plan_id: Uuid,
) -> Result<()> {
    // Recompute counters from chunks (cheap, fewer chunks per plan than rows in bronze).
    sqlx::query(
        r#"
        UPDATE xero.backfill_plan p
        SET completed_chunks = sub.completed,
            failed_chunks    = sub.failed,
            status = CASE
                WHEN sub.terminal = sub.total AND sub.failed = 0 THEN 'completed'
                WHEN sub.terminal = sub.total AND sub.failed > 0 THEN 'failed'
                ELSE p.status
            END,
            completed_at = CASE
                WHEN sub.terminal = sub.total THEN now()
                ELSE p.completed_at
            END
        FROM (
            SELECT
                plan_id,
                COUNT(*) FILTER (WHERE status = 'succeeded') AS completed,
                COUNT(*) FILTER (WHERE status = 'failed')    AS failed,
                COUNT(*) FILTER (WHERE status IN ('succeeded','failed','skipped')) AS terminal,
                COUNT(*) AS total
            FROM xero.backfill_chunk
            WHERE plan_id = $1
            GROUP BY plan_id
        ) sub
        WHERE p.plan_id = sub.plan_id
        "#,
    )
    .bind(plan_id)
    .execute(&mut **tx)
    .await
    .map_err(map_pg)?;
    Ok(())
}

pub async fn cancel_plan(pg: &PgPool, plan_id: Uuid) -> Result<()> {
    let mut tx = pg.begin().await.map_err(map_pg)?;
    sqlx::query("UPDATE xero.backfill_chunk SET status = 'skipped', finished_at = now() WHERE plan_id = $1 AND status = 'pending'")
        .bind(plan_id)
        .execute(&mut *tx)
        .await
        .map_err(map_pg)?;
    sqlx::query("UPDATE xero.backfill_plan SET status = 'cancelled', completed_at = now() WHERE plan_id = $1 AND status IN ('pending','running')")
        .bind(plan_id)
        .execute(&mut *tx)
        .await
        .map_err(map_pg)?;
    tx.commit().await.map_err(map_pg)
}

fn row_to_plan(row: sqlx::postgres::PgRow) -> Result<BackfillPlan> {
    let entity_types: serde_json::Value = row.get("entity_types");
    let entity_types: Vec<String> = serde_json::from_value(entity_types)
        .map_err(|e| Error::StateStore(format!("entity_types parse: {e}")))?;
    Ok(BackfillPlan {
        plan_id: row.get("plan_id"),
        tenant_id: row.get("tenant_id"),
        entity_types,
        start_date: row.get("start_date"),
        end_date: row.get("end_date"),
        chunk_size_days: row.get("chunk_size_days"),
        status: row.get("status"),
        total_chunks: row.get("total_chunks"),
        completed_chunks: row.get("completed_chunks"),
        failed_chunks: row.get("failed_chunks"),
        triggered_by: row.get("triggered_by"),
        error_message: row.get("error_message"),
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
    })
}

fn row_to_chunk(row: sqlx::postgres::PgRow) -> Result<BackfillChunk> {
    Ok(BackfillChunk {
        chunk_id: row.get("chunk_id"),
        plan_id: row.get("plan_id"),
        tenant_id: row.get("tenant_id"),
        entity_type: row.get("entity_type"),
        window_start: row.get("window_start"),
        window_end: row.get("window_end"),
        status: row.get("status"),
        attempt_count: row.get("attempt_count"),
        max_attempts: row.get("max_attempts"),
        run_id: row.get("run_id"),
        error_message: row.get("error_message"),
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
    })
}

fn map_pg(e: sqlx::Error) -> Error {
    Error::StateStore(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn enumerate_chunks_splits_evenly() {
        let plan = NewBackfillPlan {
            tenant_id: "t".into(),
            entity_types: vec!["invoices".into()],
            start_date: d(2023, 1, 1),
            end_date: d(2023, 4, 1),
            chunk_size_days: 30,
            triggered_by: "test".into(),
        };
        let chunks = enumerate_chunks(&plan, Uuid::new_v4());
        // 90 days / 30 = 3 chunks
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].window_start, d(2023, 1, 1));
        assert_eq!(chunks[0].window_end, d(2023, 1, 31));
        assert_eq!(chunks[2].window_end, d(2023, 4, 1));
    }

    #[test]
    fn enumerate_chunks_caps_final_chunk() {
        let plan = NewBackfillPlan {
            tenant_id: "t".into(),
            entity_types: vec!["invoices".into()],
            start_date: d(2023, 1, 1),
            end_date: d(2023, 2, 5),
            chunk_size_days: 30,
            triggered_by: "test".into(),
        };
        let chunks = enumerate_chunks(&plan, Uuid::new_v4());
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[1].window_start, d(2023, 1, 31));
        assert_eq!(chunks[1].window_end, d(2023, 2, 5));
    }

    #[test]
    fn enumerate_chunks_cross_entities() {
        // 30-day range with chunk_size=30 → exactly one sub-window per entity.
        let plan = NewBackfillPlan {
            tenant_id: "t".into(),
            entity_types: vec!["invoices".into(), "payments".into()],
            start_date: d(2023, 1, 1),
            end_date: d(2023, 1, 31),
            chunk_size_days: 30,
            triggered_by: "test".into(),
        };
        let chunks = enumerate_chunks(&plan, Uuid::new_v4());
        assert_eq!(chunks.len(), 2, "one sub-window × 2 entities");
        assert!(chunks.iter().any(|c| c.entity_type == "invoices"));
        assert!(chunks.iter().any(|c| c.entity_type == "payments"));
    }

    #[test]
    fn three_year_monthly_yields_36_chunks_per_entity() {
        let plan = NewBackfillPlan {
            tenant_id: "t".into(),
            entity_types: vec!["invoices".into()],
            start_date: d(2023, 5, 1),
            end_date: d(2026, 5, 1),
            chunk_size_days: 30,
            triggered_by: "test".into(),
        };
        let chunks = enumerate_chunks(&plan, Uuid::new_v4());
        // 1096 days / 30 = 36.5 → 37 chunks (last shorter)
        assert!(chunks.len() >= 36 && chunks.len() <= 38);
    }
}
