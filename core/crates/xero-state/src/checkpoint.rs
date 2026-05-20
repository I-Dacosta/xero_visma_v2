use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use sqlx::Row;
use xero_common::{EntityType, Error, Result, TenantId};

/// Composite primary key for a sync checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckpointKey {
    pub tenant: TenantId,
    pub entity_type: String,
}

impl CheckpointKey {
    pub fn new(tenant: impl Into<TenantId>, entity_type: &EntityType) -> Self {
        Self {
            tenant: tenant.into(),
            entity_type: entity_type.as_str().to_owned(),
        }
    }
}

/// Current watermark state for one (tenant, entity_type) pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub key: CheckpointKey,
    pub last_modified_watermark: Option<DateTime<Utc>>,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub records_seen: i64,
}

// ── DB helpers ────────────────────────────────────────────────────────────────

/// Load the checkpoint for a (tenant, entity) pair.
pub async fn load(pg: &PgPool, tenant_id: &str, entity_type: &str) -> Result<Option<Checkpoint>> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, entity_type,
               last_modified_watermark, last_sync_at, records_seen
        FROM   xero.sync_checkpoint
        WHERE  tenant_id   = $1
          AND  entity_type = $2
        "#,
    )
    .bind(tenant_id)
    .bind(entity_type)
    .fetch_optional(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(row.map(|r| Checkpoint {
        key: CheckpointKey {
            tenant: TenantId::new(r.get::<String, _>("tenant_id")),
            entity_type: r.get("entity_type"),
        },
        last_modified_watermark: r.get("last_modified_watermark"),
        last_sync_at: r.get("last_sync_at"),
        records_seen: r.get("records_seen"),
    }))
}

/// Upsert a checkpoint (update watermark + stats after a successful sync run).
///
/// Refuses to regress `last_modified_watermark`: if a stored watermark exists
/// and is strictly greater than the incoming one, the upsert is rejected with
/// `Error::StateStore`. This guards against an off-by-one in
/// `compute_next_watermark` or a misordered concurrent write rewinding the
/// cursor and re-fetching already-seen records (which would inflate the
/// dedupe pre-filter cost and risk masking upstream issues).
///
/// `last_modified_watermark = None` on the input is allowed unconditionally —
/// that's the "checkpoint reset" case used by manual ops.
pub async fn upsert(pg: &PgPool, cp: &Checkpoint) -> Result<()> {
    if let Some(new_wm) = cp.last_modified_watermark {
        let existing = load(pg, cp.key.tenant.as_str(), &cp.key.entity_type).await?;
        if let Some(existing) = existing {
            if let Some(old_wm) = existing.last_modified_watermark {
                if new_wm < old_wm {
                    return Err(Error::StateStore(format!(
                        "watermark regression refused: tenant={} entity={} \
                         existing={} attempted={}",
                        cp.key.tenant.as_str(),
                        cp.key.entity_type,
                        old_wm,
                        new_wm,
                    )));
                }
            }
        }
    }

    sqlx::query(
        r#"
        INSERT INTO xero.sync_checkpoint
            (tenant_id, entity_type, last_modified_watermark,
             last_sync_at, records_seen, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (tenant_id, entity_type) DO UPDATE SET
            last_modified_watermark = EXCLUDED.last_modified_watermark,
            last_sync_at            = EXCLUDED.last_sync_at,
            records_seen            = EXCLUDED.records_seen,
            updated_at              = NOW()
        "#,
    )
    .bind(cp.key.tenant.as_str())
    .bind(&cp.key.entity_type)
    .bind(cp.last_modified_watermark)
    .bind(cp.last_sync_at)
    .bind(cp.records_seen)
    .execute(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(())
}

/// List all checkpoints for a tenant, ordered by entity_type.
pub async fn list_for_tenant(pg: &PgPool, tenant_id: &str) -> Result<Vec<Checkpoint>> {
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, entity_type,
               last_modified_watermark, last_sync_at, records_seen
        FROM   xero.sync_checkpoint
        WHERE  tenant_id = $1
        ORDER  BY entity_type
        "#,
    )
    .bind(tenant_id)
    .fetch_all(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(rows
        .into_iter()
        .map(|r| Checkpoint {
            key: CheckpointKey {
                tenant: TenantId::new(r.get::<String, _>("tenant_id")),
                entity_type: r.get("entity_type"),
            },
            last_modified_watermark: r.get("last_modified_watermark"),
            last_sync_at: r.get("last_sync_at"),
            records_seen: r.get("records_seen"),
        })
        .collect())
}
