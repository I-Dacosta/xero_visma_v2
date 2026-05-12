use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;
use xero_common::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl TryFrom<String> for RunStatus {
    type Error = xero_common::Error;

    fn try_from(s: String) -> Result<Self> {
        match s.as_str() {
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(Error::StateStore(format!("unknown run status: {other}"))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRun {
    pub run_id: Uuid,
    pub trigger_id: Option<Uuid>,
    pub tenant_id: String,
    pub entity_type: String,
    pub job_type: String,
    pub status: RunStatus,
    pub records_fetched: i64,
    pub records_loaded: i64,
    pub records_failed: i64,
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub triggered_by: String,
}

/// Insert a new `running` row and return the generated run_id.
pub async fn start_run(
    pg: &PgPool,
    tenant_id: &str,
    entity_type: &str,
    job_type: &str,
    triggered_by: &str,
    trigger_id: Option<Uuid>,
) -> Result<Uuid> {
    let row = sqlx::query(
        r#"
        INSERT INTO xero.sync_run
            (tenant_id, entity_type, job_type, status, triggered_by, trigger_id)
        VALUES ($1, $2, $3, 'running', $4, $5)
        RETURNING run_id
        "#,
    )
    .bind(tenant_id)
    .bind(entity_type)
    .bind(job_type)
    .bind(triggered_by)
    .bind(trigger_id)
    .fetch_one(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(row.get("run_id"))
}

/// Finalise a run — set status, counters, finished_at.
pub async fn finish_run(
    pg: &PgPool,
    run_id: Uuid,
    status: RunStatus,
    records_fetched: i64,
    records_loaded: i64,
    records_failed: i64,
    error_message: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE xero.sync_run
        SET    status          = $2::xero.run_status,
               records_fetched = $3,
               records_loaded  = $4,
               records_failed  = $5,
               error_message   = $6,
               finished_at     = NOW()
        WHERE  run_id = $1
        "#,
    )
    .bind(run_id)
    .bind(status.as_str())
    .bind(records_fetched)
    .bind(records_loaded)
    .bind(records_failed)
    .bind(error_message)
    .execute(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(())
}

/// Fetch recent runs for a tenant (newest first).
pub async fn recent_runs(pg: &PgPool, tenant_id: &str, limit: i64) -> Result<Vec<SyncRun>> {
    let rows = sqlx::query(
        r#"
        SELECT run_id, trigger_id, tenant_id, entity_type, job_type,
               status::TEXT AS status,
               records_fetched, records_loaded, records_failed,
               error_message, started_at, finished_at, triggered_by
        FROM   xero.sync_run
        WHERE  tenant_id = $1
        ORDER  BY started_at DESC
        LIMIT  $2
        "#,
    )
    .bind(tenant_id)
    .bind(limit)
    .fetch_all(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    rows.into_iter()
        .map(|r| {
            let status = RunStatus::try_from(r.get::<String, _>("status"))?;
            Ok(SyncRun {
                run_id: r.get("run_id"),
                trigger_id: r.get("trigger_id"),
                tenant_id: r.get("tenant_id"),
                entity_type: r.get("entity_type"),
                job_type: r.get("job_type"),
                status,
                records_fetched: r.get("records_fetched"),
                records_loaded: r.get("records_loaded"),
                records_failed: r.get("records_failed"),
                error_message: r.get("error_message"),
                started_at: r.get("started_at"),
                finished_at: r.get("finished_at"),
                triggered_by: r.get("triggered_by"),
            })
        })
        .collect()
}
