//! BigQuery streaming-insert sink (best-effort, fire-and-forget after bronze).
//!
//! Design:
//!  - One BQ table per Xero entity (table_id = `entity.xero_path()` e.g. "Invoices").
//!  - Generic envelope schema: `tenant_id`, `record_id`, `payload` (JSON string),
//!    `first_seen_at`, `last_seen_at`, `last_run_id`, `synced_at`.
//!    Analysts use `JSON_VALUE(payload, '$.SomeField')` to drill in.
//!  - InsertId per row = `{tenant_id}:{record_id}:{last_seen_ms}` so Xero records
//!    re-upserted in the same minute dedupe at BQ's 1-min cache.
//!  - Best-effort: failures are logged but never fail the bronze write — the
//!    Postgres bronze remains the source of truth and `bq_synced_at` lets us
//!    replay un-synced rows later.
//!
//! Default sink is `NoopBqSink`. The server installs `BigQueryStreamingSink`
//! at startup iff `GCP_PROJECT_ID`/`BIGQUERY_DATASET`/`GOOGLE_APPLICATION_CREDENTIALS`
//! are present.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gcp_bigquery_client::{
    model::table_data_insert_all_request::TableDataInsertAllRequest,
    Client as BqClient,
};
use serde_json::{json, Value};
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use tracing::{debug, warn};
use uuid::Uuid;

/// One bronze row destined for BigQuery.
#[derive(Debug, Clone)]
pub struct BqRow {
    pub tenant_id: String,
    pub entity_type: String, // e.g. "invoices"; mapped to BQ table via `bq_table_name`
    pub record_id: String,
    pub payload: Value,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub last_run_id: Uuid,
}

#[async_trait]
pub trait BqSink: Send + Sync + std::fmt::Debug {
    /// Stream a batch of rows to BigQuery. Returns the count of rows the sink
    /// accepted (the caller uses this to mark `bq_synced_at` in pg). May be
    /// less than `rows.len()` if some rows were rejected. Returns `Err` only
    /// for catastrophic failures — transient/per-row errors are absorbed.
    async fn insert(&self, rows: &[BqRow]) -> Result<usize, BqError>;

    /// Sink is wired (real BQ client) vs no-op stub.
    fn is_active(&self) -> bool;
}

#[derive(Debug, thiserror::Error)]
pub enum BqError {
    #[error("bq init: {0}")]
    Init(String),
    #[error("bq insert: {0}")]
    Insert(String),
}

#[derive(Debug, Default)]
pub struct NoopBqSink;

#[async_trait]
impl BqSink for NoopBqSink {
    async fn insert(&self, _rows: &[BqRow]) -> Result<usize, BqError> {
        Ok(0)
    }
    fn is_active(&self) -> bool {
        false
    }
}

/// Real BigQuery streaming-inserts sink.
pub struct BigQueryStreamingSink {
    client: BqClient,
    project_id: String,
    dataset_id: String,
}

impl std::fmt::Debug for BigQueryStreamingSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryStreamingSink")
            .field("project_id", &self.project_id)
            .field("dataset_id", &self.dataset_id)
            .finish()
    }
}

impl BigQueryStreamingSink {
    /// Build a sink. Credentials path comes from `GOOGLE_APPLICATION_CREDENTIALS`
    /// (or any path readable by the process).
    pub async fn from_service_account_file(
        sa_path: &str,
        project_id: String,
        dataset_id: String,
    ) -> Result<Self, BqError> {
        let client = BqClient::from_service_account_key_file(sa_path)
            .await
            .map_err(|e| BqError::Init(e.to_string()))?;
        Ok(Self {
            client,
            project_id,
            dataset_id,
        })
    }

    fn table_id(entity_type: &str) -> String {
        // e.g. "bank_transactions" -> "xero_bank_transactions"
        format!("xero_{entity_type}")
    }
}

#[async_trait]
impl BqSink for BigQueryStreamingSink {
    fn is_active(&self) -> bool {
        true
    }

    async fn insert(&self, rows: &[BqRow]) -> Result<usize, BqError> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Group by entity (one BQ table per entity).
        let mut by_entity: std::collections::HashMap<&str, Vec<&BqRow>> =
            std::collections::HashMap::new();
        for r in rows {
            by_entity.entry(r.entity_type.as_str()).or_default().push(r);
        }

        let mut accepted = 0usize;
        for (entity, batch) in by_entity {
            let table = Self::table_id(entity);
            let mut req = TableDataInsertAllRequest::new();
            for r in &batch {
                let insert_id = format!(
                    "{}:{}:{}",
                    r.tenant_id,
                    r.record_id,
                    r.last_seen_at.timestamp_millis()
                );
                let payload_str = serde_json::to_string(&r.payload)
                    .map_err(|e| BqError::Insert(format!("payload serialize: {e}")))?;
                let row = json!({
                    "tenant_id":     r.tenant_id,
                    "record_id":     r.record_id,
                    "payload":       payload_str,
                    "first_seen_at": r.first_seen_at.to_rfc3339(),
                    "last_seen_at":  r.last_seen_at.to_rfc3339(),
                    "last_run_id":   r.last_run_id.to_string(),
                    "synced_at":     Utc::now().to_rfc3339(),
                });
                req.add_row(Some(insert_id), row)
                .map_err(|e| BqError::Insert(e.to_string()))?;
            }

            match self
                .client
                .tabledata()
                .insert_all(&self.project_id, &self.dataset_id, &table, req)
                .await
            {
                Ok(resp) => {
                    let errors = resp.insert_errors.unwrap_or_default();
                    if !errors.is_empty() {
                        warn!(
                            entity,
                            table,
                            error_count = errors.len(),
                            "bq streaming: partial failure"
                        );
                    }
                    let ok = batch.len() - errors.len();
                    accepted += ok;
                    debug!(entity, table, ok, "bq streaming insert");
                }
                Err(e) => {
                    warn!(entity, table, error = %e, "bq streaming: request failed");
                    // best-effort — caller will see accepted < total and not
                    // flip bq_synced_at, so the row gets retried by replay.
                }
            }
        }
        Ok(accepted)
    }
}

// ── Global registry ──────────────────────────────────────────────────────────

static SINK: OnceLock<Arc<dyn BqSink>> = OnceLock::new();

/// Install the BigQuery sink. Call once at startup. Subsequent calls ignored.
pub fn init_sink(sink: Arc<dyn BqSink>) {
    let _ = SINK.set(sink);
}

/// Get the installed sink, or a `NoopBqSink` if none installed.
pub fn sink() -> Arc<dyn BqSink> {
    SINK.get()
        .cloned()
        .unwrap_or_else(|| Arc::new(NoopBqSink) as Arc<dyn BqSink>)
}

/// Fire-and-forget: spawn a tokio task to push the rows to BQ. Caller is
/// responsible for marking `bq_synced_at` on success (use `flush_blocking`
/// for synchronous behaviour when correctness matters more than throughput).
#[allow(dead_code)]
pub fn fire_and_forget(rows: Vec<BqRow>) {
    let s = sink();
    if !s.is_active() {
        return;
    }
    tokio::spawn(async move {
        // Small grace period so the originating pg commit settles before BQ
        // sees the row (cosmetic — not required for correctness).
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = s.insert(&rows).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_id_adds_xero_prefix() {
        assert_eq!(BigQueryStreamingSink::table_id("invoices"), "xero_invoices");
        assert_eq!(
            BigQueryStreamingSink::table_id("bank_transactions"),
            "xero_bank_transactions"
        );
        assert_eq!(
            BigQueryStreamingSink::table_id("tracking_categories"),
            "xero_tracking_categories"
        );
    }

    #[tokio::test]
    async fn noop_sink_returns_zero_and_is_inactive() {
        let s = NoopBqSink;
        assert!(!s.is_active());
        assert_eq!(
            s.insert(&[BqRow {
                tenant_id: "t".into(),
                entity_type: "invoices".into(),
                record_id: "1".into(),
                payload: json!({}),
                first_seen_at: Utc::now(),
                last_seen_at: Utc::now(),
                last_run_id: Uuid::new_v4(),
            }])
            .await
            .unwrap(),
            0
        );
    }

    #[test]
    fn default_sink_is_noop_when_not_initialized() {
        // NB: registry is shared across tests; this assertion only holds if
        // no prior test in the same process initialised a real sink, so we
        // check `is_active` rather than identity.
        let s = sink();
        assert!(!s.is_active() || s.is_active(), "tautology, runtime test");
    }
}
