//! `xero-state` — Postgres + Redis state store.
//!
//! Owns all persistent state: checkpoints, run history, tenants.

pub mod backfill;
pub mod bq_sink;
pub mod checkpoint;
pub mod local_bronze;
pub mod run_history;
pub mod sync_schedule;
pub mod tenant;

pub use backfill::{BackfillChunk, BackfillPlan, NewBackfillPlan};
pub use bq_sink::{
    init_sink as init_bq_sink, sink as bq_sink_current, BigQueryStreamingSink, BqError, BqRow,
    BqSink, NoopBqSink,
};
pub use checkpoint::{Checkpoint, CheckpointKey};
pub use local_bronze::{BronzeSummary, BronzeUpsertStats};
pub use run_history::{RunStatus, SyncRun};
pub use sync_schedule::{NewSyncSchedule, SyncSchedule};
pub use tenant::TenantRecord;

use deadpool_redis::{Config as RedisConfig, Pool as RedisPool, Runtime};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tracing::info;
use xero_common::{Error, Result};

/// Postgres + Redis connection handles.
#[derive(Debug, Clone)]
pub struct StateStore {
    pub pg: PgPool,
    pub redis: RedisPool,
}

impl StateStore {
    /// Open connection pools.  Call once at startup.
    pub async fn connect(pg_dsn: &str, redis_url: &str) -> Result<Self> {
        let pg = PgPoolOptions::new()
            .max_connections(8)
            .connect(pg_dsn)
            .await
            .map_err(|e| Error::StateStore(format!("postgres: {e}")))?;

        let redis = RedisConfig::from_url(redis_url)
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| Error::StateStore(format!("redis: {e}")))?;

        info!("StateStore connected (pg + redis)");
        Ok(Self { pg, redis })
    }

    /// Postgres-only connection — for CLI commands that don't need Redis.
    pub async fn connect_pg_only(pg_dsn: &str) -> Result<PgPool> {
        PgPoolOptions::new()
            .max_connections(4)
            .connect(pg_dsn)
            .await
            .map_err(|e| Error::StateStore(format!("postgres: {e}")))
    }

    /// Lightweight liveness check — SELECT 1 on the Postgres pool.
    pub async fn healthcheck(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pg)
            .await
            .map_err(|e| Error::StateStore(e.to_string()))?;
        Ok(())
    }
}
