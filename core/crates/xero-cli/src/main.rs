//! `xero` — operator CLI for xero_service_v2.
//!
//! Commands
//! ────────
//!   db-check    Verify Postgres connectivity and print schema status.
//!   db-migrate  Apply pending SQL migrations.
//!   healthcheck Ping Postgres + Redis.
//!   serve       Start the Axum HTTP server.

use anyhow::Context;
use clap::{Parser, Subcommand};
use dashmap::DashMap;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::info;

use xero_common::AppConfig;
use xero_http::{serve, AppState};
use xero_state::StateStore;

// ── CLI definition ────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(name = "xero", version, about = "xero_service_v2 operator CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Verify Postgres connectivity and print the xero schema status.
    ///
    /// Requires DATABASE_URL.  Run `xero db-migrate` first if tables are missing.
    DbCheck,

    /// Apply all SQL migrations in the migrations/ directory.
    ///
    /// Uses sqlx Migrator — tracks applied migrations in _sqlx_migrations.
    /// Pass --dir to override the default path.
    DbMigrate {
        #[arg(long, default_value = "migrations")]
        dir: String,
    },

    /// Ping Postgres + Redis and exit with 0 if both are healthy.
    Healthcheck,

    /// Start the Axum HTTP server (health + sync-trigger endpoints).
    Serve,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Init tracing — respects RUST_LOG
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("xero_cli=debug".parse().unwrap()),
        )
        .init();

    match cli.cmd {
        // ── db-check ─────────────────────────────────────────────────────────
        Cmd::DbCheck => {
            let dsn = AppConfig::pg_dsn_only()
                .context("DATABASE_URL must be set (copy .env.example to .env)")?;

            info!("Connecting to Postgres…");
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&dsn)
                .await
                .context("cannot connect to Postgres — is `docker compose up` running?")?;

            // Liveness
            sqlx::query("SELECT 1")
                .fetch_one(&pool)
                .await
                .context("SELECT 1 failed")?;

            println!("✓  Postgres connected");

            // Schema presence
            let schema_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'xero')",
            )
            .fetch_one(&pool)
            .await
            .context("schema check failed")?;

            if !schema_exists {
                println!("⚠  'xero' schema not found — run `xero db-migrate` first");
                return Ok(());
            }
            println!("✓  Schema 'xero' exists");

            // Table inventory
            let tables: Vec<(String, i64)> = sqlx::query_as(
                r#"
                SELECT t.table_name,
                       COALESCE(c.reltuples::BIGINT, 0) AS row_estimate
                FROM   information_schema.tables  t
                JOIN   pg_class                   c ON c.relname = t.table_name
                WHERE  t.table_schema = 'xero'
                ORDER  BY t.table_name
                "#,
            )
            .fetch_all(&pool)
            .await
            .context("table listing failed")?;

            if tables.is_empty() {
                println!("⚠  No tables in 'xero' schema — run `xero db-migrate`");
            } else {
                println!("\n  Table                    │ ~rows");
                println!("  ─────────────────────────┼────────");
                for (name, rows) in &tables {
                    println!("  {name:<25}│ {rows}");
                }
            }
        }

        // ── db-migrate ───────────────────────────────────────────────────────
        Cmd::DbMigrate { dir } => {
            let dsn = AppConfig::pg_dsn_only().context("DATABASE_URL must be set")?;

            info!("Connecting to Postgres for migration…");
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&dsn)
                .await
                .context("cannot connect to Postgres")?;

            let path = std::path::Path::new(&dir);
            if !path.exists() {
                anyhow::bail!(
                    "migrations directory '{}' not found — run from the project root",
                    dir
                );
            }

            info!("Applying migrations from '{dir}'…");
            sqlx::migrate::Migrator::new(path)
                .await
                .context("failed to load migrations")?
                .run(&pool)
                .await
                .context("migration failed")?;

            println!("✓  All migrations applied");
        }

        // ── healthcheck ──────────────────────────────────────────────────────
        Cmd::Healthcheck => {
            let cfg = AppConfig::from_env().context("config error — check your .env file")?;

            info!("Connecting to Postgres + Redis…");
            let store = StateStore::connect(&cfg.pg_dsn, &cfg.redis_url)
                .await
                .context("StateStore::connect failed")?;

            store
                .healthcheck()
                .await
                .context("Postgres healthcheck failed")?;
            println!("✓  Postgres  up");

            // Ping Redis
            let mut conn = store.redis.get().await.context("Redis pool get failed")?;

            let pong: String = deadpool_redis::redis::cmd("PING")
                .query_async(&mut *conn)
                .await
                .context("Redis PING failed")?;

            if pong == "PONG" {
                println!("✓  Redis     up");
            } else {
                anyhow::bail!("unexpected Redis PING response: {pong}");
            }

            println!("\n✓  All systems healthy");
        }

        // ── serve ────────────────────────────────────────────────────────────
        Cmd::Serve => {
            let cfg = AppConfig::from_env().context("config error")?;

            info!("Starting xero_service_v2 HTTP server on {}", cfg.http_bind);
            let store = StateStore::connect(&cfg.pg_dsn, &cfg.redis_url)
                .await
                .context("StateStore::connect failed")?;

            // Install distributed rate-limit coordinator so 429s observed on
            // any replica pause requests across the whole fleet.
            xero_client::init_coordinator(Arc::new(
                xero_client::RedisRateLimitCoordinator::new(store.redis.clone()),
            ));
            info!("rate-limit coordinator installed (redis-backed)");

            // Install BigQuery sink if all required env vars are present.
            // Missing creds -> NoopBqSink remains the default, bronze still
            // writes to Postgres normally.
            install_bq_sink_if_configured().await;

            let (auth, oauth_client): (
                Arc<dyn xero_auth::TokenProvider>,
                Option<Arc<xero_auth::XeroOAuthClient>>,
            ) = if cfg.xero_custom_connection {
                for connection in &cfg.xero_cc_connections {
                    xero_state::tenant::upsert(
                        &store.pg,
                        &connection.tenant_id,
                        connection.tenant_name.as_deref(),
                        None,
                    )
                    .await
                    .context("failed to upsert custom-connection tenant")?;
                }

                (
                    Arc::new(xero_auth::MultiTenantCustomConnectionClient::new(
                        cfg.xero_cc_connections.clone(),
                    )),
                    None,
                )
            } else {
                let scopes = std::env::var("XERO_SCOPES")
                    .context("missing required env var: XERO_SCOPES")?
                    .split_whitespace()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let client = Arc::new(xero_auth::XeroOAuthClient::new(
                    cfg.xero_client_id
                        .clone()
                        .context("XERO_CLIENT_ID required")?,
                    cfg.xero_client_secret
                        .clone()
                        .context("XERO_CLIENT_SECRET required")?,
                    cfg.xero_redirect_uri.clone().unwrap_or_default(),
                    scopes,
                ));
                (client.clone(), Some(client))
            };
            let sync = Arc::new(xero_sync::SyncExecutor::new_with_tenant_header(
                store.clone(),
                !cfg.xero_custom_connection,
            ));

            serve(
                AppState {
                    store,
                    auth,
                    sync,
                    oauth_client,
                    pkce_store: Arc::new(DashMap::new()),
                },
                &cfg.http_bind,
            )
            .await
            .context("HTTP server error")?;
        }
    }

    Ok(())
}

/// Build a real BigQuery sink iff `GCP_PROJECT_ID`, `BIGQUERY_DATASET` (or
/// `GCP_DATASET_ID`) and `GOOGLE_APPLICATION_CREDENTIALS` are all set and the
/// SA JSON file is readable. Otherwise the default `NoopBqSink` stays in place
/// and bronze writes proceed normally without a BQ side-effect.
async fn install_bq_sink_if_configured() {
    let project_id = std::env::var("GCP_PROJECT_ID").ok();
    let dataset_id = std::env::var("BIGQUERY_DATASET")
        .ok()
        .or_else(|| std::env::var("GCP_DATASET_ID").ok());
    let sa_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();

    let (Some(project_id), Some(dataset_id), Some(sa_path)) = (project_id, dataset_id, sa_path)
    else {
        info!("BigQuery sink: env vars missing — running with NoopBqSink (Postgres bronze only)");
        return;
    };

    if !std::path::Path::new(&sa_path).is_file() {
        info!(
            sa_path = sa_path.as_str(),
            "BigQuery sink: SA JSON not found at GOOGLE_APPLICATION_CREDENTIALS — NoopBqSink stays"
        );
        return;
    }

    match xero_state::BigQueryStreamingSink::from_service_account_file(
        &sa_path,
        project_id.clone(),
        dataset_id.clone(),
    )
    .await
    {
        Ok(sink) => {
            xero_state::init_bq_sink(Arc::new(sink));
            info!(
                project_id = project_id.as_str(),
                dataset_id = dataset_id.as_str(),
                "BigQuery sink installed (streaming-inserts mode)"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "BigQuery sink build failed — NoopBqSink stays (bronze still works)"
            );
        }
    }
}
