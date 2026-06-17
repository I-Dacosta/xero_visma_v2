//! `xero` — operator CLI for the stateless raw → GCS uploader.
//!
//! Commands
//! ────────
//!   sync        Fetch raw Xero pages and land them in GCS (or local disk).
//!   healthcheck Mint a token per tenant; optionally probe the GCS bucket.
//!
//! There is NO Postgres, Redis, BigQuery, HTTP server, or PKCE here — this is a
//! one-shot job driven by an external scheduler (cron / Cloud Scheduler). Auth
//! is custom-connection (`client_credentials`) only.

mod health;
mod resolve;
mod sync;

use clap::{Parser, Subcommand};

use crate::sync::SyncArgs;

// ── CLI definition ──────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(name = "xero", version, about = "xero_service_v2 raw → GCS uploader")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Fetch raw Xero pages for the selected layer and upload them verbatim.
    ///
    /// The sync *mode* is derived from the flags (precedence top→bottom):
    ///   --backfill FROM:TO            → backfill (business-date chunks)
    ///   --reports                     → report snapshots (needs --as-of)
    ///   --where <expr> / --no-window  → open-sweep (status filter, no window)
    ///   --business-from/--business-to → rolling-full (explicit business window)
    ///   --full                        → master (no filter) — master entities
    ///   (default)                     → incremental (modified ≥ now − window)
    Sync(SyncArgs),

    /// Mint a token for each configured tenant and (optionally) probe the bucket.
    Healthcheck(health::HealthArgs),
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Init tracing — respects RUST_LOG, defaulting xero_cli to debug.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info").add_directive(
                    "xero_cli=debug"
                        .parse()
                        .expect("static directive is always valid"),
                )
            }),
        )
        .init();

    match cli.cmd {
        Cmd::Sync(args) => sync::run(args).await,
        Cmd::Healthcheck(args) => health::run(args).await,
    }
}
