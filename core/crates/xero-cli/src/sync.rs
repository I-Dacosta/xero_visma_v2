//! The `sync` subcommand: flags → [`SyncMode`] → [`SyncJob::run`] → manifest.
//!
//! This module owns the flag surface ([`SyncArgs`]) and the run orchestration;
//! the pure flag → run-parameter mapping lives in [`crate::resolve`]. Backfill
//! is special-cased: it expands the requested range into business-date chunks
//! (oldest→newest) and runs the job once per chunk, merging the per-chunk
//! manifests into one.

use std::sync::Arc;

use anyhow::Context;
use chrono::{NaiveDate, Utc};
use clap::Args;
use uuid::Uuid;

use xero_common::{AppConfig, EntityType, GcsConfig, SyncConfig};
use xero_gcs::{write_manifest, GcsRawSink, LocalDirSink, RawSink, RunManifest};
use xero_sync::{SyncJob, SyncMode};

use crate::resolve::{
    backfill_chunks, parse_range, resolve_entities, resolve_mode, resolve_tenants, BackfillScope,
    ModeScope,
};

/// Flags for `xero sync`. The active [`SyncMode`] is derived from these via
/// [`crate::resolve::resolve_mode`]; see the precedence on the `Sync` command doc.
#[derive(Debug, Args)]
pub struct SyncArgs {
    /// Incremental look-back window in days (modified ≥ now − N). Overrides
    /// `SYNC_WINDOW_DAYS` for this run.
    #[arg(long)]
    pub window_days: Option<i64>,

    /// Raw Xero `where=` clause (open-sweep). Implies `--no-window`.
    #[arg(long = "where")]
    pub where_clause: Option<String>,

    /// Drop the modified window without a `--where` clause (rare; pairs with
    /// `--where`, but allowed standalone for an unfiltered open-sweep).
    #[arg(long)]
    pub no_window: bool,

    /// Rolling-full business window lower bound (`YYYY-MM-DD`). Pairs with
    /// `--business-to`.
    #[arg(long)]
    pub business_from: Option<NaiveDate>,

    /// Rolling-full business window upper bound (`YYYY-MM-DD`). Defaults to the
    /// run date when only `--business-from` is given.
    #[arg(long)]
    pub business_to: Option<NaiveDate>,

    /// Master-data full refresh (no filter) for master entities.
    #[arg(long)]
    pub full: bool,

    /// Rolling-full over the configured tight window when no explicit business
    /// dates are given (transactional entities).
    #[arg(long)]
    pub full_with_window: bool,

    /// One-time historical backfill over `FROM:TO` (`YYYY-MM-DD:YYYY-MM-DD`),
    /// expanded into business-date chunks.
    #[arg(long, value_name = "FROM:TO")]
    pub backfill: Option<String>,

    /// Backfill chunk size in months (default 1).
    #[arg(long, default_value_t = 1)]
    pub chunk_months: u32,

    /// Report-snapshot mode. Requires `--as-of`.
    #[arg(long)]
    pub reports: bool,

    /// As-of date for `--reports` (`YYYY-MM-DD`). Defaults to the run date.
    #[arg(long)]
    pub as_of: Option<NaiveDate>,

    /// Restrict to a CSV of entity keys (e.g. `invoices,payments`). Empty →
    /// the mode's default entity set.
    #[arg(long, value_delimiter = ',')]
    pub entity: Vec<EntityType>,

    /// Restrict to a CSV of tenant ids. Empty → all configured tenants.
    #[arg(long, value_delimiter = ',')]
    pub tenant: Vec<String>,

    /// Write to local disk (`--local-dir`) instead of GCS.
    #[arg(long)]
    pub dry_run: bool,

    /// Local output root for `--dry-run` (default `./out`).
    #[arg(long, default_value = "./out")]
    pub local_dir: String,

    /// Max concurrent (tenant, entity) tasks. Clamped to 1..=6 internally.
    #[arg(long)]
    pub max_concurrent: Option<usize>,
}

/// Execute the `sync` subcommand end-to-end.
pub async fn run(args: SyncArgs) -> anyhow::Result<()> {
    let cfg = AppConfig::from_env().context("config error — check your .env file")?;
    // Apply CLI overrides on top of the env-derived sync config. `--window-days`
    // controls the incremental modified window the SyncJob computes.
    let sync_cfg = apply_overrides(SyncConfig::from_env().context("sync config error")?, &args);

    let run_id = Uuid::new_v4();
    let run_date = Utc::now().date_naive();
    let max_concurrent = args.max_concurrent.unwrap_or(sync_cfg.max_concurrent);

    let auth = Arc::new(xero_auth::MultiTenantCustomConnectionClient::new(
        cfg.xero_cc_connections.clone(),
    ));
    let configured: Vec<String> = auth.tenant_ids().into_iter().map(str::to_owned).collect();
    let tenants = resolve_tenants(&args.tenant, &configured)?;

    // GcsConfig carries the bucket (real sink) and prefix (object keys). The
    // bucket is only required for a real upload; dry-run still needs the prefix
    // so keys match the production layout, so synthesize a config when unset.
    let gcs_cfg = resolve_gcs_config(&args)?;
    let sink = build_sink(&args, &gcs_cfg).await?;

    // tenant_id → org display name (from XERO_ORG_N_NAME) for the x-org-name
    // object metadata tag; tenants without a configured name get an empty tag.
    let org_names: std::collections::HashMap<String, String> = cfg
        .xero_cc_connections
        .iter()
        .filter_map(|c| {
            c.tenant_name
                .clone()
                .map(|name| (c.tenant_id.clone(), name))
        })
        .collect();
    let job = SyncJob::new(
        auth,
        Arc::clone(&sink),
        gcs_cfg,
        sync_cfg.clone(),
        org_names,
    );

    // Backfill runs N chunked sub-runs; everything else is a single run.
    let manifest = if let Some(spec) = args.backfill.as_deref() {
        let entities = resolve_entities(&args, BackfillScope)?;
        run_backfill(
            &job,
            run_id,
            spec,
            args.chunk_months,
            &tenants,
            &entities,
            max_concurrent,
        )
        .await?
    } else {
        let (mode, anchor_date) = resolve_mode(&args, run_date, &sync_cfg)?;
        let entities = resolve_entities(&args, ModeScope(&mode))?;
        job.run(
            run_id,
            anchor_date,
            &tenants,
            &entities,
            mode,
            max_concurrent,
        )
        .await
    };

    write_manifest(sink.as_ref(), run_date, run_id, &manifest)
        .await
        .context("failed to write run manifest")?;

    report_summary(&manifest);
    Ok(())
}

/// Apply CLI overrides onto the env-derived [`SyncConfig`].
fn apply_overrides(mut cfg: SyncConfig, args: &SyncArgs) -> SyncConfig {
    if let Some(window_days) = args.window_days {
        cfg.window_days = window_days;
    }
    if let Some(max_concurrent) = args.max_concurrent {
        cfg.max_concurrent = max_concurrent;
    }
    cfg
}

/// Resolve the [`GcsConfig`]. A real run requires `GCS_BUCKET`; a `--dry-run`
/// tolerates its absence (the bucket is unused) but still honours `GCS_PREFIX`
/// so the local key layout matches production.
fn resolve_gcs_config(args: &SyncArgs) -> anyhow::Result<GcsConfig> {
    match GcsConfig::from_env() {
        Ok(cfg) => Ok(cfg),
        Err(e) if args.dry_run => {
            tracing::debug!(error = %e, "dry-run: GCS_BUCKET unset, using default prefix");
            let prefix = std::env::var("GCS_PREFIX").unwrap_or_else(|_| "raw/xero".to_string());
            Ok(GcsConfig {
                bucket: String::new(),
                prefix,
            })
        }
        Err(e) => Err(e).context("GCS config error (set GCS_BUCKET)"),
    }
}

/// Build the destination sink: local disk for `--dry-run`, else GCS.
async fn build_sink(args: &SyncArgs, gcs_cfg: &GcsConfig) -> anyhow::Result<Arc<dyn RawSink>> {
    if args.dry_run {
        tracing::info!(local_dir = %args.local_dir, "dry-run: writing to local disk");
        Ok(Arc::new(LocalDirSink::new(&args.local_dir)))
    } else {
        let sink = GcsRawSink::new(gcs_cfg.bucket.clone())
            .await
            .context("failed to build GCS sink — check GOOGLE_APPLICATION_CREDENTIALS")?;
        tracing::info!(bucket = gcs_cfg.bucket.as_str(), "writing to GCS bucket");
        Ok(Arc::new(sink))
    }
}

/// Parse `FROM:TO`, expand into `chunk_months`-sized business-date chunks
/// (oldest→newest), run the job per chunk, and merge the manifests.
async fn run_backfill(
    job: &SyncJob,
    run_id: Uuid,
    spec: &str,
    chunk_months: u32,
    tenants: &[String],
    entities: &[EntityType],
    max_concurrent: usize,
) -> anyhow::Result<RunManifest> {
    let (from, to) = parse_range(spec)?;
    let chunks = backfill_chunks(from, to, chunk_months);
    tracing::info!(
        from = %from, to = %to, chunks = chunks.len(), chunk_months,
        "backfill expanded into business-date chunks"
    );

    let mut merged: Option<RunManifest> = None;
    for (chunk_from, chunk_to) in chunks {
        let mode = SyncMode::Backfill {
            from: chunk_from,
            to: chunk_to,
        };
        let manifest = job
            .run(run_id, chunk_to, tenants, entities, mode, max_concurrent)
            .await;
        merged = Some(match merged {
            None => manifest,
            Some(mut acc) => {
                acc.finished_at = manifest.finished_at;
                acc.entities.extend(manifest.entities);
                acc
            }
        });
    }

    merged.context("backfill produced no chunks (empty date range)")
}

/// Print a one-line-per-entity summary plus run-level totals to stdout.
fn report_summary(m: &RunManifest) {
    let errors = m.entities.iter().filter(|e| e.error.is_some()).count();
    let pages: u32 = m.entities.iter().map(|e| e.pages).sum();
    let records: i64 = m.entities.iter().map(|e| e.records).sum();

    println!("\n  run {} — mode={}", m.run_id, m.mode);
    println!("  endpoint                 │ tenant     │ pages │ records │ termination");
    println!("  ─────────────────────────┼────────────┼───────┼─────────┼────────────");
    for e in &m.entities {
        let tenant_short = &e.tenant[..e.tenant.len().min(10)];
        println!(
            "  {:<25}│ {:<11}│ {:>5} │ {:>7} │ {}",
            e.endpoint, tenant_short, e.pages, e.records, e.termination
        );
    }
    println!(
        "\n  {} entities · {pages} pages · {records} records · {errors} error(s)",
        m.entities.len()
    );
    if errors > 0 {
        for e in m.entities.iter().filter(|e| e.error.is_some()) {
            if let Some(err) = &e.error {
                eprintln!("  ✗ {} / {}: {err}", e.tenant, e.endpoint);
            }
        }
    }
}

/// Test-only constructors shared with the `resolve` test module.
#[cfg(test)]
pub(crate) mod test_support {
    use super::{EntityType, SyncArgs};

    /// A [`SyncArgs`] with every flag at its default, overriding only `entity`.
    pub fn args_with(entity: Vec<EntityType>) -> SyncArgs {
        SyncArgs {
            window_days: None,
            where_clause: None,
            no_window: false,
            business_from: None,
            business_to: None,
            full: false,
            full_with_window: false,
            backfill: None,
            chunk_months: 1,
            reports: false,
            as_of: None,
            entity,
            tenant: Vec::new(),
            dry_run: false,
            local_dir: "./out".to_string(),
            max_concurrent: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::args_with;
    use super::*;

    #[test]
    fn apply_overrides_replaces_window_and_concurrency() {
        let base = SyncConfig {
            window_days: 3,
            rolling_full_tight_days: 30,
            rolling_full_wide_days: 90,
            max_concurrent: 6,
        };
        let mut a = args_with(vec![]);
        a.window_days = Some(7);
        a.max_concurrent = Some(2);
        let cfg = apply_overrides(base.clone(), &a);
        assert_eq!(cfg.window_days, 7);
        assert_eq!(cfg.max_concurrent, 2);

        // No flags → config untouched.
        let cfg = apply_overrides(base.clone(), &args_with(vec![]));
        assert_eq!(cfg.window_days, 3);
        assert_eq!(cfg.max_concurrent, 6);
    }

    #[test]
    fn resolve_gcs_config_yields_prefix_in_dry_run() {
        // Dry-run must always succeed and always carry a non-empty prefix,
        // whether or not GCS_BUCKET happens to be set in the environment.
        let mut a = args_with(vec![]);
        a.dry_run = true;
        let cfg = resolve_gcs_config(&a).expect("dry-run must not require a bucket");
        assert!(!cfg.prefix.is_empty());
    }
}
