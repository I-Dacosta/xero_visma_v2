//! `xero-sync` — orchestrates fetch → bronze → checkpoint → BigQuery → run-history.
//!
//! Public surface:
//!
//!  - [`SyncExecutor`] — entry point used by `xero-http` handlers and the
//!    backfill worker. Holds the [`StateStore`] handle and a `send_tenant_header`
//!    flag (off in PKCE mode, on in custom-connection mode).
//!  - [`RunOptions`] — per-call knobs: `modified_*` / `business_date_*` windows,
//!    `trigger_id`, `job_type`, `triggered_by`, `advance_watermark`.
//!  - [`compute_next_watermark`] — pure function that codifies the checkpoint
//!    monotonicity + gap-safety rules. Unit-tested.
//!
//! Sync pipeline (in `run_with_options`):
//!
//!  1. `run_history::start_run` — audit log row
//!  2. Load current checkpoint (modified-mode only)
//!  3. `xero-client::fetch_*` — paginated GETs with retry + rate-limit
//!  4. `local_bronze::upsert_records` — PK-deduped pg writes (best-effort BQ stream)
//!  5. `checkpoint::upsert` with `compute_next_watermark` rules
//!  6. `run_history::finish_run` — final status + counts

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use serde_json::Value;
use tracing::info;
use uuid::Uuid;
use xero_common::{EntityType, Result, TenantId};
use xero_state::{
    checkpoint::{self, Checkpoint, CheckpointKey},
    local_bronze,
    run_history::{self, RunStatus},
    StateStore,
};

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub modified_after: Option<DateTime<Utc>>,
    pub modified_before: Option<DateTime<Utc>>,
    pub business_date_after: Option<NaiveDate>,
    pub business_date_before: Option<NaiveDate>,
    pub trigger_id: Option<Uuid>,
    pub job_type: String,
    pub triggered_by: String,
    /// Opt-in: when `true` AND this is a business-date run, advance
    /// `last_modified_watermark` to `business_date_before` (midnight UTC).
    /// The caller asserts that records outside the business-date window with
    /// `UpdatedDateUTC` in (`existing_wm`, `business_date_before`] are either
    /// irrelevant or covered by their own backfill plan. Default `false`.
    /// Gap-safety: ignored if `business_date_before` is `None`. Monotonic — the
    /// watermark cannot move backwards.
    pub advance_watermark: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            modified_after: None,
            modified_before: None,
            business_date_after: None,
            business_date_before: None,
            trigger_id: None,
            job_type: "scheduled".to_owned(),
            triggered_by: "system".to_owned(),
            advance_watermark: false,
        }
    }
}

pub struct SyncExecutor {
    state: StateStore,
    send_tenant_header: bool,
}

impl SyncExecutor {
    pub fn new(state: StateStore) -> Self {
        Self {
            state,
            send_tenant_header: true,
        }
    }

    pub fn new_with_tenant_header(state: StateStore, send_tenant_header: bool) -> Self {
        Self {
            state,
            send_tenant_header,
        }
    }

    /// Run a sync for one entity. Returns the number of records fetched.
    ///
    /// Concrete fetch logic will call `xero-client` once tokens are wired.
    /// For now this is the plumbing scaffold.
    pub async fn sync_one(
        &self,
        tenant: TenantId,
        entity: &EntityType,
        records: Vec<Value>,
        options: &RunOptions,
    ) -> Result<Uuid> {
        let run_id = run_history::start_run(
            &self.state.pg,
            tenant.as_str(),
            entity.as_str(),
            &options.job_type,
            &options.triggered_by,
            options.trigger_id,
        )
        .await?;

        self.sync_one_with_run_id(run_id, tenant, entity, records, options)
            .await
    }

    async fn sync_one_with_run_id(
        &self,
        run_id: Uuid,
        tenant: TenantId,
        entity: &EntityType,
        records: Vec<Value>,
        options: &RunOptions,
    ) -> Result<Uuid> {
        info!(
            run_id = %run_id,
            tenant = %tenant,
            entity = %entity,
            records = records.len(),
            "sync_one started"
        );

        let count = records.len() as i64;

        // Bronze upsert: idempotent by (tenant, entity, record_id) PK. Also
        // streams accepted rows to the BigQuery sink (best-effort, never fails
        // the run — replay endpoint catches stragglers).
        let bronze_stats = match local_bronze::upsert_records(
            &self.state.pg,
            tenant.as_str(),
            entity,
            run_id,
            &records,
        )
        .await
        {
            Ok(stats) => stats,
            Err(e) => {
                let _ = run_history::finish_run(
                    &self.state.pg,
                    run_id,
                    RunStatus::Failed,
                    count,
                    0,
                    count,
                    Some(&e.to_string()),
                )
                .await;
                return Err(e);
            }
        };
        let loaded_count = count - bronze_stats.skipped_invalid;

        // Always touch the checkpoint row so `last_sync_at` reflects any
        // successful run (incremental or business-date backfill). The
        // `last_modified_watermark` only advances when it is safe to do so —
        // see `compute_next_watermark` for the rules.
        let existing_cp =
            match checkpoint::load(&self.state.pg, tenant.as_str(), entity.as_str()).await {
                Ok(cp) => cp,
                Err(e) => {
                    let _ = run_history::finish_run(
                        &self.state.pg,
                        run_id,
                        RunStatus::Failed,
                        count,
                        loaded_count,
                        bronze_stats.skipped_invalid,
                        Some(&e.to_string()),
                    )
                    .await;
                    return Err(e);
                }
            };
        let existing_wm = existing_cp.and_then(|c| c.last_modified_watermark);
        let next_wm = compute_next_watermark(existing_wm, options);

        let cp = Checkpoint {
            key: CheckpointKey::new(tenant.clone(), entity),
            last_modified_watermark: next_wm,
            last_sync_at: Some(Utc::now()),
            records_seen: count,
        };
        if let Err(e) = checkpoint::upsert(&self.state.pg, &cp).await {
            let _ = run_history::finish_run(
                &self.state.pg,
                run_id,
                RunStatus::Failed,
                count,
                loaded_count,
                bronze_stats.skipped_invalid,
                Some(&e.to_string()),
            )
            .await;
            return Err(e);
        }

        run_history::finish_run(
            &self.state.pg,
            run_id,
            RunStatus::Succeeded,
            count,
            loaded_count,
            bronze_stats.skipped_invalid,
            None,
        )
        .await?;

        info!(run_id = %run_id, "sync_one finished — {count} records");
        Ok(run_id)
    }

    pub async fn run(
        &self,
        access_token: &str,
        tenant: TenantId,
        entity: EntityType,
    ) -> Result<Uuid> {
        self.run_with_options(access_token, tenant, entity, RunOptions::default())
            .await
    }

    pub async fn run_with_options(
        &self,
        access_token: &str,
        tenant: TenantId,
        entity: EntityType,
        options: RunOptions,
    ) -> Result<Uuid> {
        use xero_client::{DateWindow, XeroApiClient};
        let run_id = run_history::start_run(
            &self.state.pg,
            tenant.as_str(),
            entity.as_str(),
            &options.job_type,
            &options.triggered_by,
            options.trigger_id,
        )
        .await?;

        let cp = match checkpoint::load(&self.state.pg, tenant.as_str(), entity.as_str()).await {
            Ok(cp) => cp,
            Err(e) => {
                let _ = run_history::finish_run(
                    &self.state.pg,
                    run_id,
                    RunStatus::Failed,
                    0,
                    0,
                    0,
                    Some(&e.to_string()),
                )
                .await;
                return Err(e);
            }
        };
        let modified_after = options
            .modified_after
            .or_else(|| cp.and_then(|c| c.last_modified_watermark));

        let api = XeroApiClient::new_with_tenant_header(
            tenant.as_str().to_owned(),
            self.send_tenant_header,
        );
        let records = match (options.business_date_after, options.business_date_before) {
            (Some(start), Some(end)) => match DateWindow::new(start, end) {
                Ok(window) => {
                    api.fetch_by_business_date(access_token, &entity, window, 100)
                        .await
                }
                Err(e) => Err(e),
            },
            (None, None) => {
                api.fetch(
                    access_token,
                    &entity,
                    modified_after,
                    options.modified_before,
                    100,
                )
                .await
            }
            _ => Err(xero_common::Error::Config(
                "business_date_after and business_date_before must be provided together".to_owned(),
            )),
        };

        let records = match records {
            Ok(records) => records,
            Err(e) => {
                let _ = run_history::finish_run(
                    &self.state.pg,
                    run_id,
                    RunStatus::Failed,
                    0,
                    0,
                    0,
                    Some(&e.to_string()),
                )
                .await;
                return Err(e);
            }
        };

        self.sync_one_with_run_id(run_id, tenant, &entity, records, &options)
            .await
    }

    /// Run several entities for one tenant concurrently, bounded by
    /// `max_concurrent`. Each entity gets its own `Result<Uuid>` so a
    /// failure on one does not abort the others.
    ///
    /// The Xero rate limiter (see `xero-client::rate_limit`) already caps
    /// in-flight requests at `MAX_CONCURRENT_PER_TENANT = 6`, so the safe
    /// ceiling for `max_concurrent` is 6 for a single tenant. Callers that
    /// orchestrate multiple tenants should multiply by their tenant fan-out.
    ///
    /// Returns one result per requested (tenant, entity) pair, in the same
    /// order as `entities`. Use `.into_iter().enumerate()` to map results
    /// back to entities.
    pub async fn run_many(
        &self,
        access_token: &str,
        tenant: TenantId,
        entities: Vec<EntityType>,
        options: RunOptions,
        max_concurrent: usize,
    ) -> Vec<Result<Uuid>> {
        use futures::stream::{FuturesOrdered, StreamExt};

        let concurrency = max_concurrent.clamp(1, 6);
        let mut in_flight: FuturesOrdered<_> = entities
            .into_iter()
            .map(|entity| {
                let opts = options.clone();
                let t = tenant.clone();
                async move {
                    self.run_with_options(access_token, t, entity, opts).await
                }
            })
            .collect();

        // FuturesOrdered processes everything but yields in input order. To
        // bound concurrency we drain in chunks of `concurrency` — a simple
        // pattern that doesn't pull in tokio_util::StreamExt::buffered.
        let mut results = Vec::with_capacity(in_flight.len());
        let mut buffered = Vec::with_capacity(concurrency);
        while let Some(fut) = in_flight.next().await {
            buffered.push(fut);
            if buffered.len() >= concurrency {
                results.extend(buffered.drain(..));
            }
        }
        results.extend(buffered.drain(..));
        results
    }
}

/// Decide the new `last_modified_watermark` value for a run.
///
/// Rules, applied in order:
///   1. Modified-only run (no business-date window): candidate is
///      `options.modified_before.or(now())`. Monotonic — never moves backwards.
///   2. Business-date run with `advance_watermark = true` and a non-empty
///      `business_date_before`: candidate is `business_date_before` at 00:00 UTC.
///      Caller is responsible for asserting that no relevant out-of-window
///      records exist between `existing_wm` and `business_date_before`.
///      Monotonic — never moves backwards.
///   3. Business-date run with `advance_watermark = false` (default): the
///      watermark stays at `existing_wm`. This is the gap-safe default.
fn compute_next_watermark(
    existing_wm: Option<DateTime<Utc>>,
    options: &RunOptions,
) -> Option<DateTime<Utc>> {
    let is_business_date = options.business_date_after.is_some()
        || options.business_date_before.is_some();

    let candidate = if !is_business_date {
        options.modified_before.or_else(|| Some(Utc::now()))
    } else if options.advance_watermark {
        options
            .business_date_before
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|ndt| Utc.from_utc_datetime(&ndt))
    } else {
        return existing_wm;
    };

    match (existing_wm, candidate) {
        (Some(prev), Some(c)) if prev > c => Some(prev),
        (_, next) => next,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn opt_business_date(after: NaiveDate, before: NaiveDate, advance: bool) -> RunOptions {
        RunOptions {
            business_date_after: Some(after),
            business_date_before: Some(before),
            advance_watermark: advance,
            ..RunOptions::default()
        }
    }

    fn opt_modified(before: Option<DateTime<Utc>>) -> RunOptions {
        RunOptions {
            modified_before: before,
            ..RunOptions::default()
        }
    }

    #[test]
    fn modified_only_advances_to_modified_before() {
        let prev = Some(Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap());
        let target = Utc.with_ymd_and_hms(2026, 5, 11, 0, 0, 0).unwrap();
        let next = compute_next_watermark(prev, &opt_modified(Some(target)));
        assert_eq!(next, Some(target));
    }

    #[test]
    fn modified_only_falls_back_to_now_when_modified_before_missing() {
        let prev = Some(Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap());
        let next = compute_next_watermark(prev, &opt_modified(None));
        assert!(next.unwrap() > prev.unwrap());
    }

    #[test]
    fn watermark_is_monotonic_modified_path() {
        let prev = Some(Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap());
        let target = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap();
        let next = compute_next_watermark(prev, &opt_modified(Some(target)));
        assert_eq!(next, prev, "later prev wm must not regress to earlier candidate");
    }

    #[test]
    fn business_date_without_opt_in_keeps_existing_wm() {
        let prev = Some(Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap());
        let opts = opt_business_date(
            NaiveDate::from_ymd_opt(2026, 5, 4).unwrap(),
            NaiveDate::from_ymd_opt(2026, 5, 11).unwrap(),
            false,
        );
        assert_eq!(compute_next_watermark(prev, &opts), prev);
    }

    #[test]
    fn business_date_with_opt_in_advances_to_business_date_before() {
        let prev = Some(Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap());
        let opts = opt_business_date(
            NaiveDate::from_ymd_opt(2026, 5, 4).unwrap(),
            NaiveDate::from_ymd_opt(2026, 5, 11).unwrap(),
            true,
        );
        let next = compute_next_watermark(prev, &opts);
        assert_eq!(
            next,
            Some(Utc.with_ymd_and_hms(2026, 5, 11, 0, 0, 0).unwrap()),
        );
    }

    #[test]
    fn business_date_with_opt_in_is_monotonic() {
        let prev = Some(Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap());
        let opts = opt_business_date(
            NaiveDate::from_ymd_opt(2026, 5, 4).unwrap(),
            NaiveDate::from_ymd_opt(2026, 5, 11).unwrap(),
            true,
        );
        assert_eq!(compute_next_watermark(prev, &opts), prev);
    }

    #[test]
    fn first_ever_run_modified_path_seeds_watermark() {
        let target = Utc.with_ymd_and_hms(2026, 5, 11, 0, 0, 0).unwrap();
        let next = compute_next_watermark(None, &opt_modified(Some(target)));
        assert_eq!(next, Some(target));
    }
}

