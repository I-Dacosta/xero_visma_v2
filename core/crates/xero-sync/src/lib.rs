//! `xero-sync` — stateless raw → GCS uploader orchestration.
//!
//! This crate fans out per-(tenant, entity) Xero fetches and writes the
//! verbatim API response bytes to a [`RawSink`] (GCS in production, local disk
//! for dry-runs). There is **no** Postgres, BigQuery, Redis, checkpoint, or
//! watermark state: every run computes its own window from [`SyncMode`] and a
//! `run_date`, and re-runs simply append new objects (downstream dedups).
//!
//! Auth is custom-connection (`client_credentials`) only, via
//! [`MultiTenantCustomConnectionClient`]. Custom-connection tokens are
//! tenant-scoped, so the API client is built with the `xero-tenant-id` header
//! DISABLED (`new_with_tenant_header(tenant, false)`).
//!
//! Public surface:
//!   - [`SyncJob`] — the orchestrator (`new` + `run`).
//!   - [`SyncMode`] — the per-run window/filter selector.
//!
//! Pipeline (`run` → `run_entity`):
//!   1. mint a token for the tenant (custom-connection)
//!   2. derive fetch params from the [`SyncMode`]
//!   3. `xero-client::fetch_raw_pages` (or `fetch_report_raw` for reports)
//!   4. for each page: build object key + metadata, `sink.put_raw`
//!   5. record an [`EntityOutcome`]; per-entity failures are isolated.

mod mode;
mod report;

pub use mode::{
    business_date_where, compact_ts, fetch_params, incremental_modified_after, rolling_window,
    FetchParams, SyncMode,
};
pub use report::{report_params, ReportSpec};

use std::sync::Arc;

use chrono::{NaiveDate, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::{debug, info, warn};
use uuid::Uuid;

use xero_auth::MultiTenantCustomConnectionClient;
use xero_client::{ExtraQuery, XeroApiClient};
use xero_common::{EntityType, GcsConfig, SyncConfig};
use xero_gcs::{
    metadata, object_key, report_metadata, report_object_key, EntityOutcome, MetaArgs, RawSink,
    ReportMetaArgs, RunManifest,
};

/// The maximum number of concurrent in-flight (tenant, entity) tasks. Xero
/// caps per-tenant concurrency at 6, so this is the safe hard ceiling.
const MAX_CONCURRENCY: usize = 6;

/// Stateless orchestrator: fetch raw Xero pages and upload them to a sink.
pub struct SyncJob {
    auth: Arc<MultiTenantCustomConnectionClient>,
    sink: Arc<dyn RawSink>,
    gcs: GcsConfig,
    cfg: SyncConfig,
}

impl SyncJob {
    /// Build a job from its collaborators. No I/O — wiring only.
    pub fn new(
        auth: Arc<MultiTenantCustomConnectionClient>,
        sink: Arc<dyn RawSink>,
        gcs: GcsConfig,
        cfg: SyncConfig,
    ) -> Self {
        Self {
            auth,
            sink,
            gcs,
            cfg,
        }
    }

    /// Run a sync across the cartesian product of `tenants` × `entities` in the
    /// given [`SyncMode`], bounded by `max_concurrent` (clamped to 1..=6).
    ///
    /// Each (tenant, entity) pair is isolated: a failure becomes an
    /// [`EntityOutcome`] with `error` set and `termination = "error"`, never
    /// aborting the rest of the run. Returns the assembled [`RunManifest`];
    /// the caller persists it via `xero_gcs::write_manifest`.
    pub async fn run(
        &self,
        run_id: Uuid,
        run_date: NaiveDate,
        tenants: &[String],
        entities: &[EntityType],
        mode: SyncMode,
        max_concurrent: usize,
    ) -> RunManifest {
        let started_at = Utc::now();
        // One compact timestamp for the whole run → all objects share a ts.
        let ts = compact_ts(started_at);
        let concurrency = max_concurrent.clamp(1, MAX_CONCURRENCY);

        info!(
            run_id = %run_id,
            run_date = %run_date,
            mode = mode.label(),
            tenants = tenants.len(),
            entities = entities.len(),
            concurrency,
            "sync run started"
        );

        // Build every (tenant, entity) unit of work up front.
        let units: Vec<(&str, &EntityType)> = tenants
            .iter()
            .flat_map(|t| entities.iter().map(move |e| (t.as_str(), e)))
            .collect();

        let mut outcomes: Vec<EntityOutcome> = Vec::with_capacity(units.len());
        let mut in_flight = FuturesUnordered::new();
        let mut next = 0usize;

        // Prime the pump up to `concurrency`, then refill on each completion.
        while next < units.len() && in_flight.len() < concurrency {
            let (tenant, entity) = units[next];
            in_flight.push(self.run_unit(run_id, run_date, &ts, tenant, entity, &mode));
            next += 1;
        }
        while let Some(outcome) = in_flight.next().await {
            outcomes.push(outcome);
            if next < units.len() {
                let (tenant, entity) = units[next];
                in_flight.push(self.run_unit(run_id, run_date, &ts, tenant, entity, &mode));
                next += 1;
            }
        }

        let finished_at = Utc::now();
        info!(
            run_id = %run_id,
            entities = outcomes.len(),
            errors = outcomes.iter().filter(|o| o.error.is_some()).count(),
            "sync run finished"
        );

        RunManifest {
            run_id: run_id.to_string(),
            started_at: started_at.to_rfc3339(),
            finished_at: finished_at.to_rfc3339(),
            mode: mode.label().to_string(),
            window_days: mode.window_days(),
            entities: outcomes,
        }
    }

    /// Run a single (tenant, entity) unit, converting any error into an
    /// [`EntityOutcome`] so the surrounding fan-out never short-circuits.
    async fn run_unit(
        &self,
        run_id: Uuid,
        run_date: NaiveDate,
        ts: &str,
        tenant: &str,
        entity: &EntityType,
        mode: &SyncMode,
    ) -> EntityOutcome {
        match self
            .run_entity(run_id, run_date, ts, tenant, entity, mode)
            .await
        {
            Ok(outcome) => outcome,
            Err(e) => {
                warn!(
                    run_id = %run_id,
                    tenant = %tenant,
                    entity = %entity.as_str(),
                    error = %e,
                    "entity sync failed (isolated)"
                );
                EntityOutcome {
                    tenant: tenant.to_string(),
                    endpoint: entity.as_str().to_string(),
                    pages: 0,
                    records: 0,
                    termination: "error".to_string(),
                    error: Some(e),
                }
            }
        }
    }

    /// Fetch + upload one (tenant, entity). Reports take the single-shot report
    /// path; everything else takes the paginated raw path.
    async fn run_entity(
        &self,
        run_id: Uuid,
        run_date: NaiveDate,
        ts: &str,
        tenant: &str,
        entity: &EntityType,
        mode: &SyncMode,
    ) -> std::result::Result<EntityOutcome, String> {
        // Custom-connection token → API client WITHOUT the tenant header.
        let token = self
            .auth
            .fetch_token_for_tenant(tenant)
            .await
            .map_err(|e| e.to_string())?;
        let api = XeroApiClient::new_with_tenant_header(tenant.to_string(), false);
        let synced_at = Utc::now().to_rfc3339();
        let run_id_str = run_id.to_string();
        let run_id_short = &run_id_str[..run_id_str.len().min(6)];

        if entity.is_report() {
            return self
                .run_report(
                    &api,
                    &token.access_token,
                    run_date,
                    ts,
                    run_id_short,
                    &run_id_str,
                    tenant,
                    entity,
                    mode,
                    &synced_at,
                )
                .await;
        }

        let params = fetch_params(mode, Utc::now(), run_date, self.cfg.window_days);
        let (pages, outcome) = api
            .fetch_raw_pages(
                &token.access_token,
                entity,
                params.modified_after,
                &params.extras,
            )
            .await
            .map_err(|e| e.to_string())?;

        let mut total_records: i64 = 0;
        for page in &pages {
            total_records += page.record_count as i64;
            let key = object_key(
                &self.gcs.prefix,
                tenant,
                entity.as_str(),
                run_date,
                ts,
                run_id_short,
                page.page,
            );
            let meta = metadata(&MetaArgs {
                tenant_id: tenant,
                org_name: "",
                endpoint: entity.as_str(),
                sync_type: mode.sync_type(),
                modified_after: params.modified_after_str.as_deref(),
                business_from: params.business_from.as_deref(),
                business_to: params.business_to.as_deref(),
                where_filter: params.where_filter.as_deref(),
                page: page.page,
                record_count: page.record_count as i64,
                http_status: page.http_status,
                run_id: &run_id_str,
                synced_at: &synced_at,
            });
            self.sink
                .put_raw(&key, &page.body, &meta)
                .await
                .map_err(|e| e.to_string())?;
            debug!(run_id = %run_id, tenant = %tenant, entity = %entity.as_str(), page = page.page, "page uploaded");
        }

        Ok(EntityOutcome {
            tenant: tenant.to_string(),
            endpoint: entity.as_str().to_string(),
            pages: outcome.pages_fetched,
            records: total_records,
            termination: outcome.termination.as_str().to_string(),
            error: None,
        })
    }

    /// Single-shot report fetch + upload.
    #[allow(clippy::too_many_arguments)]
    async fn run_report(
        &self,
        api: &XeroApiClient,
        access_token: &str,
        run_date: NaiveDate,
        ts: &str,
        run_id_short: &str,
        run_id: &str,
        tenant: &str,
        entity: &EntityType,
        mode: &SyncMode,
        synced_at: &str,
    ) -> std::result::Result<EntityOutcome, String> {
        // Reports only make sense in Reports mode; default to today otherwise.
        let as_of = match mode {
            SyncMode::Reports { as_of } => *as_of,
            _ => run_date,
        };
        let spec = report_params(entity, as_of);
        let extras = ExtraQuery {
            extra: spec.extra.clone(),
            ..ExtraQuery::default()
        };
        let page = api
            .fetch_report_raw(access_token, entity, &extras)
            .await
            .map_err(|e| e.to_string())?;

        let key = report_object_key(
            &self.gcs.prefix,
            tenant,
            entity.as_str(),
            run_date,
            ts,
            run_id_short,
            &spec.period_key,
        );
        let signature = spec.params_signature();
        let meta = report_metadata(&ReportMetaArgs {
            tenant_id: tenant,
            org_name: "",
            endpoint: entity.as_str(),
            report: entity.as_str(),
            report_date: spec.report_date.as_deref(),
            report_from: spec.report_from.as_deref(),
            report_to: spec.report_to.as_deref(),
            report_params: &signature,
            http_status: page.http_status,
            record_count: page.record_count as i64,
            run_id,
            synced_at,
        });
        self.sink
            .put_raw(&key, &page.body, &meta)
            .await
            .map_err(|e| e.to_string())?;

        Ok(EntityOutcome {
            tenant: tenant.to_string(),
            endpoint: entity.as_str().to_string(),
            pages: 1,
            records: page.record_count as i64,
            termination: "report-snapshot".to_string(),
            error: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_id_short_is_first_six_chars() {
        let run_id = Uuid::parse_str("5f3c9a2e-0000-0000-0000-000000000000").unwrap();
        let s = run_id.to_string();
        let short = &s[..s.len().min(6)];
        assert_eq!(short, "5f3c9a");
    }

    #[test]
    fn max_concurrency_clamp_bounds() {
        assert_eq!(0usize.clamp(1, MAX_CONCURRENCY), 1);
        assert_eq!(100usize.clamp(1, MAX_CONCURRENCY), 6);
        assert_eq!(4usize.clamp(1, MAX_CONCURRENCY), 4);
    }
}
