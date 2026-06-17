//! `xero-client` — Xero Accounting API REST client.

mod rate_limit;
mod retry;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde_json::Value;
use tracing::debug;
use xero_common::{EntityType, Error, Result};

const BASE_URL: &str = "https://api.xero.com/api.xro/2.0";

/// Why a paginated fetch loop stopped. Surfaced for monitoring so a silent
/// truncation (a future pagination regression, or an entity that legitimately
/// outgrows the safety cap) is VISIBLE rather than silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationReason {
    /// Healthy: paging stopped because Xero returned an empty page — we
    /// fetched everything available.
    EndedOnEmptyPage,
    /// SUSPECT: the `XERO_MAX_PAGES_PER_ENTITY` safety cap fired BEFORE an
    /// empty page was seen, so the result set is possibly truncated.
    HitMaxPagesCap,
    /// Healthy (offset/Journals path): paging stopped because the offset
    /// failed to advance, the natural end-of-data signal for that endpoint.
    OffsetNotAdvancing,
}

impl TerminationReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EndedOnEmptyPage => "ended_on_empty_page",
            Self::HitMaxPagesCap => "hit_max_pages_cap",
            Self::OffsetNotAdvancing => "offset_not_advancing",
        }
    }

    /// Whether this outcome indicates a possibly-truncated fetch worth alerting on.
    pub fn is_suspect(&self) -> bool {
        matches!(self, Self::HitMaxPagesCap)
    }
}

/// How a paginated fetch ended: page count consumed plus the termination
/// reason. Threaded back to the sync executor so it can be logged / recorded
/// alongside the existing per-run stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaginationOutcome {
    pub pages_fetched: u32,
    pub termination: TerminationReason,
}

impl PaginationOutcome {
    /// Outcome for non-paginated (single-shot) endpoints: one page, healthy stop.
    fn single_page() -> Self {
        Self {
            pages_fetched: 1,
            termination: TerminationReason::EndedOnEmptyPage,
        }
    }
}

/// A fetched record set paired with how its pagination terminated.
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub records: Vec<Value>,
    pub outcome: PaginationOutcome,
}

/// A single raw HTTP page response, captured VERBATIM for the raw-GCS uploader.
///
/// Unlike [`FetchResult`], which parses records into [`Value`]s, a `RawPage`
/// preserves the exact response bytes (`body`) so they can be persisted to GCS
/// without re-serialization (which would lose byte-for-byte fidelity, key order,
/// whitespace, etc.). `record_count` is computed by a minimal parse of the body
/// purely to drive pagination (see [`record_count_from_bytes`]); the parsed form
/// is otherwise discarded.
#[derive(Debug, Clone)]
pub struct RawPage {
    /// 1-based page number this body corresponds to.
    pub page: u32,
    /// Exact response body bytes as returned by Xero.
    pub body: bytes::Bytes,
    /// HTTP status code of the response that produced `body`.
    pub http_status: u16,
    /// Number of records in `body[entity.xero_path()]`, used for pagination.
    pub record_count: usize,
    /// UTC instant at which this page was fetched.
    pub fetched_at: DateTime<Utc>,
}

/// Hard cap on pages fetched per `fetch*` call. Override via env var
/// `XERO_MAX_PAGES_PER_ENTITY` (parsed lazily on first use, cached).
///
/// Default is effectively unbounded (`u32::MAX`) so all available data is
/// fetched. Set the env var to a smaller value to bound legitimate use.
fn max_pages_per_entity() -> u32 {
    use std::sync::OnceLock;
    static CACHED: OnceLock<u32> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("XERO_MAX_PAGES_PER_ENTITY")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(u32::MAX)
    })
}

/// Extra query knobs that the HTTP API can pass through to `fetch()`.
/// All fields are optional — `ExtraQuery::default()` produces no extra
/// query params and matches the legacy fetch behaviour.
#[derive(Debug, Clone, Default)]
pub struct ExtraQuery {
    /// Override default pageSize=100 (only honoured for paginated endpoints).
    pub page_size: Option<u32>,
    /// Xero `where=` filter (raw — caller is responsible for escaping).
    pub where_clause: Option<String>,
    /// Xero `order=` clause, e.g. `"Date DESC"`.
    pub order: Option<String>,
    /// Comma-separated IDs filter (uses Xero `IDs=` query param).
    pub ids: Option<String>,
    /// Add `includeArchived=true` where Xero supports it.
    pub include_archived: bool,
    /// Add `summaryOnly=true` where Xero supports it.
    pub summary_only: bool,
    /// Xero `Statuses=` filter, e.g. `"AUTHORISED,PAID"`.
    pub statuses: Option<String>,
    /// For per-record detail endpoints (e.g. Budgets), append
    /// `?BudgetLines=true` and fetch each listed record individually.
    pub expand_lines: bool,
    /// Free-form extra query params for forward compatibility.
    pub extra: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateWindow {
    pub start: NaiveDate,
    pub end_exclusive: NaiveDate,
}

impl DateWindow {
    pub fn new(start: NaiveDate, end_exclusive: NaiveDate) -> Result<Self> {
        if start >= end_exclusive {
            return Err(Error::Config(format!(
                "business date window must be half-open with start < end, got {start}..{end_exclusive}"
            )));
        }

        Ok(Self {
            start,
            end_exclusive,
        })
    }
}

/// Minimal Xero API client. Wraps reqwest + bearer token.
pub struct XeroApiClient {
    http: reqwest::Client,
    tenant_id: String,
    send_tenant_header: bool,
}

impl XeroApiClient {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self::new_with_tenant_header(tenant_id, true)
    }

    pub fn new_with_tenant_header(tenant_id: impl Into<String>, send_tenant_header: bool) -> Self {
        Self {
            http: reqwest::Client::builder()
                .user_agent("xero_service_v2/0.1")
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("reqwest"),
            tenant_id: tenant_id.into(),
            send_tenant_header,
        }
    }

    /// Fetch a page of records for the given entity, optionally filtered by a
    /// `ModifiedAfter` watermark.
    pub async fn fetch_page(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        page: u32,
        page_size: u32,
    ) -> Result<Vec<Value>> {
        let mut query = vec![("page".to_owned(), page.to_string())];
        if supports_page_size(entity) {
            query.push(("pageSize".to_owned(), page_size.to_string()));
        }

        self.fetch_records_with_owned_query(access_token, entity, modified_after, &query, page)
            .await
    }

    async fn fetch_records_with_query(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        query: &[(&str, String)],
        page_for_logs: u32,
    ) -> Result<Vec<Value>> {
        let url = format!("{BASE_URL}/{}", entity.xero_path());
        let limiter = rate_limit::for_tenant(&self.tenant_id);
        let mut attempt: u32 = 0;

        loop {
            let permit = limiter.acquire().await;

            let mut req = self
                .http
                .get(&url)
                .bearer_auth(access_token)
                .header("Accept", "application/json")
                .query(query);

            if self.send_tenant_header {
                req = req.header("Xero-tenant-id", &self.tenant_id);
            }

            if let Some(watermark) = modified_after {
                req = req.header(
                    "If-Modified-Since",
                    watermark.format("%Y-%m-%dT%H:%M:%S").to_string(),
                );
            }

            let send_result = req.send().await;

            match send_result {
                Ok(resp) => {
                    limiter.update_from_headers(resp.headers());
                    let status = resp.status();

                    if status == reqwest::StatusCode::NOT_MODIFIED {
                        debug!(entity = %entity, page = page_for_logs, "304 Not Modified — no new records");
                        return Ok(vec![]);
                    }

                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        if attempt >= retry::MAX_ATTEMPTS {
                            return Err(Error::XeroApi(format!(
                                "429 {} after {} attempts",
                                entity.xero_path(),
                                retry::MAX_ATTEMPTS
                            )));
                        }
                        let wait = retry::retry_after_or_default(resp.headers());
                        debug!(
                            entity = %entity,
                            page = page_for_logs,
                            attempt,
                            retry_after_secs = wait.as_secs(),
                            "429 Too Many Requests — wait + retry",
                        );
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }

                    if status.is_server_error() {
                        if attempt >= retry::MAX_ATTEMPTS {
                            let body = resp.text().await.unwrap_or_default();
                            return Err(Error::XeroApi(format!(
                                "{} {}: {body}",
                                status.as_u16(),
                                entity.xero_path()
                            )));
                        }
                        let wait = retry::exp_backoff(attempt);
                        debug!(
                            entity = %entity,
                            page = page_for_logs,
                            attempt,
                            status = %status,
                            wait_ms = wait.as_millis() as u64,
                            "5xx — backoff + retry",
                        );
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }

                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::XeroApi(format!(
                            "{} {}: {body}",
                            status.as_u16(),
                            entity.xero_path()
                        )));
                    }

                    let body: Value = resp
                        .json()
                        .await
                        .map_err(|e| Error::XeroApi(e.to_string()))?;

                    let records = body
                        .get(entity.xero_path())
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    return Ok(records);
                }
                Err(e) => {
                    if retry::is_transient_transport_err(&e) && attempt < retry::MAX_ATTEMPTS {
                        let wait = retry::exp_backoff(attempt);
                        debug!(
                            entity = %entity,
                            page = page_for_logs,
                            attempt,
                            err = %e,
                            wait_ms = wait.as_millis() as u64,
                            "transport error — backoff + retry",
                        );
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(Error::XeroApi(e.to_string()));
                }
            }
        }
    }

    /// Fetch one record by its primary key. Used to expand detail-only
    /// fields like `BudgetLines[]` that the list endpoint omits.
    /// Returns `None` if the response has no array for this entity.
    pub async fn fetch_one_by_id(
        &self,
        access_token: &str,
        entity: &EntityType,
        record_id: &str,
        extra: &ExtraQuery,
    ) -> Result<Option<Value>> {
        let url = format!("{BASE_URL}/{}/{}", entity.xero_path(), record_id);
        let mut query: Vec<(String, String)> = Vec::new();
        if extra.expand_lines && matches!(entity, EntityType::Budgets) {
            query.push(("BudgetLines".to_owned(), "true".to_owned()));
        }
        for (k, v) in &extra.extra {
            query.push((k.clone(), v.clone()));
        }

        let limiter = rate_limit::for_tenant(&self.tenant_id);
        let mut attempt: u32 = 0;
        loop {
            let permit = limiter.acquire().await;
            let mut req = self
                .http
                .get(&url)
                .bearer_auth(access_token)
                .header("Accept", "application/json")
                .query(&query);
            if self.send_tenant_header {
                req = req.header("Xero-tenant-id", &self.tenant_id);
            }
            match req.send().await {
                Ok(resp) => {
                    limiter.update_from_headers(resp.headers());
                    let status = resp.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        if attempt >= retry::MAX_ATTEMPTS {
                            return Err(Error::XeroApi(format!(
                                "429 {}/{record_id} after {} attempts",
                                entity.xero_path(),
                                retry::MAX_ATTEMPTS
                            )));
                        }
                        let wait = retry::retry_after_or_default(resp.headers());
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::XeroApi(format!(
                            "{} {}/{record_id}: {body}",
                            status.as_u16(),
                            entity.xero_path()
                        )));
                    }
                    let body: Value = resp
                        .json()
                        .await
                        .map_err(|e| Error::XeroApi(e.to_string()))?;
                    let record = body
                        .get(entity.xero_path())
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.first().cloned());
                    return Ok(record);
                }
                Err(e) => {
                    if retry::is_transient_transport_err(&e) && attempt < retry::MAX_ATTEMPTS {
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(Error::XeroApi(e.to_string()));
                }
            }
        }
    }

    /// Fetch a Xero Accounting **Report** (`Reports/<Name>`).
    ///
    /// Reports are parameterized point-in-time payloads, not lists: a single
    /// `GET {BASE}/Reports/<Name>?<params>` returns one `ReportWithRows`
    /// document under the top-level `Reports` array. We return it as a SINGLE
    /// record, wrapped so the synthesized `record_id` is reproducible:
    ///
    /// ```json
    /// { "_params": { "date": "2026-05-31", ... }, "_run_date": "<rfc3339>", "Report": <raw response> }
    /// ```
    ///
    /// `_params` is the resolved query map (per-report defaults merged with the
    /// caller's `extra.extra` overrides); `_run_date` is the UTC instant of the
    /// fetch. `xero_state::record_id_for_entity` derives a stable id from these
    /// so each (report, params, run) is a distinct immutable snapshot — re-running
    /// with the same params on the same day is idempotent, while a later day
    /// produces a new snapshot rather than overwriting the prior one.
    ///
    /// Returns a [`FetchResult`] with [`PaginationOutcome::single_page`].
    pub async fn fetch_report(
        &self,
        access_token: &str,
        entity: &EntityType,
        extra: &ExtraQuery,
    ) -> Result<FetchResult> {
        if !entity.is_report() {
            return Err(Error::Config(format!(
                "{entity} is not a report entity; fetch_report only handles Reports/*"
            )));
        }

        // Resolve the query params: per-report defaults, then caller overrides
        // (`extra.extra`) win on key collision. Kept as a sorted Vec so the
        // serialized `_params` map and the synthesized record_id are stable
        // regardless of insertion order.
        let params = resolve_report_params(entity, &extra.extra);
        validate_report_params(entity, &params)?;

        let url = format!("{BASE_URL}/{}", entity.xero_path());
        let limiter = rate_limit::for_tenant(&self.tenant_id);
        let mut attempt: u32 = 0;
        loop {
            let permit = limiter.acquire().await;
            let mut req = self
                .http
                .get(&url)
                .bearer_auth(access_token)
                .header("Accept", "application/json")
                .query(&params);
            if self.send_tenant_header {
                req = req.header("Xero-tenant-id", &self.tenant_id);
            }
            match req.send().await {
                Ok(resp) => {
                    limiter.update_from_headers(resp.headers());
                    let status = resp.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        if attempt >= retry::MAX_ATTEMPTS {
                            return Err(Error::XeroApi(format!(
                                "429 {} after {} attempts",
                                entity.xero_path(),
                                retry::MAX_ATTEMPTS
                            )));
                        }
                        let wait = retry::retry_after_or_default(resp.headers());
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if status.is_server_error() {
                        if attempt >= retry::MAX_ATTEMPTS {
                            let body = resp.text().await.unwrap_or_default();
                            return Err(Error::XeroApi(format!(
                                "{} {}: {body}",
                                status.as_u16(),
                                entity.xero_path()
                            )));
                        }
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::XeroApi(format!(
                            "{} {}: {body}",
                            status.as_u16(),
                            entity.xero_path()
                        )));
                    }
                    let raw: Value = resp
                        .json()
                        .await
                        .map_err(|e| Error::XeroApi(e.to_string()))?;
                    let wrapped = wrap_report_record(&params, raw);
                    return Ok(FetchResult {
                        records: vec![wrapped],
                        outcome: PaginationOutcome::single_page(),
                    });
                }
                Err(e) => {
                    if retry::is_transient_transport_err(&e) && attempt < retry::MAX_ATTEMPTS {
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(Error::XeroApi(e.to_string()));
                }
            }
        }
    }

    /// Fetch with arbitrary extra query knobs (page_size, where, order, IDs,
    /// includeArchived, summaryOnly, statuses, …). For `Budgets` with
    /// `extra.expand_lines = true`, list budgets then fan out per-id with
    /// `?BudgetLines=true` to populate the nested `BudgetLines[]`.
    pub async fn fetch_with_extras(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        modified_before: Option<DateTime<Utc>>,
        extra: &ExtraQuery,
    ) -> Result<Vec<Value>> {
        self.fetch_with_extras_tracked(access_token, entity, modified_after, modified_before, extra)
            .await
            .map(|r| r.records)
    }

    /// Like [`Self::fetch_with_extras`] but also returns the
    /// [`PaginationOutcome`] so callers can observe how pagination terminated.
    pub async fn fetch_with_extras_tracked(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        modified_before: Option<DateTime<Utc>>,
        extra: &ExtraQuery,
    ) -> Result<FetchResult> {
        let page_size = extra.page_size.unwrap_or(100);
        let (base, outcome) = self
            .fetch_inner(access_token, entity, modified_after, page_size, extra)
            .await?;
        let filtered = filter_records_by_modified_window(base, modified_after, modified_before);

        // Special case: some list endpoints omit detail-only nested arrays that
        // are only returned by the per-record GET. Expand each row via a per-id
        // detail fetch.
        //   - Budgets: list never returns `BudgetLines[]` (needs `?BudgetLines=true`).
        //   - ContactGroups: list never returns the `Contacts[]` membership array;
        //     `GET /ContactGroups/{id}` returns it inline.
        if extra.expand_lines && matches!(entity, EntityType::Budgets | EntityType::ContactGroups) {
            let id_field = entity.id_field();
            let mut expanded = Vec::with_capacity(filtered.len());
            for record in filtered {
                let id = record
                    .get(id_field)
                    .and_then(|v| v.as_str())
                    .map(ToOwned::to_owned);
                match id {
                    Some(record_id) => match self
                        .fetch_one_by_id(access_token, entity, &record_id, extra)
                        .await
                    {
                        Ok(Some(detail)) => expanded.push(detail),
                        Ok(None) => expanded.push(record),
                        Err(e) => {
                            debug!(error = %e, entity = %entity, record_id, "detail expand fetch failed — keeping header");
                            expanded.push(record);
                        }
                    },
                    None => expanded.push(record),
                }
            }
            return Ok(FetchResult {
                records: expanded,
                outcome,
            });
        }

        Ok(FetchResult {
            records: filtered,
            outcome,
        })
    }

    async fn fetch_inner(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        page_size: u32,
        extra: &ExtraQuery,
    ) -> Result<(Vec<Value>, PaginationOutcome)> {
        if matches!(entity, EntityType::Journals) {
            return self
                .fetch_journals(access_token, entity, modified_after)
                .await;
        }

        let extras_pairs = extra_query_pairs(extra, entity);

        if !supports_page_pagination(entity) {
            let records = self
                .fetch_records_with_owned_query(
                    access_token,
                    entity,
                    modified_after,
                    &extras_pairs,
                    1,
                )
                .await?;
            return Ok((records, PaginationOutcome::single_page()));
        }

        let mut all = Vec::new();
        let mut page = 1u32;
        let cap = max_pages_per_entity();
        let termination = loop {
            if page > cap {
                break TerminationReason::HitMaxPagesCap;
            }
            let mut q: Vec<(String, String)> = extras_pairs.clone();
            q.push(("page".to_owned(), page.to_string()));
            if supports_page_size(entity) {
                q.push(("pageSize".to_owned(), page_size.to_string()));
            }
            let records = self
                .fetch_records_with_owned_query(access_token, entity, modified_after, &q, page)
                .await?;
            let stop = should_stop_page_pagination(records.len());
            all.extend(records);
            if stop {
                break TerminationReason::EndedOnEmptyPage;
            }
            page += 1;
        };
        Ok((
            all,
            PaginationOutcome {
                pages_fetched: page.saturating_sub(1),
                termination,
            },
        ))
    }

    /// Same retry/rate-limit semantics as `fetch_records_with_query`, but
    /// accepts owned `(String, String)` pairs so callers don't have to leak
    /// memory for dynamic query-param keys.
    async fn fetch_records_with_owned_query(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        query: &[(String, String)],
        page_for_logs: u32,
    ) -> Result<Vec<Value>> {
        let url = format!("{BASE_URL}/{}", entity.xero_path());
        let limiter = rate_limit::for_tenant(&self.tenant_id);
        let mut attempt: u32 = 0;
        loop {
            let permit = limiter.acquire().await;
            let mut req = self
                .http
                .get(&url)
                .bearer_auth(access_token)
                .header("Accept", "application/json")
                .query(query);
            if self.send_tenant_header {
                req = req.header("Xero-tenant-id", &self.tenant_id);
            }
            if let Some(watermark) = modified_after {
                req = req.header(
                    "If-Modified-Since",
                    watermark.format("%Y-%m-%dT%H:%M:%S").to_string(),
                );
            }
            match req.send().await {
                Ok(resp) => {
                    limiter.update_from_headers(resp.headers());
                    let status = resp.status();
                    if status == reqwest::StatusCode::NOT_MODIFIED {
                        debug!(entity = %entity, page = page_for_logs, "304 Not Modified");
                        return Ok(vec![]);
                    }
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        if attempt >= retry::MAX_ATTEMPTS {
                            return Err(Error::XeroApi(format!(
                                "429 {} after {} attempts",
                                entity.xero_path(),
                                retry::MAX_ATTEMPTS
                            )));
                        }
                        let wait = retry::retry_after_or_default(resp.headers());
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if status.is_server_error() {
                        if attempt >= retry::MAX_ATTEMPTS {
                            let body = resp.text().await.unwrap_or_default();
                            return Err(Error::XeroApi(format!(
                                "{} {}: {body}",
                                status.as_u16(),
                                entity.xero_path()
                            )));
                        }
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::XeroApi(format!(
                            "{} {}: {body}",
                            status.as_u16(),
                            entity.xero_path()
                        )));
                    }
                    let body: Value = resp
                        .json()
                        .await
                        .map_err(|e| Error::XeroApi(e.to_string()))?;
                    let records = body
                        .get(entity.xero_path())
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    return Ok(records);
                }
                Err(e) => {
                    if retry::is_transient_transport_err(&e) && attempt < retry::MAX_ATTEMPTS {
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(Error::XeroApi(e.to_string()));
                }
            }
        }
    }

    pub async fn fetch(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        modified_before: Option<DateTime<Utc>>,
        page_size: u32,
    ) -> Result<Vec<Value>> {
        if matches!(entity, EntityType::Journals) {
            let (records, _outcome) = self
                .fetch_journals(access_token, entity, modified_after)
                .await?;
            return Ok(filter_records_by_modified_window(
                records,
                modified_after,
                modified_before,
            ));
        }

        if !supports_page_pagination(entity) {
            let records = self
                .fetch_records_with_query(access_token, entity, modified_after, &[], 1)
                .await?;
            return Ok(filter_records_by_modified_window(
                records,
                modified_after,
                modified_before,
            ));
        }

        let mut all = Vec::new();
        let mut page = 1u32;
        let cap = max_pages_per_entity();
        loop {
            if page > cap {
                return Err(Error::XeroApi(format!(
                    "{} exceeded max page limit ({cap})",
                    entity.xero_path()
                )));
            }

            let records = self
                .fetch_page(access_token, entity, modified_after, page, page_size)
                .await?;
            let stop = should_stop_page_pagination(records.len());
            all.extend(records);
            if stop {
                break;
            }
            page += 1;
        }
        Ok(filter_records_by_modified_window(
            all,
            modified_after,
            modified_before,
        ))
    }

    pub async fn fetch_by_business_date(
        &self,
        access_token: &str,
        entity: &EntityType,
        window: DateWindow,
        page_size: u32,
    ) -> Result<Vec<Value>> {
        self.fetch_by_business_date_tracked(access_token, entity, window, page_size)
            .await
            .map(|r| r.records)
    }

    /// Like [`Self::fetch_by_business_date`] but also returns the
    /// [`PaginationOutcome`] so callers can observe how pagination terminated.
    pub async fn fetch_by_business_date_tracked(
        &self,
        access_token: &str,
        entity: &EntityType,
        window: DateWindow,
        page_size: u32,
    ) -> Result<FetchResult> {
        let Some(mode) = business_date_query_mode(entity) else {
            return Err(Error::Config(format!(
                "{entity} does not expose a supported business-date filter"
            )));
        };

        if matches!(mode, BusinessDateQueryMode::LocalOnly(_)) {
            // Pre-filter via If-Modified-Since so Xero doesn't ship every journal
            // since the start of time. Records modified before the business-date
            // window are guaranteed to fall outside it, so this is safe.
            let prefilter = window
                .start
                .and_hms_opt(0, 0, 0)
                .map(|ndt| Utc.from_utc_datetime(&ndt));
            let (records, outcome) = self.fetch_journals(access_token, entity, prefilter).await?;
            return Ok(FetchResult {
                records: filter_records_by_business_date_window(entity, records, window),
                outcome,
            });
        }

        if !supports_page_pagination(entity) {
            let query = business_date_query(entity, window, 1, page_size)?;
            let records = self
                .fetch_records_with_query(access_token, entity, None, &query, 1)
                .await?;
            return Ok(FetchResult {
                records: filter_records_by_business_date_window(entity, records, window),
                outcome: PaginationOutcome::single_page(),
            });
        }

        let mut all = Vec::new();
        let mut page = 1u32;
        let cap = max_pages_per_entity();
        let termination = loop {
            if page > cap {
                break TerminationReason::HitMaxPagesCap;
            }

            let query = business_date_query(entity, window, page, page_size)?;
            let records = self
                .fetch_records_with_query(access_token, entity, None, &query, page)
                .await?;
            let stop = should_stop_page_pagination(records.len());
            all.extend(records);
            if stop {
                break TerminationReason::EndedOnEmptyPage;
            }
            page += 1;
        };

        Ok(FetchResult {
            records: filter_records_by_business_date_window(entity, all, window),
            outcome: PaginationOutcome {
                pages_fetched: page.saturating_sub(1),
                termination,
            },
        })
    }

    async fn fetch_journals(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
    ) -> Result<(Vec<Value>, PaginationOutcome)> {
        let mut all = Vec::new();
        let mut offset = 0i64;
        let mut pages = 0u32;
        let cap = max_pages_per_entity();

        let termination = loop {
            if pages >= cap {
                break TerminationReason::HitMaxPagesCap;
            }
            pages += 1;

            let records = self
                .fetch_records_with_query(
                    access_token,
                    entity,
                    modified_after,
                    &[("offset", offset.to_string())],
                    pages,
                )
                .await?;

            let next_offset = max_journal_number(&records).unwrap_or(offset);
            let stop_empty = records.is_empty();
            let stop_offset =
                !stop_empty && should_stop_offset_pagination(records.len(), offset, next_offset);
            all.extend(records);

            if stop_empty {
                break TerminationReason::EndedOnEmptyPage;
            }
            if stop_offset {
                break TerminationReason::OffsetNotAdvancing;
            }

            offset = next_offset;
        };

        Ok((
            all,
            PaginationOutcome {
                pages_fetched: pages,
                termination,
            },
        ))
    }

    /// Fetch all pages for `entity` as VERBATIM raw bodies, for the raw-GCS
    /// uploader. Mirrors the pagination shape of [`Self::fetch_inner`] —
    /// retry, rate-limit, `If-Modified-Since`, `extra_query_pairs`, the
    /// `max_pages_per_entity()` cap, and the same [`TerminationReason`]s — but
    /// instead of parsing records into [`Value`]s it keeps each response body
    /// byte-for-byte in a [`RawPage`]. Each body is parsed MINIMALLY only to
    /// compute `record_count` (via [`record_count_from_bytes`]) which drives
    /// `should_stop_page_pagination` / `should_stop_offset_pagination`.
    ///
    /// Non-paginated entities yield a single [`RawPage`]. Journals use the
    /// offset path. Does NOT alter any existing `fetch_*` method.
    pub async fn fetch_raw_pages(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        extras: &ExtraQuery,
    ) -> Result<(Vec<RawPage>, PaginationOutcome)> {
        if matches!(entity, EntityType::Journals) {
            return self
                .fetch_raw_journals(access_token, entity, modified_after)
                .await;
        }

        let extras_pairs = extra_query_pairs(extras, entity);
        let page_size = extras.page_size.unwrap_or(100);

        if !supports_page_pagination(entity) {
            let raw = self
                .fetch_raw_with_query(access_token, entity, modified_after, &extras_pairs, 1)
                .await?;
            return Ok((vec![raw], PaginationOutcome::single_page()));
        }

        let mut all = Vec::new();
        let mut page = 1u32;
        let cap = max_pages_per_entity();
        let termination = loop {
            if page > cap {
                break TerminationReason::HitMaxPagesCap;
            }
            let mut q: Vec<(String, String)> = extras_pairs.clone();
            q.push(("page".to_owned(), page.to_string()));
            if supports_page_size(entity) {
                q.push(("pageSize".to_owned(), page_size.to_string()));
            }
            let raw = self
                .fetch_raw_with_query(access_token, entity, modified_after, &q, page)
                .await?;
            let stop = should_stop_page_pagination(raw.record_count);
            all.push(raw);
            if stop {
                break TerminationReason::EndedOnEmptyPage;
            }
            page += 1;
        };

        Ok((
            all,
            PaginationOutcome {
                pages_fetched: page.saturating_sub(1),
                termination,
            },
        ))
    }

    /// Offset-paginated raw fetch for the Journals endpoint. Mirrors
    /// [`Self::fetch_journals`] but captures verbatim bodies into [`RawPage`]s.
    async fn fetch_raw_journals(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
    ) -> Result<(Vec<RawPage>, PaginationOutcome)> {
        let mut all = Vec::new();
        let mut offset = 0i64;
        let mut pages = 0u32;
        let cap = max_pages_per_entity();

        let termination = loop {
            if pages >= cap {
                break TerminationReason::HitMaxPagesCap;
            }
            pages += 1;

            let raw = self
                .fetch_raw_with_query(
                    access_token,
                    entity,
                    modified_after,
                    &[("offset".to_owned(), offset.to_string())],
                    pages,
                )
                .await?;

            let next_offset = max_journal_number_from_bytes(entity, &raw.body).unwrap_or(offset);
            let stop_empty = raw.record_count == 0;
            let stop_offset =
                !stop_empty && should_stop_offset_pagination(raw.record_count, offset, next_offset);
            all.push(raw);

            if stop_empty {
                break TerminationReason::EndedOnEmptyPage;
            }
            if stop_offset {
                break TerminationReason::OffsetNotAdvancing;
            }

            offset = next_offset;
        };

        Ok((
            all,
            PaginationOutcome {
                pages_fetched: pages,
                termination,
            },
        ))
    }

    /// Issue one GET with `query` and capture the response VERBATIM into a
    /// [`RawPage`]. Same retry / rate-limit / `If-Modified-Since` semantics as
    /// [`Self::fetch_records_with_owned_query`]. A `304 Not Modified` yields an
    /// empty-body page with `record_count = 0` so pagination terminates cleanly.
    async fn fetch_raw_with_query(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        query: &[(String, String)],
        page_for_logs: u32,
    ) -> Result<RawPage> {
        let url = format!("{BASE_URL}/{}", entity.xero_path());
        let limiter = rate_limit::for_tenant(&self.tenant_id);
        let mut attempt: u32 = 0;
        loop {
            let permit = limiter.acquire().await;
            let mut req = self
                .http
                .get(&url)
                .bearer_auth(access_token)
                .header("Accept", "application/json")
                .query(query);
            if self.send_tenant_header {
                req = req.header("Xero-tenant-id", &self.tenant_id);
            }
            if let Some(watermark) = modified_after {
                req = req.header(
                    "If-Modified-Since",
                    watermark.format("%Y-%m-%dT%H:%M:%S").to_string(),
                );
            }
            match req.send().await {
                Ok(resp) => {
                    limiter.update_from_headers(resp.headers());
                    let status = resp.status();
                    if status == reqwest::StatusCode::NOT_MODIFIED {
                        debug!(entity = %entity, page = page_for_logs, "304 Not Modified (raw)");
                        return Ok(RawPage {
                            page: page_for_logs,
                            body: bytes::Bytes::new(),
                            http_status: status.as_u16(),
                            record_count: 0,
                            fetched_at: Utc::now(),
                        });
                    }
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        if attempt >= retry::MAX_ATTEMPTS {
                            return Err(Error::XeroApi(format!(
                                "429 {} after {} attempts",
                                entity.xero_path(),
                                retry::MAX_ATTEMPTS
                            )));
                        }
                        let wait = retry::retry_after_or_default(resp.headers());
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if status.is_server_error() {
                        if attempt >= retry::MAX_ATTEMPTS {
                            let body = resp.text().await.unwrap_or_default();
                            return Err(Error::XeroApi(format!(
                                "{} {}: {body}",
                                status.as_u16(),
                                entity.xero_path()
                            )));
                        }
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::XeroApi(format!(
                            "{} {}: {body}",
                            status.as_u16(),
                            entity.xero_path()
                        )));
                    }
                    let http_status = status.as_u16();
                    let body = resp
                        .bytes()
                        .await
                        .map_err(|e| Error::XeroApi(e.to_string()))?;
                    let record_count = record_count_from_bytes(entity, &body);
                    return Ok(RawPage {
                        page: page_for_logs,
                        body,
                        http_status,
                        record_count,
                        fetched_at: Utc::now(),
                    });
                }
                Err(e) => {
                    if retry::is_transient_transport_err(&e) && attempt < retry::MAX_ATTEMPTS {
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(Error::XeroApi(e.to_string()));
                }
            }
        }
    }

    /// Fetch a Xero **Report** (`Reports/<Name>`) as a VERBATIM raw body.
    ///
    /// Reuses [`resolve_report_params`] + [`validate_report_params`] for the
    /// query (same as [`Self::fetch_report`]) and the same retry / rate-limit
    /// semantics, but keeps the response bytes byte-for-byte in a [`RawPage`]
    /// (`page = 1`, `record_count = 1`) instead of wrapping/parsing them.
    pub async fn fetch_report_raw(
        &self,
        access_token: &str,
        entity: &EntityType,
        extras: &ExtraQuery,
    ) -> Result<RawPage> {
        if !entity.is_report() {
            return Err(Error::Config(format!(
                "{entity} is not a report entity; fetch_report_raw only handles Reports/*"
            )));
        }

        let params = resolve_report_params(entity, &extras.extra);
        validate_report_params(entity, &params)?;

        let url = format!("{BASE_URL}/{}", entity.xero_path());
        let limiter = rate_limit::for_tenant(&self.tenant_id);
        let mut attempt: u32 = 0;
        loop {
            let permit = limiter.acquire().await;
            let mut req = self
                .http
                .get(&url)
                .bearer_auth(access_token)
                .header("Accept", "application/json")
                .query(&params);
            if self.send_tenant_header {
                req = req.header("Xero-tenant-id", &self.tenant_id);
            }
            match req.send().await {
                Ok(resp) => {
                    limiter.update_from_headers(resp.headers());
                    let status = resp.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        if attempt >= retry::MAX_ATTEMPTS {
                            return Err(Error::XeroApi(format!(
                                "429 {} after {} attempts",
                                entity.xero_path(),
                                retry::MAX_ATTEMPTS
                            )));
                        }
                        let wait = retry::retry_after_or_default(resp.headers());
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if status.is_server_error() {
                        if attempt >= retry::MAX_ATTEMPTS {
                            let body = resp.text().await.unwrap_or_default();
                            return Err(Error::XeroApi(format!(
                                "{} {}: {body}",
                                status.as_u16(),
                                entity.xero_path()
                            )));
                        }
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::XeroApi(format!(
                            "{} {}: {body}",
                            status.as_u16(),
                            entity.xero_path()
                        )));
                    }
                    let http_status = status.as_u16();
                    let body = resp
                        .bytes()
                        .await
                        .map_err(|e| Error::XeroApi(e.to_string()))?;
                    return Ok(RawPage {
                        page: 1,
                        body,
                        http_status,
                        record_count: 1,
                        fetched_at: Utc::now(),
                    });
                }
                Err(e) => {
                    if retry::is_transient_transport_err(&e) && attempt < retry::MAX_ATTEMPTS {
                        let wait = retry::exp_backoff(attempt);
                        drop(permit);
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(Error::XeroApi(e.to_string()));
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum BusinessDateQueryMode {
    Where(&'static str),
    DateFromTo,
    LocalOnly(&'static str),
}

fn business_date_query_mode(entity: &EntityType) -> Option<BusinessDateQueryMode> {
    match entity {
        EntityType::BatchPayments
        | EntityType::Bills
        | EntityType::BankTransactions
        | EntityType::BankTransfers
        | EntityType::CreditNotes
        | EntityType::Invoices
        | EntityType::ManualJournals
        | EntityType::Overpayments
        | EntityType::Payments
        | EntityType::Prepayments
        | EntityType::Receipts => Some(BusinessDateQueryMode::Where("Date")),
        EntityType::PurchaseOrders | EntityType::Quotes => Some(BusinessDateQueryMode::DateFromTo),
        EntityType::Journals => Some(BusinessDateQueryMode::LocalOnly("JournalDate")),
        _ => None,
    }
}

fn business_date_query(
    entity: &EntityType,
    window: DateWindow,
    page: u32,
    page_size: u32,
) -> Result<Vec<(&'static str, String)>> {
    let Some(mode) = business_date_query_mode(entity) else {
        return Err(Error::Config(format!(
            "{entity} does not expose a supported business-date filter"
        )));
    };

    let mut query = match mode {
        BusinessDateQueryMode::Where(field) => vec![("where", where_date_window(field, window))],
        BusinessDateQueryMode::DateFromTo => vec![
            ("DateFrom", window.start.to_string()),
            ("DateTo", inclusive_end_date(window)?.to_string()),
        ],
        BusinessDateQueryMode::LocalOnly(_) => Vec::new(),
    };
    if supports_page_pagination(entity) {
        query.push(("page", page.to_string()));
    }
    if supports_page_size(entity) {
        query.push(("pageSize", page_size.to_string()));
    }
    Ok(query)
}

/// Convert an [`ExtraQuery`] into the actual Xero query-string pairs.
/// Skips fields that don't apply to the entity.
fn extra_query_pairs(extra: &ExtraQuery, entity: &EntityType) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    if matches!(entity, EntityType::Bills) {
        out.push((
            "where".to_owned(),
            bill_where_clause(extra.where_clause.as_deref()),
        ));
    } else if let Some(w) = extra
        .where_clause
        .as_deref()
        .map(str::trim)
        .filter(|w| !w.is_empty())
    {
        // Skip an empty `where` (e.g. an unfiltered open-sweep) so we never send
        // a malformed `?where=` to Xero.
        out.push(("where".to_owned(), w.to_owned()));
    }
    if let Some(o) = &extra.order {
        out.push(("order".to_owned(), o.clone()));
    }
    if let Some(ids) = &extra.ids {
        out.push(("IDs".to_owned(), ids.clone()));
    }
    if extra.include_archived
        && matches!(
            entity,
            EntityType::Bills
                | EntityType::Contacts
                | EntityType::Invoices
                | EntityType::TrackingCategories
        )
    {
        out.push(("includeArchived".to_owned(), "true".to_owned()));
    }
    if extra.summary_only
        && matches!(
            entity,
            EntityType::Bills | EntityType::Contacts | EntityType::Invoices
        )
    {
        out.push(("summaryOnly".to_owned(), "true".to_owned()));
    }
    if let Some(s) = &extra.statuses {
        let key = match entity {
            EntityType::PurchaseOrders | EntityType::Quotes => "Status",
            _ => "Statuses",
        };
        out.push((key.to_owned(), s.clone()));
    }
    for (k, v) in &extra.extra {
        out.push((k.clone(), v.clone()));
    }
    out
}

/// Per-report DEFAULT query params. Date params default to "today" (UTC) so a
/// scheduled run captures the as-of snapshot for the day it runs.
///
/// Param names follow the Xero Accounting OpenAPI (`xero_accounting.yaml`):
///   - ProfitAndLoss → `fromDate` + `toDate` (a date *range* report)
///   - BalanceSheet / TrialBalance → `date` (single as-of date)
///   - AgedReceivablesByContact / AgedPayablesByContact → `date`
///     (NB: `contactId` is REQUIRED by Xero but is a per-contact UUID with no
///     sensible default — the caller MUST supply it via `extra_params`;
///     without it Xero returns 400.)
///   - BankSummary → `fromDate` + `toDate`
///   - ExecutiveSummary → `date` (single as-of date)
///   - BudgetSummary → `date` + `periods` + `timeframe`
fn default_report_params(entity: &EntityType) -> Vec<(String, String)> {
    let today = Utc::now().date_naive().to_string();
    match entity {
        EntityType::ReportProfitAndLoss | EntityType::ReportBankSummary => vec![
            ("fromDate".to_owned(), today.clone()),
            ("toDate".to_owned(), today),
        ],
        EntityType::ReportBalanceSheet
        | EntityType::ReportTrialBalance
        | EntityType::ReportAgedReceivablesByContact
        | EntityType::ReportAgedPayablesByContact
        | EntityType::ReportExecutiveSummary => {
            vec![("date".to_owned(), today)]
        }
        EntityType::ReportBudgetSummary => vec![
            ("date".to_owned(), today),
            ("periods".to_owned(), "3".to_owned()),
            ("timeframe".to_owned(), "1".to_owned()),
        ],
        // Non-report: no defaults (fetch_report rejects these before calling).
        _ => Vec::new(),
    }
}

/// Merge per-report defaults with caller overrides (`extra_params`). On a key
/// collision the caller's value wins. Returned sorted by key so the serialized
/// `_params` map and the synthesized `record_id` are deterministic regardless
/// of insertion order.
fn resolve_report_params(
    entity: &EntityType,
    overrides: &[(String, String)],
) -> Vec<(String, String)> {
    use std::collections::BTreeMap;
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in default_report_params(entity) {
        map.insert(k, v);
    }
    for (k, v) in overrides {
        map.insert(k.clone(), v.clone());
    }
    map.into_iter().collect()
}

fn validate_report_params(entity: &EntityType, params: &[(String, String)]) -> Result<()> {
    if matches!(
        entity,
        EntityType::ReportAgedReceivablesByContact | EntityType::ReportAgedPayablesByContact
    ) && !params
        .iter()
        .any(|(key, value)| key == "contactId" && !value.trim().is_empty())
    {
        return Err(Error::Config(format!(
            "{entity} requires extra_params contactId=<xero-contact-uuid>"
        )));
    }

    Ok(())
}

fn bill_where_clause(user_where: Option<&str>) -> String {
    let bill_filter = r#"Type=="ACCPAY""#;
    match user_where.map(str::trim).filter(|w| !w.is_empty()) {
        Some(w) => format!("{bill_filter} && ({w})"),
        None => bill_filter.to_owned(),
    }
}

/// Wrap a raw report response so the synthesized `record_id` is reproducible.
/// `_params` is a JSON object (sorted keys → stable), `_run_date` is the fetch
/// instant, `Report` is the raw Xero payload (top-level `Reports[]` preserved).
fn wrap_report_record(params: &[(String, String)], raw: Value) -> Value {
    let params_obj: serde_json::Map<String, Value> = params
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    serde_json::json!({
        "_params": Value::Object(params_obj),
        "_run_date": Utc::now().to_rfc3339(),
        "Report": raw,
    })
}

fn supports_page_pagination(entity: &EntityType) -> bool {
    matches!(
        entity,
        EntityType::BankTransactions
            | EntityType::Bills
            | EntityType::Contacts
            | EntityType::CreditNotes
            | EntityType::Invoices
            | EntityType::LinkedTransactions
            | EntityType::ManualJournals
            | EntityType::Overpayments
            | EntityType::Payments
            | EntityType::Prepayments
            | EntityType::PurchaseOrders
            | EntityType::Quotes
    )
}

fn supports_page_size(entity: &EntityType) -> bool {
    matches!(
        entity,
        EntityType::BankTransactions
            | EntityType::Bills
            | EntityType::Contacts
            | EntityType::CreditNotes
            | EntityType::Invoices
            | EntityType::ManualJournals
            | EntityType::Overpayments
            | EntityType::Payments
            | EntityType::Prepayments
            | EntityType::PurchaseOrders
    )
}

fn where_date_window(field: &str, window: DateWindow) -> String {
    format!(
        "{field}>=DateTime({}, {}, {}) && {field}<DateTime({}, {}, {})",
        window.start.year(),
        window.start.month(),
        window.start.day(),
        window.end_exclusive.year(),
        window.end_exclusive.month(),
        window.end_exclusive.day()
    )
}

fn inclusive_end_date(window: DateWindow) -> Result<NaiveDate> {
    window.end_exclusive.pred_opt().ok_or_else(|| {
        Error::Config(format!(
            "business date window end cannot be converted to inclusive DateTo: {}",
            window.end_exclusive
        ))
    })
}

/// Minimal parse of a raw response body to count the records under
/// `entity.xero_path()`. Used by [`XeroApiClient::fetch_raw_pages`] to drive
/// pagination WITHOUT discarding the verbatim bytes. A body that fails to parse,
/// or that lacks the entity array, counts as zero records (a terminal page).
///
/// Pure and self-contained so it is unit-testable without live HTTP.
fn record_count_from_bytes(entity: &EntityType, body: &[u8]) -> usize {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| {
            v.get(entity.xero_path())
                .and_then(|arr| arr.as_array())
                .map(|arr| arr.len())
        })
        .unwrap_or(0)
}

/// Pagination stop predicate for page-based endpoints.
///
/// Xero paginates page-by-page and the ONLY reliable end-of-data signals are
/// an EMPTY page (0 records) or a 4xx past the last page. A short (partial)
/// page is NOT a reliable end signal — Xero may return fewer than `page_size`
/// records on a non-final page — so we must keep paging until an empty page.
///
/// Returns `true` when pagination should STOP after consuming this page.
fn should_stop_page_pagination(records_on_page: usize) -> bool {
    records_on_page == 0
}

/// Pagination stop predicate for the offset-based Journals endpoint.
///
/// Terminates on an empty page or when the offset fails to advance (which
/// would otherwise loop forever). A short page (< 100 records) is NOT a
/// reliable end signal and must not terminate early.
///
/// Returns `true` when pagination should STOP after consuming this page.
fn should_stop_offset_pagination(
    records_on_page: usize,
    current_offset: i64,
    next_offset: i64,
) -> bool {
    records_on_page == 0 || next_offset <= current_offset
}

fn max_journal_number(records: &[Value]) -> Option<i64> {
    records
        .iter()
        .filter_map(|record| {
            record
                .get("JournalNumber")
                .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        })
        .max()
}

/// Bytes-based equivalent of [`max_journal_number`] for the raw-GCS path:
/// minimally parses `body[entity.xero_path()]` and returns the largest
/// `JournalNumber`. Returns `None` on parse failure or when no records exist.
fn max_journal_number_from_bytes(entity: &EntityType, body: &[u8]) -> Option<i64> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let records = value.get(entity.xero_path())?.as_array()?;
    max_journal_number(records)
}

fn filter_records_by_modified_window(
    records: Vec<Value>,
    modified_after: Option<DateTime<Utc>>,
    modified_before: Option<DateTime<Utc>>,
) -> Vec<Value> {
    if modified_after.is_none() && modified_before.is_none() {
        return records;
    }

    records
        .into_iter()
        .filter(|record| {
            let Some(updated_at) = record_modified_at(record) else {
                return modified_before.is_none();
            };

            let after_ok = modified_after
                .map(|after| updated_at >= after)
                .unwrap_or(true);
            let before_ok = modified_before
                .map(|before| updated_at < before)
                .unwrap_or(true);

            after_ok && before_ok
        })
        .collect()
}

fn filter_records_by_business_date_window(
    entity: &EntityType,
    records: Vec<Value>,
    window: DateWindow,
) -> Vec<Value> {
    records
        .into_iter()
        .filter(|record| {
            record_business_date(entity, record)
                .map(|date| date >= window.start && date < window.end_exclusive)
                .unwrap_or(false)
        })
        .collect()
}

fn record_modified_at(record: &Value) -> Option<DateTime<Utc>> {
    [
        "UpdatedDateUTC",
        "UpdatedDateUtc",
        "updatedDateUtc",
        "updated_date_utc",
        "updated_at",
    ]
    .iter()
    .find_map(|field| record.get(field).and_then(parse_datetime_value))
}

fn record_business_date(entity: &EntityType, record: &Value) -> Option<NaiveDate> {
    let fields: &[&str] = match business_date_query_mode(entity)? {
        BusinessDateQueryMode::LocalOnly(field) => &[field],
        _ => &["Date", "DateString"],
    };

    fields
        .iter()
        .find_map(|field| record.get(*field).and_then(parse_date_value))
}

fn parse_datetime_value(value: &Value) -> Option<DateTime<Utc>> {
    let raw = value.as_str()?.trim();

    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| parse_xero_json_date(raw))
}

fn parse_date_value(value: &Value) -> Option<NaiveDate> {
    let raw = value.as_str()?.trim();

    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc).date_naive())
        .ok()
        .or_else(|| parse_xero_json_date(raw).map(|dt| dt.date_naive()))
        .or_else(|| NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok())
        .or_else(|| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S")
                .map(|dt| dt.date())
                .ok()
        })
}

fn parse_xero_json_date(raw: &str) -> Option<DateTime<Utc>> {
    let millis = raw
        .strip_prefix("/Date(")?
        .chars()
        .take_while(|ch| ch.is_ascii_digit() || *ch == '-')
        .collect::<String>()
        .parse::<i64>()
        .ok()?;

    Utc.timestamp_millis_opt(millis).single()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn filters_records_to_half_open_modified_window() {
        let records = vec![
            json!({"UpdatedDateUTC": "2026-04-26T23:59:59Z", "InvoiceID": "in"}),
            json!({"UpdatedDateUTC": "2026-04-27T00:00:00Z", "InvoiceID": "out"}),
        ];

        let filtered = filter_records_by_modified_window(
            records,
            Some(Utc.with_ymd_and_hms(2026, 4, 20, 0, 0, 0).unwrap()),
            Some(Utc.with_ymd_and_hms(2026, 4, 27, 0, 0, 0).unwrap()),
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["InvoiceID"], "in");
    }

    #[test]
    fn parses_xero_json_date_format_for_window_filtering() {
        let records = vec![
            json!({"UpdatedDateUTC": "/Date(1777247999000+0000)/", "InvoiceID": "in"}),
            json!({"UpdatedDateUTC": "/Date(1777248000000+0000)/", "InvoiceID": "out"}),
        ];

        let filtered = filter_records_by_modified_window(
            records,
            Some(Utc.with_ymd_and_hms(2026, 4, 26, 0, 0, 0).unwrap()),
            Some(Utc.with_ymd_and_hms(2026, 4, 27, 0, 0, 0).unwrap()),
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["InvoiceID"], "in");
    }

    #[test]
    fn filters_records_to_half_open_business_date_window() {
        let window = DateWindow::new(
            Utc.with_ymd_and_hms(2026, 4, 20, 0, 0, 0)
                .unwrap()
                .date_naive(),
            Utc.with_ymd_and_hms(2026, 4, 27, 0, 0, 0)
                .unwrap()
                .date_naive(),
        )
        .unwrap();
        let records = vec![
            json!({"Date": "/Date(1776643200000+0000)/", "InvoiceID": "in"}),
            json!({"Date": "/Date(1777248000000+0000)/", "InvoiceID": "out"}),
        ];

        let filtered =
            filter_records_by_business_date_window(&EntityType::Invoices, records, window);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["InvoiceID"], "in");
    }

    #[test]
    fn filters_journals_by_journal_date() {
        let window = DateWindow::new(
            Utc.with_ymd_and_hms(2026, 4, 20, 0, 0, 0)
                .unwrap()
                .date_naive(),
            Utc.with_ymd_and_hms(2026, 4, 27, 0, 0, 0)
                .unwrap()
                .date_naive(),
        )
        .unwrap();
        let records = vec![
            json!({"JournalDate": "/Date(1776816000000+0000)/", "JournalID": "in"}),
            json!({"JournalDate": "/Date(1777248000000+0000)/", "JournalID": "out"}),
        ];

        let filtered =
            filter_records_by_business_date_window(&EntityType::Journals, records, window);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["JournalID"], "in");
    }

    #[test]
    fn business_date_query_uses_where_for_invoice_dates() {
        let window = DateWindow::new(
            Utc.with_ymd_and_hms(2026, 4, 20, 0, 0, 0)
                .unwrap()
                .date_naive(),
            Utc.with_ymd_and_hms(2026, 4, 27, 0, 0, 0)
                .unwrap()
                .date_naive(),
        )
        .unwrap();

        let query = business_date_query(&EntityType::Invoices, window, 3, 100).unwrap();

        assert!(query.contains(&("page", "3".to_owned())));
        assert!(query.contains(&("pageSize", "100".to_owned())));
        assert!(query.iter().any(|(key, value)| {
            *key == "where"
                && value.contains("Date>=DateTime(2026, 4, 20)")
                && value.contains("Date<DateTime(2026, 4, 27)")
        }));
    }

    #[test]
    fn bills_alias_adds_purchase_invoice_filter() {
        let extra = ExtraQuery {
            where_clause: Some("Status==\"AUTHORISED\"".to_owned()),
            ..ExtraQuery::default()
        };

        let query = extra_query_pairs(&extra, &EntityType::Bills);
        let where_clause = query
            .iter()
            .find(|(key, _)| key == "where")
            .map(|(_, value)| value.as_str());

        assert_eq!(
            where_clause,
            Some(r#"Type=="ACCPAY" && (Status=="AUTHORISED")"#)
        );
    }

    #[test]
    fn quote_business_date_query_omits_page_size_param() {
        let window = DateWindow::new(
            Utc.with_ymd_and_hms(2026, 4, 20, 0, 0, 0)
                .unwrap()
                .date_naive(),
            Utc.with_ymd_and_hms(2026, 4, 27, 0, 0, 0)
                .unwrap()
                .date_naive(),
        )
        .unwrap();

        let query = business_date_query(&EntityType::Quotes, window, 2, 100).unwrap();

        assert!(query.contains(&("page", "2".to_owned())));
        assert!(!query.iter().any(|(key, _)| *key == "pageSize"));
    }

    #[test]
    fn business_date_query_uses_date_from_to_for_purchase_orders() {
        let window = DateWindow::new(
            Utc.with_ymd_and_hms(2026, 4, 20, 0, 0, 0)
                .unwrap()
                .date_naive(),
            Utc.with_ymd_and_hms(2026, 4, 27, 0, 0, 0)
                .unwrap()
                .date_naive(),
        )
        .unwrap();

        let query = business_date_query(&EntityType::PurchaseOrders, window, 1, 100).unwrap();

        assert!(query.contains(&("DateFrom", "2026-04-20".to_owned())));
        assert!(query.contains(&("DateTo", "2026-04-26".to_owned())));
    }

    #[test]
    fn business_date_query_omits_page_params_for_non_paginated_endpoint() {
        let window = DateWindow::new(
            Utc.with_ymd_and_hms(2026, 4, 20, 0, 0, 0)
                .unwrap()
                .date_naive(),
            Utc.with_ymd_and_hms(2026, 4, 27, 0, 0, 0)
                .unwrap()
                .date_naive(),
        )
        .unwrap();

        let query = business_date_query(&EntityType::BatchPayments, window, 1, 100).unwrap();

        assert!(query.iter().any(|(key, _)| *key == "where"));
        assert!(!query.iter().any(|(key, _)| *key == "page"));
        assert!(!query.iter().any(|(key, _)| *key == "pageSize"));
    }

    #[test]
    fn items_are_not_treated_as_paginated() {
        assert!(!supports_page_pagination(&EntityType::Items));
    }

    // ---- Reports (WS3) ----

    #[test]
    fn report_default_params_match_spec_shape() {
        // Date-range reports take fromDate + toDate.
        let pnl = resolve_report_params(&EntityType::ReportProfitAndLoss, &[]);
        assert!(pnl.iter().any(|(k, _)| k == "fromDate"));
        assert!(pnl.iter().any(|(k, _)| k == "toDate"));
        // Single as-of-date reports take `date`.
        let bs = resolve_report_params(&EntityType::ReportBalanceSheet, &[]);
        assert_eq!(bs.len(), 1);
        assert_eq!(bs[0].0, "date");
        // BudgetSummary takes date + periods + timeframe.
        let budget = resolve_report_params(&EntityType::ReportBudgetSummary, &[]);
        assert!(budget.iter().any(|(k, _)| k == "periods"));
        assert!(budget.iter().any(|(k, v)| k == "timeframe" && v == "1"));
    }

    #[test]
    fn report_params_override_wins_and_is_sorted() {
        let overrides = vec![
            ("date".to_owned(), "2026-05-31".to_owned()),
            ("contactId".to_owned(), "abc-123".to_owned()),
        ];
        let p = resolve_report_params(&EntityType::ReportBalanceSheet, &overrides);
        // override replaces the default `date`
        let date = p.iter().find(|(k, _)| k == "date").map(|(_, v)| v.as_str());
        assert_eq!(date, Some("2026-05-31"));
        // extra key carried through
        assert!(p.iter().any(|(k, v)| k == "contactId" && v == "abc-123"));
        // sorted by key → deterministic id-synthesis input
        let keys: Vec<&String> = p.iter().map(|(k, _)| k).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "params must be key-sorted");
    }

    #[test]
    fn aged_by_contact_report_requires_contact_id() {
        let params = resolve_report_params(&EntityType::ReportAgedReceivablesByContact, &[]);
        let err = validate_report_params(&EntityType::ReportAgedReceivablesByContact, &params)
            .unwrap_err();

        assert!(err.to_string().contains("contactId"));

        let params = resolve_report_params(
            &EntityType::ReportAgedReceivablesByContact,
            &[(
                "contactId".to_owned(),
                "00000000-0000-0000-0000-000000000000".to_owned(),
            )],
        );
        validate_report_params(&EntityType::ReportAgedReceivablesByContact, &params).unwrap();
    }

    #[test]
    fn wrap_report_record_has_params_run_date_and_report() {
        let params = vec![("date".to_owned(), "2026-05-31".to_owned())];
        let raw = json!({"Reports": [{"ReportName": "BalanceSheet", "Rows": []}]});
        let wrapped = wrap_report_record(&params, raw);
        assert_eq!(wrapped["_params"]["date"], "2026-05-31");
        assert!(wrapped["_run_date"].is_string());
        assert!(wrapped["Report"]["Reports"].is_array());
    }

    // ---- pagination termination predicate (Bug: silent data loss) ----

    #[test]
    fn short_non_final_page_does_not_terminate_page_pagination() {
        // A page with fewer records than page_size is NOT the end of data —
        // the only reliable end signal is an empty page. Pagination MUST
        // continue to the next page.
        let page_size = 100usize;
        let short_page = 37usize; // < page_size, but non-empty
        assert!(short_page < page_size);
        assert!(
            !should_stop_page_pagination(short_page),
            "a short non-empty page must NOT stop pagination"
        );
    }

    #[test]
    fn empty_page_terminates_page_pagination() {
        assert!(
            should_stop_page_pagination(0),
            "an empty page must stop pagination"
        );
    }

    #[test]
    fn full_page_continues_page_pagination() {
        assert!(
            !should_stop_page_pagination(100),
            "a full page must continue pagination"
        );
    }

    #[test]
    fn max_pages_cap_bounds_page_pagination_loop() {
        // Simulate the fetch loop's bounding logic: even if Xero kept
        // returning full (non-terminating) pages forever, the
        // XERO_MAX_PAGES_PER_ENTITY cap must stop it. Records-per-page is
        // chosen so the predicate never signals stop, isolating the cap.
        let cap = 5u32;
        let records_per_page = 100usize; // never triggers should_stop_page_pagination
        let mut pages_fetched = 0u32;
        let mut page = 1u32;
        let mut capped = false;
        loop {
            if page > cap {
                capped = true;
                break;
            }
            pages_fetched += 1;
            if should_stop_page_pagination(records_per_page) {
                break;
            }
            page += 1;
        }
        assert!(capped, "loop must be bounded by the max-pages cap");
        assert_eq!(
            pages_fetched, cap,
            "exactly `cap` pages fetched before stop"
        );
    }

    #[test]
    fn short_non_final_page_does_not_terminate_offset_pagination() {
        // < 100 records on an offset page is NOT the end — keep going so
        // long as the offset advances.
        let short = 42usize;
        let current = 100i64;
        let next = 142i64; // offset advanced
        assert!(
            !should_stop_offset_pagination(short, current, next),
            "a short offset page with advancing offset must NOT stop"
        );
    }

    #[test]
    fn empty_page_terminates_offset_pagination() {
        assert!(
            should_stop_offset_pagination(0, 100, 100),
            "an empty offset page must stop"
        );
    }

    #[test]
    fn non_advancing_offset_terminates_offset_pagination() {
        // Guards against an infinite loop when the offset can't advance.
        assert!(
            should_stop_offset_pagination(100, 500, 500),
            "a non-advancing offset must stop"
        );
        assert!(
            should_stop_offset_pagination(100, 500, 400),
            "a regressing offset must stop"
        );
    }

    // ---- pagination-completeness backstop: PaginationOutcome / TerminationReason ----

    /// Mirror of the page-based loop in `fetch_inner` / `fetch_by_business_date`:
    /// returns the same `PaginationOutcome` for a given (cap, per-page record
    /// counts) without needing live HTTP. `page_record_counts[i]` is the number
    /// of records the i-th fetched page returns.
    fn simulate_page_loop(cap: u32, page_record_counts: &[usize]) -> PaginationOutcome {
        let mut page = 1u32;
        let termination = loop {
            if page > cap {
                break TerminationReason::HitMaxPagesCap;
            }
            let records_on_page = page_record_counts
                .get((page - 1) as usize)
                .copied()
                .unwrap_or(100); // default: full, non-terminating page
            if should_stop_page_pagination(records_on_page) {
                break TerminationReason::EndedOnEmptyPage;
            }
            page += 1;
        };
        PaginationOutcome {
            pages_fetched: page.saturating_sub(1),
            termination,
        }
    }

    /// Mirror of the offset-based loop in `fetch_journals`.
    fn simulate_offset_loop(cap: u32, page_record_counts: &[usize]) -> PaginationOutcome {
        let mut pages = 0u32;
        let mut offset = 0i64;
        let termination = loop {
            if pages >= cap {
                break TerminationReason::HitMaxPagesCap;
            }
            pages += 1;
            let records_on_page = page_record_counts
                .get((pages - 1) as usize)
                .copied()
                .unwrap_or(100);
            // Advance offset on full pages, stall on short ones (test knob).
            let next_offset = if records_on_page >= 100 {
                offset + 100
            } else {
                offset
            };
            let stop_empty = records_on_page == 0;
            let stop_offset =
                !stop_empty && should_stop_offset_pagination(records_on_page, offset, next_offset);
            if stop_empty {
                break TerminationReason::EndedOnEmptyPage;
            }
            if stop_offset {
                break TerminationReason::OffsetNotAdvancing;
            }
            offset = next_offset;
        };
        PaginationOutcome {
            pages_fetched: pages,
            termination,
        }
    }

    #[test]
    fn page_loop_that_hits_cap_reports_hit_max_pages_cap() {
        // Every page is full (never empty), so only the cap can stop it.
        let outcome = simulate_page_loop(3, &[100, 100, 100, 100, 100]);
        assert_eq!(outcome.termination, TerminationReason::HitMaxPagesCap);
        assert!(outcome.termination.is_suspect(), "cap-hit must be suspect");
        assert_eq!(
            outcome.pages_fetched, 3,
            "exactly `cap` pages fetched before the cap fires"
        );
    }

    #[test]
    fn page_loop_that_ends_on_empty_reports_ended_on_empty_page() {
        // Two full pages, then an empty page terminates cleanly, well under cap.
        let outcome = simulate_page_loop(10, &[100, 100, 0]);
        assert_eq!(outcome.termination, TerminationReason::EndedOnEmptyPage);
        assert!(
            !outcome.termination.is_suspect(),
            "a clean empty-page stop is healthy, not suspect"
        );
        assert_eq!(
            outcome.pages_fetched, 2,
            "counts the two data-bearing pages; the terminating empty page is not counted"
        );
    }

    #[test]
    fn offset_loop_that_hits_cap_reports_hit_max_pages_cap() {
        let outcome = simulate_offset_loop(4, &[100, 100, 100, 100, 100]);
        assert_eq!(outcome.termination, TerminationReason::HitMaxPagesCap);
        assert!(outcome.termination.is_suspect());
        assert_eq!(outcome.pages_fetched, 4);
    }

    #[test]
    fn offset_loop_that_ends_on_empty_reports_ended_on_empty_page() {
        let outcome = simulate_offset_loop(10, &[100, 0]);
        assert_eq!(outcome.termination, TerminationReason::EndedOnEmptyPage);
        assert!(!outcome.termination.is_suspect());
        assert_eq!(outcome.pages_fetched, 2);
    }

    #[test]
    fn offset_loop_that_stalls_reports_offset_not_advancing() {
        // A short page that does not advance the offset is a healthy stop.
        let outcome = simulate_offset_loop(10, &[42]);
        assert_eq!(outcome.termination, TerminationReason::OffsetNotAdvancing);
        assert!(!outcome.termination.is_suspect());
        assert_eq!(outcome.pages_fetched, 1);
    }

    #[test]
    fn termination_reason_str_and_suspect_flags() {
        assert_eq!(
            TerminationReason::EndedOnEmptyPage.as_str(),
            "ended_on_empty_page"
        );
        assert_eq!(
            TerminationReason::HitMaxPagesCap.as_str(),
            "hit_max_pages_cap"
        );
        assert_eq!(
            TerminationReason::OffsetNotAdvancing.as_str(),
            "offset_not_advancing"
        );
        assert!(TerminationReason::HitMaxPagesCap.is_suspect());
        assert!(!TerminationReason::EndedOnEmptyPage.is_suspect());
        assert!(!TerminationReason::OffsetNotAdvancing.is_suspect());
    }

    // ---- raw-GCS fetch: RawPage byte fidelity + record_count parsing ----

    /// Build a `RawPage` from a JSON fixture the same way `fetch_raw_with_query`
    /// does — verbatim bytes + a minimal record-count parse — without needing
    /// live HTTP. This exercises exactly the byte-capture + count logic.
    fn raw_page_from_fixture(entity: &EntityType, page: u32, raw_json: &str) -> RawPage {
        let body = bytes::Bytes::copy_from_slice(raw_json.as_bytes());
        let record_count = record_count_from_bytes(entity, &body);
        RawPage {
            page,
            body,
            http_status: 200,
            record_count,
            fetched_at: Utc::now(),
        }
    }

    #[test]
    fn raw_page_holds_exact_bytes_verbatim() {
        // Deliberately irregular whitespace + key order: the body must be kept
        // byte-for-byte, NOT reserialized (which would normalize formatting).
        let raw =
            "{\n  \"Invoices\": [ {\"InvoiceID\":\"a\",\"Total\": 1.50} ],\n\"Foo\":\"bar\"\n}";
        let page = raw_page_from_fixture(&EntityType::Invoices, 1, raw);

        assert_eq!(
            page.body.as_ref(),
            raw.as_bytes(),
            "RawPage.body must hold the exact response bytes verbatim"
        );
        assert_eq!(page.page, 1);
        assert_eq!(page.http_status, 200);
    }

    #[test]
    fn record_count_from_bytes_counts_entity_array_len() {
        let raw = r#"{"Invoices":[{"InvoiceID":"a"},{"InvoiceID":"b"},{"InvoiceID":"c"}]}"#;
        assert_eq!(
            record_count_from_bytes(&EntityType::Invoices, raw.as_bytes()),
            3
        );

        // Empty array → terminal page.
        let empty = r#"{"Invoices":[]}"#;
        assert_eq!(
            record_count_from_bytes(&EntityType::Invoices, empty.as_bytes()),
            0
        );
    }

    #[test]
    fn record_count_from_bytes_zero_for_missing_array_or_bad_json() {
        // Array under a different key than the entity path → 0.
        let other = r#"{"Contacts":[{"ContactID":"x"}]}"#;
        assert_eq!(
            record_count_from_bytes(&EntityType::Invoices, other.as_bytes()),
            0
        );
        // Malformed JSON → 0 (treated as terminal, never panics).
        assert_eq!(
            record_count_from_bytes(&EntityType::Invoices, b"{not json"),
            0
        );
    }

    #[test]
    fn raw_page_record_count_drives_page_stop_predicate() {
        let full = raw_page_from_fixture(
            &EntityType::Invoices,
            1,
            r#"{"Invoices":[{"InvoiceID":"a"}]}"#,
        );
        let empty = raw_page_from_fixture(&EntityType::Invoices, 2, r#"{"Invoices":[]}"#);

        assert_eq!(full.record_count, 1);
        assert!(!should_stop_page_pagination(full.record_count));
        assert_eq!(empty.record_count, 0);
        assert!(should_stop_page_pagination(empty.record_count));
    }

    #[test]
    fn max_journal_number_from_bytes_extracts_largest() {
        let raw = r#"{"Journals":[{"JournalNumber":10},{"JournalNumber":42},{"JournalNumber":7}]}"#;
        assert_eq!(
            max_journal_number_from_bytes(&EntityType::Journals, raw.as_bytes()),
            Some(42)
        );
        // No records / bad json → None (offset stays put → pagination stops).
        assert_eq!(
            max_journal_number_from_bytes(&EntityType::Journals, br#"{"Journals":[]}"#),
            None
        );
        assert_eq!(
            max_journal_number_from_bytes(&EntityType::Journals, b"oops"),
            None
        );
    }
}
