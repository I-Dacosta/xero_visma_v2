//! `xero-client` — Xero Accounting API REST client.

mod rate_limit;
mod retry;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde_json::Value;
use tracing::debug;
use xero_common::{EntityType, Error, Result};

pub use rate_limit::{
    init_coordinator, NoOpCoordinator, RateLimitCoordinator, RateLimitState,
    RedisRateLimitCoordinator,
};

const BASE_URL: &str = "https://api.xero.com/api.xro/2.0";

/// Hard cap on pages fetched per `fetch*` call. Override via env var
/// `XERO_MAX_PAGES_PER_ENTITY` (parsed lazily on first use, cached).
///
/// Default 5000 ⇒ 500k records per call at pageSize=100; backfill chunks
/// should stay well under this. Cap exists to prevent runaway pagination
/// on a misbehaving endpoint, not to bound legitimate use.
fn max_pages_per_entity() -> u32 {
    use std::sync::OnceLock;
    static CACHED: OnceLock<u32> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("XERO_MAX_PAGES_PER_ENTITY")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(5000)
    })
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
        self.fetch_records_with_query(
            access_token,
            entity,
            modified_after,
            &[
                ("page", page.to_string()),
                ("pageSize", page_size.to_string()),
            ],
            page,
        )
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
                        // Broadcast the pause to other pods via the distributed
                        // coordinator so they back off too. Best-effort.
                        limiter.publish_pause(wait).await;
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

    pub async fn fetch(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        modified_before: Option<DateTime<Utc>>,
        page_size: u32,
    ) -> Result<Vec<Value>> {
        if matches!(entity, EntityType::Journals) {
            let records = self
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
            if records.is_empty() {
                break;
            }
            let page_is_partial = records.len() < page_size as usize;
            all.extend(records);
            if page_is_partial {
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
            let records = self.fetch_journals(access_token, entity, prefilter).await?;
            return Ok(filter_records_by_business_date_window(
                entity, records, window,
            ));
        }

        if !supports_page_pagination(entity) {
            let query = business_date_query(entity, window, 1, page_size)?;
            let records = self
                .fetch_records_with_query(access_token, entity, None, &query, 1)
                .await?;
            return Ok(filter_records_by_business_date_window(
                entity, records, window,
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

            let query = business_date_query(entity, window, page, page_size)?;
            let records = self
                .fetch_records_with_query(access_token, entity, None, &query, page)
                .await?;
            if records.is_empty() {
                break;
            }

            let page_is_partial = records.len() < page_size as usize;
            all.extend(records);
            if page_is_partial {
                break;
            }
            page += 1;
        }

        Ok(filter_records_by_business_date_window(entity, all, window))
    }

    async fn fetch_journals(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
    ) -> Result<Vec<Value>> {
        let mut all = Vec::new();
        let mut offset = 0i64;
        let mut pages = 0u32;
        let cap = max_pages_per_entity();

        loop {
            pages += 1;
            if pages > cap {
                return Err(Error::XeroApi(format!(
                    "{} exceeded max offset-page limit ({cap})",
                    entity.xero_path()
                )));
            }

            let records = self
                .fetch_records_with_query(
                    access_token,
                    entity,
                    modified_after,
                    &[("offset", offset.to_string())],
                    pages,
                )
                .await?;

            if records.is_empty() {
                break;
            }

            let next_offset = max_journal_number(&records).unwrap_or(offset);
            let page_is_partial = records.len() < 100;
            all.extend(records);

            if page_is_partial || next_offset <= offset {
                break;
            }

            offset = next_offset;
        }

        Ok(all)
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
        query.push(("pageSize", page_size.to_string()));
    }
    Ok(query)
}

fn supports_page_pagination(entity: &EntityType) -> bool {
    matches!(
        entity,
        EntityType::BankTransactions
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
}
