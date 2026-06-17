//! Custom object metadata (the `x-*` map) and `.meta.json` sidecar.
//!
//! Builds the deterministic `x-*` metadata map described in
//! `docs/NEW_ARCHITECTURE_RAW_GCS.md` (lines ~110-127). All values are stored
//! as `String`; the map is a `BTreeMap` so iteration order is deterministic
//! (stable sidecar bytes, stable test assertions).

use std::collections::BTreeMap;

/// Which sync layer produced an object. Tagged into `x-sync-type` so downstream
/// can tell churn pulls apart from sweeps / full reloads / reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncType {
    /// `modified >= now - N days` rolling window.
    #[default]
    Incremental,
    /// Status filter, no modified window (e.g. open AR/AP).
    OpenSweep,
    /// Business-date range reload (tight or wide).
    RollingFull,
    /// Small master-data tables, unfiltered.
    Master,
    /// One-time historical backfill, business-date chunks.
    Backfill,
    /// Point-in-time report snapshot.
    ReportSnapshot,
}

impl SyncType {
    /// Stable lowercase wire value for the `x-sync-type` metadata key.
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncType::Incremental => "incremental",
            SyncType::OpenSweep => "open-sweep",
            SyncType::RollingFull => "rolling-full",
            SyncType::Master => "master",
            SyncType::Backfill => "backfill",
            SyncType::ReportSnapshot => "report-snapshot",
        }
    }
}

/// Deterministically-ordered custom object metadata (`x-*` → value).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectMeta(pub BTreeMap<String, String>);

impl ObjectMeta {
    /// Look up a metadata value by key (test/inspection helper).
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Number of metadata entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the metadata map is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Typed inputs for a per-page object's metadata (non-report path).
///
/// Grouped into a struct to keep [`metadata`] from taking a dozen positional
/// arguments. Optional fields are emitted only when `Some`.
#[derive(Debug, Clone, Default)]
pub struct MetaArgs<'a> {
    pub tenant_id: &'a str,
    pub org_name: &'a str,
    pub endpoint: &'a str,
    pub sync_type: SyncType,
    /// Modified-window lower bound (`incremental`), ISO8601.
    pub modified_after: Option<&'a str>,
    /// Business-date window lower bound (`rolling-full`/`backfill`).
    pub business_from: Option<&'a str>,
    /// Business-date window upper bound.
    pub business_to: Option<&'a str>,
    /// Status / where filter (`open-sweep`).
    pub where_filter: Option<&'a str>,
    pub page: u32,
    pub record_count: i64,
    pub http_status: u16,
    pub run_id: &'a str,
    /// `x-synced-at` ISO8601 timestamp.
    pub synced_at: &'a str,
}

/// Typed inputs for a report snapshot's metadata.
#[derive(Debug, Clone, Default)]
pub struct ReportMetaArgs<'a> {
    pub tenant_id: &'a str,
    pub org_name: &'a str,
    pub endpoint: &'a str,
    /// Report name (e.g. `BalanceSheet`).
    pub report: &'a str,
    /// As-of date for snapshot reports (mutually exclusive with from/to).
    pub report_date: Option<&'a str>,
    /// Period start for period reports.
    pub report_from: Option<&'a str>,
    /// Period end for period reports.
    pub report_to: Option<&'a str>,
    /// Full sorted param signature (reproducible key).
    pub report_params: &'a str,
    pub http_status: u16,
    pub record_count: i64,
    pub run_id: &'a str,
    pub synced_at: &'a str,
}

/// The always-present fields shared by every object's metadata.
struct BaseArgs<'a> {
    tenant_id: &'a str,
    org_name: &'a str,
    endpoint: &'a str,
    sync_type: SyncType,
    http_status: u16,
    record_count: i64,
    run_id: &'a str,
    synced_at: &'a str,
}

/// Common, always-present base keys shared by every object.
fn base_map(b: &BaseArgs<'_>) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("x-vendor".to_string(), "xero".to_string());
    m.insert("x-tenant-id".to_string(), b.tenant_id.to_string());
    m.insert("x-org-name".to_string(), b.org_name.to_string());
    m.insert("x-endpoint".to_string(), b.endpoint.to_string());
    m.insert("x-api-version".to_string(), "2.0".to_string());
    m.insert("x-sync-type".to_string(), b.sync_type.as_str().to_string());
    m.insert("x-record-count".to_string(), b.record_count.to_string());
    m.insert("x-http-status".to_string(), b.http_status.to_string());
    m.insert("x-run-id".to_string(), b.run_id.to_string());
    m.insert("x-synced-at".to_string(), b.synced_at.to_string());
    m
}

/// Build the `x-*` metadata map for a per-page (non-report) object.
pub fn metadata(args: &MetaArgs<'_>) -> ObjectMeta {
    let mut m = base_map(&BaseArgs {
        tenant_id: args.tenant_id,
        org_name: args.org_name,
        endpoint: args.endpoint,
        sync_type: args.sync_type,
        http_status: args.http_status,
        record_count: args.record_count,
        run_id: args.run_id,
        synced_at: args.synced_at,
    });
    m.insert("x-page".to_string(), args.page.to_string());
    if let Some(v) = args.modified_after {
        m.insert("x-modified-after".to_string(), v.to_string());
    }
    if let Some(v) = args.business_from {
        m.insert("x-business-from".to_string(), v.to_string());
    }
    if let Some(v) = args.business_to {
        m.insert("x-business-to".to_string(), v.to_string());
    }
    if let Some(v) = args.where_filter {
        m.insert("x-where".to_string(), v.to_string());
    }
    ObjectMeta(m)
}

/// Build the `x-*` metadata map for a report snapshot object.
pub fn report_metadata(args: &ReportMetaArgs<'_>) -> ObjectMeta {
    let mut m = base_map(&BaseArgs {
        tenant_id: args.tenant_id,
        org_name: args.org_name,
        endpoint: args.endpoint,
        sync_type: SyncType::ReportSnapshot,
        http_status: args.http_status,
        record_count: args.record_count,
        run_id: args.run_id,
        synced_at: args.synced_at,
    });
    m.insert("x-report".to_string(), args.report.to_string());
    if let Some(v) = args.report_date {
        m.insert("x-report-date".to_string(), v.to_string());
    }
    if let Some(v) = args.report_from {
        m.insert("x-report-from".to_string(), v.to_string());
    }
    if let Some(v) = args.report_to {
        m.insert("x-report-to".to_string(), v.to_string());
    }
    m.insert(
        "x-report-params".to_string(),
        args.report_params.to_string(),
    );
    ObjectMeta(m)
}

/// The key of the `.meta.json` sidecar for a given object key.
pub fn sidecar_key(object_key: &str) -> String {
    format!("{object_key}.meta.json")
}

/// Pretty-JSON bytes of the metadata map (the sidecar body).
///
/// Infallible by contract: the map is a `BTreeMap<String, String>`, which
/// `serde_json` can always serialise (no floats, no non-string keys, no
/// non-serialisable types), so the only theoretical error path is unreachable.
pub fn sidecar_bytes(meta: &ObjectMeta) -> Vec<u8> {
    serde_json::to_vec_pretty(&meta.0).expect("BTreeMap<String, String> is always serialisable")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── SyncType ───────────────────────────────────────────────────────────

    #[test]
    fn sync_type_as_str_uses_documented_wire_values() {
        assert_eq!(SyncType::Incremental.as_str(), "incremental");
        assert_eq!(SyncType::OpenSweep.as_str(), "open-sweep");
        assert_eq!(SyncType::RollingFull.as_str(), "rolling-full");
        assert_eq!(SyncType::Master.as_str(), "master");
        assert_eq!(SyncType::Backfill.as_str(), "backfill");
        assert_eq!(SyncType::ReportSnapshot.as_str(), "report-snapshot");
    }

    // ── metadata (page object) ───────────────────────────────────────────────

    fn page_args() -> MetaArgs<'static> {
        MetaArgs {
            tenant_id: "e0c3a1b2",
            org_name: "Aquatiq Australia Pty Ltd",
            endpoint: "invoices",
            sync_type: SyncType::Incremental,
            modified_after: Some("2026-06-14T03:00:00Z"),
            business_from: None,
            business_to: None,
            where_filter: None,
            page: 1,
            record_count: 137,
            http_status: 200,
            run_id: "5f3c9a2e",
            synced_at: "2026-06-17T03:00:01Z",
        }
    }

    #[test]
    fn metadata_includes_all_required_base_keys() {
        let meta = metadata(&page_args());
        assert_eq!(meta.get("x-vendor"), Some("xero"));
        assert_eq!(meta.get("x-tenant-id"), Some("e0c3a1b2"));
        assert_eq!(meta.get("x-org-name"), Some("Aquatiq Australia Pty Ltd"));
        assert_eq!(meta.get("x-endpoint"), Some("invoices"));
        assert_eq!(meta.get("x-api-version"), Some("2.0"));
        assert_eq!(meta.get("x-sync-type"), Some("incremental"));
        assert_eq!(meta.get("x-page"), Some("1"));
        assert_eq!(meta.get("x-record-count"), Some("137"));
        assert_eq!(meta.get("x-http-status"), Some("200"));
        assert_eq!(meta.get("x-run-id"), Some("5f3c9a2e"));
        assert_eq!(meta.get("x-synced-at"), Some("2026-06-17T03:00:01Z"));
    }

    #[test]
    fn metadata_emits_optional_modified_after_when_present() {
        let meta = metadata(&page_args());
        assert_eq!(meta.get("x-modified-after"), Some("2026-06-14T03:00:00Z"));
        assert!(meta.get("x-business-from").is_none());
        assert!(meta.get("x-business-to").is_none());
        assert!(meta.get("x-where").is_none());
    }

    #[test]
    fn metadata_omits_modified_after_when_none() {
        let mut args = page_args();
        args.modified_after = None;
        let meta = metadata(&args);
        assert!(meta.get("x-modified-after").is_none());
    }

    #[test]
    fn metadata_emits_business_window_and_where_for_rolling_and_sweep() {
        let args = MetaArgs {
            sync_type: SyncType::RollingFull,
            modified_after: None,
            business_from: Some("2026-03-19"),
            business_to: Some("2026-06-17"),
            where_filter: Some("Status==\"AUTHORISED\""),
            ..page_args()
        };
        let meta = metadata(&args);
        assert_eq!(meta.get("x-sync-type"), Some("rolling-full"));
        assert_eq!(meta.get("x-business-from"), Some("2026-03-19"));
        assert_eq!(meta.get("x-business-to"), Some("2026-06-17"));
        assert_eq!(meta.get("x-where"), Some("Status==\"AUTHORISED\""));
    }

    #[test]
    fn metadata_ordering_is_deterministic() {
        let a = metadata(&page_args());
        let b = metadata(&page_args());
        let ka: Vec<_> = a.0.keys().collect();
        let kb: Vec<_> = b.0.keys().collect();
        assert_eq!(ka, kb);
        // BTreeMap is sorted — verify a couple of relative orderings.
        assert!(ka.windows(2).all(|w| w[0] <= w[1]));
    }

    // ── report_metadata ──────────────────────────────────────────────────────

    fn report_asof_args() -> ReportMetaArgs<'static> {
        ReportMetaArgs {
            tenant_id: "t",
            org_name: "Org",
            endpoint: "report_balance_sheet",
            report: "BalanceSheet",
            report_date: Some("2026-06-17"),
            report_from: None,
            report_to: None,
            report_params: "date=2026-06-17",
            http_status: 200,
            record_count: 1,
            run_id: "rid",
            synced_at: "2026-06-17T03:00:01Z",
        }
    }

    #[test]
    fn report_metadata_asof_variant() {
        let meta = report_metadata(&report_asof_args());
        assert_eq!(meta.get("x-sync-type"), Some("report-snapshot"));
        assert_eq!(meta.get("x-report"), Some("BalanceSheet"));
        assert_eq!(meta.get("x-report-date"), Some("2026-06-17"));
        assert_eq!(meta.get("x-report-params"), Some("date=2026-06-17"));
        assert!(meta.get("x-report-from").is_none());
        assert!(meta.get("x-report-to").is_none());
        // A report object has no x-page.
        assert!(meta.get("x-page").is_none());
    }

    #[test]
    fn report_metadata_period_variant() {
        let args = ReportMetaArgs {
            report: "ProfitAndLoss",
            endpoint: "report_profit_and_loss",
            report_date: None,
            report_from: Some("2026-06-01"),
            report_to: Some("2026-06-17"),
            report_params: "fromDate=2026-06-01&toDate=2026-06-17",
            ..report_asof_args()
        };
        let meta = report_metadata(&args);
        assert_eq!(meta.get("x-report-from"), Some("2026-06-01"));
        assert_eq!(meta.get("x-report-to"), Some("2026-06-17"));
        assert!(meta.get("x-report-date").is_none());
    }

    // ── sidecar ──────────────────────────────────────────────────────────────

    #[test]
    fn sidecar_key_appends_meta_json() {
        assert_eq!(
            sidecar_key("raw/xero/t/2.0/invoices/2026-06-17/TS_rid_p001.json"),
            "raw/xero/t/2.0/invoices/2026-06-17/TS_rid_p001.json.meta.json"
        );
    }

    #[test]
    fn sidecar_bytes_is_pretty_json_round_trippable() {
        let meta = metadata(&page_args());
        let bytes = sidecar_bytes(&meta);
        let text = String::from_utf8(bytes).expect("utf8");
        // Pretty JSON is indented (multi-line).
        assert!(text.contains('\n'));
        let parsed: BTreeMap<String, String> = serde_json::from_str(&text).expect("parse back");
        assert_eq!(parsed, meta.0);
    }
}
