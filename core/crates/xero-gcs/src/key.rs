//! Pure object-key builders for the raw → GCS layout.
//!
//! These functions are deterministic and side-effect free: the caller supplies
//! the ISO8601-compact UTC timestamp (`ts`, e.g. `20260617T030001Z`) — the
//! builders never call `Utc::now()` so they stay fully unit-testable.
//!
//! Layout (see `docs/NEW_ARCHITECTURE_RAW_GCS.md`):
//! ```text
//! {prefix}/{tenant}/2.0/{endpoint}/{run_date}/{ts}_{run_id}_p{page:03}.json
//! ```

use chrono::NaiveDate;

/// Xero Accounting API version segment (hard-coded, matches `api.xro/2.0`).
const API_VERSION: &str = "2.0";

/// Sanitize an identifier into a slash-free, lowercased path segment.
///
/// Ported verbatim from `xero-state::bq_sink::sanitize_table_segment`. Maps any
/// character that is not ASCII-alphanumeric to `_` and lowercases the result,
/// so `Reports/ProfitAndLoss` → `reports_profitandloss`. Already-safe
/// snake_case ids (e.g. `bank_transactions`) pass through unchanged.
///
/// The load-bearing guarantee: the output NEVER contains `/`, so a report
/// endpoint cannot create a phantom folder in the object key.
pub fn sanitize_segment(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Build the object key for one raw API page.
///
/// ```text
/// {prefix}/{tenant}/2.0/{sanitize(endpoint)}/{run_date}/{ts}_{sanitize(run_id_short)}_p{page:03}.json
/// ```
pub fn object_key(
    prefix: &str,
    tenant: &str,
    endpoint: &str,
    run_date: NaiveDate,
    ts: &str,
    run_id_short: &str,
    page: u32,
) -> String {
    format!(
        "{prefix}/{tenant}/{api}/{endpoint}/{date}/{ts}_{run_id}_p{page:03}.json",
        prefix = prefix,
        tenant = tenant,
        api = API_VERSION,
        endpoint = sanitize_segment(endpoint),
        date = run_date.format("%Y-%m-%d"),
        ts = ts,
        run_id = sanitize_segment(run_id_short),
        page = page,
    )
}

/// Build the object key for a single report snapshot.
///
/// Same prefix/tenant/api/endpoint/date structure as [`object_key`], but the
/// filename ends with the (sanitized) business-period key instead of `_pNNN`:
///
/// ```text
/// {prefix}/{tenant}/2.0/{sanitize(endpoint)}/{run_date}/{ts}_{run_id_short}__{sanitize(period_key)}.json
/// ```
pub fn report_object_key(
    prefix: &str,
    tenant: &str,
    endpoint: &str,
    run_date: NaiveDate,
    ts: &str,
    run_id_short: &str,
    period_key: &str,
) -> String {
    format!(
        "{prefix}/{tenant}/{api}/{endpoint}/{date}/{ts}_{run_id}__{period}.json",
        prefix = prefix,
        tenant = tenant,
        api = API_VERSION,
        endpoint = sanitize_segment(endpoint),
        date = run_date.format("%Y-%m-%d"),
        ts = ts,
        run_id = sanitize_segment(run_id_short),
        period = sanitize_segment(period_key),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 17).expect("valid date")
    }

    // ── sanitize_segment (ported verbatim from bq_sink tests) ──────────────

    #[test]
    fn sanitize_strips_slashes_for_report_paths() {
        // The load-bearing guarantee: the output must NEVER contain a slash.
        assert_eq!(
            sanitize_segment("Reports/ProfitAndLoss"),
            "reports_profitandloss"
        );
        assert!(!sanitize_segment("Reports/ProfitAndLoss").contains('/'));
        assert!(!sanitize_segment("Reports/AgedReceivablesByContact").contains('/'));
        assert!(!sanitize_segment("Reports/BalanceSheet").contains('/'));
        // Already-safe snake_case ids pass through unchanged.
        assert_eq!(sanitize_segment("bank_transactions"), "bank_transactions");
        assert_eq!(
            sanitize_segment("report_profit_and_loss"),
            "report_profit_and_loss"
        );
    }

    #[test]
    fn sanitize_lowercases_and_replaces_non_alphanumeric() {
        assert_eq!(sanitize_segment("Invoices"), "invoices");
        assert_eq!(sanitize_segment("Tax Rates"), "tax_rates");
        assert_eq!(sanitize_segment("a.b-c"), "a_b_c");
    }

    #[test]
    fn sanitize_maps_non_ascii_alphanumeric_to_underscore() {
        // Non-ASCII letters are not ASCII-alphanumeric → mapped to '_'.
        assert_eq!(sanitize_segment("café"), "caf_");
        assert_eq!(sanitize_segment("Ærø"), "_r_");
    }

    #[test]
    fn sanitize_output_never_contains_slash() {
        for input in [
            "Reports/ProfitAndLoss",
            "a/b/c/d",
            "///",
            "Reports/Aged/By/Contact",
        ] {
            assert!(
                !sanitize_segment(input).contains('/'),
                "slash leaked for input {input:?}"
            );
        }
    }

    // ── object_key ─────────────────────────────────────────────────────────

    #[test]
    fn object_key_matches_documented_layout() {
        let key = object_key(
            "raw/xero",
            "e0c3a1b2",
            "invoices",
            date(),
            "20260617T030001Z",
            "5f3c9a",
            1,
        );
        assert_eq!(
            key,
            "raw/xero/e0c3a1b2/2.0/invoices/2026-06-17/20260617T030001Z_5f3c9a_p001.json"
        );
    }

    #[test]
    fn object_key_zero_pads_page_to_three_digits() {
        let key = object_key("raw/xero", "t", "invoices", date(), "TS", "rid", 42);
        assert!(key.ends_with("TS_rid_p042.json"), "got {key}");

        let key = object_key("raw/xero", "t", "invoices", date(), "TS", "rid", 7);
        assert!(key.ends_with("TS_rid_p007.json"), "got {key}");

        let key = object_key("raw/xero", "t", "invoices", date(), "TS", "rid", 123);
        assert!(key.ends_with("TS_rid_p123.json"), "got {key}");
    }

    #[test]
    fn object_key_sanitizes_report_endpoint_into_single_segment() {
        let key = object_key(
            "raw/xero",
            "t",
            "Reports/ProfitAndLoss",
            date(),
            "TS",
            "rid",
            1,
        );
        // Endpoint slash must NOT introduce an extra path segment.
        assert!(key.contains("/reports_profitandloss/"), "got {key}");
        assert_eq!(
            key,
            "raw/xero/t/2.0/reports_profitandloss/2026-06-17/TS_rid_p001.json"
        );
    }

    #[test]
    fn object_key_run_id_short_is_sanitized() {
        let key = object_key("raw/xero", "t", "invoices", date(), "TS", "5F3C-9A", 1);
        assert!(key.ends_with("TS_5f3c_9a_p001.json"), "got {key}");
    }

    #[test]
    fn object_key_contains_api_version_segment() {
        let key = object_key("raw/xero", "t", "invoices", date(), "TS", "rid", 1);
        assert!(key.contains("/2.0/"), "missing api version: {key}");
    }

    // ── report_object_key ──────────────────────────────────────────────────

    #[test]
    fn report_object_key_uses_double_underscore_period_suffix() {
        let key = report_object_key(
            "raw/xero",
            "t",
            "report_balance_sheet",
            date(),
            "20260617T030001Z",
            "5f3c9a",
            "asof-2026-06-17",
        );
        assert_eq!(
            key,
            "raw/xero/t/2.0/report_balance_sheet/2026-06-17/20260617T030001Z_5f3c9a__asof_2026_06_17.json"
        );
    }

    #[test]
    fn report_object_key_has_no_page_suffix() {
        let key = report_object_key(
            "raw/xero",
            "t",
            "report_profit_and_loss",
            date(),
            "TS",
            "rid",
            "2026-06-01_2026-06-17",
        );
        assert!(!key.contains("_p0"), "report key must not have page: {key}");
        assert!(key.ends_with("__2026_06_01_2026_06_17.json"), "got {key}");
    }

    #[test]
    fn report_object_key_sanitizes_run_id_short() {
        // A UUID-style run_id (hyphens + upper-case) must be flattened and
        // lowercased exactly like object_key does — every filename segment is
        // slash-free and lowercased.
        let key = report_object_key(
            "raw/xero",
            "t",
            "report_balance_sheet",
            date(),
            "TS",
            "5F3C-9A",
            "asof-2026-06-17",
        );
        assert!(
            key.contains("TS_5f3c_9a__asof_2026_06_17.json"),
            "run_id must be sanitized: {key}"
        );
        assert!(!key.contains("5F3C-9A"), "raw run_id leaked: {key}");
    }

    #[test]
    fn report_object_key_sanitizes_period_key() {
        let key = report_object_key(
            "raw/xero",
            "t",
            "report_balance_sheet",
            date(),
            "TS",
            "rid",
            "asof-2026/06/17",
        );
        // Period key slashes must be flattened — no extra path segments.
        assert!(key.ends_with("__asof_2026_06_17.json"), "got {key}");
        // Slash count is fixed regardless of the period key's own slashes:
        // prefix(raw/xero=1) + tenant + api + endpoint + run_date + filename = 6.
        assert_eq!(key.matches('/').count(), 6, "unexpected segments: {key}");
    }
}
