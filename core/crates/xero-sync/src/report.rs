//! Pure `Reports/* → fetch params` mapping for the `Reports { as_of }` mode.
//!
//! Side-effect free (no I/O, no implicit `Utc::now()`): the caller supplies the
//! `as_of` date, so the period derivation and the object-key `period_key` are
//! fully unit-testable.

use chrono::{Datelike, NaiveDate};
use xero_common::EntityType;

/// The resolved per-report fetch + metadata spec produced by [`report_params`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportSpec {
    /// `extra` query overrides forwarded to `fetch_report_raw` (date params).
    pub extra: Vec<(String, String)>,
    /// Filename business-period key (e.g. `asof-2026-06-17`).
    pub period_key: String,
    /// `x-report-date` metadata (as-of reports).
    pub report_date: Option<String>,
    /// `x-report-from` metadata (period reports).
    pub report_from: Option<String>,
    /// `x-report-to` metadata (period reports).
    pub report_to: Option<String>,
}

impl ReportSpec {
    /// Full sorted param signature for `x-report-params` (reproducible key).
    pub fn params_signature(&self) -> String {
        let mut pairs: Vec<String> = self.extra.iter().map(|(k, v)| format!("{k}={v}")).collect();
        pairs.sort();
        pairs.join("&")
    }
}

/// Derive a report's `period_key` (used in the object filename) + the
/// `extra` query overrides that pin the report to `as_of`.
///
/// `as-of` reports get `date=<as_of>` and `period_key = asof-<as_of>`.
/// `period` reports (P&L / BankSummary) get `fromDate=<month start>` and
/// `toDate=<as_of>` (month-to-date) and `period_key = <from>_<to>`.
pub fn report_params(entity: &EntityType, as_of: NaiveDate) -> ReportSpec {
    let as_of_str = as_of.format("%Y-%m-%d").to_string();
    match entity {
        EntityType::ReportProfitAndLoss | EntityType::ReportBankSummary => {
            let from = as_of.with_day(1).unwrap_or(as_of);
            let from_str = from.format("%Y-%m-%d").to_string();
            ReportSpec {
                extra: vec![
                    ("fromDate".to_owned(), from_str.clone()),
                    ("toDate".to_owned(), as_of_str.clone()),
                ],
                period_key: format!("{from_str}_{as_of_str}"),
                report_date: None,
                report_from: Some(from_str),
                report_to: Some(as_of_str),
            }
        }
        _ => ReportSpec {
            extra: vec![("date".to_owned(), as_of_str.clone())],
            period_key: format!("asof-{as_of_str}"),
            report_date: Some(as_of_str),
            report_from: None,
            report_to: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 17).unwrap()
    }

    #[test]
    fn report_params_asof_for_balance_sheet() {
        let spec = report_params(&EntityType::ReportBalanceSheet, run_date());
        assert_eq!(spec.period_key, "asof-2026-06-17");
        assert_eq!(spec.report_date.as_deref(), Some("2026-06-17"));
        assert!(spec.report_from.is_none());
        assert_eq!(
            spec.extra,
            vec![("date".to_owned(), "2026-06-17".to_owned())]
        );
    }

    #[test]
    fn report_params_period_for_profit_and_loss_is_month_to_date() {
        let spec = report_params(&EntityType::ReportProfitAndLoss, run_date());
        assert_eq!(spec.report_from.as_deref(), Some("2026-06-01"));
        assert_eq!(spec.report_to.as_deref(), Some("2026-06-17"));
        assert!(spec.report_date.is_none());
        assert_eq!(spec.period_key, "2026-06-01_2026-06-17");
        assert_eq!(
            spec.extra,
            vec![
                ("fromDate".to_owned(), "2026-06-01".to_owned()),
                ("toDate".to_owned(), "2026-06-17".to_owned()),
            ]
        );
    }

    #[test]
    fn report_params_signature_is_sorted() {
        let spec = report_params(&EntityType::ReportProfitAndLoss, run_date());
        // fromDate sorts before toDate.
        assert_eq!(
            spec.params_signature(),
            "fromDate=2026-06-01&toDate=2026-06-17"
        );
    }
}
