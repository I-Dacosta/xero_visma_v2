//! Sync modes and the pure `SyncMode → fetch params` mapping.
//!
//! Everything here is side-effect free (no I/O, no `Utc::now()` inside the
//! mappers — the caller supplies `now`) so the window math and the
//! mode→params derivation are fully unit-testable.

use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use xero_client::ExtraQuery;
use xero_gcs::SyncType;

/// The sync layer this run executes. Stateless — each variant fully describes
/// the window/filter for the run; nothing is read from or written to a
/// watermark store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMode {
    /// `UpdatedDateUTC >= now - window_days`. No extra filter.
    Incremental,
    /// Status / `where=` filter, no modified window (open AR/AP sweep).
    OpenSweep { where_clause: String },
    /// Business-date `Date` window covering the last `days` days.
    RollingFull { days: i64 },
    /// Master data: no filter at all (small tables, full pull).
    Master,
    /// One-time historical backfill over an explicit business-date range.
    Backfill { from: NaiveDate, to: NaiveDate },
    /// Point-in-time report snapshots as of `as_of`.
    Reports { as_of: NaiveDate },
}

impl SyncMode {
    /// The `x-sync-type` tag persisted in object metadata + manifest `mode`.
    pub fn sync_type(&self) -> SyncType {
        match self {
            SyncMode::Incremental => SyncType::Incremental,
            SyncMode::OpenSweep { .. } => SyncType::OpenSweep,
            SyncMode::RollingFull { .. } => SyncType::RollingFull,
            SyncMode::Master => SyncType::Master,
            SyncMode::Backfill { .. } => SyncType::Backfill,
            SyncMode::Reports { .. } => SyncType::ReportSnapshot,
        }
    }

    /// Stable lowercase label for the run manifest `mode` field.
    pub fn label(&self) -> &'static str {
        self.sync_type().as_str()
    }

    /// `window_days` recorded in the manifest, when the mode has a window.
    pub fn window_days(&self) -> Option<i64> {
        match self {
            SyncMode::RollingFull { days } => Some(*days),
            SyncMode::Backfill { from, to } => Some((*to - *from).num_days()),
            _ => None,
        }
    }
}

/// The fetch parameters derived from a [`SyncMode`] for one (non-report)
/// entity: the `modified_after` lower bound, the `ExtraQuery` filter, and the
/// human-readable window strings that flow into object metadata.
#[derive(Debug, Clone, Default)]
pub struct FetchParams {
    /// Modified-window lower bound (only set for [`SyncMode::Incremental`]).
    pub modified_after: Option<DateTime<Utc>>,
    /// Pass-through query knobs (where / business-date filter).
    pub extras: ExtraQuery,
    /// `x-modified-after` metadata value (ISO8601), when present.
    pub modified_after_str: Option<String>,
    /// `x-business-from` metadata value (`YYYY-MM-DD`), when present.
    pub business_from: Option<String>,
    /// `x-business-to` metadata value (`YYYY-MM-DD`), when present.
    pub business_to: Option<String>,
    /// `x-where` metadata value, when present.
    pub where_filter: Option<String>,
}

/// Compact ISO8601 UTC timestamp used in object filenames, e.g.
/// `20260617T030001Z`. Computed once per run by the caller and threaded down.
pub fn compact_ts(now: DateTime<Utc>) -> String {
    now.format("%Y%m%dT%H%M%SZ").to_string()
}

/// The lower bound for an incremental run: `now - window_days`.
pub fn incremental_modified_after(now: DateTime<Utc>, window_days: i64) -> DateTime<Utc> {
    now - Duration::days(window_days)
}

/// The business-date window `[run_date - (days-1) .. run_date]` (inclusive),
/// expressed as Xero's `Date` filter bounds. A `days <= 0` collapses to a
/// single day (just `run_date`).
pub fn rolling_window(run_date: NaiveDate, days: i64) -> (NaiveDate, NaiveDate) {
    let span = days.max(1) - 1;
    (run_date - Duration::days(span), run_date)
}

/// Build a raw Xero `where=` clause restricting `Date` to a half-open business
/// window `[from, to]` (inclusive of both ends via `>=` / `<=`).
pub fn business_date_where(from: NaiveDate, to: NaiveDate) -> String {
    format!(
        "Date >= DateTime({y1},{m1},{d1}) && Date <= DateTime({y2},{m2},{d2})",
        y1 = from.year(),
        m1 = from.month(),
        d1 = from.day(),
        y2 = to.year(),
        m2 = to.month(),
        d2 = to.day(),
    )
}

/// Map a [`SyncMode`] + `now`/`run_date` to the concrete fetch params for a
/// NON-report entity. Reports are dispatched separately (see `lib.rs`), so
/// passing a report mode here yields the empty (master-like) params.
pub fn fetch_params(
    mode: &SyncMode,
    now: DateTime<Utc>,
    run_date: NaiveDate,
    window_days: i64,
) -> FetchParams {
    match mode {
        SyncMode::Incremental => {
            let ma = incremental_modified_after(now, window_days);
            FetchParams {
                modified_after: Some(ma),
                modified_after_str: Some(ma.to_rfc3339()),
                ..FetchParams::default()
            }
        }
        SyncMode::OpenSweep { where_clause } => FetchParams {
            extras: ExtraQuery {
                where_clause: Some(where_clause.clone()),
                ..ExtraQuery::default()
            },
            where_filter: Some(where_clause.clone()),
            ..FetchParams::default()
        },
        SyncMode::RollingFull { days } => {
            let (from, to) = rolling_window(run_date, *days);
            params_for_business_window(from, to)
        }
        SyncMode::Backfill { from, to } => params_for_business_window(*from, *to),
        // Master + (defensively) Reports: no filter, no window.
        SyncMode::Master | SyncMode::Reports { .. } => FetchParams::default(),
    }
}

/// Shared builder for the business-date window modes (rolling-full / backfill).
fn params_for_business_window(from: NaiveDate, to: NaiveDate) -> FetchParams {
    let where_clause = business_date_where(from, to);
    FetchParams {
        extras: ExtraQuery {
            where_clause: Some(where_clause),
            ..ExtraQuery::default()
        },
        business_from: Some(from.format("%Y-%m-%d").to_string()),
        business_to: Some(to.format("%Y-%m-%d").to_string()),
        ..FetchParams::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 17, 3, 0, 1).unwrap()
    }

    fn run_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 17).unwrap()
    }

    // ── compact_ts ───────────────────────────────────────────────────────────

    #[test]
    fn compact_ts_is_iso8601_compact_utc() {
        assert_eq!(compact_ts(now()), "20260617T030001Z");
    }

    // ── window math ──────────────────────────────────────────────────────────

    #[test]
    fn incremental_subtracts_window_days() {
        let ma = incremental_modified_after(now(), 3);
        assert_eq!(ma, Utc.with_ymd_and_hms(2026, 6, 14, 3, 0, 1).unwrap());
    }

    #[test]
    fn rolling_window_is_inclusive_of_both_ends() {
        let (from, to) = rolling_window(run_date(), 30);
        assert_eq!(to, run_date());
        // 30-day inclusive window: run_date - 29.
        assert_eq!(from, NaiveDate::from_ymd_opt(2026, 5, 19).unwrap());
    }

    #[test]
    fn rolling_window_collapses_to_single_day_for_nonpositive_days() {
        let (from, to) = rolling_window(run_date(), 0);
        assert_eq!(from, to);
        let (from, to) = rolling_window(run_date(), 1);
        assert_eq!(from, to);
    }

    #[test]
    fn business_date_where_drops_leading_zeros() {
        let from = NaiveDate::from_ymd_opt(2026, 3, 5).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 6, 17).unwrap();
        assert_eq!(
            business_date_where(from, to),
            "Date >= DateTime(2026,3,5) && Date <= DateTime(2026,6,17)"
        );
    }

    // ── SyncMode → FetchParams ───────────────────────────────────────────────

    #[test]
    fn incremental_sets_modified_after_only() {
        let p = fetch_params(&SyncMode::Incremental, now(), run_date(), 3);
        assert_eq!(
            p.modified_after,
            Some(Utc.with_ymd_and_hms(2026, 6, 14, 3, 0, 1).unwrap())
        );
        assert!(p.extras.where_clause.is_none());
        assert!(p.modified_after_str.is_some());
        assert!(p.business_from.is_none());
        assert!(p.where_filter.is_none());
    }

    #[test]
    fn open_sweep_sets_where_no_modified_window() {
        let p = fetch_params(
            &SyncMode::OpenSweep {
                where_clause: "Status==\"AUTHORISED\"".to_owned(),
            },
            now(),
            run_date(),
            3,
        );
        assert!(p.modified_after.is_none());
        assert_eq!(
            p.extras.where_clause.as_deref(),
            Some("Status==\"AUTHORISED\"")
        );
        assert_eq!(p.where_filter.as_deref(), Some("Status==\"AUTHORISED\""));
    }

    #[test]
    fn rolling_full_sets_business_window_via_where() {
        let p = fetch_params(&SyncMode::RollingFull { days: 30 }, now(), run_date(), 3);
        assert!(p.modified_after.is_none());
        assert_eq!(p.business_from.as_deref(), Some("2026-05-19"));
        assert_eq!(p.business_to.as_deref(), Some("2026-06-17"));
        assert!(p
            .extras
            .where_clause
            .as_deref()
            .unwrap()
            .contains("Date >="));
    }

    #[test]
    fn backfill_sets_explicit_business_window() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 6, 17).unwrap();
        let p = fetch_params(&SyncMode::Backfill { from, to }, now(), run_date(), 3);
        assert_eq!(p.business_from.as_deref(), Some("2020-01-01"));
        assert_eq!(p.business_to.as_deref(), Some("2026-06-17"));
        assert!(p.extras.where_clause.is_some());
    }

    #[test]
    fn master_sets_no_filter() {
        let p = fetch_params(&SyncMode::Master, now(), run_date(), 3);
        assert!(p.modified_after.is_none());
        assert!(p.extras.where_clause.is_none());
        assert!(p.modified_after_str.is_none());
        assert!(p.business_from.is_none());
        assert!(p.business_to.is_none());
        assert!(p.where_filter.is_none());
        assert!(p.extras.extra.is_empty());
    }

    // ── sync_type / label / window_days ──────────────────────────────────────

    #[test]
    fn sync_type_maps_each_mode() {
        assert_eq!(SyncMode::Incremental.sync_type(), SyncType::Incremental);
        assert_eq!(
            SyncMode::OpenSweep {
                where_clause: "x".into()
            }
            .sync_type(),
            SyncType::OpenSweep
        );
        assert_eq!(
            SyncMode::RollingFull { days: 30 }.sync_type(),
            SyncType::RollingFull
        );
        assert_eq!(SyncMode::Master.sync_type(), SyncType::Master);
        assert_eq!(
            SyncMode::Backfill {
                from: run_date(),
                to: run_date(),
            }
            .sync_type(),
            SyncType::Backfill
        );
        assert_eq!(
            SyncMode::Reports { as_of: run_date() }.sync_type(),
            SyncType::ReportSnapshot
        );
    }

    #[test]
    fn window_days_only_for_windowed_modes() {
        assert_eq!(SyncMode::Incremental.window_days(), None);
        assert_eq!(SyncMode::Master.window_days(), None);
        assert_eq!(SyncMode::RollingFull { days: 90 }.window_days(), Some(90));
        let from = NaiveDate::from_ymd_opt(2026, 5, 18).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 6, 17).unwrap();
        assert_eq!(SyncMode::Backfill { from, to }.window_days(), Some(30));
    }
}
