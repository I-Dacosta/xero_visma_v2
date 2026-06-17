//! Pure flag → run-parameter resolution for the `sync` subcommand.
//!
//! Everything here is side-effect free (no I/O): flags + dates in, resolved
//! [`SyncMode`] / entity / tenant / backfill-chunk values out. Keeping it pure
//! makes the precedence rules and window math fully unit-testable, and keeps
//! `sync.rs` focused on orchestration.

use anyhow::{bail, Context};
use chrono::{Days, Months, NaiveDate};

use xero_common::{EntityType, SyncConfig};
use xero_sync::SyncMode;

use crate::sync::SyncArgs;

/// Resolve the tenant set: explicit CSV (validated against config) or all.
pub fn resolve_tenants(requested: &[String], configured: &[String]) -> anyhow::Result<Vec<String>> {
    if requested.is_empty() {
        if configured.is_empty() {
            bail!("no custom-connection tenants configured (set XERO_ORG_N_* / XERO_CC_*)");
        }
        return Ok(configured.to_vec());
    }
    for tenant in requested {
        if !configured.contains(tenant) {
            bail!("requested tenant not configured: {tenant}");
        }
    }
    Ok(requested.to_vec())
}

/// Map the flags to a [`SyncMode`] plus the effective *anchor date* the run
/// should use. For explicit `--business-from/to` rolling-full, the anchor is
/// `business_to` so the window lands exactly on `[from, to]` (since
/// `RollingFull` anchors its upper bound at the run date); every other mode
/// uses the real `run_date`.
///
/// Backfill is handled by the caller (it expands into multiple runs), so the
/// `--backfill` flag is not consulted here.
pub fn resolve_mode(
    args: &SyncArgs,
    run_date: NaiveDate,
    sync_cfg: &SyncConfig,
) -> anyhow::Result<(SyncMode, NaiveDate)> {
    if args.reports {
        return Ok((
            SyncMode::Reports {
                as_of: args.as_of.unwrap_or(run_date),
            },
            run_date,
        ));
    }
    if let Some(where_clause) = args.where_clause.clone() {
        return Ok((SyncMode::OpenSweep { where_clause }, run_date));
    }
    if args.business_from.is_some() || args.business_to.is_some() {
        let from = args
            .business_from
            .context("--business-to requires --business-from")?;
        let to = args.business_to.unwrap_or(run_date);
        if to < from {
            bail!("--business-to ({to}) precedes --business-from ({from})");
        }
        let days = (to - from).num_days() + 1;
        // Anchor the rolling window's upper bound at `to`.
        return Ok((SyncMode::RollingFull { days }, to));
    }
    if args.full_with_window {
        return Ok((
            SyncMode::RollingFull {
                days: sync_cfg.rolling_full_tight_days,
            },
            run_date,
        ));
    }
    if args.full {
        return Ok((SyncMode::Master, run_date));
    }
    if args.no_window {
        // Unfiltered open-sweep: no modified window, no where clause.
        return Ok((
            SyncMode::OpenSweep {
                where_clause: String::new(),
            },
            run_date,
        ));
    }
    // Default: incremental. `--window-days` overrides the config default via the
    // env the SyncJob reads; here it is informational only.
    let _ = args.window_days;
    Ok((SyncMode::Incremental, run_date))
}

/// Selector for which default entity set applies, given the resolved mode.
pub struct ModeScope<'a>(pub &'a SyncMode);
/// Backfill always covers the full entity set.
pub struct BackfillScope;

pub trait EntityScope {
    fn default_entities(&self) -> Vec<EntityType>;
    /// Whether report entities are permitted in this scope.
    fn allows_reports(&self) -> bool;
}

impl EntityScope for ModeScope<'_> {
    fn default_entities(&self) -> Vec<EntityType> {
        match self.0 {
            SyncMode::Reports { .. } => EntityType::reports_default().to_vec(),
            SyncMode::Master => EntityType::master_data().to_vec(),
            SyncMode::OpenSweep { .. } => EntityType::open_status().to_vec(),
            SyncMode::RollingFull { .. } => transactional_entities(),
            // Incremental covers the whole accounting set (already report-free).
            SyncMode::Incremental | SyncMode::Backfill { .. } => EntityType::all().to_vec(),
        }
    }

    fn allows_reports(&self) -> bool {
        matches!(self.0, SyncMode::Reports { .. })
    }
}

impl EntityScope for BackfillScope {
    fn default_entities(&self) -> Vec<EntityType> {
        EntityType::all().to_vec()
    }
    fn allows_reports(&self) -> bool {
        false
    }
}

/// Resolve the entity set: explicit `--entity` CSV (validated against the
/// scope's report policy) or the scope default.
pub fn resolve_entities(
    args: &SyncArgs,
    scope: impl EntityScope,
) -> anyhow::Result<Vec<EntityType>> {
    if args.entity.is_empty() {
        return Ok(scope.default_entities());
    }
    if !scope.allows_reports() {
        if let Some(report) = args.entity.iter().find(|e| e.is_report()) {
            bail!(
                "report entity {} is only valid with --reports",
                report.as_str()
            );
        }
    } else if let Some(non_report) = args.entity.iter().find(|e| !e.is_report()) {
        bail!(
            "--reports only accepts report entities; got {}",
            non_report.as_str()
        );
    }
    Ok(args.entity.clone())
}

/// Transactional entities = the full accounting set minus master data.
fn transactional_entities() -> Vec<EntityType> {
    EntityType::all()
        .iter()
        .filter(|e| !e.is_master())
        .cloned()
        .collect()
}

/// Split `[from, to]` (inclusive) into consecutive `[chunk_from, chunk_to]`
/// windows of `months` months each, oldest first.
pub fn backfill_chunks(from: NaiveDate, to: NaiveDate, months: u32) -> Vec<(NaiveDate, NaiveDate)> {
    let months = months.max(1);
    let mut chunks = Vec::new();
    let mut cursor = from;
    while cursor <= to {
        // Next chunk start = cursor + months; chunk end = day before that, capped at `to`.
        let next_start = cursor
            .checked_add_months(Months::new(months))
            .unwrap_or(NaiveDate::MAX);
        let chunk_end = next_start
            .checked_sub_days(Days::new(1))
            .unwrap_or(next_start)
            .min(to);
        chunks.push((cursor, chunk_end));
        cursor = next_start;
    }
    chunks
}

/// Parse a `YYYY-MM-DD:YYYY-MM-DD` range.
pub fn parse_range(spec: &str) -> anyhow::Result<(NaiveDate, NaiveDate)> {
    let (from, to) = spec
        .split_once(':')
        .context("--backfill must be FROM:TO (YYYY-MM-DD:YYYY-MM-DD)")?;
    let from: NaiveDate = from
        .trim()
        .parse()
        .with_context(|| format!("invalid backfill FROM date: {from}"))?;
    let to: NaiveDate = to
        .trim()
        .parse()
        .with_context(|| format!("invalid backfill TO date: {to}"))?;
    if to < from {
        bail!("backfill TO ({to}) precedes FROM ({from})");
    }
    Ok((from, to))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::test_support::args_with;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).expect("valid date")
    }

    fn cfg() -> SyncConfig {
        SyncConfig {
            window_days: 3,
            rolling_full_tight_days: 30,
            rolling_full_wide_days: 90,
            max_concurrent: 6,
        }
    }

    // ── tenants ──────────────────────────────────────────────────────────────

    #[test]
    fn resolve_tenants_defaults_to_all_configured() {
        let configured = vec!["a".to_string(), "b".to_string()];
        assert_eq!(
            resolve_tenants(&[], &configured).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn resolve_tenants_rejects_unconfigured_request() {
        let configured = vec!["a".to_string()];
        assert!(resolve_tenants(&["z".to_string()], &configured).is_err());
    }

    #[test]
    fn resolve_tenants_errors_when_none_configured() {
        assert!(resolve_tenants(&[], &[]).is_err());
    }

    // ── parse_range / backfill_chunks ──────────────────────────────────────────

    #[test]
    fn parse_range_splits_from_to() {
        let (from, to) = parse_range("2020-01-01:2026-06-17").unwrap();
        assert_eq!(from, d(2020, 1, 1));
        assert_eq!(to, d(2026, 6, 17));
    }

    #[test]
    fn parse_range_rejects_reversed_and_malformed() {
        assert!(parse_range("2026-06-17:2020-01-01").is_err());
        assert!(parse_range("2020-01-01").is_err());
        assert!(parse_range("not-a-date:2020-01-01").is_err());
    }

    #[test]
    fn backfill_chunks_are_contiguous_and_inclusive() {
        let chunks = backfill_chunks(d(2026, 1, 1), d(2026, 3, 31), 1);
        assert_eq!(
            chunks,
            vec![
                (d(2026, 1, 1), d(2026, 1, 31)),
                (d(2026, 2, 1), d(2026, 2, 28)),
                (d(2026, 3, 1), d(2026, 3, 31)),
            ]
        );
    }

    #[test]
    fn backfill_chunks_caps_final_chunk_at_to() {
        // `to` falls inside the first month-chunk, so there is exactly one chunk
        // capped at `to` (never extending to the natural chunk end of 02-14).
        let chunks = backfill_chunks(d(2026, 1, 15), d(2026, 2, 10), 1);
        assert_eq!(chunks, vec![(d(2026, 1, 15), d(2026, 2, 10))]);
    }

    #[test]
    fn backfill_chunks_single_day_range() {
        let chunks = backfill_chunks(d(2026, 6, 17), d(2026, 6, 17), 1);
        assert_eq!(chunks, vec![(d(2026, 6, 17), d(2026, 6, 17))]);
    }

    #[test]
    fn backfill_chunks_multi_month_size() {
        let chunks = backfill_chunks(d(2026, 1, 1), d(2026, 6, 30), 3);
        assert_eq!(
            chunks,
            vec![
                (d(2026, 1, 1), d(2026, 3, 31)),
                (d(2026, 4, 1), d(2026, 6, 30)),
            ]
        );
    }

    #[test]
    fn backfill_chunks_zero_months_clamps_to_one() {
        let chunks = backfill_chunks(d(2026, 1, 1), d(2026, 2, 28), 0);
        assert_eq!(chunks.first().unwrap(), &(d(2026, 1, 1), d(2026, 1, 31)));
    }

    // ── transactional / scopes ──────────────────────────────────────────────────

    #[test]
    fn transactional_excludes_master() {
        let tx = transactional_entities();
        assert!(tx.iter().all(|e| !e.is_master()));
        assert!(tx.contains(&EntityType::Invoices));
        assert!(!tx.contains(&EntityType::Accounts));
    }

    // ── resolve_mode ────────────────────────────────────────────────────────────

    #[test]
    fn resolve_mode_precedence() {
        let cfg = cfg();
        let rd = d(2026, 6, 17);

        // reports beats everything else
        let mut a = args_with(vec![]);
        a.reports = true;
        a.where_clause = Some("x".into());
        assert!(matches!(
            resolve_mode(&a, rd, &cfg).unwrap().0,
            SyncMode::Reports { .. }
        ));

        // where → open-sweep
        let mut a = args_with(vec![]);
        a.where_clause = Some("Status==\"AUTHORISED\"".into());
        assert_eq!(
            resolve_mode(&a, rd, &cfg).unwrap().0,
            SyncMode::OpenSweep {
                where_clause: "Status==\"AUTHORISED\"".into()
            }
        );

        // business window → rolling-full with inclusive day count, anchored at `to`
        let mut a = args_with(vec![]);
        a.business_from = Some(d(2026, 5, 18));
        a.business_to = Some(d(2026, 6, 17));
        let (mode, anchor) = resolve_mode(&a, rd, &cfg).unwrap();
        assert_eq!(mode, SyncMode::RollingFull { days: 31 });
        assert_eq!(anchor, d(2026, 6, 17));

        // full-with-window uses configured tight days
        let mut a = args_with(vec![]);
        a.full_with_window = true;
        assert_eq!(
            resolve_mode(&a, rd, &cfg).unwrap().0,
            SyncMode::RollingFull { days: 30 }
        );

        // full (master) → master
        let mut a = args_with(vec![]);
        a.full = true;
        assert_eq!(resolve_mode(&a, rd, &cfg).unwrap().0, SyncMode::Master);

        // default → incremental, anchored at run_date
        let (mode, anchor) = resolve_mode(&args_with(vec![]), rd, &cfg).unwrap();
        assert_eq!(mode, SyncMode::Incremental);
        assert_eq!(anchor, rd);
    }

    #[test]
    fn resolve_mode_rejects_business_to_before_from() {
        let mut a = args_with(vec![]);
        a.business_from = Some(d(2026, 6, 17));
        a.business_to = Some(d(2026, 1, 1));
        assert!(resolve_mode(&a, d(2026, 6, 17), &cfg()).is_err());
    }

    // ── resolve_entities ────────────────────────────────────────────────────────

    #[test]
    fn resolve_entities_defaults_per_mode() {
        let a = args_with(vec![]);
        let master = resolve_entities(&a, ModeScope(&SyncMode::Master)).unwrap();
        assert_eq!(master, EntityType::master_data().to_vec());

        let reports = resolve_entities(
            &a,
            ModeScope(&SyncMode::Reports {
                as_of: d(2026, 6, 17),
            }),
        )
        .unwrap();
        assert_eq!(reports, EntityType::reports_default().to_vec());
        // Per-contact aged reports are excluded from the default set (they need
        // a contactId; aging is derived downstream).
        assert!(!reports.contains(&EntityType::ReportAgedReceivablesByContact));
        assert!(!reports.contains(&EntityType::ReportAgedPayablesByContact));

        let open = resolve_entities(
            &a,
            ModeScope(&SyncMode::OpenSweep {
                where_clause: "x".into(),
            }),
        )
        .unwrap();
        assert_eq!(open, EntityType::open_status().to_vec());

        let backfill = resolve_entities(&a, BackfillScope).unwrap();
        assert_eq!(backfill, EntityType::all().to_vec());
    }

    #[test]
    fn resolve_entities_rejects_report_outside_reports_mode() {
        let a = args_with(vec![EntityType::ReportBalanceSheet]);
        let err = resolve_entities(&a, ModeScope(&SyncMode::Incremental)).unwrap_err();
        assert!(err.to_string().contains("only valid with --reports"));
    }

    #[test]
    fn resolve_entities_rejects_non_report_in_reports_mode() {
        let a = args_with(vec![EntityType::Invoices]);
        let scope = ModeScope(&SyncMode::Reports {
            as_of: d(2026, 6, 17),
        });
        let err = resolve_entities(&a, scope).unwrap_err();
        assert!(err.to_string().contains("only accepts report entities"));
    }
}
