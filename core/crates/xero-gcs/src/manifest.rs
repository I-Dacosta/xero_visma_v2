//! Per-run manifest — the GCS replacement for the old `run_history` table.
//!
//! One manifest object per run, written OUTSIDE the `GCS_PREFIX` so raw data
//! and audit metadata stay cleanly separable:
//! ```text
//! _manifests/xero/{run_date}/{run_id}.json
//! ```

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Manifest path prefix. Deliberately NOT under `raw/` (the `GCS_PREFIX`).
const MANIFEST_ROOT: &str = "_manifests/xero";

/// Outcome of syncing a single (tenant, endpoint) pair within a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityOutcome {
    pub tenant: String,
    pub endpoint: String,
    pub pages: u32,
    pub records: i64,
    /// How pagination terminated (e.g. `empty-page`, `max-pages`, `error`).
    pub termination: String,
    /// Present only when this entity failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A whole run's manifest: timing, mode, window, and per-entity outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunManifest {
    pub run_id: String,
    pub started_at: String,
    pub finished_at: String,
    /// Sync mode label (e.g. `incremental`, `backfill`, `reports`).
    pub mode: String,
    /// Modified/business window size in days, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_days: Option<i64>,
    pub entities: Vec<EntityOutcome>,
}

/// Build the manifest object key.
///
/// ```text
/// _manifests/xero/{run_date}/{run_id}.json
/// ```
/// Guaranteed to live OUTSIDE the raw prefix (never starts with `raw/`).
pub fn manifest_key(run_date: NaiveDate, run_id: Uuid) -> String {
    format!(
        "{root}/{date}/{run_id}.json",
        root = MANIFEST_ROOT,
        date = run_date.format("%Y-%m-%d"),
        run_id = run_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 17).expect("valid date")
    }

    fn run_id() -> Uuid {
        Uuid::parse_str("5f3c9a2e-0000-0000-0000-000000000000").expect("valid uuid")
    }

    #[test]
    fn manifest_key_matches_documented_layout() {
        assert_eq!(
            manifest_key(date(), run_id()),
            "_manifests/xero/2026-06-17/5f3c9a2e-0000-0000-0000-000000000000.json"
        );
    }

    #[test]
    fn manifest_key_lives_outside_raw_prefix() {
        let key = manifest_key(date(), run_id());
        assert!(
            !key.starts_with("raw/"),
            "manifest leaked into raw prefix: {key}"
        );
        assert!(key.starts_with("_manifests/"), "got {key}");
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let m = RunManifest {
            run_id: "rid".to_string(),
            started_at: "2026-06-17T03:00:00Z".to_string(),
            finished_at: "2026-06-17T03:01:00Z".to_string(),
            mode: "incremental".to_string(),
            window_days: Some(3),
            entities: vec![EntityOutcome {
                tenant: "t".to_string(),
                endpoint: "invoices".to_string(),
                pages: 2,
                records: 137,
                termination: "empty-page".to_string(),
                error: None,
            }],
        };
        let json = serde_json::to_string(&m).expect("serialize");
        let back: RunManifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m, back);
        // `error: None` is omitted from the serialized form.
        assert!(
            !json.contains("\"error\""),
            "null error should be skipped: {json}"
        );
    }

    #[test]
    fn entity_outcome_keeps_error_when_present() {
        let e = EntityOutcome {
            tenant: "t".to_string(),
            endpoint: "invoices".to_string(),
            pages: 0,
            records: 0,
            termination: "error".to_string(),
            error: Some("rate limited".to_string()),
        };
        let json = serde_json::to_string(&e).expect("serialize");
        assert!(json.contains("rate limited"), "got {json}");
    }
}
