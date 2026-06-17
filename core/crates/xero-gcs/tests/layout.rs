//! Deterministic GCS-layout integration test (no live Xero / GCS calls).
//!
//! Proves that the on-disk layout produced by `LocalDirSink` — which mirrors
//! the GCS object key layout byte-for-byte — matches the documented scheme in
//! `docs/NEW_ARCHITECTURE_RAW_GCS.md`:
//!
//! ```text
//! object:   {prefix}/{tenant}/2.0/{endpoint}/{YYYY-MM-DD}/{ts}_{run_id_short}_p{NNN}.json
//! report:   {prefix}/{tenant}/2.0/{endpoint}/{YYYY-MM-DD}/{ts}_{run_id_short}__{period_key}.json
//! sidecar:  <object-key>.meta.json
//! manifest: _manifests/xero/{YYYY-MM-DD}/{run_id}.json   (OUTSIDE the prefix)
//! ```
//!
//! Hermetic and fast: one `tempdir`, no network, no `Utc::now()` (the timestamp
//! is supplied by the caller, matching how the key builders are designed).

use std::collections::BTreeMap;

use chrono::NaiveDate;
use uuid::Uuid;

use xero_gcs::{
    manifest_key, metadata, object_key, report_metadata, report_object_key, sidecar_key,
    write_manifest, EntityOutcome, LocalDirSink, MetaArgs, RawSink, ReportMetaArgs, RunManifest,
    SyncType,
};

/// Fixed inputs shared across the simulated run so every key is deterministic.
const PREFIX: &str = "raw/xero";
const TS: &str = "20260617T030001Z";
const ORG_NAME: &str = "Aquatiq Australia Pty Ltd";
const SYNCED_AT: &str = "2026-06-17T03:00:01Z";

fn run_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 6, 17).expect("valid date")
}

fn run_id() -> Uuid {
    Uuid::parse_str("5f3c9a2e-0000-0000-0000-000000000000").expect("valid uuid")
}

/// The short run-id used in object filenames (first 8 hex chars of the uuid).
fn run_id_short() -> String {
    run_id().simple().to_string()[..8].to_string()
}

/// Read a sidecar file and parse it back into the metadata map.
async fn read_sidecar_map(root: &std::path::Path, object_key: &str) -> BTreeMap<String, String> {
    let side_path = root.join(sidecar_key(object_key));
    let bytes = tokio::fs::read(&side_path)
        .await
        .unwrap_or_else(|e| panic!("sidecar not found at {}: {e}", side_path.display()));
    serde_json::from_slice(&bytes).expect("sidecar parses as JSON object")
}

/// Assert that no single path segment contains a stray slash beyond the fixed
/// number of separators in the documented layout. `expected_separators` is the
/// count of `/` between segments; anything more means a segment leaked a slash.
fn assert_no_intra_segment_slash(key: &str, expected_separators: usize) {
    assert_eq!(
        key.matches('/').count(),
        expected_separators,
        "unexpected '/' count — a path segment leaked a slash: {key}"
    );
}

#[tokio::test]
async fn full_run_lands_documented_gcs_layout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let sink = LocalDirSink::new(root);

    // A tenant uuid (the GCS layout uses it verbatim as a path segment).
    let tenant = "e0c3a1b2-1111-2222-3333-444455556666";
    let rid_short = run_id_short();

    // ── 1. Two object pages for `invoices` (p001, p002) ─────────────────────
    let mut object_keys = Vec::new();
    for page in 1u32..=2 {
        let key = object_key(PREFIX, tenant, "invoices", run_date(), TS, &rid_short, page);
        let meta = metadata(&MetaArgs {
            tenant_id: tenant,
            org_name: ORG_NAME,
            endpoint: "invoices",
            sync_type: SyncType::Incremental,
            modified_after: Some("2026-06-14T03:00:00Z"),
            page,
            record_count: 100,
            http_status: 200,
            run_id: &run_id().to_string(),
            synced_at: SYNCED_AT,
            ..Default::default()
        });
        let body = format!(r#"{{"Invoices":[],"page":{page}}}"#).into_bytes();
        sink.put_raw(&key, &body, &meta)
            .await
            .expect("put_raw page");
        object_keys.push(key);
    }

    // ── 2. One report snapshot (balance sheet, as-of) ───────────────────────
    let period_key = "asof-2026-06-17";
    let report_key = report_object_key(
        PREFIX,
        tenant,
        "report_balance_sheet",
        run_date(),
        TS,
        &rid_short,
        period_key,
    );
    let report_meta = report_metadata(&ReportMetaArgs {
        tenant_id: tenant,
        org_name: ORG_NAME,
        endpoint: "report_balance_sheet",
        report: "BalanceSheet",
        report_date: Some("2026-06-17"),
        report_params: "date=2026-06-17",
        http_status: 200,
        record_count: 1,
        run_id: &run_id().to_string(),
        synced_at: SYNCED_AT,
        ..Default::default()
    });
    sink.put_raw(&report_key, br#"{"Reports":[]}"#, &report_meta)
        .await
        .expect("put_raw report");

    // ── 3. The run manifest ─────────────────────────────────────────────────
    let manifest = RunManifest {
        run_id: run_id().to_string(),
        started_at: "2026-06-17T03:00:00Z".to_string(),
        finished_at: "2026-06-17T03:01:00Z".to_string(),
        mode: "incremental".to_string(),
        window_days: Some(3),
        entities: vec![
            EntityOutcome {
                tenant: tenant.to_string(),
                endpoint: "invoices".to_string(),
                pages: 2,
                records: 200,
                termination: "empty-page".to_string(),
                error: None,
            },
            EntityOutcome {
                tenant: tenant.to_string(),
                endpoint: "report_balance_sheet".to_string(),
                pages: 1,
                records: 1,
                termination: "single-snapshot".to_string(),
                error: None,
            },
        ],
    };
    write_manifest(&sink, run_date(), run_id(), &manifest)
        .await
        .expect("write_manifest");

    // ── Assertions ──────────────────────────────────────────────────────────

    // (a) Object path matches the documented scheme exactly, for p001.
    let expected_p001 =
        format!("{PREFIX}/{tenant}/2.0/invoices/2026-06-17/{TS}_{rid_short}_p001.json");
    assert_eq!(object_keys[0], expected_p001, "p001 key mismatch");
    assert!(object_keys[1].ends_with("_p002.json"), "p002 suffix");
    for key in &object_keys {
        assert!(
            root.join(key).exists(),
            "object body missing on disk: {key}"
        );
    }

    // (b) The `.meta.json` sidecar exists next to each object and parses to a
    //     JSON map carrying the x-* keys with x-vendor=xero and x-api-version=2.0.
    for key in &object_keys {
        let side_path = root.join(sidecar_key(key));
        assert!(side_path.exists(), "sidecar missing next to object: {key}");
        let map = read_sidecar_map(root, key).await;
        assert_eq!(map.get("x-vendor").map(String::as_str), Some("xero"));
        assert_eq!(map.get("x-api-version").map(String::as_str), Some("2.0"));
        assert_eq!(map.get("x-endpoint").map(String::as_str), Some("invoices"));
        assert_eq!(map.get("x-tenant-id").map(String::as_str), Some(tenant));
        // Every metadata key is an x-* key.
        assert!(
            map.keys().all(|k| k.starts_with("x-")),
            "non x-* metadata key present: {:?}",
            map.keys().collect::<Vec<_>>()
        );
    }

    // (c) The report object ends with `__{sanitized period_key}.json` and its
    //     sidecar marks it as a report snapshot.
    assert!(
        report_key.ends_with("__asof_2026_06_17.json"),
        "report key must end with sanitized period suffix: {report_key}"
    );
    assert!(
        !report_key.contains("_p0"),
        "report key must not carry a page suffix: {report_key}"
    );
    assert!(root.join(&report_key).exists(), "report body missing");
    let report_map = read_sidecar_map(root, &report_key).await;
    assert_eq!(report_map.get("x-vendor").map(String::as_str), Some("xero"));
    assert_eq!(
        report_map.get("x-api-version").map(String::as_str),
        Some("2.0")
    );
    assert_eq!(
        report_map.get("x-sync-type").map(String::as_str),
        Some("report-snapshot")
    );
    assert_eq!(
        report_map.get("x-report").map(String::as_str),
        Some("BalanceSheet")
    );

    // (d) The manifest exists at `_manifests/xero/{date}/{run_id}.json`, lives
    //     OUTSIDE the prefix, and round-trips back to the same RunManifest.
    let mkey = manifest_key(run_date(), run_id());
    assert_eq!(
        mkey,
        format!("_manifests/xero/2026-06-17/{}.json", run_id()),
        "manifest key mismatch"
    );
    assert!(
        !mkey.starts_with(PREFIX) && !mkey.starts_with("raw/"),
        "manifest leaked into the raw prefix: {mkey}"
    );
    let mpath = root.join(&mkey);
    assert!(mpath.exists(), "manifest not written at {mkey}");
    let back: RunManifest =
        serde_json::from_slice(&tokio::fs::read(&mpath).await.expect("read manifest"))
            .expect("manifest round-trips");
    assert_eq!(back, manifest, "manifest did not round-trip");

    // (e) Sanitize guarantee: no path segment contains an interior slash.
    //     Object/report keys have exactly 6 separators
    //     (prefix `raw/xero` = 1) + tenant + 2.0 + endpoint + date + filename.
    for key in &object_keys {
        assert_no_intra_segment_slash(key, 6);
    }
    assert_no_intra_segment_slash(&report_key, 6);
    // The manifest key has 3 separators: `_manifests/xero` (1) + date + file.
    assert_no_intra_segment_slash(&mkey, 3);
}
