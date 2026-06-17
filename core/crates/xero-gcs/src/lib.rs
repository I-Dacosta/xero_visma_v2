//! `xero-gcs` — raw object-storage sink for Xero API responses.
//!
//! Lands verbatim Xero HTTP response bytes as GCS objects (one per page),
//! with x-* custom metadata + a `.meta.json` sidecar, plus a per-run manifest.
//! No Postgres, no BigQuery. This crate is the storage contract that
//! `xero-sync` consumes. Filled in by Wave A (see docs/TRANSFORMATION_PLAN.md).
//!
//! ## Modules
//! - [`error`] — crate error type.
//! - [`key`] — pure object-key builders (`object_key`, `report_object_key`).
//! - [`meta`] — `x-*` custom metadata + `.meta.json` sidecar.
//! - [`manifest`] — per-run manifest type + key.
//! - [`sink`] — `RawSink` trait + `GcsRawSink` / `LocalDirSink` + `write_manifest`.

pub mod error;
pub mod key;
pub mod manifest;
pub mod meta;
pub mod sink;

pub use error::{GcsError, Result};
pub use key::{object_key, report_object_key, sanitize_segment};
pub use manifest::{manifest_key, EntityOutcome, RunManifest};
pub use meta::{
    metadata, report_metadata, sidecar_bytes, sidecar_key, MetaArgs, ObjectMeta, ReportMetaArgs,
    SyncType,
};
pub use sink::{write_manifest, GcsRawSink, LocalDirSink, RawSink};
