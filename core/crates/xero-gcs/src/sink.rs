//! Raw object sinks: the `RawSink` trait plus a GCS and a local-disk impl.
//!
//! Both impls write the object body AND a `.meta.json` sidecar. The GCS impl
//! attaches the `ObjectMeta` map as native GCS custom object metadata; the
//! local impl just mirrors the same key layout on disk for `--dry-run`.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use chrono::NaiveDate;
use uuid::Uuid;

use gcloud_storage::client::{Client, ClientConfig};
use gcloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use gcloud_storage::http::objects::Object;

use crate::error::{GcsError, Result};
use crate::manifest::{manifest_key, RunManifest};
use crate::meta::{sidecar_bytes, sidecar_key, ObjectMeta};

/// Content type for both the raw object body and the sidecar.
const CONTENT_TYPE_JSON: &str = "application/json";

/// A sink that persists verbatim raw API response bytes plus a metadata sidecar.
#[async_trait::async_trait]
pub trait RawSink: Send + Sync {
    /// Persist `body` at `key` with `meta` as object metadata, then persist the
    /// `.meta.json` sidecar alongside it.
    async fn put_raw(&self, key: &str, body: &[u8], meta: &ObjectMeta) -> Result<()>;
}

/// Production sink: writes to a GCS bucket with full custom object metadata.
pub struct GcsRawSink {
    client: Client,
    bucket: String,
}

impl GcsRawSink {
    /// Build a GCS sink, authenticating from `GOOGLE_APPLICATION_CREDENTIALS`
    /// (or the other sources `with_auth` supports).
    pub async fn new(bucket: String) -> Result<Self> {
        if bucket.trim().is_empty() {
            return Err(GcsError::Config(
                "bucket name must not be empty".to_string(),
            ));
        }
        let config = ClientConfig::default()
            .with_auth()
            .await
            .map_err(|e| GcsError::Auth(e.to_string()))?;
        let client = Client::new(config);
        Ok(Self { client, bucket })
    }

    /// The bucket this sink writes to.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Convert the deterministic `BTreeMap` metadata into the `HashMap` the
    /// GCS API expects for `Object.metadata`.
    fn meta_to_hashmap(meta: &ObjectMeta) -> HashMap<String, String> {
        meta.0.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Upload `body` at `key` with custom object metadata via a multipart
    /// upload (the only upload form that carries `Object.metadata`).
    async fn upload_with_meta(&self, key: &str, body: Vec<u8>, meta: &ObjectMeta) -> Result<()> {
        let object = Object {
            name: key.to_string(),
            bucket: self.bucket.clone(),
            content_type: Some(CONTENT_TYPE_JSON.to_string()),
            metadata: Some(Self::meta_to_hashmap(meta)),
            ..Default::default()
        };
        let req = UploadObjectRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };
        let upload_type = UploadType::Multipart(Box::new(object));
        self.client
            .upload_object(&req, body, &upload_type)
            .await
            .map(|_| ())
            .map_err(|e| GcsError::Upload(format!("upload {key} failed: {e}")))
    }

    /// Upload `body` at `key` with no custom metadata (used for the sidecar).
    async fn upload_plain(&self, key: &str, body: Vec<u8>) -> Result<()> {
        let mut media = Media::new(key.to_string());
        media.content_type = CONTENT_TYPE_JSON.into();
        let req = UploadObjectRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };
        let upload_type = UploadType::Simple(media);
        self.client
            .upload_object(&req, body, &upload_type)
            .await
            .map(|_| ())
            .map_err(|e| GcsError::Upload(format!("upload {key} failed: {e}")))
    }
}

#[async_trait::async_trait]
impl RawSink for GcsRawSink {
    async fn put_raw(&self, key: &str, body: &[u8], meta: &ObjectMeta) -> Result<()> {
        // 1. Object body with native custom metadata.
        self.upload_with_meta(key, body.to_vec(), meta).await?;
        // 2. Sidecar (queryable copy of the metadata, survives metadata-stripping copies).
        let side_key = sidecar_key(key);
        let side_body = sidecar_bytes(meta);
        self.upload_plain(&side_key, side_body).await?;
        Ok(())
    }
}

/// Dry-run sink: mirrors the same key layout under `root` on local disk.
pub struct LocalDirSink {
    root: PathBuf,
}

impl LocalDirSink {
    /// Create a local sink rooted at `root` (created lazily on first write).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The root directory this sink writes under.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve `key` to a path strictly under `root`, rejecting any key that
    /// would escape the root directory.
    ///
    /// The check is lexical (no filesystem access, so it works before the file
    /// exists): the joined path is walked component by component. An absolute
    /// key, a Windows prefix/root component, or a `..` that would climb above
    /// `root` all cause an [`GcsError::Io`] rather than a silent escape.
    fn resolve_within_root(&self, key: &str) -> Result<PathBuf> {
        let joined = self.root.join(key);
        let mut resolved = PathBuf::new();
        let mut depth: usize = 0;
        let root_depth = self.root.components().count();
        for component in joined.components() {
            match component {
                Component::CurDir => {}
                Component::ParentDir => {
                    if depth <= root_depth {
                        return Err(GcsError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("key {key:?} escapes the sink root directory"),
                        )));
                    }
                    depth -= 1;
                    resolved.pop();
                }
                other => {
                    resolved.push(other);
                    depth += 1;
                }
            }
        }
        if !resolved.starts_with(&self.root) {
            return Err(GcsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("key {key:?} escapes the sink root directory"),
            )));
        }
        Ok(resolved)
    }

    /// Write `body` to `root/key`, creating parent directories as needed.
    ///
    /// Rejects any `key` that would resolve outside `root` (path traversal).
    async fn write_file(&self, key: &str, body: &[u8]) -> Result<()> {
        let path = self.resolve_within_root(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, body).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl RawSink for LocalDirSink {
    async fn put_raw(&self, key: &str, body: &[u8], meta: &ObjectMeta) -> Result<()> {
        self.write_file(key, body).await?;
        let side_key = sidecar_key(key);
        let side_body = sidecar_bytes(meta);
        self.write_file(&side_key, &side_body).await?;
        Ok(())
    }
}

/// Serialize and write a run manifest to its (prefix-free) manifest key.
///
/// Writes pretty JSON via `put_raw` with a minimal `ObjectMeta` so the manifest
/// carries at least vendor + sync-type tags.
pub async fn write_manifest(
    sink: &dyn RawSink,
    run_date: NaiveDate,
    run_id: Uuid,
    m: &RunManifest,
) -> Result<()> {
    let key = manifest_key(run_date, run_id);
    let body = serde_json::to_vec_pretty(m)?;
    let meta = manifest_meta();
    sink.put_raw(&key, &body, &meta).await
}

/// Minimal metadata for a manifest object.
fn manifest_meta() -> ObjectMeta {
    let mut map = std::collections::BTreeMap::new();
    map.insert("x-vendor".to_string(), "xero".to_string());
    map.insert("x-sync-type".to_string(), "manifest".to_string());
    ObjectMeta(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::EntityOutcome;
    use std::collections::BTreeMap;

    fn sample_meta() -> ObjectMeta {
        let mut map = BTreeMap::new();
        map.insert("x-vendor".to_string(), "xero".to_string());
        map.insert("x-endpoint".to_string(), "invoices".to_string());
        map.insert("x-page".to_string(), "1".to_string());
        ObjectMeta(map)
    }

    #[tokio::test]
    async fn local_dir_sink_round_trips_object_and_sidecar() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = LocalDirSink::new(dir.path());

        let key = "raw/xero/t/2.0/invoices/2026-06-17/TS_rid_p001.json";
        let body = br#"{"Id":"x","Status":"OK","Invoices":[]}"#;
        let meta = sample_meta();

        sink.put_raw(key, body, &meta).await.expect("put_raw");

        // Object body matches verbatim.
        let obj_path = dir.path().join(key);
        let read_body = tokio::fs::read(&obj_path).await.expect("read object");
        assert_eq!(read_body, body);

        // Sidecar exists and parses back to the same map.
        let side_path = dir.path().join(sidecar_key(key));
        let read_side = tokio::fs::read(&side_path).await.expect("read sidecar");
        let parsed: BTreeMap<String, String> =
            serde_json::from_slice(&read_side).expect("parse sidecar");
        assert_eq!(parsed, meta.0);
    }

    #[tokio::test]
    async fn local_dir_sink_rejects_parent_dir_traversal_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = LocalDirSink::new(dir.path());
        let err = sink
            .put_raw("../../etc/passwd", b"{}", &sample_meta())
            .await
            .expect_err("traversal key must be rejected");
        assert!(
            matches!(err, GcsError::Io(_)),
            "expected Io error, got {err:?}"
        );
        // Nothing should have been written outside the root.
        assert!(!dir.path().parent().unwrap().join("etc/passwd").exists());
    }

    #[tokio::test]
    async fn local_dir_sink_rejects_absolute_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = LocalDirSink::new(dir.path());
        let err = sink
            .put_raw("/tmp/xero_escape_attempt.json", b"{}", &sample_meta())
            .await
            .expect_err("absolute key must be rejected");
        assert!(
            matches!(err, GcsError::Io(_)),
            "expected Io error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn local_dir_sink_allows_interior_parent_dir() {
        // A `..` that stays within root must still be allowed.
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = LocalDirSink::new(dir.path());
        sink.put_raw("a/b/../c/file.json", b"{}", &sample_meta())
            .await
            .expect("interior .. should resolve within root");
        assert!(dir.path().join("a/c/file.json").exists());
    }

    #[tokio::test]
    async fn local_dir_sink_creates_nested_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = LocalDirSink::new(dir.path());
        let key = "a/deeply/nested/key/file.json";
        sink.put_raw(key, b"{}", &sample_meta())
            .await
            .expect("put_raw");
        assert!(dir.path().join(key).exists());
    }

    #[tokio::test]
    async fn write_manifest_lands_outside_raw_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = LocalDirSink::new(dir.path());
        let run_date = NaiveDate::from_ymd_opt(2026, 6, 17).expect("date");
        let run_id = Uuid::parse_str("5f3c9a2e-0000-0000-0000-000000000000").expect("uuid");

        let m = RunManifest {
            run_id: run_id.to_string(),
            started_at: "2026-06-17T03:00:00Z".to_string(),
            finished_at: "2026-06-17T03:01:00Z".to_string(),
            mode: "incremental".to_string(),
            window_days: Some(3),
            entities: vec![EntityOutcome {
                tenant: "t".to_string(),
                endpoint: "invoices".to_string(),
                pages: 1,
                records: 1,
                termination: "empty-page".to_string(),
                error: None,
            }],
        };

        write_manifest(&sink, run_date, run_id, &m)
            .await
            .expect("write_manifest");

        let key = manifest_key(run_date, run_id);
        assert!(!key.starts_with("raw/"));
        let path = dir.path().join(&key);
        assert!(path.exists(), "manifest not written at {key}");
        let back: RunManifest =
            serde_json::from_slice(&tokio::fs::read(&path).await.expect("read"))
                .expect("parse manifest");
        assert_eq!(back, m);
    }
}
