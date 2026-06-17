//! Error type for the `xero-gcs` crate.

/// Errors that can occur while building object keys/metadata or uploading
/// raw Xero response bytes to an object store.
#[derive(Debug, thiserror::Error)]
pub enum GcsError {
    /// An upload (or sidecar upload) to the backing object store failed.
    #[error("gcs upload failed: {0}")]
    Upload(String),

    /// A local filesystem operation failed (used by `LocalDirSink`).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// (De)serializing JSON (sidecar / manifest) failed.
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Authenticating against the object store failed.
    #[error("gcs auth failed: {0}")]
    Auth(String),

    /// A configuration value (e.g. bucket name) was invalid or missing.
    #[error("gcs config error: {0}")]
    Config(String),
}

/// Convenience alias for results in this crate.
pub type Result<T> = std::result::Result<T, GcsError>;
