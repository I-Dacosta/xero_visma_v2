use thiserror::Error;

/// All failure variants for xero_service_v2.
#[derive(Debug, Error)]
pub enum Error {
    #[error("auth: {0}")]
    Auth(String),

    #[error("state store: {0}")]
    StateStore(String),

    #[error("xero api: {0}")]
    XeroApi(String),

    #[error("config: {0}")]
    Config(String),

    #[error("sync: {0}")]
    Sync(String),

    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("http: {0}")]
    Http(String),
}

pub type Result<T> = std::result::Result<T, Error>;
