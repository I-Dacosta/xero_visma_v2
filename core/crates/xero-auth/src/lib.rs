//! `xero-auth` — Xero custom-connection (`client_credentials`) auth.
//!
//! This crate provides token acquisition for the stateless raw→GCS uploader.
//! Only the Xero "Custom Connection" (`client_credentials` grant) flow is
//! supported — there is no user-facing OAuth redirect / PKCE flow and no
//! external token cache (tokens are cached in-memory per client).

pub mod custom_connection;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use custom_connection::{CustomConnectionClient, MultiTenantCustomConnectionClient};

// ── Token data ────────────────────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize)]
pub struct TokenData {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
    pub scopes: Vec<String>,
    pub tenant_id: String,
}

// Manual `Debug` that redacts the bearer/refresh tokens so a stray `{:?}`
// (ours or a future caller's) can never leak credentials into logs.
impl std::fmt::Debug for TokenData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenData")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .field("scopes", &self.scopes)
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

impl TokenData {
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }

    /// Return true when expiry is within `buffer_secs` seconds.
    pub fn needs_refresh(&self, buffer_secs: i64) -> bool {
        let refresh_at = self.expires_at - chrono::Duration::seconds(buffer_secs);
        Utc::now() >= refresh_at
    }
}

// ── Xero OAuth endpoints ───────────────────────────────────────────────────────

/// Xero token endpoint — used by the `client_credentials` grant.
pub const TOKEN_URL: &str = "https://identity.xero.com/connect/token";

// ── TokenProvider trait ─────────────────────────────────────────────────────

#[async_trait::async_trait]
pub trait TokenProvider: Send + Sync {
    async fn get_valid_token(&self, tenant_id: &str) -> xero_common::Result<crate::TokenData>;
}

#[async_trait::async_trait]
impl TokenProvider for custom_connection::CustomConnectionClient {
    async fn get_valid_token(&self, tenant_id: &str) -> xero_common::Result<crate::TokenData> {
        if tenant_id != self.tenant_id() {
            return Err(xero_common::Error::Auth(
                "custom connection tenant mismatch".to_owned(),
            ));
        }

        self.fetch_token().await
    }
}

#[async_trait::async_trait]
impl TokenProvider for custom_connection::MultiTenantCustomConnectionClient {
    async fn get_valid_token(&self, tenant_id: &str) -> xero_common::Result<crate::TokenData> {
        self.fetch_token_for_tenant(tenant_id).await
    }
}
