//! `xero-auth` — Xero OAuth 2.0 PKCE client + token cache.

pub mod custom_connection;
pub mod pkce;
pub mod token;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use custom_connection::{CustomConnectionClient, MultiTenantCustomConnectionClient};
pub use pkce::PkceChallenge;
pub use token::TokenCache;

// ── Token data ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenData {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
    pub scopes: Vec<String>,
    pub tenant_id: String,
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

// ── OAuth client ──────────────────────────────────────────────────────────────

/// Xero OAuth endpoints.
pub const AUTH_URL: &str = "https://login.xero.com/identity/connect/authorize";
pub const TOKEN_URL: &str = "https://identity.xero.com/connect/token";
pub const CONNECTIONS_URL: &str = "https://api.xero.com/connections";
pub const REVOKE_URL: &str = "https://identity.xero.com/connect/revocation";

/// Minimal OAuth client — handles token exchange and refresh.
pub struct XeroOAuthClient {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
    http: reqwest::Client,
}

impl XeroOAuthClient {
    pub fn new(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        redirect_uri: impl Into<String>,
        scopes: Vec<String>,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            redirect_uri: redirect_uri.into(),
            scopes,
            http: reqwest::Client::builder()
                .user_agent("xero_service_v2/0.1")
                .build()
                .expect("reqwest client"),
        }
    }

    /// Build the authorisation URL including PKCE challenge.
    pub fn authorisation_url(&self, state: &str, challenge: &PkceChallenge) -> String {
        let scopes = self.scopes.join(" ");
        format!(
            "{AUTH_URL}?response_type=code\
             &client_id={}\
             &redirect_uri={}\
             &scope={scopes}\
             &state={state}\
             &code_challenge={}\
             &code_challenge_method=S256",
            urlencoding::encode(&self.client_id),
            urlencoding::encode(&self.redirect_uri),
            challenge.challenge,
        )
    }

    /// Exchange an authorisation code for tokens.
    pub async fn exchange_code(
        &self,
        code: &str,
        verifier: &str,
    ) -> xero_common::Result<serde_json::Value> {
        let params = [
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &self.redirect_uri),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("code_verifier", verifier),
        ];

        let resp = self
            .http
            .post(TOKEN_URL)
            .form(&params)
            .send()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        if !status.is_success() {
            return Err(xero_common::Error::Auth(format!(
                "token exchange failed ({status}): {body}"
            )));
        }

        Ok(body)
    }

    /// Refresh an expired access token using the refresh token.
    pub async fn refresh_token(
        &self,
        refresh_token: &str,
    ) -> xero_common::Result<serde_json::Value> {
        let params = [
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];

        let resp = self
            .http
            .post(TOKEN_URL)
            .form(&params)
            .send()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        if !status.is_success() {
            return Err(xero_common::Error::Auth(format!(
                "token refresh failed ({status}): {body}"
            )));
        }

        Ok(body)
    }

    pub async fn get_connections(
        &self,
        access_token: &str,
    ) -> xero_common::Result<Vec<serde_json::Value>> {
        let resp = self
            .http
            .get(CONNECTIONS_URL)
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;
        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;
        if !status.is_success() {
            return Err(xero_common::Error::Auth(format!(
                "connections failed ({status}): {body}"
            )));
        }
        Ok(body.as_array().cloned().unwrap_or_default())
    }

    /// Read a tenant token from Redis cache.
    pub async fn get_valid_token(&self, tenant_id: &str) -> xero_common::Result<TokenData> {
        let redis_url = std::env::var("REDIS_URL").map_err(|_| {
            xero_common::Error::Auth("missing required env var: REDIS_URL".to_string())
        })?;
        let cache = TokenCache::new(&redis_url)?;

        match cache.get(tenant_id).await {
            Some(token) if token.is_expired() => Err(xero_common::Error::Auth(format!(
                "token expired for tenant: {tenant_id}"
            ))),
            Some(token) => Ok(token),
            None => Err(xero_common::Error::Auth(format!(
                "no token found for tenant: {tenant_id}"
            ))),
        }
    }
}

// cargo needs this for the encode helper — add as dep
mod urlencoding {
    pub fn encode(s: &str) -> String {
        url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
    }
}

// ── TokenProvider trait ─────────────────────────────────────────────────────

#[async_trait::async_trait]
pub trait TokenProvider: Send + Sync {
    async fn get_valid_token(&self, tenant_id: &str) -> xero_common::Result<crate::TokenData>;
}

#[async_trait::async_trait]
impl TokenProvider for XeroOAuthClient {
    async fn get_valid_token(&self, tenant_id: &str) -> xero_common::Result<crate::TokenData> {
        XeroOAuthClient::get_valid_token(self, tenant_id).await
    }
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
