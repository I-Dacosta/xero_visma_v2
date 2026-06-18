//! Custom Connection (client_credentials grant) support for Xero.
//!
//! Used when the Xero app is configured as a "Custom Connection" and no
//! user-facing OAuth redirect flow is needed.

use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::Utc;
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use crate::{TokenData, TOKEN_URL};

// ── Token response shape ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
    scope: Option<String>,
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Fetches access tokens via the OAuth 2.0 `client_credentials` grant.
pub struct CustomConnectionClient {
    client_id: String,
    client_secret: String,
    tenant_id: String,
    http: reqwest::Client,
    cached_token: Mutex<Option<TokenData>>,
}

impl CustomConnectionClient {
    /// Build a new client. Panics if the underlying `reqwest::Client` cannot
    /// be constructed (should never happen on any supported platform).
    pub fn new(client_id: String, client_secret: String, tenant_id: String) -> Self {
        let http = reqwest::Client::builder()
            .user_agent("xero_service_v2/0.1")
            .build()
            .expect("Failed to build reqwest client for CustomConnectionClient");

        Self {
            client_id,
            client_secret,
            tenant_id,
            http,
            cached_token: Mutex::new(None),
        }
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Exchange client credentials for a fresh access token.
    pub async fn fetch_token(&self) -> xero_common::Result<TokenData> {
        if let Some(token) = self.cached_token()? {
            if !token.needs_refresh(60) {
                return Ok(token);
            }
        }

        let token = self.fetch_token_uncached().await?;
        *self.token_lock()? = Some(token.clone());
        Ok(token)
    }

    fn cached_token(&self) -> xero_common::Result<Option<TokenData>> {
        Ok(self.token_lock()?.clone())
    }

    fn token_lock(&self) -> xero_common::Result<MutexGuard<'_, Option<TokenData>>> {
        self.cached_token.lock().map_err(|_| {
            xero_common::Error::Auth("custom connection token cache poisoned".to_owned())
        })
    }

    async fn fetch_token_uncached(&self) -> xero_common::Result<TokenData> {
        let creds = STANDARD.encode(format!("{}:{}", self.client_id, self.client_secret));
        // Custom-connection `client_credentials` takes NO scope param: Xero
        // issues a token covering every scope configured on the custom
        // connection app. Requesting a scope the app lacks — or any OIDC/PKCE
        // scope (openid/profile/email/offline_access) — fails with
        // `invalid_scope`, so we omit it and accept the app's full granted set.
        let form = [("grant_type", "client_credentials")];

        let resp = self
            .http
            .post(TOKEN_URL)
            .header("Authorization", format!("Basic {creds}"))
            .form(&form)
            .send()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        let status = resp.status();

        // Deserialise first so we can surface any error body before checking status.
        let tr: TokenResponse = resp
            .json()
            .await
            .map_err(|e| xero_common::Error::Auth(format!("cc token parse error: {e}")))?;

        if !status.is_success() {
            return Err(xero_common::Error::Auth(format!(
                "cc token fetch failed (HTTP {status})"
            )));
        }

        Ok(TokenData {
            access_token: tr.access_token,
            refresh_token: String::new(), // not issued for client_credentials
            expires_at: Utc::now() + chrono::Duration::seconds(tr.expires_in),
            scopes: tr
                .scope
                .unwrap_or_default()
                .split_whitespace()
                .map(ToOwned::to_owned)
                .collect(),
            tenant_id: self.tenant_id.clone(),
        })
    }
}

/// Routes custom-connection token requests to the credentials configured for
/// the requested Xero tenant.
pub struct MultiTenantCustomConnectionClient {
    clients: HashMap<String, CustomConnectionClient>,
}

impl MultiTenantCustomConnectionClient {
    pub fn new(connections: Vec<xero_common::XeroCustomConnectionConfig>) -> Self {
        let clients = connections
            .into_iter()
            .map(|conn| {
                let tenant_id = conn.tenant_id;
                let client = CustomConnectionClient::new(
                    conn.client_id,
                    conn.client_secret,
                    tenant_id.clone(),
                );
                (tenant_id, client)
            })
            .collect();

        Self { clients }
    }

    pub fn tenant_ids(&self) -> Vec<&str> {
        self.clients.keys().map(String::as_str).collect()
    }

    pub fn has_tenant(&self, tenant_id: &str) -> bool {
        self.clients.contains_key(tenant_id)
    }

    pub async fn fetch_token_for_tenant(&self, tenant_id: &str) -> xero_common::Result<TokenData> {
        let Some(client) = self.clients.get(tenant_id) else {
            return Err(xero_common::Error::Auth(format!(
                "custom connection tenant not configured: {tenant_id}"
            )));
        };

        client.fetch_token().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TokenProvider;

    #[tokio::test]
    async fn rejects_requests_for_a_different_tenant_without_fetching_token() {
        let client = CustomConnectionClient::new(
            "client-id".to_owned(),
            "client-secret".to_owned(),
            "configured-tenant".to_owned(),
        );

        let err = client.get_valid_token("other-tenant").await.unwrap_err();

        assert!(err
            .to_string()
            .contains("custom connection tenant mismatch"));
    }

    #[tokio::test]
    async fn multi_tenant_client_rejects_unconfigured_tenant_without_fetching_token() {
        let client =
            MultiTenantCustomConnectionClient::new(vec![xero_common::XeroCustomConnectionConfig {
                tenant_name: Some("Tenant One".to_owned()),
                tenant_id: "tenant-one".to_owned(),
                client_id: "client-id".to_owned(),
                client_secret: "client-secret".to_owned(),
            }]);

        let err = client.get_valid_token("tenant-two").await.unwrap_err();

        assert!(err
            .to_string()
            .contains("custom connection tenant not configured"));
    }
}
