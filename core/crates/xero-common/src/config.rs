use std::{
    collections::{BTreeSet, HashMap},
    env,
};

use crate::{Error, Result};

/// Runtime configuration sourced from environment variables.
///
/// Load via [`AppConfig::from_env`] after calling `dotenvy::dotenv()`.
#[derive(Debug, Clone)]
pub struct XeroCustomConnectionConfig {
    pub tenant_name: Option<String>,
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub pg_dsn: String,
    pub redis_url: String,
    pub xero_client_id: Option<String>,
    pub xero_client_secret: Option<String>,
    /// Required for the standard OAuth flow; not needed for custom-connection mode.
    pub xero_redirect_uri: Option<String>,
    pub http_bind: String,
    pub log_level: String,

    // ── Custom-connection (client_credentials) fields ──────────────────────
    /// Set `XERO_CUSTOM_CONNECTION=true` to enable client-credentials mode.
    pub xero_custom_connection: bool,
    pub xero_cc_client_id: Option<String>,
    pub xero_cc_client_secret: Option<String>,
    pub xero_cc_tenant_id: Option<String>,
    pub xero_cc_tenant_name: Option<String>,
    pub xero_cc_connections: Vec<XeroCustomConnectionConfig>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv(); // ignore missing .env in production

        Self::from_map(&env::vars().collect())
    }

    /// Load only the database URL — useful for CLI commands that only need the DB.
    pub fn pg_dsn_only() -> Result<String> {
        let _ = dotenvy::dotenv();
        required("DATABASE_URL")
    }

    fn from_map(vars: &HashMap<String, String>) -> Result<Self> {
        let xero_custom_connection = custom_connection_enabled(
            value(vars, "XERO_CUSTOM_CONNECTION").as_deref(),
            value(vars, "XERO_CONNECTION_TYPE").as_deref(),
        )?;

        let xero_client_id = value(vars, "XERO_CLIENT_ID");
        let xero_client_secret = value(vars, "XERO_CLIENT_SECRET");

        let org_index = custom_connection_org_index(vars)?;
        let org_client_id_label = format!("XERO_ORG_{org_index}_CLIENT_ID");
        let org_client_secret_label = format!("XERO_ORG_{org_index}_CLIENT_SECRET");
        let org_tenant_id_label = format!("XERO_ORG_{org_index}_TENANT_ID");

        let xero_cc_connections = custom_connection_configs(vars)?;
        let selected_connection = selected_custom_connection(vars, &org_index)?;
        let xero_cc_client_id = selected_connection
            .as_ref()
            .map(|conn| conn.client_id.clone());
        let xero_cc_client_secret = selected_connection
            .as_ref()
            .map(|conn| conn.client_secret.clone());
        let xero_cc_tenant_id = selected_connection
            .as_ref()
            .map(|conn| conn.tenant_id.clone());
        let xero_cc_tenant_name = selected_connection
            .as_ref()
            .and_then(|conn| conn.tenant_name.clone());

        if xero_custom_connection {
            if xero_cc_connections.is_empty() {
                return Err(Error::Config(
                    "missing custom connection credentials: configure XERO_CC_* or XERO_ORG_N_*"
                        .to_owned(),
                ));
            }
            require_present(
                &format!("XERO_CC_CLIENT_ID or {org_client_id_label}"),
                &xero_cc_client_id,
            )?;
            require_present(
                &format!("XERO_CC_CLIENT_SECRET or {org_client_secret_label}"),
                &xero_cc_client_secret,
            )?;
            require_present(
                &format!("XERO_CC_TENANT_ID or {org_tenant_id_label}"),
                &xero_cc_tenant_id,
            )?;
        } else {
            require_present("XERO_CLIENT_ID", &xero_client_id)?;
            require_present("XERO_CLIENT_SECRET", &xero_client_secret)?;
        }

        Ok(Self {
            pg_dsn: required_from(vars, "DATABASE_URL")?,
            redis_url: value(vars, "REDIS_URL")
                .unwrap_or_else(|| "redis://localhost:6380/0".into()),
            xero_client_id,
            xero_client_secret,
            xero_redirect_uri: value(vars, "XERO_REDIRECT_URI"),
            http_bind: value(vars, "XERO_HTTP_BIND").unwrap_or_else(|| "0.0.0.0:5002".into()),
            log_level: value(vars, "RUST_LOG").unwrap_or_else(|| "info".into()),
            xero_custom_connection,
            xero_cc_client_id,
            xero_cc_client_secret,
            xero_cc_tenant_id,
            xero_cc_tenant_name,
            xero_cc_connections,
        })
    }

    #[cfg(test)]
    fn from_pairs<const N: usize>(pairs: [(&str, &str); N]) -> Result<Self> {
        let vars = pairs
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect::<HashMap<_, _>>();
        Self::from_map(&vars)
    }
}

fn required(key: &str) -> Result<String> {
    env::var(key).map_err(|_| Error::Config(format!("missing required env var: {key}")))
}

fn required_from(vars: &HashMap<String, String>, key: &str) -> Result<String> {
    value(vars, key).ok_or_else(|| Error::Config(format!("missing required env var: {key}")))
}

fn require_present(label: &str, value: &Option<String>) -> Result<()> {
    if value.is_some() {
        Ok(())
    } else {
        Err(Error::Config(format!("missing required env var: {label}")))
    }
}

fn selected_custom_connection(
    vars: &HashMap<String, String>,
    org_index: &str,
) -> Result<Option<XeroCustomConnectionConfig>> {
    explicit_custom_connection(vars)?.map_or_else(
        || numbered_custom_connection(vars, org_index),
        |conn| Ok(Some(conn)),
    )
}

fn custom_connection_configs(
    vars: &HashMap<String, String>,
) -> Result<Vec<XeroCustomConnectionConfig>> {
    let mut connections = Vec::new();

    if let Some(conn) = explicit_custom_connection(vars)? {
        connections.push(conn);
    }

    for index in numbered_org_indices(vars) {
        if let Some(conn) = numbered_custom_connection(vars, &index.to_string())? {
            if !connections
                .iter()
                .any(|existing: &XeroCustomConnectionConfig| existing.tenant_id == conn.tenant_id)
            {
                connections.push(conn);
            }
        }
    }

    Ok(connections)
}

fn explicit_custom_connection(
    vars: &HashMap<String, String>,
) -> Result<Option<XeroCustomConnectionConfig>> {
    let client_id = value(vars, "XERO_CC_CLIENT_ID");
    let client_secret = value(vars, "XERO_CC_CLIENT_SECRET");
    let tenant_id = value(vars, "XERO_CC_TENANT_ID");
    let tenant_name = value(vars, "XERO_CC_TENANT_NAME");

    if client_id.is_none()
        && client_secret.is_none()
        && tenant_id.is_none()
        && tenant_name.is_none()
    {
        return Ok(None);
    }

    Ok(Some(XeroCustomConnectionConfig {
        tenant_name,
        tenant_id: client_or_org_required("XERO_CC_TENANT_ID", tenant_id)?,
        client_id: client_or_org_required("XERO_CC_CLIENT_ID", client_id)?,
        client_secret: client_or_org_required("XERO_CC_CLIENT_SECRET", client_secret)?,
    }))
}

fn numbered_custom_connection(
    vars: &HashMap<String, String>,
    org_index: &str,
) -> Result<Option<XeroCustomConnectionConfig>> {
    let client_id = org_value(vars, org_index, "CLIENT_ID");
    let client_secret = org_value(vars, org_index, "CLIENT_SECRET");
    let tenant_id = org_value(vars, org_index, "TENANT_ID");
    let tenant_name = org_value(vars, org_index, "NAME");

    if client_id.is_none()
        && client_secret.is_none()
        && tenant_id.is_none()
        && tenant_name.is_none()
    {
        return Ok(None);
    }

    Ok(Some(XeroCustomConnectionConfig {
        tenant_name,
        tenant_id: client_or_org_required(&format!("XERO_ORG_{org_index}_TENANT_ID"), tenant_id)?,
        client_id: client_or_org_required(&format!("XERO_ORG_{org_index}_CLIENT_ID"), client_id)?,
        client_secret: client_or_org_required(
            &format!("XERO_ORG_{org_index}_CLIENT_SECRET"),
            client_secret,
        )?,
    }))
}

fn client_or_org_required(label: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| Error::Config(format!("missing required env var: {label}")))
}

fn numbered_org_indices(vars: &HashMap<String, String>) -> BTreeSet<u16> {
    vars.keys()
        .filter_map(|key| {
            let rest = key.strip_prefix("XERO_ORG_")?;
            let (index, suffix) = rest.split_once('_')?;
            if matches!(suffix, "CLIENT_ID" | "CLIENT_SECRET" | "TENANT_ID" | "NAME") {
                index.parse::<u16>().ok()
            } else {
                None
            }
        })
        .filter(|index| *index > 0)
        .collect()
}

fn first_present(vars: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| value(vars, key))
}

fn org_value(vars: &HashMap<String, String>, org_index: &str, suffix: &str) -> Option<String> {
    value(vars, &format!("XERO_ORG_{org_index}_{suffix}"))
}

fn value(vars: &HashMap<String, String>, key: &str) -> Option<String> {
    vars.get(key)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

fn custom_connection_org_index(vars: &HashMap<String, String>) -> Result<String> {
    let index = first_present(
        vars,
        &["XERO_CUSTOM_CONNECTION_ORG_INDEX", "XERO_ORG_INDEX"],
    )
    .unwrap_or_else(|| "1".to_owned());

    if index.parse::<u16>().is_ok_and(|n| n > 0) {
        Ok(index)
    } else {
        Err(Error::Config(format!(
            "XERO_CUSTOM_CONNECTION_ORG_INDEX must be a positive integer, got: {index}"
        )))
    }
}

fn custom_connection_enabled(flag: Option<&str>, connection_type: Option<&str>) -> Result<bool> {
    if flag
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
    {
        return Ok(true);
    }

    match connection_type.map(|v| v.trim().to_ascii_lowercase()) {
        Some(v)
            if matches!(
                v.as_str(),
                "custom" | "custom_connection" | "client_credentials" | "cc"
            ) =>
        {
            Ok(true)
        }
        Some(v) if matches!(v.as_str(), "pkce" | "oauth" | "oauth_pkce") => Ok(false),
        Some(v) => Err(Error::Config(format!(
            "unsupported XERO_CONNECTION_TYPE: {v}"
        ))),
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_connection_uses_org_credentials_without_pkce_credentials() {
        let cfg = AppConfig::from_pairs([
            ("DATABASE_URL", "postgresql://localhost/xero"),
            ("XERO_CONNECTION_TYPE", "custom"),
            ("XERO_ORG_1_NAME", "Aquatiq Australia Pty Ltd"),
            ("XERO_ORG_1_CLIENT_ID", "org-client-id"),
            ("XERO_ORG_1_CLIENT_SECRET", "org-client-secret"),
            ("XERO_ORG_1_TENANT_ID", "tenant-123"),
        ])
        .unwrap();

        assert!(cfg.xero_custom_connection);
        assert_eq!(cfg.xero_cc_client_id.as_deref(), Some("org-client-id"));
        assert_eq!(
            cfg.xero_cc_client_secret.as_deref(),
            Some("org-client-secret")
        );
        assert_eq!(cfg.xero_cc_tenant_id.as_deref(), Some("tenant-123"));
        assert_eq!(
            cfg.xero_cc_tenant_name.as_deref(),
            Some("Aquatiq Australia Pty Ltd")
        );
        assert!(cfg.xero_client_id.is_none());
        assert!(cfg.xero_client_secret.is_none());
    }

    #[test]
    fn custom_connection_can_select_numbered_org_credentials() {
        let cfg = AppConfig::from_pairs([
            ("DATABASE_URL", "postgresql://localhost/xero"),
            ("XERO_CONNECTION_TYPE", "custom"),
            ("XERO_CUSTOM_CONNECTION_ORG_INDEX", "2"),
            ("XERO_ORG_1_NAME", "Aquatiq Australia Pty Ltd"),
            ("XERO_ORG_1_CLIENT_ID", "au-client-id"),
            ("XERO_ORG_1_CLIENT_SECRET", "au-client-secret"),
            ("XERO_ORG_1_TENANT_ID", "au-tenant"),
            ("XERO_ORG_2_NAME", "Aquatiq New Zealand Limited"),
            ("XERO_ORG_2_CLIENT_ID", "nz-client-id"),
            ("XERO_ORG_2_CLIENT_SECRET", "nz-client-secret"),
            ("XERO_ORG_2_TENANT_ID", "nz-tenant"),
        ])
        .unwrap();

        assert!(cfg.xero_custom_connection);
        assert_eq!(cfg.xero_cc_client_id.as_deref(), Some("nz-client-id"));
        assert_eq!(
            cfg.xero_cc_client_secret.as_deref(),
            Some("nz-client-secret")
        );
        assert_eq!(cfg.xero_cc_tenant_id.as_deref(), Some("nz-tenant"));
        assert_eq!(
            cfg.xero_cc_tenant_name.as_deref(),
            Some("Aquatiq New Zealand Limited")
        );
    }

    #[test]
    fn custom_connection_collects_all_numbered_org_credentials() {
        let cfg = AppConfig::from_pairs([
            ("DATABASE_URL", "postgresql://localhost/xero"),
            ("XERO_CONNECTION_TYPE", "custom"),
            ("XERO_ORG_1_NAME", "Aquatiq Australia Pty Ltd"),
            ("XERO_ORG_1_CLIENT_ID", "au-client-id"),
            ("XERO_ORG_1_CLIENT_SECRET", "au-client-secret"),
            ("XERO_ORG_1_TENANT_ID", "au-tenant"),
            ("XERO_ORG_2_NAME", "Aquatiq New Zealand Limited"),
            ("XERO_ORG_2_CLIENT_ID", "nz-client-id"),
            ("XERO_ORG_2_CLIENT_SECRET", "nz-client-secret"),
            ("XERO_ORG_2_TENANT_ID", "nz-tenant"),
        ])
        .unwrap();

        assert_eq!(cfg.xero_cc_connections.len(), 2);
        assert!(cfg
            .xero_cc_connections
            .iter()
            .any(|conn| conn.tenant_id == "au-tenant" && conn.client_id == "au-client-id"));
        assert!(cfg
            .xero_cc_connections
            .iter()
            .any(|conn| conn.tenant_id == "nz-tenant" && conn.client_id == "nz-client-id"));
    }

    #[test]
    fn pkce_mode_still_requires_shared_client_credentials() {
        let err = AppConfig::from_pairs([
            ("DATABASE_URL", "postgresql://localhost/xero"),
            ("XERO_CONNECTION_TYPE", "pkce"),
        ])
        .unwrap_err();

        assert!(err.to_string().contains("XERO_CLIENT_ID"));
    }
}
