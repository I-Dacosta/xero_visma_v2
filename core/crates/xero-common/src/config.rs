use std::{
    collections::{BTreeSet, HashMap},
    env,
};

use crate::{Error, Result};

/// A single Xero Custom Connection (client-credentials) entry.
///
/// Each connection authenticates one tenant via `client_credentials`; no PKCE,
/// no redirect URI, no refresh-token storage is involved.
#[derive(Debug, Clone)]
pub struct XeroCustomConnectionConfig {
    pub tenant_name: Option<String>,
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: String,
}

/// Runtime configuration for the stateless raw-GCS uploader.
///
/// Custom-connection (client_credentials) auth is mandatory — there is no
/// Postgres, Redis, BigQuery, or PKCE/OAuth configuration. Load via
/// [`AppConfig::from_env`] after `dotenvy::dotenv()`.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub log_level: String,

    // ── Custom-connection (client_credentials) fields ──────────────────────
    /// Credentials for the selected (default) connection, resolved from
    /// `XERO_CC_*` or the `XERO_ORG_N_*` selected by the org index.
    pub xero_cc_client_id: Option<String>,
    pub xero_cc_client_secret: Option<String>,
    pub xero_cc_tenant_id: Option<String>,
    pub xero_cc_tenant_name: Option<String>,
    /// All configured custom connections (explicit `XERO_CC_*` plus every
    /// `XERO_ORG_N_*` block), deduplicated by tenant id.
    pub xero_cc_connections: Vec<XeroCustomConnectionConfig>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv(); // ignore missing .env in production

        Self::from_map(&env::vars().collect())
    }

    fn from_map(vars: &HashMap<String, String>) -> Result<Self> {
        // Custom-connection is the only supported mode. Validate XERO_CONNECTION_TYPE
        // (when present) and reject anything that is not a custom-connection alias.
        require_custom_connection(value(vars, "XERO_CONNECTION_TYPE").as_deref())?;

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

        Ok(Self {
            log_level: value(vars, "RUST_LOG").unwrap_or_else(|| "info".into()),
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

/// Destination configuration for the raw-GCS uploader: which bucket and key
/// prefix raw Xero payloads are written under.
#[derive(Debug, Clone)]
pub struct GcsConfig {
    /// Target GCS bucket name (required, `GCS_BUCKET`).
    pub bucket: String,
    /// Key prefix for raw objects (default `raw/xero`, `GCS_PREFIX`).
    pub prefix: String,
}

impl GcsConfig {
    /// Load from process environment after `dotenvy::dotenv()`.
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();
        Self::from_map(&env::vars().collect())
    }

    fn from_map(vars: &HashMap<String, String>) -> Result<Self> {
        let bucket = required_from(vars, "GCS_BUCKET")?;
        let prefix = value(vars, "GCS_PREFIX").unwrap_or_else(|| "raw/xero".to_owned());

        // The manifest namespace (root `_manifests/xero` in xero-gcs) sits
        // outside the raw prefix by design. Reject a GCS_PREFIX that would place
        // raw object keys inside that reserved namespace, which would silently
        // collide with manifest objects.
        if prefix.trim_start_matches('/').starts_with("_manifests") {
            return Err(Error::Config(format!(
                "GCS_PREFIX must not start with '_manifests' (reserved manifest namespace): {prefix}"
            )));
        }

        Ok(Self { bucket, prefix })
    }
}

/// Tuning knobs for the rolling/full sync windows and bounded concurrency.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Incremental sync look-back window in days (default 3, `SYNC_WINDOW_DAYS`).
    pub window_days: i64,
    /// Tight rolling-full re-sync horizon in days
    /// (default 30, `SYNC_ROLLING_FULL_TIGHT_DAYS`).
    pub rolling_full_tight_days: i64,
    /// Wide rolling-full re-sync horizon in days
    /// (default 90, `SYNC_ROLLING_FULL_WIDE_DAYS`).
    pub rolling_full_wide_days: i64,
    /// Max concurrent in-flight sync tasks (default 6, `SYNC_MAX_CONCURRENT`).
    pub max_concurrent: usize,
}

impl SyncConfig {
    /// Load from process environment after `dotenvy::dotenv()`.
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();
        Self::from_map(&env::vars().collect())
    }

    fn from_map(vars: &HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            window_days: parsed_or(vars, "SYNC_WINDOW_DAYS", 3)?,
            rolling_full_tight_days: parsed_or(vars, "SYNC_ROLLING_FULL_TIGHT_DAYS", 30)?,
            rolling_full_wide_days: parsed_or(vars, "SYNC_ROLLING_FULL_WIDE_DAYS", 90)?,
            max_concurrent: parsed_or(vars, "SYNC_MAX_CONCURRENT", 6)?,
        })
    }
}

/// Parse an optional env var into `T`, falling back to `default` when unset or
/// empty. Returns a config error (never panics) when the value is malformed.
fn parsed_or<T>(vars: &HashMap<String, String>, key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match value(vars, key) {
        Some(raw) => raw
            .parse::<T>()
            .map_err(|err| Error::Config(format!("invalid {key}: {err}"))),
        None => Ok(default),
    }
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

/// Custom-connection (client_credentials) is the only supported auth mode.
///
/// `XERO_CONNECTION_TYPE` is optional; when set it must name a custom-connection
/// alias. Any other value (including the legacy `pkce`/`oauth`/`hybrid` modes) is
/// a hard configuration error.
fn require_custom_connection(connection_type: Option<&str>) -> Result<()> {
    match connection_type.map(|v| v.trim().to_ascii_lowercase()) {
        None => Ok(()),
        Some(v)
            if matches!(
                v.as_str(),
                "custom" | "custom_connection" | "client_credentials" | "cc"
            ) =>
        {
            Ok(())
        }
        Some(v) => Err(Error::Config(format!(
            "unsupported XERO_CONNECTION_TYPE: {v} (custom-connection is mandatory)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_connection_uses_org_credentials() {
        let cfg = AppConfig::from_pairs([
            ("XERO_CONNECTION_TYPE", "custom"),
            ("XERO_ORG_1_NAME", "Aquatiq Australia Pty Ltd"),
            ("XERO_ORG_1_CLIENT_ID", "org-client-id"),
            ("XERO_ORG_1_CLIENT_SECRET", "org-client-secret"),
            ("XERO_ORG_1_TENANT_ID", "tenant-123"),
        ])
        .unwrap();

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
    }

    #[test]
    fn custom_connection_defaults_without_connection_type() {
        // XERO_CONNECTION_TYPE is optional — custom-connection is implied.
        let cfg = AppConfig::from_pairs([
            ("XERO_ORG_1_CLIENT_ID", "org-client-id"),
            ("XERO_ORG_1_CLIENT_SECRET", "org-client-secret"),
            ("XERO_ORG_1_TENANT_ID", "tenant-123"),
        ])
        .unwrap();

        assert_eq!(cfg.xero_cc_client_id.as_deref(), Some("org-client-id"));
        assert_eq!(cfg.xero_cc_tenant_id.as_deref(), Some("tenant-123"));
    }

    #[test]
    fn custom_connection_can_select_numbered_org_credentials() {
        let cfg = AppConfig::from_pairs([
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
    fn missing_credentials_is_a_config_error() {
        let err = AppConfig::from_pairs([("XERO_CONNECTION_TYPE", "custom")]).unwrap_err();
        assert!(err
            .to_string()
            .contains("missing custom connection credentials"));
    }

    #[test]
    fn non_custom_connection_type_is_rejected() {
        for ty in ["pkce", "oauth", "oauth_pkce", "hybrid", "mixed", "bogus"] {
            let err = AppConfig::from_pairs([
                ("XERO_CONNECTION_TYPE", ty),
                ("XERO_ORG_1_CLIENT_ID", "org-client-id"),
                ("XERO_ORG_1_CLIENT_SECRET", "org-client-secret"),
                ("XERO_ORG_1_TENANT_ID", "tenant-123"),
            ])
            .unwrap_err();
            assert!(
                err.to_string().contains("unsupported XERO_CONNECTION_TYPE"),
                "expected rejection for connection type {ty}, got: {err}"
            );
        }
    }

    fn map<const N: usize>(pairs: [(&str, &str); N]) -> HashMap<String, String> {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect()
    }

    // ---- GcsConfig (raw-GCS) ----

    #[test]
    fn gcs_config_uses_default_prefix() {
        let cfg = GcsConfig::from_map(&map([("GCS_BUCKET", "my-bucket")])).unwrap();
        assert_eq!(cfg.bucket, "my-bucket");
        assert_eq!(cfg.prefix, "raw/xero");
    }

    #[test]
    fn gcs_config_overrides_prefix() {
        let cfg = GcsConfig::from_map(&map([
            ("GCS_BUCKET", "my-bucket"),
            ("GCS_PREFIX", "staging/xero"),
        ]))
        .unwrap();
        assert_eq!(cfg.bucket, "my-bucket");
        assert_eq!(cfg.prefix, "staging/xero");
    }

    #[test]
    fn gcs_config_requires_bucket() {
        let err = GcsConfig::from_map(&map([("GCS_PREFIX", "staging/xero")])).unwrap_err();
        assert!(err.to_string().contains("GCS_BUCKET"));
    }

    #[test]
    fn gcs_config_rejects_manifests_prefix() {
        let err = GcsConfig::from_map(&map([
            ("GCS_BUCKET", "my-bucket"),
            ("GCS_PREFIX", "_manifests/xero"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("_manifests"));
    }

    #[test]
    fn gcs_config_rejects_leading_slash_manifests_prefix() {
        let err = GcsConfig::from_map(&map([
            ("GCS_BUCKET", "my-bucket"),
            ("GCS_PREFIX", "/_manifests/xero"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("_manifests"));
    }

    // ---- SyncConfig (raw-GCS) ----

    #[test]
    fn sync_config_uses_defaults() {
        let cfg = SyncConfig::from_map(&map([])).unwrap();
        assert_eq!(cfg.window_days, 3);
        assert_eq!(cfg.rolling_full_tight_days, 30);
        assert_eq!(cfg.rolling_full_wide_days, 90);
        assert_eq!(cfg.max_concurrent, 6);
    }

    #[test]
    fn sync_config_overrides_all_fields() {
        let cfg = SyncConfig::from_map(&map([
            ("SYNC_WINDOW_DAYS", "7"),
            ("SYNC_ROLLING_FULL_TIGHT_DAYS", "45"),
            ("SYNC_ROLLING_FULL_WIDE_DAYS", "180"),
            ("SYNC_MAX_CONCURRENT", "12"),
        ]))
        .unwrap();
        assert_eq!(cfg.window_days, 7);
        assert_eq!(cfg.rolling_full_tight_days, 45);
        assert_eq!(cfg.rolling_full_wide_days, 180);
        assert_eq!(cfg.max_concurrent, 12);
    }

    #[test]
    fn sync_config_rejects_malformed_value() {
        let err = SyncConfig::from_map(&map([("SYNC_MAX_CONCURRENT", "lots")])).unwrap_err();
        assert!(err.to_string().contains("SYNC_MAX_CONCURRENT"));
    }
}
