//! The `healthcheck` subcommand: prove auth + (optionally) storage are wired.
//!
//! Mints a `client_credentials` token for every configured tenant and, when
//! `--check-bucket` is passed, constructs the GCS sink (which authenticates
//! against `GOOGLE_APPLICATION_CREDENTIALS`) to confirm bucket access is set up.
//! No data is fetched or written.

use anyhow::Context;
use clap::Args;

use xero_common::{AppConfig, GcsConfig};

/// Flags for `xero healthcheck`.
#[derive(Debug, Args)]
pub struct HealthArgs {
    /// Also construct the GCS sink to verify bucket auth/config.
    #[arg(long)]
    check_bucket: bool,
}

/// Execute the `healthcheck` subcommand. Exits non-zero on the first failure.
pub async fn run(args: HealthArgs) -> anyhow::Result<()> {
    let cfg = AppConfig::from_env().context("config error — check your .env file")?;

    let auth = xero_auth::MultiTenantCustomConnectionClient::new(cfg.xero_cc_connections.clone());
    let tenants: Vec<String> = auth.tenant_ids().into_iter().map(str::to_owned).collect();

    if tenants.is_empty() {
        anyhow::bail!("no custom-connection tenants configured (set XERO_ORG_N_* / XERO_CC_*)");
    }

    let mut failures = 0usize;
    for tenant in &tenants {
        match auth.fetch_token_for_tenant(tenant).await {
            Ok(_) => println!("✓  token minted for tenant {tenant}"),
            Err(e) => {
                failures += 1;
                eprintln!("✗  token mint failed for tenant {tenant}: {e}");
            }
        }
    }

    if args.check_bucket {
        let gcs = GcsConfig::from_env().context("GCS config error (set GCS_BUCKET)")?;
        match xero_gcs::GcsRawSink::new(gcs.bucket.clone()).await {
            Ok(sink) => println!("✓  GCS sink ready for bucket {}", sink.bucket()),
            Err(e) => {
                failures += 1;
                eprintln!("✗  GCS sink build failed for bucket {}: {e}", gcs.bucket);
            }
        }
    }

    if failures > 0 {
        anyhow::bail!("healthcheck failed: {failures} check(s) did not pass");
    }
    println!("\n✓  All systems healthy");
    Ok(())
}
