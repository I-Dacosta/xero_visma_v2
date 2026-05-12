use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use sqlx::Row;
use xero_common::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantRecord {
    pub tenant_id: String,
    pub tenant_name: Option<String>,
    pub short_code: Option<String>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub async fn list_active(pg: &PgPool) -> Result<Vec<TenantRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, tenant_name, short_code, is_active, created_at, updated_at
        FROM   xero.tenants
        WHERE  is_active = TRUE
        ORDER  BY tenant_name
        "#,
    )
    .fetch_all(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(rows
        .into_iter()
        .map(|r| TenantRecord {
            tenant_id: r.get("tenant_id"),
            tenant_name: r.get("tenant_name"),
            short_code: r.get("short_code"),
            is_active: r.get("is_active"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

pub async fn upsert(
    pg: &PgPool,
    tenant_id: &str,
    tenant_name: Option<&str>,
    short_code: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO xero.tenants (tenant_id, tenant_name, short_code)
        VALUES ($1, $2, $3)
        ON CONFLICT (tenant_id) DO UPDATE SET
            tenant_name = COALESCE(EXCLUDED.tenant_name, xero.tenants.tenant_name),
            short_code  = COALESCE(EXCLUDED.short_code,  xero.tenants.short_code),
            updated_at  = NOW()
        "#,
    )
    .bind(tenant_id)
    .bind(tenant_name)
    .bind(short_code)
    .execute(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(())
}
