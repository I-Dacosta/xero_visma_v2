use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;
use xero_common::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSchedule {
    pub schedule_id: Uuid,
    pub tenant_id: String,
    pub name: String,
    pub cron_expression: String,
    pub entities: Vec<String>,
    pub from_date: Option<NaiveDate>,
    pub to_date: Option<NaiveDate>,
    pub created_at: DateTime<Utc>,
    pub disabled_at: Option<DateTime<Utc>>,
    pub last_triggered_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NewSyncSchedule {
    pub schedule_id: Uuid,
    pub tenant_id: String,
    pub name: String,
    pub cron_expression: String,
    pub entities: Vec<String>,
    pub from_date: Option<NaiveDate>,
    pub to_date: Option<NaiveDate>,
}

pub async fn create(pg: &PgPool, new_schedule: &NewSyncSchedule) -> Result<SyncSchedule> {
    let row = sqlx::query(
        r#"
        INSERT INTO xero.sync_schedule
            (schedule_id, tenant_id, name, cron_expression, entities, from_date, to_date)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING schedule_id, tenant_id, name, cron_expression, entities,
                  from_date, to_date, created_at, disabled_at, last_triggered_at
        "#,
    )
    .bind(new_schedule.schedule_id)
    .bind(&new_schedule.tenant_id)
    .bind(&new_schedule.name)
    .bind(&new_schedule.cron_expression)
    .bind(
        serde_json::to_value(&new_schedule.entities)
            .map_err(|e| Error::StateStore(e.to_string()))?,
    )
    .bind(new_schedule.from_date)
    .bind(new_schedule.to_date)
    .fetch_one(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    row_to_schedule(row)
}

pub async fn list_by_tenant(pg: &PgPool, tenant_id: &str) -> Result<Vec<SyncSchedule>> {
    let rows = sqlx::query(
        r#"
        SELECT schedule_id, tenant_id, name, cron_expression, entities,
               from_date, to_date, created_at, disabled_at, last_triggered_at
        FROM   xero.sync_schedule
        WHERE  tenant_id = $1
          AND  disabled_at IS NULL
        ORDER  BY created_at DESC
        "#,
    )
    .bind(tenant_id)
    .fetch_all(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    rows.into_iter().map(row_to_schedule).collect()
}

pub async fn get(pg: &PgPool, tenant_id: &str, schedule_id: Uuid) -> Result<Option<SyncSchedule>> {
    let row = sqlx::query(
        r#"
        SELECT schedule_id, tenant_id, name, cron_expression, entities,
               from_date, to_date, created_at, disabled_at, last_triggered_at
        FROM   xero.sync_schedule
        WHERE  tenant_id = $1
          AND  schedule_id = $2
          AND  disabled_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(schedule_id)
    .fetch_optional(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    row.map(row_to_schedule).transpose()
}

pub async fn disable(pg: &PgPool, tenant_id: &str, schedule_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE xero.sync_schedule
        SET    disabled_at = NOW()
        WHERE  tenant_id = $1
          AND  schedule_id = $2
          AND  disabled_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(schedule_id)
    .execute(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(())
}

pub async fn touch_last_triggered(pg: &PgPool, tenant_id: &str, schedule_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE xero.sync_schedule
        SET    last_triggered_at = NOW()
        WHERE  tenant_id = $1
          AND  schedule_id = $2
          AND  disabled_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(schedule_id)
    .execute(pg)
    .await
    .map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(())
}

fn row_to_schedule(row: sqlx::postgres::PgRow) -> Result<SyncSchedule> {
    let entities_value: serde_json::Value = row.get("entities");
    let entities: Vec<String> =
        serde_json::from_value(entities_value).map_err(|e| Error::StateStore(e.to_string()))?;

    Ok(SyncSchedule {
        schedule_id: row.get("schedule_id"),
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        cron_expression: row.get("cron_expression"),
        entities,
        from_date: row.get("from_date"),
        to_date: row.get("to_date"),
        created_at: row.get("created_at"),
        disabled_at: row.get("disabled_at"),
        last_triggered_at: row.get("last_triggered_at"),
    })
}
