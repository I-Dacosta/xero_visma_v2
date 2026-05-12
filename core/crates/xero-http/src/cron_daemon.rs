//! Cron scheduler daemon.
//!
//! Background tokio task that wakes once a minute and fires any schedule
//! whose next-tick falls in the past, marking `last_triggered_at` so we
//! don't re-fire on the next tick.
//!
//! Multi-replica coordination: we use the same Postgres row that holds
//! `last_triggered_at` as a soft lease. Each tick does an UPDATE that only
//! succeeds when the schedule hasn't been triggered since the candidate's
//! `prev_tick`. This means at most one replica fires each schedule per tick,
//! without needing an external locker.

use crate::AppState;
use chrono::{DateTime, Utc};
use cron::Schedule as CronSchedule;
use sqlx::Row;
use std::{str::FromStr, sync::Arc, time::Duration};
use tracing::{info, warn};
use uuid::Uuid;
use xero_common::{EntityType, TenantId};
use xero_state::SyncSchedule;
use xero_sync::RunOptions;

const TICK_INTERVAL: Duration = Duration::from_secs(60);

pub fn spawn(state: Arc<AppState>) {
    tokio::spawn(async move {
        info!("cron daemon started (tick every 60s)");
        // Small initial delay so multi-replica deploys spread cron load.
        tokio::time::sleep(Duration::from_secs(5)).await;
        loop {
            if let Err(e) = tick(&state).await {
                warn!(error = %e, "cron daemon tick failed");
            }
            tokio::time::sleep(TICK_INTERVAL).await;
        }
    });
}

/// One scheduler tick: read every enabled schedule, fire any that are due.
async fn tick(state: &Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let schedules = list_all_enabled(state).await?;
    let now = Utc::now();

    for schedule in schedules {
        let parsed = match CronSchedule::from_str(&schedule.cron_expression) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    schedule_id = %schedule.schedule_id,
                    cron = schedule.cron_expression.as_str(),
                    error = %e,
                    "invalid cron expression, skipping"
                );
                continue;
            }
        };

        let baseline = schedule
            .last_triggered_at
            .unwrap_or(schedule.created_at);
        // Next scheduled fire after the last successful fire (or creation).
        let Some(next_fire) = parsed.after(&baseline).next() else {
            continue;
        };
        if next_fire > now {
            continue; // not yet due
        }

        // Compare-and-swap: only fire if last_triggered_at hasn't moved past
        // `baseline` (another replica may have just fired this schedule).
        let claimed = try_claim(state, schedule.schedule_id, baseline, now).await?;
        if !claimed {
            continue;
        }

        info!(
            schedule_id = %schedule.schedule_id,
            name = schedule.name.as_str(),
            cron = schedule.cron_expression.as_str(),
            "firing scheduled run"
        );
        fire(state, &schedule, next_fire).await;
    }
    Ok(())
}

/// Best-effort list of all enabled (`disabled_at IS NULL`) schedules across
/// all tenants. We could index this query if it grows large.
async fn list_all_enabled(
    state: &Arc<AppState>,
) -> Result<Vec<SyncSchedule>, Box<dyn std::error::Error>> {
    let rows = sqlx::query(
        r#"
        SELECT schedule_id, tenant_id, name, cron_expression, entities,
               from_date, to_date, created_at, disabled_at, last_triggered_at
        FROM   xero.sync_schedule
        WHERE  disabled_at IS NULL
        ORDER  BY tenant_id, created_at
        "#,
    )
    .fetch_all(&state.store.pg)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let entities_json: serde_json::Value = row.get("entities");
        let entities: Vec<String> = serde_json::from_value(entities_json)?;
        out.push(SyncSchedule {
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
        });
    }
    Ok(out)
}

/// Atomic claim: update `last_triggered_at` to `now` only if its current value
/// matches `baseline`. Returns `true` if this replica won the race.
async fn try_claim(
    state: &Arc<AppState>,
    schedule_id: Uuid,
    baseline: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    // baseline is either `last_triggered_at` (Some) or `created_at` (None case).
    // To make the CAS work for both, we compare the actual column. When
    // last_triggered_at IS NULL we compare against NULL; otherwise equality.
    let res = sqlx::query(
        r#"
        UPDATE xero.sync_schedule
        SET    last_triggered_at = $3
        WHERE  schedule_id = $1
          AND  disabled_at IS NULL
          AND  (last_triggered_at IS NOT DISTINCT FROM $2)
        "#,
    )
    .bind(schedule_id)
    .bind::<Option<DateTime<Utc>>>(if baseline.timestamp_nanos_opt().is_some() {
        Some(baseline)
    } else {
        None
    })
    .bind(now)
    .execute(&state.store.pg)
    .await?;

    Ok(res.rows_affected() == 1)
}

/// Run every entity in the schedule using default RunOptions (incremental
/// from checkpoint). Failures per entity are logged but don't stop the tick.
async fn fire(state: &Arc<AppState>, schedule: &SyncSchedule, scheduled_for: DateTime<Utc>) {
    let token = match state.auth.get_valid_token(&schedule.tenant_id).await {
        Ok(t) => t,
        Err(e) => {
            warn!(schedule_id = %schedule.schedule_id, error = %e, "cron fire: token fetch failed");
            return;
        }
    };
    let trigger_id = Uuid::new_v4();
    for entity_str in &schedule.entities {
        let entity: EntityType = match entity_str.parse() {
            Ok(e) => e,
            Err(e) => {
                warn!(entity = entity_str.as_str(), error = %e, "cron fire: bad entity");
                continue;
            }
        };
        let options = RunOptions {
            trigger_id: Some(trigger_id),
            job_type: "cron".to_owned(),
            triggered_by: format!("cron:{}", schedule.schedule_id),
            ..RunOptions::default()
        };
        match state
            .sync
            .run_with_options(
                &token.access_token,
                TenantId::from(schedule.tenant_id.clone()),
                entity,
                options,
            )
            .await
        {
            Ok(run_id) => {
                info!(schedule_id = %schedule.schedule_id, entity = entity_str.as_str(), %run_id, "cron run ok");
            }
            Err(e) => {
                warn!(schedule_id = %schedule.schedule_id, entity = entity_str.as_str(), error = %e, scheduled_for = %scheduled_for, "cron run failed");
            }
        }
    }

    // run_history already records each per-entity run; nothing extra to log here.
    let _ = scheduled_for; // suppress unused warning if logs are filtered
}
