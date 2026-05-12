//! Backfill orchestrator — HTTP handlers + background worker.
//!
//! A backfill plan is decomposed into per-(entity, date-window) chunks. Chunks
//! are picked up by the `run_worker_loop` background task and executed as
//! normal sync runs (so all the existing retry/limiter/checkpoint code reuses).
//!
//! Synchronous escape hatch `POST /backfill/run-next` lets you single-step
//! one chunk for testing or to drain a queue from an external scheduler.

use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tracing::{info, warn};
use uuid::Uuid;
use xero_common::{EntityType, TenantId};
use xero_state::backfill::{self, NewBackfillPlan};
use xero_sync::RunOptions;

#[derive(Debug, Deserialize)]
pub(crate) struct StartBackfillRequest {
    pub entities: Vec<String>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    #[serde(default = "default_chunk_size")]
    pub chunk_size_days: i32,
    #[serde(default)]
    pub triggered_by: Option<String>,
    /// Opt-in: advance the incremental watermark on each chunk. Default `false`
    /// (gap-safe). When `true`, callers must accept the documented caveats.
    #[serde(default)]
    pub advance_watermark: bool,
}

fn default_chunk_size() -> i32 {
    30
}

pub(crate) async fn start_backfill_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
    Json(body): Json<StartBackfillRequest>,
) -> impl IntoResponse {
    if body.entities.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "entities must not be empty" })),
        )
            .into_response();
    }
    if body.end_date <= body.start_date {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "end_date must be strictly after start_date" })),
        )
            .into_response();
    }
    if body.chunk_size_days < 1 || body.chunk_size_days > 365 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "chunk_size_days must be 1..=365" })),
        )
            .into_response();
    }
    // Validate every entity now so we fail fast.
    for entity in &body.entities {
        if entity.parse::<EntityType>().is_err() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("unknown entity: {entity}") })),
            )
                .into_response();
        }
    }

    let new_plan = NewBackfillPlan {
        tenant_id,
        entity_types: body.entities,
        start_date: body.start_date,
        end_date: body.end_date,
        chunk_size_days: body.chunk_size_days,
        triggered_by: body
            .triggered_by
            .unwrap_or_else(|| "manual-backfill".to_owned()),
    };

    let plan_id = Uuid::new_v4();
    match backfill::create_plan_with_chunks(&state.store.pg, plan_id, &new_plan).await {
        Ok((plan, chunk_count)) => (
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "plan": plan,
                "chunks_created": chunk_count,
                "advance_watermark": body.advance_watermark,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub(crate) async fn list_backfill_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    match backfill::list_plans_for_tenant(&state.store.pg, &tenant_id, 50).await {
        Ok(plans) => (StatusCode::OK, Json(json!({ "status": "ok", "plans": plans }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub(crate) async fn get_backfill_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_id, plan_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let Ok(plan_id) = Uuid::parse_str(&plan_id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid plan_id" })),
        )
            .into_response();
    };
    let plan = match backfill::get_plan(&state.store.pg, plan_id).await {
        Ok(Some(p)) if p.tenant_id == tenant_id => p,
        Ok(Some(_)) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "plan not found for tenant" })),
            )
                .into_response()
        }
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "plan not found" })),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };
    let chunks = match backfill::list_chunks_for_plan(&state.store.pg, plan_id).await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };
    (
        StatusCode::OK,
        Json(json!({ "status": "ok", "plan": plan, "chunks": chunks })),
    )
        .into_response()
}

pub(crate) async fn cancel_backfill_handler(
    State(state): State<Arc<AppState>>,
    Path((_tenant_id, plan_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let Ok(plan_id) = Uuid::parse_str(&plan_id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid plan_id" })),
        )
            .into_response();
    };
    match backfill::cancel_plan(&state.store.pg, plan_id).await {
        Ok(_) => (StatusCode::OK, Json(json!({ "status": "ok" }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub(crate) async fn run_next_backfill_chunk_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match run_one_chunk(&state).await {
        Ok(Some(report)) => (StatusCode::OK, Json(report)).into_response(),
        Ok(None) => (
            StatusCode::OK,
            Json(json!({ "status": "idle", "message": "no pending chunks" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Pick one pending chunk and execute it. Returns the chunk report.
async fn run_one_chunk(state: &AppState) -> xero_common::Result<Option<Value>> {
    let Some(chunk) = backfill::claim_next_pending_chunk(&state.store.pg).await? else {
        return Ok(None);
    };

    let entity: EntityType = match chunk.entity_type.parse() {
        Ok(e) => e,
        Err(e) => {
            backfill::mark_chunk_failed(&state.store.pg, chunk.chunk_id, &e.to_string()).await?;
            return Ok(Some(json!({
                "status": "failed",
                "chunk_id": chunk.chunk_id,
                "entity": chunk.entity_type,
                "error": e.to_string(),
            })));
        }
    };

    let token = match state.auth.get_valid_token(&chunk.tenant_id).await {
        Ok(t) => t,
        Err(e) => {
            backfill::mark_chunk_failed(&state.store.pg, chunk.chunk_id, &e.to_string()).await?;
            return Ok(Some(json!({
                "status": "failed",
                "chunk_id": chunk.chunk_id,
                "entity": chunk.entity_type,
                "error": format!("auth: {e}"),
            })));
        }
    };

    let options = RunOptions {
        business_date_after: Some(chunk.window_start),
        business_date_before: Some(chunk.window_end),
        trigger_id: Some(chunk.plan_id),
        job_type: "backfill_chunk".to_owned(),
        triggered_by: format!("backfill:{}", chunk.plan_id),
        // Worker-mode is conservative — caller can flip later by retrying
        // the chunk manually if they want watermark advancement.
        advance_watermark: false,
        ..RunOptions::default()
    };

    match state
        .sync
        .run_with_options(
            &token.access_token,
            TenantId::from(chunk.tenant_id.clone()),
            entity,
            options,
        )
        .await
    {
        Ok(run_id) => {
            backfill::mark_chunk_succeeded(&state.store.pg, chunk.chunk_id, run_id).await?;
            Ok(Some(json!({
                "status": "ok",
                "chunk_id": chunk.chunk_id,
                "plan_id": chunk.plan_id,
                "entity": chunk.entity_type,
                "window_start": chunk.window_start,
                "window_end": chunk.window_end,
                "run_id": run_id,
            })))
        }
        Err(e) => {
            backfill::mark_chunk_failed(&state.store.pg, chunk.chunk_id, &e.to_string()).await?;
            Ok(Some(json!({
                "status": "failed",
                "chunk_id": chunk.chunk_id,
                "plan_id": chunk.plan_id,
                "entity": chunk.entity_type,
                "window_start": chunk.window_start,
                "window_end": chunk.window_end,
                "error": e.to_string(),
            })))
        }
    }
}

/// Background worker — loops forever picking up pending chunks. Spawn once
/// at server startup. Sleeps between idle ticks to avoid hammering pg.
pub fn spawn_worker(state: Arc<AppState>) {
    tokio::spawn(async move {
        info!("backfill worker started");
        let idle_sleep = Duration::from_secs(5);
        let error_sleep = Duration::from_secs(10);
        loop {
            match run_one_chunk(&state).await {
                Ok(Some(report)) => {
                    let status = report.get("status").and_then(Value::as_str).unwrap_or("?");
                    info!(status, report = ?report, "backfill chunk processed");
                }
                Ok(None) => {
                    tokio::time::sleep(idle_sleep).await;
                }
                Err(e) => {
                    warn!(error = %e, "backfill worker error, backing off");
                    tokio::time::sleep(error_sleep).await;
                }
            }
        }
    });
}
