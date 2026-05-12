//! `xero-http` — Axum HTTP server exposing sync trigger + health endpoints.

mod backfill;
mod cron_daemon;

pub use backfill::spawn_worker as spawn_backfill_worker;
pub use cron_daemon::spawn as spawn_cron_daemon;

use backfill::{
    cancel_backfill_handler, get_backfill_handler, list_backfill_handler,
    run_next_backfill_chunk_handler, start_backfill_handler,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, NaiveDate, Utc};
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use uuid::Uuid;
use xero_auth::{PkceChallenge, TokenCache, TokenData, XeroOAuthClient};
use xero_common::{EntityType, TenantId};
use xero_state::{
    checkpoint, local_bronze, run_history, sync_schedule, NewSyncSchedule, StateStore,
};
use xero_sync::RunOptions;

/// Shared server state threaded through Axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub store: StateStore,
    pub auth: Arc<dyn xero_auth::TokenProvider>,
    pub sync: Arc<xero_sync::SyncExecutor>,
    pub oauth_client: Option<Arc<XeroOAuthClient>>,
    pub pkce_store: Arc<DashMap<String, String>>,
}

/// Build the Axum router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/readyz", get(readyz_handler))
        .route("/api/xero/login", get(login_handler))
        .route("/api/xero/callback", get(callback_handler))
        .route("/sync/:tenant/:entity", post(sync_handler))
        .route("/tenants/:tenant/sync/batch", post(sync_batch_handler))
        .route(
            "/tenants/:tenant/bronze/summary",
            get(bronze_summary_handler),
        )
        .route("/tenants/:tenant/checkpoints", get(checkpoints_handler))
        .route("/tenants/:tenant/runs", get(runs_handler))
        .route(
            "/tenants/:tenant/schedules",
            post(create_schedule_handler).get(list_schedule_handler),
        )
        .route(
            "/tenants/:tenant/schedules/:schedule_id/trigger",
            post(trigger_schedule_handler),
        )
        .route(
            "/tenants/:tenant/schedules/:schedule_id",
            axum::routing::delete(disable_schedule_handler),
        )
        .route(
            "/tenants/:tenant/backfill",
            post(start_backfill_handler).get(list_backfill_handler),
        )
        .route(
            "/tenants/:tenant/backfill/:plan_id",
            get(get_backfill_handler),
        )
        .route(
            "/tenants/:tenant/backfill/:plan_id/cancel",
            post(cancel_backfill_handler),
        )
        .route("/backfill/run-next", post(run_next_backfill_chunk_handler))
        .route("/tenants/:tenant/bq/replay", post(bq_replay_handler))
        .with_state(Arc::new(state))
        .layer(TraceLayer::new_for_http())
}

#[derive(Debug, Default, Deserialize)]
struct SyncRequest {
    modified_after: Option<DateTime<Utc>>,
    modified_before: Option<DateTime<Utc>>,
    business_date_after: Option<NaiveDate>,
    business_date_before: Option<NaiveDate>,
    job_type: Option<String>,
    triggered_by: Option<String>,
    /// Opt-in: advance the incremental watermark to `business_date_before`
    /// after a successful business-date run. Default `false` (gap-safe).
    #[serde(default)]
    advance_watermark: bool,
}

#[derive(Debug, Deserialize)]
struct BatchSyncRequest {
    entities: Vec<String>,
    modified_after: Option<DateTime<Utc>>,
    modified_before: Option<DateTime<Utc>>,
    business_date_after: Option<NaiveDate>,
    business_date_before: Option<NaiveDate>,
    job_type: Option<String>,
    triggered_by: Option<String>,
    #[serde(default)]
    advance_watermark: bool,
}

#[derive(Debug, Deserialize)]
struct CreateScheduleRequest {
    name: String,
    cron_expression: String,
    entities: Vec<String>,
    from_date: Option<NaiveDate>,
    to_date: Option<NaiveDate>,
}

#[derive(Debug, Default, Deserialize)]
struct BqReplayRequest {
    #[serde(default)]
    entity_type: Option<String>,
    #[serde(default = "default_replay_limit")]
    limit: i64,
}

fn default_replay_limit() -> i64 {
    500
}

async fn bq_replay_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
    body: Option<Json<BqReplayRequest>>,
) -> impl IntoResponse {
    let body = body.map(|b| b.0).unwrap_or_default();
    let limit = body.limit.clamp(1, 5000);
    match local_bronze::replay_bq_pending(
        &state.store.pg,
        &tenant_id,
        body.entity_type.as_deref(),
        limit,
    )
    .await
    {
        Ok((candidates, accepted)) => (
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "candidates": candidates,
                "accepted":   accepted,
                "remaining":  (candidates - accepted).max(0),
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

#[derive(Debug, Default, Deserialize)]
struct TriggerScheduleRequest {
    modified_after: Option<DateTime<Utc>>,
    modified_before: Option<DateTime<Utc>>,
    business_date_after: Option<NaiveDate>,
    business_date_before: Option<NaiveDate>,
    #[serde(default)]
    advance_watermark: bool,
}

#[derive(Debug, Default, Deserialize)]
struct RunsQuery {
    limit: Option<i64>,
    entity: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OAuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

async fn login_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let oauth_client = match &state.oauth_client {
        Some(client) => client,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "oauth login not enabled (custom connection mode)" })),
            )
                .into_response();
        }
    };

    let pkce = PkceChallenge::generate();
    let csrf_state = Uuid::new_v4().to_string();
    state
        .pkce_store
        .insert(csrf_state.clone(), pkce.verifier.clone());

    let auth_url = oauth_client.authorisation_url(&csrf_state, &pkce);
    Redirect::temporary(&auth_url).into_response()
}

async fn callback_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<OAuthCallbackQuery>,
) -> impl IntoResponse {
    if let Some(err) = query.error {
        let details = query.error_description.unwrap_or_default();
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("oauth callback error: {err} {details}") })),
        )
            .into_response();
    }

    let code = match query.code {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "missing required query param: code" })),
            )
                .into_response();
        }
    };

    let csrf_state = match query.state {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "missing required query param: state" })),
            )
                .into_response();
        }
    };

    let verifier = match state.pkce_store.remove(&csrf_state) {
        Some((_, verifier)) => verifier,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid or expired oauth state" })),
            )
                .into_response();
        }
    };

    let oauth_client = match &state.oauth_client {
        Some(client) => client,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "oauth callback not enabled (custom connection mode)" })),
            )
                .into_response();
        }
    };

    let token_payload = match oauth_client.exchange_code(&code, &verifier).await {
        Ok(payload) => payload,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("token exchange failed: {e}") })),
            )
                .into_response();
        }
    };

    let access_token = match token_payload.get("access_token").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v.to_owned(),
        _ => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "token exchange response missing access_token" })),
            )
                .into_response();
        }
    };

    let refresh_token = match token_payload.get("refresh_token").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v.to_owned(),
        _ => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "token exchange response missing refresh_token" })),
            )
                .into_response();
        }
    };

    let expires_in = token_payload
        .get("expires_in")
        .and_then(|v| v.as_i64())
        .unwrap_or(1800)
        .max(1);

    let scopes = token_payload
        .get("scope")
        .and_then(|v| v.as_str())
        .map(|s| {
            s.split_whitespace()
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let connections = match oauth_client.get_connections(&access_token).await {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "no Xero connections found for token" })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("fetch connections failed: {e}") })),
            )
                .into_response();
        }
    };

    let primary_connection = &connections[0];
    let tenant_id = match primary_connection.get("tenantId").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v.to_owned(),
        _ => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "connection payload missing tenantId" })),
            )
                .into_response();
        }
    };
    let tenant_name = primary_connection
        .get("tenantName")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned);

    let token_data = TokenData {
        access_token,
        refresh_token,
        expires_at: Utc::now() + chrono::Duration::seconds(expires_in),
        scopes,
        tenant_id: tenant_id.clone(),
    };

    let redis_url = match std::env::var("REDIS_URL") {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "missing required env var: REDIS_URL" })),
            )
                .into_response();
        }
    };

    let cache = match TokenCache::new(&redis_url) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("failed to initialize token cache: {e}") })),
            )
                .into_response();
        }
    };

    if let Err(e) = cache.set(&token_data).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("failed to cache token: {e}") })),
        )
            .into_response();
    }

    if let Err(e) =
        xero_state::tenant::upsert(&state.store.pg, &tenant_id, tenant_name.as_deref(), None).await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("failed to upsert tenant in postgres: {e}") })),
        )
            .into_response();
    }

    if let Err(e) = sqlx::query(
        r#"
        DELETE FROM xero.tenants
        WHERE tenant_id = 'PENDING_OAUTH_EXCHANGE'
          AND NOT EXISTS (
              SELECT 1
              FROM xero.tenants
              WHERE tenant_id = $1
          )
        "#,
    )
    .bind(&tenant_id)
    .execute(&state.store.pg)
    .await
    {
        tracing::warn!(error = %e, "failed to cleanup placeholder tenant row");
    }

    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "message": "oauth exchange complete and token cached"
        })),
    )
        .into_response()
}

async fn health_handler() -> impl IntoResponse {
    Json(json!({ "status": "ok", "service": "xero_service_v2" }))
}

async fn readyz_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.store.healthcheck().await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({ "status": "ok",    "postgres": "up" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "error", "postgres": e.to_string() })),
        )
            .into_response(),
    }
}

async fn sync_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_str, entity_str)): Path<(String, String)>,
    body: Option<Json<SyncRequest>>,
) -> impl IntoResponse {
    let tenant = TenantId::from(tenant_str);
    let entity: EntityType = match entity_str.parse() {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("unknown entity: {e}") })),
            )
                .into_response()
        }
    };
    let token = match state.auth.get_valid_token(tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };
    let body = body.map(|b| b.0).unwrap_or_default();
    let trigger_id = Uuid::new_v4();
    let options = RunOptions {
        modified_after: body.modified_after,
        modified_before: body.modified_before,
        business_date_after: body.business_date_after,
        business_date_before: body.business_date_before,
        trigger_id: Some(trigger_id),
        job_type: body.job_type.unwrap_or_else(|| "manual".to_owned()),
        triggered_by: body.triggered_by.unwrap_or_else(|| "manual-api".to_owned()),
        advance_watermark: body.advance_watermark,
    };

    match state
        .sync
        .run_with_options(&token.access_token, tenant, entity, options)
        .await
    {
        Ok(run_id) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "trigger_id": trigger_id, "run_id": run_id })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn sync_batch_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_str): Path<String>,
    Json(body): Json<BatchSyncRequest>,
) -> impl IntoResponse {
    if body.entities.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "entities must not be empty" })),
        )
            .into_response();
    }

    let entities = match parse_entities(&body.entities) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    let tenant = TenantId::from(tenant_str);
    let token = match state.auth.get_valid_token(tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    let mut results = Vec::with_capacity(entities.len());
    let trigger_id = Uuid::new_v4();
    for entity in entities {
        let options = RunOptions {
            modified_after: body.modified_after,
            modified_before: body.modified_before,
            business_date_after: body.business_date_after,
            business_date_before: body.business_date_before,
            trigger_id: Some(trigger_id),
            job_type: body
                .job_type
                .clone()
                .unwrap_or_else(|| "backfill".to_owned()),
            triggered_by: body
                .triggered_by
                .clone()
                .unwrap_or_else(|| "manual-batch".to_owned()),
            advance_watermark: body.advance_watermark,
        };

        match state
            .sync
            .run_with_options(&token.access_token, tenant.clone(), entity.clone(), options)
            .await
        {
            Ok(run_id) => {
                results.push(json!({ "entity": entity.as_str(), "status": "ok", "run_id": run_id }))
            }
            Err(e) => results.push(
                json!({ "entity": entity.as_str(), "status": "error", "error": e.to_string() }),
            ),
        }
    }

    (
        StatusCode::OK,
        Json(json!({ "status": "ok", "trigger_id": trigger_id, "results": results })),
    )
        .into_response()
}

async fn bronze_summary_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    match local_bronze::summary_for_tenant(&state.store.pg, &tenant_id).await {
        Ok(summary) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "summary": summary })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn checkpoints_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    match checkpoint::list_for_tenant(&state.store.pg, &tenant_id).await {
        Ok(checkpoints) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "checkpoints": checkpoints })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn runs_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
    Query(q): Query<RunsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let runs = match run_history::recent_runs(&state.store.pg, &tenant_id, limit).await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    let filtered = if let Some(entity) = q.entity {
        runs.into_iter()
            .filter(|r| r.entity_type == entity)
            .collect::<Vec<_>>()
    } else {
        runs
    };

    (
        StatusCode::OK,
        Json(json!({ "status": "ok", "runs": filtered })),
    )
        .into_response()
}

async fn create_schedule_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
    Json(body): Json<CreateScheduleRequest>,
) -> impl IntoResponse {
    if body.entities.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "entities must not be empty" })),
        )
            .into_response();
    }

    if body.name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "name must not be empty" })),
        )
            .into_response();
    }

    let entities = match parse_entities(&body.entities) {
        Ok(v) => v
            .into_iter()
            .map(|e| e.as_str().to_owned())
            .collect::<Vec<_>>(),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    let new_schedule = NewSyncSchedule {
        schedule_id: Uuid::new_v4(),
        tenant_id,
        name: body.name,
        cron_expression: body.cron_expression,
        entities,
        from_date: body.from_date,
        to_date: body.to_date,
    };

    match sync_schedule::create(&state.store.pg, &new_schedule).await {
        Ok(schedule) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "schedule": schedule })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn list_schedule_handler(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<String>,
) -> impl IntoResponse {
    match sync_schedule::list_by_tenant(&state.store.pg, &tenant_id).await {
        Ok(schedules) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "schedules": schedules })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn disable_schedule_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_id, schedule_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let schedule_id = match Uuid::parse_str(&schedule_id) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid schedule id: {e}") })),
            )
                .into_response()
        }
    };

    match sync_schedule::disable(&state.store.pg, &tenant_id, schedule_id).await {
        Ok(_) => (StatusCode::OK, Json(json!({ "status": "ok" }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn trigger_schedule_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_id, schedule_id)): Path<(String, String)>,
    body: Option<Json<TriggerScheduleRequest>>,
) -> impl IntoResponse {
    let schedule_id = match Uuid::parse_str(&schedule_id) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid schedule id: {e}") })),
            )
                .into_response()
        }
    };

    let schedule = match sync_schedule::get(&state.store.pg, &tenant_id, schedule_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "schedule not found" })),
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

    let entities = match parse_entities(&schedule.entities) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    let tenant = TenantId::from(tenant_id.clone());
    let token = match state.auth.get_valid_token(tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    let body = body.map(|b| b.0).unwrap_or_default();
    let mut results = Vec::with_capacity(entities.len());
    let trigger_id = Uuid::new_v4();
    for entity in entities {
        let options = RunOptions {
            modified_after: body.modified_after,
            modified_before: body.modified_before,
            business_date_after: body.business_date_after,
            business_date_before: body.business_date_before,
            trigger_id: Some(trigger_id),
            job_type: "scheduled".to_owned(),
            triggered_by: "manual-trigger".to_owned(),
            advance_watermark: body.advance_watermark,
        };

        match state
            .sync
            .run_with_options(&token.access_token, tenant.clone(), entity.clone(), options)
            .await
        {
            Ok(run_id) => {
                results.push(json!({ "entity": entity.as_str(), "status": "ok", "run_id": run_id }))
            }
            Err(e) => results.push(
                json!({ "entity": entity.as_str(), "status": "error", "error": e.to_string() }),
            ),
        }
    }

    if let Err(e) =
        sync_schedule::touch_last_triggered(&state.store.pg, &tenant_id, schedule_id).await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        Json(json!({ "status": "ok", "schedule_id": schedule_id, "trigger_id": trigger_id, "results": results })),
    )
        .into_response()
}

fn parse_entities(raw_entities: &[String]) -> xero_common::Result<Vec<EntityType>> {
    raw_entities
        .iter()
        .map(|e| e.parse::<EntityType>())
        .collect::<xero_common::Result<Vec<_>>>()
}

/// Start the HTTP server — call from `xero-cli serve`. Also spawns the
/// backfill worker so chunks queued via the API are processed immediately.
pub async fn serve(state: AppState, bind_addr: &str) -> xero_common::Result<()> {
    let addr: std::net::SocketAddr = bind_addr
        .parse()
        .map_err(|e: std::net::AddrParseError| xero_common::Error::Config(e.to_string()))?;

    let shared = Arc::new(state.clone());
    spawn_backfill_worker(Arc::clone(&shared));
    spawn_cron_daemon(shared);
    let app = router(state);
    tracing::info!("xero-http listening on {addr}");

    axum::serve(
        tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| xero_common::Error::Http(e.to_string()))?,
        app,
    )
    .await
    .map_err(|e| xero_common::Error::Http(e.to_string()))
}
