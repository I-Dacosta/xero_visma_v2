//! Per-tenant adaptive rate limiting for the Xero Accounting API.
//!
//! Xero documented limits (per tenant unless noted):
//!   - 60 calls / minute
//!   - 5000 calls / day
//!   - 10 concurrent calls (app-wide, shared with other tenants)
//!
//! Two layers:
//!
//!  1. **Local (in-process):** `TenantRateLimiter` caps in-flight concurrency,
//!     reads response headers to track remaining quota, and proactively sleeps
//!     when `min_remaining` is dangerously low so the next request doesn't 429.
//!
//!  2. **Distributed (cross-pod):** `RateLimitCoordinator` is a trait whose
//!     `RedisRateLimitCoordinator` impl shares 429 pause signals across
//!     replicas using a single Redis key per tenant. Any pod that observes a
//!     429 publishes `xero_rl:{tenant}:pause_until` with PEXPIRE; every pod
//!     consults that key before issuing a request. Default impl is `NoOp` so
//!     local-only deployments keep working without Redis.

use async_trait::async_trait;
use deadpool_redis::{redis::AsyncCommands, Pool as RedisPool};
use reqwest::header::HeaderMap;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{info, trace, warn};

const PROACTIVE_THROTTLE_THRESHOLD: u32 = 5;
const MAX_CONCURRENT_PER_TENANT: u32 = 6;
const REDIS_KEY_PREFIX: &str = "xero_rl";

/// 429 backoff: base * 2^(n-1) up to MAX_BACKOFF, plus jitter. Mirrors the
/// reqwest community pattern but applied per-tenant so one tenant's bad
/// minute doesn't slow another's traffic.
const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
const BACKOFF_JITTER_MAX: Duration = Duration::from_millis(250);

// ── Distributed coordinator (cross-pod 429 signal) ───────────────────────────

/// Shared rate-limit signal across replicas. Implementations broadcast 429
/// pauses so that every pod sees the back-off, not just the unlucky one.
#[async_trait]
pub trait RateLimitCoordinator: Send + Sync + std::fmt::Debug {
    /// Returns `Some(duration)` if this tenant is currently paused by any pod.
    async fn pause_until(&self, tenant: &str) -> Option<Duration>;

    /// Publish a pause to all pods. `dur` is the remaining `Retry-After`.
    async fn publish_pause(&self, tenant: &str, dur: Duration);
}

#[derive(Debug, Default)]
pub struct NoOpCoordinator;

#[async_trait]
impl RateLimitCoordinator for NoOpCoordinator {
    async fn pause_until(&self, _tenant: &str) -> Option<Duration> {
        None
    }
    async fn publish_pause(&self, _tenant: &str, _dur: Duration) {}
}

#[derive(Debug, Clone)]
pub struct RedisRateLimitCoordinator {
    pool: RedisPool,
}

impl RedisRateLimitCoordinator {
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn key(tenant: &str) -> String {
        format!("{REDIS_KEY_PREFIX}:{tenant}:pause_until")
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

#[async_trait]
impl RateLimitCoordinator for RedisRateLimitCoordinator {
    async fn pause_until(&self, tenant: &str) -> Option<Duration> {
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!(tenant, error = %e, "rate-limit coord: redis get failed (fail-open)");
                return None;
            }
        };
        let key = Self::key(tenant);
        let value: Option<String> = conn.get(&key).await.ok().flatten();
        let expires_ms: u64 = value.and_then(|v| v.parse().ok())?;
        let now = Self::now_ms();
        if expires_ms > now {
            Some(Duration::from_millis(expires_ms - now))
        } else {
            None
        }
    }

    async fn publish_pause(&self, tenant: &str, dur: Duration) {
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!(tenant, error = %e, "rate-limit coord: redis get failed (fail-open)");
                return;
            }
        };
        let key = Self::key(tenant);
        let expires_ms = Self::now_ms() + dur.as_millis() as u64;
        // PSETEX = SET with PX. We use SET … PX to be explicit & atomic.
        let res: Result<(), _> = deadpool_redis::redis::cmd("SET")
            .arg(&key)
            .arg(expires_ms.to_string())
            .arg("PX")
            .arg(dur.as_millis() as u64)
            .query_async(&mut conn)
            .await;
        if let Err(e) = res {
            warn!(tenant, error = %e, "rate-limit coord: publish_pause failed");
        }
    }
}

// ── Coordinator registry (initialised once at startup) ───────────────────────

static COORDINATOR: OnceLock<Arc<dyn RateLimitCoordinator>> = OnceLock::new();

/// Install the distributed coordinator. Call once at startup before any
/// sync runs. Subsequent calls are ignored.
pub fn init_coordinator(coord: Arc<dyn RateLimitCoordinator>) {
    let _ = COORDINATOR.set(coord);
}

fn coordinator() -> Option<&'static Arc<dyn RateLimitCoordinator>> {
    COORDINATOR.get()
}

// ── Local in-process limiter ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct RateLimitState {
    pub min_remaining: Option<u32>,
    pub day_remaining: Option<u32>,
    pub app_min_remaining: Option<u32>,
    /// Consecutive 429s observed since the last 2xx. Reset on success.
    pub consecutive_429: u32,
    /// Total 429s observed for this tenant since process start.
    pub total_429: u64,
    /// Total successful requests for this tenant since process start.
    pub total_success: u64,
}

/// Snapshot of one tenant's rate-limit state, suitable for the observability
/// endpoint. `tenant_id` is duplicated into the snapshot so a caller can
/// flatten multiple tenants into one JSON payload without joining.
#[derive(Debug, Clone, Default)]
pub struct TenantRateLimitSnapshot {
    pub tenant_id: String,
    pub in_flight: u32,
    pub state: RateLimitState,
}

pub struct TenantRateLimiter {
    state: Mutex<RateLimitState>,
    in_flight: Mutex<u32>,
    tenant_id: String,
}

static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<TenantRateLimiter>>>> = OnceLock::new();

/// Get (or lazily create) the shared limiter for a tenant.
pub fn for_tenant(tenant_id: &str) -> Arc<TenantRateLimiter> {
    let reg = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = reg.lock().expect("rate-limit registry poisoned");
    map.entry(tenant_id.to_owned())
        .or_insert_with(|| Arc::new(TenantRateLimiter::new(tenant_id.to_owned())))
        .clone()
}

impl TenantRateLimiter {
    fn new(tenant_id: String) -> Self {
        Self {
            state: Mutex::new(RateLimitState::default()),
            in_flight: Mutex::new(0),
            tenant_id,
        }
    }

    /// Block until safe to issue another request. Returns a RAII permit;
    /// in-flight counter is decremented on Drop.
    pub async fn acquire(self: &Arc<Self>) -> AcquiredPermit {
        // 1. Cross-pod pause signal (if Redis coordinator installed).
        if let Some(coord) = coordinator() {
            if let Some(wait) = coord.pause_until(&self.tenant_id).await {
                trace!(tenant = %self.tenant_id, wait_ms = wait.as_millis() as u64, "distributed pause");
                tokio::time::sleep(wait).await;
            }
        }

        // 2. Local in-flight concurrency cap.
        loop {
            {
                let in_flight = self.in_flight.lock().expect("in_flight poisoned");
                if *in_flight < MAX_CONCURRENT_PER_TENANT {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // 3. Local proactive throttle from observed min_remaining.
        if let Some(wait) = self.proactive_wait() {
            trace!(tenant = %self.tenant_id, wait_ms = wait.as_millis() as u64, "proactive throttle (min_remaining low)");
            tokio::time::sleep(wait).await;
        }

        *self.in_flight.lock().expect("in_flight poisoned") += 1;
        AcquiredPermit {
            limiter: Arc::clone(self),
        }
    }

    fn proactive_wait(&self) -> Option<Duration> {
        let s = self.state.lock().ok()?;
        let rem = s.min_remaining?;
        if rem == 0 {
            Some(Duration::from_secs(2))
        } else if rem <= PROACTIVE_THROTTLE_THRESHOLD {
            let factor = PROACTIVE_THROTTLE_THRESHOLD + 1 - rem;
            Some(Duration::from_millis(150u64 * factor as u64))
        } else {
            None
        }
    }

    fn release(&self) {
        if let Ok(mut v) = self.in_flight.lock() {
            if *v > 0 {
                *v -= 1;
            }
        }
    }

    pub fn update_from_headers(&self, headers: &HeaderMap) {
        let mut state = match self.state.lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        if let Some(v) = parse_header_u32(headers, "x-minlimit-remaining") {
            state.min_remaining = Some(v);
        }
        if let Some(v) = parse_header_u32(headers, "x-daylimit-remaining") {
            state.day_remaining = Some(v);
        }
        if let Some(v) = parse_header_u32(headers, "x-appminlimit-remaining") {
            state.app_min_remaining = Some(v);
        }
    }

    /// Tenant id this limiter is bound to (useful for callers publishing
    /// distributed signals).
    #[allow(dead_code)]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Publish a Retry-After pause to all pods via the distributed coordinator.
    /// No-op if no Redis coordinator was installed.
    pub async fn publish_pause(&self, dur: Duration) {
        if let Some(coord) = coordinator() {
            coord.publish_pause(&self.tenant_id, dur).await;
        }
    }

    /// Snapshot the last observed limit state. Surface via a future
    /// `/tenants/:t/rate-limit` observability endpoint.
    pub fn snapshot(&self) -> RateLimitState {
        self.state.lock().map(|s| *s).unwrap_or_default()
    }

    /// Full snapshot including in-flight count and tenant id, for the
    /// /metrics/rate-limit endpoint.
    pub fn snapshot_with_inflight(&self) -> TenantRateLimitSnapshot {
        let state = self.snapshot();
        let in_flight = self
            .in_flight
            .lock()
            .map(|v| *v)
            .unwrap_or(0);
        TenantRateLimitSnapshot {
            tenant_id: self.tenant_id.clone(),
            in_flight,
            state,
        }
    }

    /// Record a successful (2xx) response — resets the 429 backoff chain.
    pub fn record_success(&self) {
        if let Ok(mut s) = self.state.lock() {
            if s.consecutive_429 > 0 {
                info!(
                    tenant = %self.tenant_id,
                    "rate-limit backoff chain ended after {} consecutive 429s",
                    s.consecutive_429,
                );
            }
            s.consecutive_429 = 0;
            s.total_success = s.total_success.saturating_add(1);
        }
    }

    /// Record a 429 response and compute the backoff duration the caller
    /// should sleep before its next attempt.
    ///
    /// Honours an explicit `Retry-After` value when provided. Otherwise
    /// applies exponential backoff with jitter:
    ///
    ///     wait = min(BACKOFF_BASE * 2^(n-1), BACKOFF_MAX) + rand[0..JITTER_MAX]
    ///
    /// where `n` is the consecutive-429 count after recording this one.
    /// Publishes the wait via the distributed coordinator so other pods see
    /// the back-off too.
    pub async fn record_429(&self, retry_after: Option<Duration>) -> Duration {
        let wait = {
            let mut s = match self.state.lock() {
                Ok(s) => s,
                Err(_) => return retry_after.unwrap_or(BACKOFF_BASE),
            };
            s.consecutive_429 = s.consecutive_429.saturating_add(1);
            s.total_429 = s.total_429.saturating_add(1);
            compute_429_wait(s.consecutive_429, retry_after)
        };
        info!(
            tenant = %self.tenant_id,
            wait_ms = wait.as_millis() as u64,
            "429 received — applying exponential backoff",
        );
        self.publish_pause(wait).await;
        wait
    }
}

fn compute_429_wait(consecutive_429: u32, retry_after: Option<Duration>) -> Duration {
    if let Some(ra) = retry_after {
        // Server-provided hint trumps our heuristic. Capped at MAX to avoid
        // a malformed/huge Retry-After hanging the worker indefinitely.
        return ra.min(BACKOFF_MAX);
    }
    let n = consecutive_429.max(1);
    // Exponential, capped. Using checked_shl to avoid overflow for big n.
    let shift = (n - 1).min(20);
    let scaled = BACKOFF_BASE
        .checked_mul(1u32 << shift)
        .unwrap_or(BACKOFF_MAX);
    let base = scaled.min(BACKOFF_MAX);
    let jitter = jitter_duration(BACKOFF_JITTER_MAX);
    base + jitter
}

fn jitter_duration(max: Duration) -> Duration {
    // Cheap PRNG: nanos of "now" mod max. Good enough for jitter; we don't
    // need cryptographic randomness here.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let max_ms = max.as_millis() as u64;
    if max_ms == 0 {
        Duration::from_millis(0)
    } else {
        Duration::from_millis(nanos % max_ms)
    }
}

/// Snapshot every registered tenant. Used by the /metrics/rate-limit endpoint.
pub fn snapshot_all_tenants() -> Vec<TenantRateLimitSnapshot> {
    let reg = match REGISTRY.get() {
        Some(r) => r,
        None => return Vec::new(),
    };
    let map = match reg.lock() {
        Ok(m) => m,
        Err(_) => return Vec::new(),
    };
    map.values().map(|l| l.snapshot_with_inflight()).collect()
}

pub struct AcquiredPermit {
    limiter: Arc<TenantRateLimiter>,
}

impl Drop for AcquiredPermit {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

fn parse_header_u32(headers: &HeaderMap, name: &str) -> Option<u32> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn registry_returns_same_arc_for_same_tenant() {
        let a = for_tenant("t1");
        let b = for_tenant("t1");
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn registry_returns_distinct_for_different_tenants() {
        let a = for_tenant("t-a");
        let b = for_tenant("t-b");
        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn update_from_headers_parses_known_keys() {
        let lim = TenantRateLimiter::new("unit".into());
        let mut h = HeaderMap::new();
        h.insert("x-minlimit-remaining", HeaderValue::from_static("3"));
        h.insert("x-daylimit-remaining", HeaderValue::from_static("4990"));
        h.insert("x-appminlimit-remaining", HeaderValue::from_static("9"));
        lim.update_from_headers(&h);
        let s = lim.snapshot();
        assert_eq!(s.min_remaining, Some(3));
        assert_eq!(s.day_remaining, Some(4990));
        assert_eq!(s.app_min_remaining, Some(9));
    }

    #[test]
    fn proactive_wait_kicks_in_below_threshold() {
        let lim = TenantRateLimiter::new("unit".into());
        let mut h = HeaderMap::new();
        h.insert("x-minlimit-remaining", HeaderValue::from_static("2"));
        lim.update_from_headers(&h);
        assert!(lim.proactive_wait().is_some());
    }

    #[test]
    fn proactive_wait_silent_above_threshold() {
        let lim = TenantRateLimiter::new("unit".into());
        let mut h = HeaderMap::new();
        h.insert("x-minlimit-remaining", HeaderValue::from_static("30"));
        lim.update_from_headers(&h);
        assert!(lim.proactive_wait().is_none());
    }

    #[tokio::test]
    async fn noop_coordinator_never_pauses() {
        let coord = NoOpCoordinator;
        assert!(coord.pause_until("any").await.is_none());
        coord.publish_pause("any", Duration::from_secs(10)).await;
        assert!(coord.pause_until("any").await.is_none());
    }

    #[test]
    fn compute_429_wait_uses_retry_after_when_present() {
        let w = compute_429_wait(1, Some(Duration::from_secs(7)));
        assert_eq!(w, Duration::from_secs(7));
    }

    #[test]
    fn compute_429_wait_caps_retry_after_at_max() {
        let w = compute_429_wait(1, Some(Duration::from_secs(3600)));
        assert!(w <= BACKOFF_MAX);
    }

    #[test]
    fn compute_429_wait_exponential_growth() {
        let w1 = compute_429_wait(1, None);
        let w2 = compute_429_wait(2, None);
        let w3 = compute_429_wait(3, None);
        // Within jitter envelope, each subsequent wait should be larger.
        // Strip jitter by checking the lower bound.
        assert!(w2 >= w1.saturating_sub(BACKOFF_JITTER_MAX));
        assert!(w3 >= w2.saturating_sub(BACKOFF_JITTER_MAX));
        // And the wait should never exceed MAX + JITTER.
        assert!(w3 <= BACKOFF_MAX + BACKOFF_JITTER_MAX);
    }

    #[test]
    fn compute_429_wait_caps_at_max_for_long_chains() {
        // After many consecutive 429s, the wait should saturate at MAX.
        let w = compute_429_wait(50, None);
        assert!(w <= BACKOFF_MAX + BACKOFF_JITTER_MAX);
    }

    #[tokio::test]
    async fn record_429_then_success_resets_chain() {
        let lim = TenantRateLimiter::new("rt-test".into());
        let _wait = lim.record_429(None).await;
        let _wait = lim.record_429(None).await;
        assert_eq!(lim.snapshot().consecutive_429, 2);
        assert_eq!(lim.snapshot().total_429, 2);

        lim.record_success();
        assert_eq!(lim.snapshot().consecutive_429, 0);
        // Cumulative count is preserved.
        assert_eq!(lim.snapshot().total_429, 2);
        assert_eq!(lim.snapshot().total_success, 1);
    }

    #[tokio::test]
    async fn record_429_with_retry_after_uses_hint() {
        let lim = TenantRateLimiter::new("rt-hint".into());
        let wait = lim.record_429(Some(Duration::from_secs(3))).await;
        assert_eq!(wait, Duration::from_secs(3));
    }

    #[test]
    fn snapshot_with_inflight_returns_tenant_id() {
        let lim = TenantRateLimiter::new("rt-snap".into());
        let snap = lim.snapshot_with_inflight();
        assert_eq!(snap.tenant_id, "rt-snap");
        assert_eq!(snap.in_flight, 0);
    }
}
