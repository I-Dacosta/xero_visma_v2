//! Per-tenant adaptive rate limiting for the Xero Accounting API.
//!
//! Xero documented limits (per tenant unless noted):
//!   - 60 calls / minute
//!   - 5000 calls / day
//!   - 10 concurrent calls (app-wide, shared with other tenants)
//!
//! A single in-process [`TenantRateLimiter`] per tenant caps in-flight
//! concurrency, reads response headers to track remaining quota, and
//! proactively sleeps when `min_remaining` is dangerously low so the next
//! request doesn't 429. The uploader is stateless and single-instance, so no
//! cross-pod coordination is needed.

use reqwest::header::HeaderMap;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tracing::trace;

const PROACTIVE_THROTTLE_THRESHOLD: u32 = 5;
const MAX_CONCURRENT_PER_TENANT: u32 = 6;

// ── Local in-process limiter ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
struct RateLimitState {
    min_remaining: Option<u32>,
    day_remaining: Option<u32>,
    app_min_remaining: Option<u32>,
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
        // 1. Local in-flight concurrency cap.
        loop {
            {
                let in_flight = self.in_flight.lock().expect("in_flight poisoned");
                if *in_flight < MAX_CONCURRENT_PER_TENANT {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // 2. Local proactive throttle from observed min_remaining.
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
        let s = lim.state.lock().expect("state poisoned");
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
}
