//! Retry policy: exponential backoff with jitter for transient failures.
//!
//! - 429 Too Many Requests → honour `Retry-After`
//! - 5xx                   → exponential backoff with full jitter
//! - reqwest transport errors (timeout, connect) → same backoff
//! - 4xx (other)           → no retry
//!
//! Backoff is capped at MAX_DELAY to bound total tail latency.

use rand::Rng;
use reqwest::header::HeaderMap;
use std::time::Duration;

pub const MAX_ATTEMPTS: u32 = 5;
const BASE_DELAY_MS: u64 = 500;
const MAX_DELAY_MS: u64 = 30_000;
const FALLBACK_RETRY_AFTER_SECS: u64 = 30;

/// Compute exponential backoff with full jitter for the given attempt number.
/// `attempt` is 0-indexed (0 = first retry after initial failure).
pub fn exp_backoff(attempt: u32) -> Duration {
    let shift = attempt.min(6);
    let exp = BASE_DELAY_MS.saturating_mul(1u64 << shift);
    let capped = exp.min(MAX_DELAY_MS);
    let mut rng = rand::thread_rng();
    let jitter = rng.gen_range(0..(capped / 4).max(1));
    Duration::from_millis(capped + jitter)
}

/// Read `Retry-After` (seconds) header, falling back to a sane default.
pub fn retry_after_or_default(headers: &HeaderMap) -> Duration {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(FALLBACK_RETRY_AFTER_SECS))
}

/// Is the reqwest transport error worth retrying?
pub fn is_transient_transport_err(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn exp_backoff_grows_then_caps() {
        let d0 = exp_backoff(0).as_millis();
        let d1 = exp_backoff(1).as_millis();
        let d_huge = exp_backoff(20).as_millis();
        assert!(d0 >= BASE_DELAY_MS as u128);
        assert!(d1 >= d0); // monotonic up to cap (modulo jitter)
        assert!((d_huge as u64) <= MAX_DELAY_MS + MAX_DELAY_MS / 4);
    }

    #[test]
    fn retry_after_parses_seconds() {
        let mut h = HeaderMap::new();
        h.insert(reqwest::header::RETRY_AFTER, HeaderValue::from_static("17"));
        assert_eq!(retry_after_or_default(&h), Duration::from_secs(17));
    }

    #[test]
    fn retry_after_falls_back_on_missing_or_garbage() {
        let h = HeaderMap::new();
        assert_eq!(
            retry_after_or_default(&h),
            Duration::from_secs(FALLBACK_RETRY_AFTER_SECS)
        );
        let mut h = HeaderMap::new();
        h.insert(
            reqwest::header::RETRY_AFTER,
            HeaderValue::from_static("nope"),
        );
        assert_eq!(
            retry_after_or_default(&h),
            Duration::from_secs(FALLBACK_RETRY_AFTER_SECS)
        );
    }
}
