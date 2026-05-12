//! Token cache — Redis primary, no-op fallback.

use super::TokenData;
use chrono::{Duration, Utc};
use deadpool_redis::{redis::AsyncCommands, Config as RedisConfig, Pool as RedisPool, Runtime};
use tracing::{debug, warn};

const TOKEN_KEY_PREFIX: &str = "xero:token";
/// Refresh window: trigger refresh 5 min before expiry.
const REFRESH_BUFFER_SECS: i64 = 300;

pub struct TokenCache {
    pool: RedisPool,
}

impl TokenCache {
    pub fn new(redis_url: &str) -> xero_common::Result<Self> {
        let pool = RedisConfig::from_url(redis_url)
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;
        Ok(Self { pool })
    }

    fn key(tenant_id: &str) -> String {
        format!("{TOKEN_KEY_PREFIX}:{tenant_id}")
    }

    pub async fn get(&self, tenant_id: &str) -> Option<TokenData> {
        let mut conn = self.pool.get().await.ok()?;
        let raw: Option<String> = conn
            .get(Self::key(tenant_id))
            .await
            .map_err(|e| warn!("Redis GET error: {e}"))
            .ok()
            .flatten();

        raw.and_then(|s| {
            serde_json::from_str(&s)
                .map_err(|e| warn!("corrupt token in Redis: {e}"))
                .ok()
        })
    }

    pub async fn set(&self, token: &TokenData) -> xero_common::Result<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        let ttl = (token.expires_at - Utc::now() - Duration::seconds(REFRESH_BUFFER_SECS))
            .num_seconds()
            .max(1) as u64;

        let raw = serde_json::to_string(token)?;

        conn.set_ex::<_, _, ()>(Self::key(&token.tenant_id), raw, ttl)
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        debug!(tenant = %token.tenant_id, ttl, "token cached in Redis");
        Ok(())
    }

    pub async fn delete(&self, tenant_id: &str) -> xero_common::Result<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        conn.del::<_, ()>(Self::key(tenant_id))
            .await
            .map_err(|e| xero_common::Error::Auth(e.to_string()))?;

        Ok(())
    }
}
