//! Tiny in-memory TTL cells — enough to absorb the wallet's 5–15 s polling
//! without a database, keeping the "not an indexer" promise.

use std::future::Future;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

pub struct TtlCell<T> {
    ttl: Duration,
    slot: RwLock<Option<(Instant, T)>>,
}

impl<T: Clone> TtlCell<T> {
    pub fn new(ttl: Duration) -> Self {
        Self { ttl, slot: RwLock::new(None) }
    }

    async fn fresh(&self) -> Option<T> {
        match &*self.slot.read().await {
            Some((at, value)) if at.elapsed() < self.ttl => Some(value.clone()),
            _ => None,
        }
    }

    async fn store(&self, value: &T) {
        *self.slot.write().await = Some((Instant::now(), value.clone()));
    }

    /// Errors are not cached, so a failing upstream is retried on the next
    /// request. Concurrent misses may run `init` more than once — harmless at
    /// this scale and simpler than request coalescing.
    pub async fn get_or_try_init<F, Fut, E>(&self, init: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(value) = self.fresh().await {
            return Ok(value);
        }
        let value = init().await?;
        self.store(&value).await;
        Ok(value)
    }

    /// Infallible variant for best-effort values where a fallback (e.g. 0) is
    /// cached for the TTL instead of hammering a failing call per request.
    pub async fn get_or_init<F, Fut>(&self, init: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        if let Some(value) = self.fresh().await {
            return value;
        }
        let value = init().await;
        self.store(&value).await;
        value
    }
}

pub struct Caches {
    pub info: TtlCell<crate::api::dto::InfoResponse>,
    pub hashrate: TtlCell<f64>,
    pub block_reward: TtlCell<f64>,
    pub market: TtlCell<serde_json::Value>,
}

impl Default for Caches {
    fn default() -> Self {
        Self {
            // The wallet polls /info every 15 s; 2 s keeps many wallets from
            // stacking identical node calls while staying visibly live.
            info: TtlCell::new(Duration::from_secs(2)),
            hashrate: TtlCell::new(Duration::from_secs(30)),
            block_reward: TtlCell::new(Duration::from_secs(30)),
            market: TtlCell::new(Duration::from_secs(30)),
        }
    }
}
