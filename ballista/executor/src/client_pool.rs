// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Connection pool for `BallistaClient` instances.
//!
//! `DefaultBallistaClientPool` keeps a small set of shared `BallistaClient`s per
//! `(host, port, config)` endpoint in a `DashMap`. Callers `BallistaClientPool::acquire`
//! a `PooledClient` that wraps a cheap clone of one of those clients (chosen
//! round-robin); the underlying `tonic::Channel` multiplexes concurrent requests
//! and reconnects transparently, so dropping the clone is a no-op and no new
//! connection is opened per request.
//!
//! A optional background tokio task evicts endpoint connections that have not been
//! used within the configured `idle_timeout`.

use ballista_core::client::BallistaClient;
use ballista_core::client_pool::{BallistaClientPool, PooledClient};
use ballista_core::error::Result;
use ballista_core::extension::BallistaConfigGrpcEndpoint;
use ballista_core::utils::GrpcClientConfig;
use dashmap::DashMap;
use std::fmt::Debug;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// DefaultBallistaClientPool
// ---------------------------------------------------------------------------

/// A small set of shared clients per endpoint. Each underlying `tonic::Channel`
/// multiplexes concurrent requests and reconnects transparently; `acquire` hands
/// out clones round-robin across the set. Keeping a few connections per
/// `(host, port, config)` — rather than one per request — removes the connection
/// churn that caused intermittent shuffle FetchFailures, while spreading load
/// across enough connections that a single one does not stall under high
/// shuffle-fetch concurrency.
struct CachedEntry {
    clients: Vec<BallistaClient>,
    /// Round-robin cursor over `clients`.
    next: usize,
    last_used: Instant,
}

type ClientMap = DashMap<(String, u16, GrpcClientConfig), CachedEntry>;

/// Number of connections kept per endpoint.
const CONNECTIONS_PER_ENDPOINT: usize = 8;

struct Inner {
    clients: ClientMap,
    idle_timeout: Duration,
}

/// Default pool implementation.
///
/// Keeps up to `CONNECTIONS_PER_ENDPOINT` shared `BallistaClient`s per
/// `(host, port, config)` and hands out clones round-robin, each multiplexing
/// over its connection.
/// Idle clients are evicted by a background tokio task that runs at `idle_timeout / 3`
/// intervals (minimum 15 s). The task exits automatically when the pool `Arc`
/// is dropped.
///
/// The `DefaultBallistaClientPool` uses the (host, port, config) to identify a connection.
/// Therefore changing connection config might leave pooled connections
/// with older config unused until they expire.

#[derive(Clone)]
pub struct DefaultBallistaClientPool {
    inner: Arc<Inner>,
}

impl Debug for DefaultBallistaClientPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultBallistaClientPool").finish()
    }
}

impl DefaultBallistaClientPool {
    /// Create a pool that evicts connections idle longer
    /// than defined `idle_timeout`.
    pub fn with_eviction_thread(idle_timeout: Duration) -> Self {
        Self::new(idle_timeout, true)
    }

    /// Create a pool that evicts connections idle longer than `idle_timeout`,
    /// if `enable_eviction_thread` is enabled
    pub fn new(idle_timeout: Duration, enable_eviction_thread: bool) -> Self {
        let inner = Arc::new(Inner {
            clients: DashMap::new(),
            idle_timeout,
        });

        let weak: Weak<Inner> = Arc::downgrade(&inner);
        // there is no empirical evidence why 15 is selected.
        // we can revisit if interval < 15 is needed
        let check_interval = Duration::from_secs((idle_timeout.as_secs() / 3).max(15));

        if enable_eviction_thread {
            tokio::spawn(async move {
                log::debug!(
                    "client connection pool - eviction thread started ... interval: {check_interval:?}"
                );
                let mut ticker = tokio::time::interval(check_interval);
                loop {
                    ticker.tick().await;

                    match weak.upgrade() {
                        None => break,
                        Some(pool) => {
                            log::trace!("client connection pool - evicting connections");
                            evict(&pool.clients, pool.idle_timeout)
                        }
                    }
                }
                log::debug!("client connection pool - eviction thread ... DONE");
            });
        }

        Self { inner }
    }

    #[cfg(test)]
    /// Number of cached endpoint connections currently held.
    pub fn idle_count(&self) -> usize {
        self.inner.clients.len()
    }
}

fn evict(clients: &ClientMap, timeout: Duration) {
    let deadline = Instant::now()
        .checked_sub(timeout)
        .unwrap_or_else(Instant::now);

    // Drop cached clients whose endpoint has not been used within the timeout so
    // the pool shrinks under low utilization.
    clients.retain(|_, entry| entry.last_used >= deadline);
}

/// Wrap a clone of a shared, cached client in a [PooledClient] whose drop is a
/// no-op: the master copy stays in the pool, so per-request clones are never
/// returned to (or closed out of) the pool.
fn shared(client: BallistaClient) -> PooledClient {
    PooledClient::new(client, Box::new(|_| {}))
}

#[async_trait::async_trait]
impl BallistaClientPool for DefaultBallistaClientPool {
    async fn acquire(
        &self,
        host: &str,
        port: u16,
        config: &GrpcClientConfig,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    ) -> Result<PooledClient> {
        let key = (host.to_string(), port, config.clone());

        // Fast path: the endpoint already has its full set of connections —
        // round-robin a clone. Cloning a BallistaClient clones its tonic Channel,
        // which multiplexes this request over the existing connection
        // (reconnecting transparently if it dropped) rather than opening a new
        // one. The DashMap shard lock is held only to pick and clone.
        if let Some(mut entry) = self.inner.clients.get_mut(&key)
            && entry.clients.len() >= CONNECTIONS_PER_ENDPOINT
        {
            let idx = entry.next % entry.clients.len();
            entry.next = entry.next.wrapping_add(1);
            entry.last_used = Instant::now();
            let client = entry.clients[idx].clone();
            log::trace!(
                "client connection pool - reusing shared connection {idx} - host:{host}, port:{port}"
            );
            return Ok(shared(client));
        }

        // Slow path: grow the endpoint's connection set (up to
        // CONNECTIONS_PER_ENDPOINT). Create the connection without holding a shard
        // lock across the await; a racing task may have filled the set meanwhile,
        // in which case the extra connection is simply dropped.
        log::trace!(
            "client connection pool - opening NEW connection - host:{host}, port:{port}"
        );
        let client = BallistaClient::try_new(
            host,
            port,
            config.max_message_size,
            config.use_tls,
            customize_endpoint,
            config.io_retries_times,
            config.io_retry_wait_time_ms,
        )
        .await?;

        let mut entry = self
            .inner
            .clients
            .entry(key)
            .or_insert_with(|| CachedEntry {
                clients: Vec::with_capacity(CONNECTIONS_PER_ENDPOINT),
                next: 0,
                last_used: Instant::now(),
            });
        entry.last_used = Instant::now();
        if entry.clients.len() < CONNECTIONS_PER_ENDPOINT {
            entry.clients.push(client);
        }
        let idx = entry.next % entry.clients.len();
        entry.next = entry.next.wrapping_add(1);
        let picked = entry.clients[idx].clone();
        Ok(shared(picked))
    }

    async fn evict_idle(&self) {
        evict(&self.inner.clients, self.inner.idle_timeout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::client::BallistaClient;
    use std::time::Duration;

    fn make_pool(timeout: Duration) -> DefaultBallistaClientPool {
        DefaultBallistaClientPool::new(timeout, false)
    }

    /// Inject an `IdleEntry` with a specific `idle_since` directly into the
    /// pool's DashMap, bypassing `acquire` so no real server is needed.
    fn inject_idle(
        pool: &DefaultBallistaClientPool,
        host: &str,
        port: u16,
        age: Duration,
    ) {
        let client = BallistaClient::new_for_test(host, port);
        pool.inner.clients.insert(
            (host.to_string(), port, GrpcClientConfig::default()),
            CachedEntry {
                clients: vec![client],
                next: 0,
                last_used: Instant::now() - age,
            },
        );
    }

    #[tokio::test]
    async fn idle_count_starts_at_zero() {
        let pool = make_pool(Duration::from_secs(60));
        assert_eq!(pool.idle_count(), 0);
    }

    #[tokio::test]
    async fn evict_idle_does_not_panic_on_empty_pool() {
        let pool = make_pool(Duration::from_secs(60));
        pool.evict_idle().await;
        assert_eq!(pool.idle_count(), 0);
    }

    /// An entry older than `idle_timeout` must be removed by `evict_idle`.
    #[tokio::test]
    async fn evict_idle_removes_expired_entries() {
        let timeout = Duration::from_millis(100);
        let pool = make_pool(timeout);

        inject_idle(&pool, "host-a", 1234, timeout + Duration::from_millis(50));
        assert_eq!(pool.idle_count(), 1);

        pool.evict_idle().await;
        assert_eq!(pool.idle_count(), 0);
    }

    /// An entry younger than `idle_timeout` must survive eviction.
    #[tokio::test]
    async fn evict_idle_keeps_fresh_entries() {
        let timeout = Duration::from_secs(60);
        let pool = make_pool(timeout);

        inject_idle(&pool, "host-b", 2345, Duration::from_millis(10));
        assert_eq!(pool.idle_count(), 1);

        pool.evict_idle().await;
        assert_eq!(pool.idle_count(), 1);
    }

    /// Dropping a [PooledClient] must return the client to the pool.
    #[tokio::test]
    async fn pooled_client_returns_on_drop() {
        let pool = make_pool(Duration::from_secs(300));
        let client = BallistaClient::new_for_test("host-c", 3456);
        let key = ("host-c".to_string(), 3456u16, GrpcClientConfig::default());

        let inner_ref = Arc::clone(&pool.inner);
        let return_key = key.clone();
        let guard = PooledClient::new(
            client,
            Box::new(move |c| {
                inner_ref.clients.insert(
                    return_key,
                    CachedEntry {
                        clients: vec![c],
                        next: 0,
                        last_used: Instant::now(),
                    },
                );
            }),
        );

        assert_eq!(pool.idle_count(), 0);
        drop(guard);
        assert_eq!(pool.idle_count(), 1);
    }

    /// Calling `discard()` must close the connection instead of returning it.
    #[tokio::test]
    async fn discard_does_not_return_to_pool() {
        let pool = make_pool(Duration::from_secs(300));
        let client = BallistaClient::new_for_test("host-d", 4567);

        let inner_ref = Arc::clone(&pool.inner);
        let guard = PooledClient::new(
            client,
            Box::new(move |c| {
                inner_ref.clients.insert(
                    ("host-d".to_string(), 4567u16, GrpcClientConfig::default()),
                    CachedEntry {
                        clients: vec![c],
                        next: 0,
                        last_used: Instant::now(),
                    },
                );
            }),
        );

        guard.discard();
        assert_eq!(pool.idle_count(), 0);
    }

    /// Mixed scenario: one expired and one fresh entry — only the expired one
    /// is removed, the other survives.
    #[tokio::test]
    async fn evict_idle_partial_removal() {
        let timeout = Duration::from_millis(100);
        let pool = make_pool(timeout);

        inject_idle(&pool, "host-e", 5678, timeout + Duration::from_millis(50)); // stale
        inject_idle(&pool, "host-f", 6789, Duration::from_millis(10)); // fresh
        assert_eq!(pool.idle_count(), 2);

        pool.evict_idle().await;
        assert_eq!(pool.idle_count(), 1);
    }
}
