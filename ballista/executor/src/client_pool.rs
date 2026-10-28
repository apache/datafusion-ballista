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
//! `DefaultBallistaClientPool` maintains a `VecDeque` of idle clients per
//! `(host, port, config)` key backed by a `DashMap`. Callers `BallistaClientPool::acquire`
//! a `PooledClient` guard; when the guard is dropped the underlying client is
//! returned to the idle deque automatically.
//!
//! Connections could be discarded calling `PooledClient::discard` which will result
//! of dropping connection rather than returning it to the pool. This could be
//! used for error handling.
//!
//! A optional background tokio task evicts idle connections that have not been used
//! within the configured `idle_timeout`.
//!
//! Besides the checked-out clients, the pool keeps one shared connection per
//! `(host, port, config)` for `BallistaClientPool::shared`. Any number of
//! callers multiplex over it, and it lives until a caller discards it after
//! a failure, or the pool is dropped. Eviction also drops the slots of
//! discarded connections, so keys for executors that have gone away do not
//! accumulate.

use ballista_core::client::BallistaClient;
use ballista_core::client_pool::{BallistaClientPool, PooledClient, SharedClient};
use ballista_core::error::Result;
use ballista_core::extension::BallistaConfigGrpcEndpoint;
use ballista_core::utils::GrpcClientConfig;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// DefaultBallistaClientPool
// ---------------------------------------------------------------------------

struct IdleEntry {
    client: BallistaClient,
    idle_since: Instant,
}

type PoolKey = (String, u16, GrpcClientConfig);

type IdleMap = DashMap<PoolKey, VecDeque<IdleEntry>>;

/// The shared connection for one key, and the lock that serializes opening
/// it. The connection sits behind a plain mutex so that the common case, a
/// connection already open, takes no async lock.
#[derive(Default)]
struct SharedSlot {
    connecting: tokio::sync::Mutex<()>,
    connection: Mutex<Option<Arc<SharedClient>>>,
}

impl SharedSlot {
    /// The open connection, unless there is none or it was discarded.
    fn live(&self) -> Option<Arc<SharedClient>> {
        self.connection
            .lock()
            .unwrap()
            .as_ref()
            .filter(|client| !client.is_discarded())
            .cloned()
    }

    /// Whether eviction can drop the slot: it holds no live connection and
    /// none is being opened.
    fn is_dead(&self) -> bool {
        self.live().is_none() && self.connecting.try_lock().is_ok()
    }
}

type SharedMap = DashMap<PoolKey, Arc<SharedSlot>>;

struct Inner {
    idle: IdleMap,
    idle_timeout: Duration,
    shared: SharedMap,
}

/// Default pool implementation.
///
/// Keeps `BallistaClients` as `VecDeque<IdleEntry>` per `(host, port, config)`.
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
            idle: DashMap::new(),
            idle_timeout,
            shared: DashMap::new(),
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
                            evict(&pool)
                        }
                    }
                }
                log::debug!("client connection pool - eviction thread ... DONE");
            });
        }

        Self { inner }
    }

    #[cfg(test)]
    /// Total number of idle connections currently held across all endpoints.
    pub fn idle_count(&self) -> usize {
        self.inner.idle.iter().map(|e| e.value().len()).sum()
    }
}

fn evict(inner: &Inner) {
    let deadline = Instant::now()
        .checked_sub(inner.idle_timeout)
        .unwrap_or_else(Instant::now);

    // Drain expired entries from the front of each deque (oldest = front).
    // This way pool can shrink in case of low utilization.
    inner.idle.retain(|_, deque| {
        while deque.front().is_some_and(|e| e.idle_since < deadline) {
            // evict from front of the queue
            deque.pop_front();
        }
        !deque.is_empty()
    });
    inner.shared.retain(|_, slot| !slot.is_dead());
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

        // Pop the most-recently-used idle client. The DashMap shard lock is
        // held only for the duration of the pop — released before the async
        // BallistaClient::try_new call below.
        let maybe_idle_client = self
            .inner
            .idle
            .get_mut(&key)
            .and_then(|mut deque| deque.pop_back()) // acquire from back of the queue
            .map(|e| e.client);

        let client = match maybe_idle_client {
            Some(client) => {
                log::trace!(
                    "client connection pool - returning cached connection - host:{host}, port:{port}"
                );
                client
            }
            None => {
                log::trace!(
                    "client connection pool - returning NEW connection - host:{host}, port:{port}"
                );
                BallistaClient::try_new(
                    host,
                    port,
                    config.max_message_size,
                    config.use_tls,
                    customize_endpoint,
                    config.io_retries_times,
                    config.io_retry_wait_time_ms,
                    config.initial_connection_window_size,
                    config.initial_stream_window_size,
                )
                .await?
            }
        };

        // The return closure captures only an Arc — synchronous, safe for Drop.
        let inner_ref = Arc::clone(&self.inner);
        let return_key = key;
        Ok(PooledClient::new(
            client,
            Box::new(move |c| {
                inner_ref
                    .idle
                    .entry(return_key)
                    .or_default()
                    .push_back(IdleEntry {
                        client: c,
                        idle_since: Instant::now(),
                    });
            }),
        ))
    }

    async fn shared(
        &self,
        host: &str,
        port: u16,
        config: &GrpcClientConfig,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    ) -> Result<Arc<SharedClient>> {
        let key = (host.to_string(), port, config.clone());
        let slot = self.inner.shared.entry(key).or_default().clone();

        if let Some(client) = slot.live() {
            return Ok(client);
        }
        // Held while connecting, so a burst of callers waits for the one
        // connection instead of each opening its own. Checked again under
        // it: the burst's first caller has connected by the time the rest
        // get here.
        let _connecting = slot.connecting.lock().await;
        if let Some(client) = slot.live() {
            return Ok(client);
        }

        log::debug!(
            "client connection pool - opening shared connection - host:{host}, port:{port}"
        );
        let client = BallistaClient::try_new(
            host,
            port,
            config.max_message_size,
            config.use_tls,
            customize_endpoint,
            config.io_retries_times,
            config.io_retry_wait_time_ms,
            config.initial_connection_window_size,
            config.initial_stream_window_size,
        )
        .await?;

        let shared = Arc::new(SharedClient::new(client));
        *slot.connection.lock().unwrap() = Some(shared.clone());
        Ok(shared)
    }

    async fn evict_idle(&self) {
        evict(&self.inner);
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
        pool.inner
            .idle
            .entry((host.to_string(), port, GrpcClientConfig::default()))
            .or_default()
            .push_back(IdleEntry {
                client,
                idle_since: Instant::now() - age,
            });
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
                inner_ref
                    .idle
                    .entry(return_key)
                    .or_default()
                    .push_back(IdleEntry {
                        client: c,
                        idle_since: Instant::now(),
                    });
            }),
        );

        assert_eq!(pool.idle_count(), 0);
        drop(guard);
        assert_eq!(pool.idle_count(), 1);
    }

    /// Serves a Flight service on a port of its own, for clients that
    /// connect eagerly.
    async fn serve_flight() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let work_dir = std::env::temp_dir().to_string_lossy().into_owned();
        tokio::spawn(
            ballista_core::utils::create_grpc_server(&Default::default())
                .add_service(
                    arrow_flight::flight_service_server::FlightServiceServer::new(
                        crate::flight_service::BallistaFlightService::new(work_dir),
                    ),
                )
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(
                    listener,
                )),
        );
        port
    }

    /// Every caller of `shared` for one key gets the same connection, a
    /// different configuration gets another, and a discarded connection is
    /// replaced by the next call.
    #[tokio::test]
    async fn shared_connection_is_one_per_key_until_discarded() {
        let port = serve_flight().await;
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();

        let first = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        let again = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        assert!(Arc::ptr_eq(&first, &again));

        let other_config = GrpcClientConfig {
            initial_stream_window_size: 65_535,
            ..config.clone()
        };
        let other = pool
            .shared("127.0.0.1", port, &other_config, None)
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &other));

        first.discard();
        let replacement = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &replacement));
        assert!(Arc::ptr_eq(
            &replacement,
            &pool.shared("127.0.0.1", port, &config, None).await.unwrap()
        ));
    }

    /// Eviction drops the slot of a discarded shared connection, so slots for
    /// executors that have gone away do not pile up, and keeps a live one.
    #[tokio::test]
    async fn evict_idle_drops_discarded_shared_connections() {
        let port = serve_flight().await;
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();
        let other_config = GrpcClientConfig {
            initial_stream_window_size: 65_535,
            ..config.clone()
        };

        let gone = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        let kept = pool
            .shared("127.0.0.1", port, &other_config, None)
            .await
            .unwrap();
        gone.discard();
        pool.evict_idle().await;

        assert_eq!(pool.inner.shared.len(), 1);
        assert!(Arc::ptr_eq(
            &kept,
            &pool
                .shared("127.0.0.1", port, &other_config, None)
                .await
                .unwrap()
        ));
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
                inner_ref
                    .idle
                    .entry(("host-d".to_string(), 4567u16, GrpcClientConfig::default()))
                    .or_default()
                    .push_back(IdleEntry {
                        client: c,
                        idle_since: Instant::now(),
                    });
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
        inject_idle(&pool, "host-e", 5678, Duration::from_millis(10)); // fresh
        assert_eq!(pool.idle_count(), 2);

        pool.evict_idle().await;
        assert_eq!(pool.idle_count(), 1);
    }
}
