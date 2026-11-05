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
//! a failure, it goes unrequested for `idle_timeout`, or the pool is
//! dropped. Discarding removes the slot eagerly; eviction drops the slots
//! that remain, so keys for executors that have gone away do not
//! accumulate.

use ballista_core::client::BallistaClient;
use ballista_core::client_pool::{BallistaClientPool, PooledClient, SharedClient};
use ballista_core::error::Result;
use ballista_core::extension::BallistaConfigGrpcEndpoint;
use ballista_core::utils::GrpcClientConfig;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
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
    state: Mutex<SharedSlotState>,
}

#[derive(Default)]
struct SharedSlotState {
    connection: Option<Arc<SharedClient>>,
    /// When the connection was last handed out by `shared`; eviction uses it
    /// to tell a connection nobody asks for any more from an active one.
    last_handout: Option<Instant>,
}

impl SharedSlot {
    /// The open connection for a new caller, unless there is none or it was
    /// discarded, stamped as requested just now.
    fn handout(&self) -> Option<Arc<SharedClient>> {
        let mut state = self.state.lock().unwrap();
        let client = state
            .connection
            .as_ref()
            .filter(|client| !client.is_discarded())
            .cloned()?;
        state.last_handout = Some(Instant::now());
        Some(client)
    }

    /// Whether `client` is the connection this slot currently holds. A
    /// discard from a connection the slot has since replaced must not drop
    /// the slot.
    fn holds(&self, client: &Arc<SharedClient>) -> bool {
        self.state
            .lock()
            .unwrap()
            .connection
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, client))
    }

    /// Whether the slot survives an eviction pass: without a live
    /// connection, only while a connect is in flight on it; with one, until
    /// it has gone unrequested for `idle_timeout`.
    fn survives_eviction(&self, now: Instant, idle_timeout: Duration) -> bool {
        let state = self.state.lock().unwrap();
        let live = state
            .connection
            .as_ref()
            .is_some_and(|client| !client.is_discarded());
        if !live {
            return self.connecting.try_lock().is_err();
        }
        match state.last_handout {
            Some(last) => now.duration_since(last) < idle_timeout,
            None => true,
        }
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
    let now = Instant::now();
    let deadline = now.checked_sub(inner.idle_timeout).unwrap_or(now);

    // Drain expired entries from the front of each deque (oldest = front).
    // This way pool can shrink in case of low utilization.
    inner.idle.retain(|_, deque| {
        while deque.front().is_some_and(|e| e.idle_since < deadline) {
            // evict from front of the queue
            deque.pop_front();
        }
        !deque.is_empty()
    });
    inner
        .shared
        .retain(|_, slot| slot.survives_eviction(now, inner.idle_timeout));
}

/// Re-attach `slot`, carrying the connection `ours` just opened, to the map.
/// Eviction can drop the slot while its `shared` caller was still
/// connecting; without this the fresh connection would be invisible to the
/// next caller, which would open another one. When a different slot has
/// already reconnected, defer to its connection and let `ours` close on
/// drop instead.
fn reattach_shared_slot(
    inner: &Inner,
    key: &PoolKey,
    slot: Arc<SharedSlot>,
    ours: Arc<SharedClient>,
) -> Arc<SharedClient> {
    match inner.shared.entry(key.clone()) {
        Entry::Occupied(mut e) => {
            if Arc::ptr_eq(e.get(), &slot) {
                ours
            } else if let Some(other) = e.get().handout() {
                other
            } else {
                // The slot in the map is dead; swap in the live one.
                e.insert(slot);
                ours
            }
        }
        Entry::Vacant(v) => {
            v.insert(slot);
            ours
        }
    }
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
        let slot = self.inner.shared.entry(key.clone()).or_default().clone();

        if let Some(client) = slot.handout() {
            return Ok(client);
        }
        // Held while connecting, so a burst of callers waits for the one
        // connection instead of each opening its own. Checked again under
        // it: the burst's first caller has connected by the time the rest
        // get here.
        let _connecting = slot.connecting.lock().await;
        if let Some(client) = slot.handout() {
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
        {
            let inner = Arc::downgrade(&self.inner);
            let slot_ref = Arc::downgrade(&slot);
            let me = Arc::downgrade(&shared);
            let hook_key = key.clone();
            shared.set_discard_hook(Box::new(move || {
                let (Some(inner), Some(slot), Some(me)) =
                    (inner.upgrade(), slot_ref.upgrade(), me.upgrade())
                else {
                    return;
                };
                // Drop the slot right away rather than on the next eviction
                // pass, but only when the discard came from the connection
                // the slot still holds, not from one it has replaced.
                inner.shared.remove_if(&hook_key, |_, current| {
                    Arc::ptr_eq(current, &slot) && current.holds(&me)
                });
            }));
        }
        {
            let mut state = slot.state.lock().unwrap();
            state.connection = Some(shared.clone());
            state.last_handout = Some(Instant::now());
        }
        drop(_connecting);
        Ok(reattach_shared_slot(&self.inner, &key, slot, shared))
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

    /// Discarding a shared connection drops its slot right away; eviction
    /// keeps the other, live one.
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

    /// A discarded connection's slot leaves the map at once, without waiting
    /// for an eviction pass, and the next caller reconnects.
    #[tokio::test]
    async fn discard_removes_shared_slot_eagerly() {
        let port = serve_flight().await;
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();

        let conn = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        assert_eq!(pool.inner.shared.len(), 1);

        conn.discard();
        assert_eq!(pool.inner.shared.len(), 0);

        let next = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        assert!(!next.is_discarded());
    }

    /// A discard that loses the race against a reconnect onto the same slot
    /// must not drop the slot's fresh connection.
    #[tokio::test]
    async fn discard_does_not_drop_slot_that_reconnected_first() {
        let port = serve_flight().await;
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();
        let key = ("127.0.0.1".to_string(), port, config.clone());

        let old = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        let slot = pool.inner.shared.get(&key).unwrap().clone();

        // The reconnect lands first: when the hook runs, the slot already
        // holds the replacement connection.
        let replacement = Arc::new(SharedClient::new(BallistaClient::new_for_test(
            "127.0.0.1",
            port,
        )));
        slot.state.lock().unwrap().connection = Some(replacement.clone());

        old.discard();
        assert_eq!(pool.inner.shared.len(), 1);
        assert!(slot.holds(&replacement));
    }

    /// A shared connection nobody requests any more is dropped by eviction
    /// once it has sat unrequested for `idle_timeout`; one with a recent
    /// handout survives.
    #[tokio::test]
    async fn evict_idle_drops_stale_shared_connection() {
        let port = serve_flight().await;
        let timeout = Duration::from_millis(100);
        let pool = make_pool(timeout);
        let config = GrpcClientConfig::default();
        let key = ("127.0.0.1".to_string(), port, config.clone());

        let _stale = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        // Age the handout past the idle timeout.
        let slot = pool.inner.shared.get(&key).unwrap().clone();
        slot.state.lock().unwrap().last_handout =
            Some(Instant::now() - timeout - Duration::from_millis(50));
        pool.evict_idle().await;
        assert_eq!(pool.inner.shared.len(), 0);

        // A fresh connection is handed out now, so eviction keeps it.
        let active = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        pool.evict_idle().await;
        assert_eq!(pool.inner.shared.len(), 1);
        assert!(Arc::ptr_eq(
            &active,
            &pool.shared("127.0.0.1", port, &config, None).await.unwrap()
        ));
    }

    /// A slot left empty by a failed connect is collected by eviction.
    #[tokio::test]
    async fn evict_idle_drops_slot_left_by_failed_connect() {
        // Bind and drop to get a port nothing listens on.
        let port = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();

        let result = pool.shared("127.0.0.1", port, &config, None).await;
        assert!(result.is_err());
        assert_eq!(pool.inner.shared.len(), 1);

        pool.evict_idle().await;
        assert_eq!(pool.inner.shared.len(), 0);
    }

    /// Re-attaching a slot that eviction dropped mid-connect lets the next
    /// caller reuse its connection instead of opening another one.
    #[tokio::test]
    async fn reattach_recovers_slot_evicted_mid_connect() {
        let port = serve_flight().await;
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();
        let key = ("127.0.0.1".to_string(), port, config.clone());

        let conn = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        let slot = pool.inner.shared.get(&key).unwrap().clone();

        // Eviction wins the race: the slot leaves the map while a caller is
        // still connecting on it.
        pool.inner.shared.remove(&key);
        assert_eq!(pool.inner.shared.len(), 0);

        let returned = reattach_shared_slot(&pool.inner, &key, slot, conn.clone());
        assert!(Arc::ptr_eq(&returned, &conn));
        assert!(Arc::ptr_eq(
            &conn,
            &pool.shared("127.0.0.1", port, &config, None).await.unwrap()
        ));
    }

    /// When a different slot has already reconnected, re-attaching defers to
    /// its connection instead of replacing it.
    #[tokio::test]
    async fn reattach_defers_to_live_slot_in_map() {
        let port = serve_flight().await;
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();
        let key = ("127.0.0.1".to_string(), port, config.clone());

        let current = pool.shared("127.0.0.1", port, &config, None).await.unwrap();

        // A detached slot carrying a separately opened connection.
        let detached = Arc::new(SharedSlot::default());
        let other = Arc::new(SharedClient::new(BallistaClient::new_for_test(
            "127.0.0.1",
            port,
        )));
        detached.state.lock().unwrap().connection = Some(other.clone());

        let returned = reattach_shared_slot(&pool.inner, &key, detached, other);
        assert!(Arc::ptr_eq(&returned, &current));
        assert_eq!(pool.inner.shared.len(), 1);
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
