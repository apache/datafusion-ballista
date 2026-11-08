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
//! dropped. Eviction removes discarded and expired slots, so keys for
//! executors that have gone away do not accumulate.

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
    state: Mutex<Option<SharedSlotState>>,
}

struct SharedSlotState {
    connection: Arc<SharedClient>,
    /// When the connection was last handed out by `shared`; eviction uses it
    /// to tell a connection nobody asks for any more from an active one.
    last_handout: Instant,
}

impl SharedSlot {
    /// The open connection for a new caller, unless there is none or it was
    /// discarded, stamped as requested just now.
    fn handout(&self) -> Option<Arc<SharedClient>> {
        let mut state = self.state.lock().unwrap();
        let state = state.as_mut()?;
        if state.connection.is_discarded() {
            return None;
        }
        state.last_handout = Instant::now();
        Some(state.connection.clone())
    }

    /// Whether the slot survives an eviction pass: without a live
    /// connection, only while a connect is in flight on it; with one, until
    /// it has gone unrequested for `idle_timeout`.
    fn survives_eviction(&self, now: Instant, idle_timeout: Duration) -> bool {
        let state = self.state.lock().unwrap();
        match state.as_ref() {
            Some(state) if !state.connection.is_discarded() => {
                now.duration_since(state.last_handout) < idle_timeout
            }
            _ => self.connecting.try_lock().is_err(),
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
/// Shared connections also expire after `idle_timeout` (the executor's
/// `client_ttl`) without a new handout, so a later caller reconnects after
/// a quiet period. Streams already opened on a retired connection continue.
/// The timeout measures handouts, not stream activity: a stream running longer
/// than `client_ttl` can overlap with a new connection opened to the same peer.
/// This bounds how long the pool retains an unrequested connection without
/// requiring streams to carry a usage guard back to the pool.
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

/// Re-attach `slot`, carrying the connection `ours` just handed out, to the map.
/// Eviction can drop the slot after a `shared` caller clones it, before either
/// a cached handout or a connect completes. Without this the connection is
/// invisible to the next caller, which would open another one. When a different
/// slot has already reconnected, defer to its connection. Callers that already hold
/// the detached slot may still use `ours`; it closes once those callers and
/// their in-flight streams finish.
fn reattach_shared_slot(
    inner: &Inner,
    key: &PoolKey,
    slot: &Arc<SharedSlot>,
    ours: Arc<SharedClient>,
) -> Arc<SharedClient> {
    match inner.shared.entry(key.clone()) {
        Entry::Occupied(mut e) => {
            if Arc::ptr_eq(e.get(), slot) {
                ours
            } else if let Some(other) = e.get().handout() {
                other
            } else {
                // The slot in the map is dead; swap in the live one.
                e.insert(slot.clone());
                ours
            }
        }
        Entry::Vacant(v) => {
            v.insert(slot.clone());
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
            return Ok(reattach_shared_slot(&self.inner, &key, &slot, client));
        }
        // Held while connecting, so a burst of callers waits for the one
        // connection instead of each opening its own. Checked again under
        // it: the burst's first caller has connected by the time the rest
        // get here.
        let _connecting = slot.connecting.lock().await;
        if let Some(client) = slot.handout() {
            return Ok(reattach_shared_slot(&self.inner, &key, &slot, client));
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
        *slot.state.lock().unwrap() = Some(SharedSlotState {
            connection: shared.clone(),
            last_handout: Instant::now(),
        });
        drop(_connecting);
        Ok(reattach_shared_slot(&self.inner, &key, &slot, shared))
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

    /// Eviction removes a discarded connection's slot and keeps the other,
    /// live one.
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
        // Discard preserves the slot until eviction, so this checks the
        // discarded-slot eviction path rather than eager removal.
        assert_eq!(pool.inner.shared.len(), 2);
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

    /// Discard preserves the shared reconnect lock, so a burst of retries
    /// opens one replacement even if eviction runs while they are waiting.
    #[tokio::test]
    async fn discarded_shared_slot_serializes_reconnects() {
        use futures::FutureExt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let port = serve_flight().await;
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();
        let key = ("127.0.0.1".to_string(), port, config.clone());
        let connects = Arc::new(AtomicUsize::new(0));
        let customize = Arc::new(BallistaConfigGrpcEndpoint::new({
            let connects = connects.clone();
            Arc::new(move |endpoint| {
                connects.fetch_add(1, Ordering::SeqCst);
                Ok(endpoint)
            })
        }));
        let old = pool
            .shared("127.0.0.1", port, &config, Some(customize.clone()))
            .await
            .unwrap();
        let slot = pool.inner.shared.get(&key).unwrap().clone();
        let connecting = slot.connecting.lock().await;

        old.discard();
        assert!(Arc::ptr_eq(
            pool.inner.shared.get(&key).unwrap().value(),
            &slot
        ));
        let first = pool.shared("127.0.0.1", port, &config, Some(customize.clone()));
        let second = pool.shared("127.0.0.1", port, &config, Some(customize.clone()));
        tokio::pin!(first, second);
        assert!(first.as_mut().now_or_never().is_none());
        assert!(second.as_mut().now_or_never().is_none());
        pool.evict_idle().await;
        assert!(Arc::ptr_eq(
            pool.inner.shared.get(&key).unwrap().value(),
            &slot
        ));

        drop(connecting);
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &old));
        assert_eq!(connects.load(Ordering::SeqCst), 2);

        // A late discard on the old connection must not affect its replacement.
        old.discard();
        assert!(Arc::ptr_eq(
            &first,
            &pool.shared("127.0.0.1", port, &config, None).await.unwrap()
        ));
    }

    /// A shared connection nobody requests any more is dropped by eviction
    /// once it has sat unrequested for `idle_timeout`; one with a recent
    /// handout survives.
    #[tokio::test]
    async fn evict_idle_drops_stale_shared_connection() {
        let port = serve_flight().await;
        let timeout = Duration::from_secs(60);
        let pool = make_pool(timeout);
        let config = GrpcClientConfig::default();
        let key = ("127.0.0.1".to_string(), port, config.clone());

        let _stale = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        // Age the handout past the idle timeout.
        let slot = pool.inner.shared.get(&key).unwrap().clone();
        slot.state.lock().unwrap().as_mut().unwrap().last_handout =
            Instant::now() - timeout - Duration::from_secs(1);
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

    /// An open pipelined stream outlives the pool's last Channel handle,
    /// including batches the producer sends only after the slot is evicted.
    #[tokio::test]
    async fn evict_idle_keeps_open_pipelined_stream_readable() {
        use ballista_core::execution_plans::{
            PipelinedRegistry, PipelinedShuffleWriterExec,
        };
        use ballista_core::serde::scheduler::{
            ExecutorMetadata, ExecutorOperatingSystemSpecification,
            ExecutorSpecification, PartitionId, PipelinedPartitionLocation,
        };
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::execution::TaskContext;
        use datafusion::physical_plan::SendableRecordBatchStream;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
        use datafusion::prelude::{SessionConfig, SessionContext};
        use futures::{FutureExt, StreamExt, TryStreamExt};
        use tokio::sync::mpsc;
        use tokio_stream::wrappers::ReceiverStream;

        #[derive(Debug)]
        struct Input {
            schema: SchemaRef,
            receiver:
                Mutex<Option<mpsc::Receiver<datafusion::error::Result<RecordBatch>>>>,
        }

        impl PartitionStream for Input {
            fn schema(&self) -> &SchemaRef {
                &self.schema
            }

            fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
                Box::pin(RecordBatchStreamAdapter::new(
                    self.schema.clone(),
                    ReceiverStream::new(self.receiver.lock().unwrap().take().unwrap()),
                ))
            }
        }

        tokio::time::timeout(Duration::from_secs(10), async {
            let schema =
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
            let first = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let last = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
            )
            .unwrap();
            let (tx, rx) = mpsc::channel(1);
            let input = Arc::new(
                StreamingTableExec::try_new(
                    schema.clone(),
                    vec![Arc::new(Input {
                        schema,
                        receiver: Mutex::new(Some(rx)),
                    })],
                    None,
                    vec![],
                    false,
                    None,
                )
                .unwrap(),
            );
            let registry = Arc::new(PipelinedRegistry::new("executor"));
            let writer =
                PipelinedShuffleWriterExec::try_new("job".into(), 1, input, None, false)
                    .unwrap();
            let ctx = SessionContext::new_with_config(
                SessionConfig::new().with_extension(registry.clone()),
            )
            .task_ctx();
            let producer =
                tokio::spawn(async move { writer.execute_pipelined(ctx).await });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let work_dir = tempfile::tempdir().unwrap();
            let service = crate::flight_service::BallistaFlightService::new(
                work_dir.path().to_string_lossy().into_owned(),
            )
            .with_pipelined_registry(registry);
            let server = tokio::spawn(
                ballista_core::utils::create_grpc_server(&Default::default())
                    .add_service(
                        arrow_flight::flight_service_server::FlightServiceServer::new(
                            service,
                        ),
                    )
                    .serve_with_incoming(tonic::transport::server::TcpIncoming::from(
                        listener,
                    )),
            );
            let location = PipelinedPartitionLocation {
                partition_id: PartitionId::new(&"job".into(), 1, 0),
                task_id: 0,
                executor_meta: ExecutorMetadata {
                    id: "executor".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port,
                    grpc_port: 0,
                    specification: ExecutorSpecification::default(),
                    os_info: ExecutorOperatingSystemSpecification::default(),
                },
            };
            let idle_timeout = Duration::from_secs(60);
            let pool = make_pool(idle_timeout);
            let config = GrpcClientConfig::default();
            let key = ("127.0.0.1".to_owned(), port, config.clone());
            let connection = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
            let weak = Arc::downgrade(&connection);
            tx.send(Ok(first.clone())).await.unwrap();
            let mut stream = connection
                .client()
                .fetch_pipelined_partition(&location, Duration::from_secs(5))
                .await
                .unwrap();
            assert_eq!(stream.next().await.unwrap().unwrap(), first);

            // Match open_remote_once: only the stream, not a client clone or
            // Arc<SharedClient>, survives after opening the response.
            drop(connection);
            {
                let slot = pool.inner.shared.get(&key).unwrap();
                slot.state.lock().unwrap().as_mut().unwrap().last_handout =
                    Instant::now() - idle_timeout - Duration::from_secs(1);
            }
            pool.evict_idle().await;
            assert!(pool.inner.shared.is_empty());
            assert!(weak.upgrade().is_none());
            // Let the transport react to losing its last Channel handle
            // before allowing the producer to send anything else.
            tokio::task::yield_now().await;
            assert!(stream.next().now_or_never().is_none());

            // Nothing after the first batch could have been buffered before
            // eviction: the producer receives the final batch only now.
            tx.send(Ok(last.clone())).await.unwrap();
            drop(tx);
            assert_eq!(stream.try_collect::<Vec<_>>().await.unwrap(), vec![last]);
            producer.await.unwrap().unwrap();
            server.abort();
        })
        .await
        .expect("pipelined stream should finish after shared connection eviction");
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

        let returned = reattach_shared_slot(&pool.inner, &key, &slot, conn.clone());
        assert!(Arc::ptr_eq(&returned, &conn));
        assert!(Arc::ptr_eq(
            &conn,
            &pool.shared("127.0.0.1", port, &config, None).await.unwrap()
        ));
    }

    /// Eviction between cloning a slot and a cached handout must not leave
    /// that connection detached from the pool.
    #[tokio::test]
    async fn reattach_recovers_slot_evicted_before_cached_handout() {
        let port = serve_flight().await;
        let timeout = Duration::from_secs(60);
        let pool = make_pool(timeout);
        let config = GrpcClientConfig::default();
        let key = ("127.0.0.1".to_string(), port, config.clone());
        let connection = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        let slot = pool.inner.shared.get(&key).unwrap().clone();
        slot.state.lock().unwrap().as_mut().unwrap().last_handout =
            Instant::now() - timeout - Duration::from_secs(1);
        pool.evict_idle().await;
        assert!(pool.inner.shared.is_empty());

        let cached = slot.handout().unwrap();
        let returned = reattach_shared_slot(&pool.inner, &key, &slot, cached);
        assert!(Arc::ptr_eq(&returned, &connection));
        pool.evict_idle().await;
        assert!(Arc::ptr_eq(
            &connection,
            &pool.shared("127.0.0.1", port, &config, None).await.unwrap()
        ));
    }

    /// A caller waiting on a detached slot must also defer to the new slot
    /// when it finds a cached connection after taking the connecting lock.
    #[tokio::test]
    async fn shared_waiter_defers_to_live_slot_in_map() {
        use futures::FutureExt;

        let port = serve_flight().await;
        let pool = make_pool(Duration::from_secs(60));
        let config = GrpcClientConfig::default();
        let key = ("127.0.0.1".to_string(), port, config.clone());
        let old = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        let slot = pool.inner.shared.get(&key).unwrap().clone();
        let connecting = slot.connecting.lock().await;
        old.discard();

        let waiting = pool.shared("127.0.0.1", port, &config, None);
        tokio::pin!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
        pool.inner.shared.remove(&key);
        let current = pool.shared("127.0.0.1", port, &config, None).await.unwrap();
        // Another caller completed a connection on the detached slot before
        // the waiter acquired its lock.
        *slot.state.lock().unwrap() = Some(SharedSlotState {
            connection: Arc::new(SharedClient::new(BallistaClient::new_for_test(
                "127.0.0.1",
                port,
            ))),
            last_handout: Instant::now(),
        });
        drop(connecting);

        assert!(Arc::ptr_eq(&waiting.await.unwrap(), &current));
        assert!(Arc::ptr_eq(
            &current,
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
        *detached.state.lock().unwrap() = Some(SharedSlotState {
            connection: other.clone(),
            last_handout: Instant::now(),
        });

        let returned = reattach_shared_slot(&pool.inner, &key, &detached, other);
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
