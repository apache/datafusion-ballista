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

//! Lazily-initialized, idle-evicting cache for a scheduler [`tonic::transport::Channel`].
//!
//! [`SchedulerChannelCache`] holds at most one `Channel` per cache instance.
//! It is intended to be embedded in a long-lived owner (e.g. a query planner
//! that lives for the lifetime of a `SessionContext`) and cloned cheaply into
//! per-query execution nodes.
//!
//! # Lifecycle
//!
//! * **Lazy init.** The first call to [`SchedulerChannelCache::get_or_init`]
//!   runs the supplied build closure to produce a `Channel`. Concurrent
//!   first-callers do *not* race: an internal [`tokio::sync::OnceCell`]
//!   guarantees exactly one `build()` invocation; the rest suspend and
//!   wake when the result is ready.
//! * **Idle eviction.** A background tokio task ticks every
//!   `idle_timeout / 3` (minimum 15 s). If the cached `Channel` has not
//!   been observed by [`get_or_init`](SchedulerChannelCache::get_or_init)
//!   for `idle_timeout`, the task swaps in a fresh empty cell so the
//!   underlying TCP connection drops.
//! * **Failure invalidation.** Callers may call
//!   [`invalidate`](SchedulerChannelCache::invalidate) on transport-level
//!   RPC errors to force the next acquire to reconnect. Use the
//!   [`is_transport_error`] helper to recognise such errors.
//!
//! # `tonic::transport::Channel` semantics
//!
//! A `Channel` wraps an HTTP/2 connection. Cloning a `Channel` is cheap and
//! shares the underlying connection — concurrent RPCs from cloned channels
//! multiplex over HTTP/2 streams without serialising. We therefore cache
//! one `Channel` and hand out clones.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::OnceCell;
use tonic::transport::Channel;

/// Default idle timeout for [`SchedulerChannelCache::with_default_timeout`].
pub const DEFAULT_SCHEDULER_CHANNEL_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Lazily-initialized, idle-evicting, single-flight cache for a scheduler
/// [`tonic::transport::Channel`].
///
/// See the module-level docs for the lifecycle.
#[derive(Clone)]
pub struct SchedulerChannelCache {
    inner: Arc<Inner>,
    idle_timeout: Duration,
}

impl std::fmt::Debug for SchedulerChannelCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerChannelCache")
            .field("idle_timeout", &self.idle_timeout)
            .finish_non_exhaustive()
    }
}

struct Inner {
    state: Mutex<Slot>,
}

struct Slot {
    /// Current init cell. Cloned out under the lock and used outside it.
    /// Eviction or [`SchedulerChannelCache::invalidate`] swaps this with a
    /// fresh empty `OnceCell::new()`. Callers already awaiting the old cell
    /// continue to receive whatever the in-flight build produced — only
    /// future callers (and future builds) use the new cell.
    cell: Arc<OnceCell<Channel>>,
    /// Last time a caller observed this slot. Drives idle eviction.
    last_used: Instant,
}

impl SchedulerChannelCache {
    /// Creates a cache with the specified idle timeout and starts the
    /// background eviction task.
    pub fn new(idle_timeout: Duration) -> Self {
        let initial_slot = Slot {
            cell: Arc::new(OnceCell::new()),
            last_used: Instant::now(),
        };
        let inner = Arc::new(Inner {
            state: Mutex::new(initial_slot),
        });

        Self::spawn_eviction_task(Arc::downgrade(&inner), idle_timeout);

        Self {
            inner,
            idle_timeout,
        }
    }

    /// Convenience constructor using
    /// [`DEFAULT_SCHEDULER_CHANNEL_IDLE_TIMEOUT`].
    pub fn with_default_timeout() -> Self {
        Self::new(DEFAULT_SCHEDULER_CHANNEL_IDLE_TIMEOUT)
    }

    /// Returns a `Channel`, building one if the cache is empty.
    ///
    /// Concurrent first-callers do not race to connect — exactly one
    /// `build()` future runs and the rest await its result.
    pub async fn get_or_init<F, Fut, E>(&self, build: F) -> Result<Channel, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Channel, E>>,
    {
        // 1. Take a snapshot of the current cell under the lock.
        //    Stale-and-initialized → swap in a fresh empty cell.
        //    Stale-but-uninitialized (build is in flight) → keep waiting,
        //    do not orphan the in-flight builder.
        //    Always refresh last_used so active traffic keeps the cell warm.
        let cell = {
            let mut guard = self.inner.state.lock();
            let stale = guard.cell.initialized()
                && guard.last_used.elapsed() >= self.idle_timeout;
            if stale {
                guard.cell = Arc::new(OnceCell::new());
            }
            guard.last_used = Instant::now();
            Arc::clone(&guard.cell)
        }; // parking_lot::Mutex guard dropped before .await below.

        // 2. Single-flight init. Across N concurrent callers holding clones
        //    of the same OnceCell, exactly one runs `build()`; the rest
        //    suspend and wake when the result is ready.
        let channel = cell.get_or_try_init(build).await?;
        Ok(channel.clone())
    }

    /// Drops the cached channel so the next [`get_or_init`] call reconnects.
    /// Call on transport-level RPC errors (see [`is_transport_error`]).
    ///
    /// Callers that already hold a clone of the previous cell continue to
    /// receive whatever its in-flight build produced. Only future callers
    /// see the fresh, empty cell and trigger a new build.
    ///
    /// [`get_or_init`]: Self::get_or_init
    pub fn invalidate(&self) {
        let mut guard = self.inner.state.lock();
        guard.cell = Arc::new(OnceCell::new());
        guard.last_used = Instant::now();
    }

    fn spawn_eviction_task(weak: Weak<Inner>, idle_timeout: Duration) {
        // No empirical evidence behind 15 — chosen to match the executor's
        // DefaultBallistaClientPool. We can revisit if needed.
        let check_interval = Duration::from_secs((idle_timeout.as_secs() / 3).max(15));

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            log::debug!(
                "SchedulerChannelCache: no current tokio runtime; \
                 skipping background idle-eviction task"
            );
            return;
        };

        handle.spawn(async move {
            let mut ticker = tokio::time::interval(check_interval);
            loop {
                ticker.tick().await;
                match weak.upgrade() {
                    None => break, // cache dropped → exit task
                    Some(inner) => {
                        let mut guard = inner.state.lock();
                        // Only evict an *initialized* cell that's gone idle.
                        // An uninitialized cell means a build is in flight —
                        // leave it alone so we don't orphan the builder.
                        let stale = guard.cell.initialized()
                            && guard.last_used.elapsed() >= idle_timeout;
                        if stale {
                            guard.cell = Arc::new(OnceCell::new());
                        }
                    }
                }
            }
        });
    }

    /// Test-only: returns true iff the slot's cell currently holds a
    /// populated `Channel`.
    #[cfg(test)]
    pub(crate) fn is_initialized(&self) -> bool {
        self.inner.state.lock().cell.initialized()
    }

    /// Returns `true` iff `self` and `other` are clones of the same
    /// cache (i.e. they share the same underlying state and would see
    /// each other's writes). Intended for tests that want to verify
    /// the cache is being plumbed through correctly.
    pub fn shares_inner_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Returns `true` if `status` looks like a transport-level failure
/// (i.e. the underlying connection is broken). Callers should
/// [`SchedulerChannelCache::invalidate`] on these so the next request
/// reconnects.
///
/// We treat both `Unavailable` and `Unknown` as transport errors because
/// tonic surfaces some `hyper`/`h2` failures via `Code::Unknown`.
pub fn is_transport_error(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Unknown
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tonic::transport::Endpoint;

    /// Build a `Channel` value without any network I/O. The channel is
    /// valid as a Rust value — we can cache, clone, and drop it — but no
    /// real RPC will succeed on it. That is fine for unit tests of the
    /// cache primitive itself.
    fn fake_channel() -> Channel {
        Endpoint::from_static("http://127.0.0.1:1").connect_lazy()
    }

    #[tokio::test]
    async fn fresh_cache_is_uninitialized() {
        let cache = SchedulerChannelCache::new(Duration::from_secs(60));
        assert!(!cache.is_initialized());
    }

    #[tokio::test]
    async fn get_or_init_caches_first_call() {
        let cache = SchedulerChannelCache::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));

        let calls_a = Arc::clone(&calls);
        cache
            .get_or_init(|| async move {
                calls_a.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(fake_channel())
            })
            .await
            .unwrap();

        let calls_b = Arc::clone(&calls);
        cache
            .get_or_init(|| async move {
                calls_b.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(fake_channel())
            })
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.is_initialized());
    }

    #[tokio::test]
    async fn single_flight_under_concurrent_callers() {
        let cache = SchedulerChannelCache::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));
        // Hold the build until we explicitly release it so all callers
        // pile up on the same OnceCell.
        let release = Arc::new(Notify::new());

        let mut handles = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let calls = Arc::clone(&calls);
            let release = Arc::clone(&release);
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_init(|| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        release.notified().await;
                        Ok::<_, std::io::Error>(fake_channel())
                    })
                    .await
                    .unwrap();
            }));
        }

        // Give all tasks a chance to enter get_or_try_init before releasing.
        tokio::time::sleep(Duration::from_millis(50)).await;
        release.notify_waiters();

        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.is_initialized());
    }

    #[tokio::test]
    async fn build_failure_does_not_poison() {
        let cache = SchedulerChannelCache::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));

        // First wave: all builders fail.
        //
        // tokio::sync::OnceCell::get_or_try_init does NOT broadcast errors:
        // on failure it releases the init permit and the next waiter
        // becomes the active builder. This is desirable — a transient
        // failure (broken connection that comes back, scheduler that
        // restarts mid-init) gives the next caller a fresh attempt
        // without poisoning the cell. So `calls` may be anywhere from
        // 1 (callers funneled sequentially) up to N (each waiter ran
        // its own attempt). What we care about for non-poisoning is
        // that the cell stays uninitialized and the next caller
        // succeeds.
        let mut handles = Vec::new();
        for _ in 0..4 {
            let cache = cache.clone();
            let calls = Arc::clone(&calls);
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_init(|| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<Channel, _>(std::io::Error::other("boom"))
                    })
                    .await
            }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_err());
        }
        let after_failures = calls.load(Ordering::SeqCst);
        assert!(after_failures >= 1, "at least one builder must have run");
        assert!(after_failures <= 4, "at most one builder per waiter");
        assert!(!cache.is_initialized());

        // Second wave: succeeds, proving no poisoning.
        let calls_b = Arc::clone(&calls);
        cache
            .get_or_init(|| async move {
                calls_b.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(fake_channel())
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), after_failures + 1);
        assert!(cache.is_initialized());
    }

    #[tokio::test]
    async fn invalidate_swaps_in_fresh_cell() {
        let cache = SchedulerChannelCache::new(Duration::from_secs(60));
        let calls = Arc::new(AtomicUsize::new(0));

        for _ in 0..2 {
            let calls = Arc::clone(&calls);
            cache
                .get_or_init(|| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, std::io::Error>(fake_channel())
                })
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.is_initialized());

        cache.invalidate();
        assert!(!cache.is_initialized());

        let calls_after = Arc::clone(&calls);
        cache
            .get_or_init(|| async move {
                calls_after.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(fake_channel())
            })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn idle_eviction_drops_initialized_cell() {
        let idle = Duration::from_millis(100);
        let cache = SchedulerChannelCache::new(idle);

        cache
            .get_or_init(|| async { Ok::<_, std::io::Error>(fake_channel()) })
            .await
            .unwrap();
        assert!(cache.is_initialized());

        // Wait long enough for at least one eviction tick after expiry.
        // check_interval = max(idle/3, 15s) = 15s in unit-test world, so
        // we drive eviction manually instead.
        // Helper: simulate a tick by replicating the eviction logic.
        tokio::time::sleep(idle + Duration::from_millis(50)).await;
        manually_evict_if_stale(&cache, idle);
        assert!(!cache.is_initialized());
    }

    #[tokio::test]
    async fn idle_eviction_does_not_swap_in_flight_cell() {
        let idle = Duration::from_millis(100);
        let cache = SchedulerChannelCache::new(idle);
        let release = Arc::new(Notify::new());
        let release_for_task = Arc::clone(&release);

        let cache_for_task = cache.clone();
        let handle = tokio::spawn(async move {
            cache_for_task
                .get_or_init(|| async move {
                    release_for_task.notified().await;
                    Ok::<_, std::io::Error>(fake_channel())
                })
                .await
                .unwrap();
        });

        // Let the task enter get_or_try_init, then sleep past the idle
        // threshold while the build is still pending.
        tokio::time::sleep(idle + Duration::from_millis(50)).await;
        manually_evict_if_stale(&cache, idle);

        // Cell pointer must NOT have been swapped — the in-flight build
        // is still on the original cell.
        assert!(!cache.is_initialized()); // not initialized YET, but cell unchanged

        release.notify_waiters();
        handle.await.unwrap();
        assert!(cache.is_initialized()); // build completed on the original cell
    }

    #[tokio::test]
    async fn dropping_cache_drops_inner() {
        let cache = SchedulerChannelCache::new(Duration::from_secs(60));
        // Test lives inside the module, so we can downgrade Arc<Inner>
        // directly without exposing a public accessor.
        let weak = Arc::downgrade(&cache.inner);
        assert!(weak.upgrade().is_some());
        drop(cache);
        assert!(
            weak.upgrade().is_none(),
            "Inner must drop when no strong refs remain — eviction task \
             must hold only a Weak"
        );
    }

    #[test]
    fn is_transport_error_matches_unavailable_and_unknown() {
        assert!(is_transport_error(&tonic::Status::unavailable("x")));
        assert!(is_transport_error(&tonic::Status::unknown("x")));
        assert!(!is_transport_error(&tonic::Status::invalid_argument("x")));
        assert!(!is_transport_error(&tonic::Status::not_found("x")));
        assert!(!is_transport_error(&tonic::Status::permission_denied("x")));
    }

    /// Replicates the eviction task's per-tick logic so unit tests don't
    /// have to wait for the real check interval (which is at least 15 s).
    fn manually_evict_if_stale(cache: &SchedulerChannelCache, idle_timeout: Duration) {
        let mut guard = cache.inner.state.lock();
        let stale = guard.cell.initialized() && guard.last_used.elapsed() >= idle_timeout;
        if stale {
            guard.cell = Arc::new(OnceCell::new());
        }
    }
}
