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

//! Peak-usage tracking wrapper around a [`MemoryPool`].
//!
//! [`TrackedMemoryPool`] wraps another [`MemoryPool`] and records the highest
//! `reserved()` value it ever observed. It logs one `tracing` event on
//! construction and one on drop, so a task's peak memory pool usage is
//! visible in executor logs without any additional plumbing.
//!
//! Ballista builds one [`FairSpillPool`] per task (via
//! [`memory_pool_policy`](crate::executor_process::ExecutorProcessConfig)),
//! so wrapping the per-task pool gives per-task peak-memory logs. Correlate
//! events by the `pool_id` field.
//!
//! [`FairSpillPool`]: datafusion::execution::memory_pool::FairSpillPool

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::error::Result;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use tracing::info;

/// Monotonic pool identifier. Wraps around after `usize::MAX` pools, which is
/// a non-concern in practice.
static POOL_ID: AtomicUsize = AtomicUsize::new(0);

/// Log target used for construction and drop events. Filter with
/// `RUST_LOG=ballista_executor::memory_pool=info` (or `=debug`) to see only
/// these events, or leave at the default `ballista=info` since the executor
/// crate is `ballista_executor`.
const LOG_TARGET: &str = "ballista_executor::memory_pool";

/// A [`MemoryPool`] wrapper that records peak `reserved()` bytes and logs
/// them on `Drop`.
///
/// All trait methods delegate to `inner`; `grow` and successful `try_grow`
/// additionally update the peak. Peak is inclusive of any concurrent activity
/// on the wrapped pool — meaningful because Ballista's per-task pool is not
/// shared across tasks (see the module docs).
pub struct TrackedMemoryPool {
    inner: Arc<dyn MemoryPool>,
    peak: AtomicUsize,
    id: usize,
}

impl TrackedMemoryPool {
    /// Wraps `inner` and emits a "pool created" log line with a fresh
    /// monotonic id.
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        let id = POOL_ID.fetch_add(1, Ordering::Relaxed);
        info!(
            target: LOG_TARGET,
            pool_id = id,
            limit_bytes = memory_limit_bytes(inner.memory_limit()),
            "memory pool created",
        );
        Self {
            inner,
            peak: AtomicUsize::new(0),
            id,
        }
    }

    /// Highest `reserved()` observed since construction.
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Monotonic pool id, used as the correlation key in log events.
    pub fn pool_id(&self) -> usize {
        self.id
    }

    fn observe(&self, current: usize) {
        // CAS-loop the peak forward. Contention is unlikely (per-task pool),
        // and even under contention the loop terminates in bounded retries.
        let mut peak = self.peak.load(Ordering::Relaxed);
        while current > peak {
            match self.peak.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => peak = observed,
            }
        }
    }
}

/// Renders `MemoryLimit` as either a byte count or a text tag (`unknown` /
/// `infinite`) so structured log consumers see one field type.
fn memory_limit_bytes(limit: MemoryLimit) -> String {
    match limit {
        MemoryLimit::Finite(bytes) => bytes.to_string(),
        MemoryLimit::Infinite => "infinite".to_string(),
        MemoryLimit::Unknown => "unknown".to_string(),
    }
}

impl fmt::Debug for TrackedMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackedMemoryPool")
            .field("id", &self.id)
            .field("peak", &self.peak.load(Ordering::Relaxed))
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for TrackedMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TrackedMemoryPool(id={} peak={} inner={})",
            self.id,
            self.peak.load(Ordering::Relaxed),
            self.inner,
        )
    }
}

impl MemoryPool for TrackedMemoryPool {
    fn name(&self) -> &str {
        "TrackedMemoryPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.observe(self.inner.reserved());
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let result = self.inner.try_grow(reservation, additional);
        if result.is_ok() {
            self.observe(self.inner.reserved());
        }
        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

impl Drop for TrackedMemoryPool {
    fn drop(&mut self) {
        info!(
            target: LOG_TARGET,
            pool_id = self.id,
            peak_bytes = self.peak.load(Ordering::Relaxed),
            limit_bytes = memory_limit_bytes(self.inner.memory_limit()),
            "memory pool dropped",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::FairSpillPool;

    fn reserve(
        pool: &Arc<dyn MemoryPool>,
        name: &str,
        bytes: usize,
    ) -> MemoryReservation {
        let consumer = MemoryConsumer::new(name);
        let reservation = consumer.register(pool);
        reservation.grow(bytes);
        reservation
    }

    #[test]
    fn tracks_peak_across_grow_and_shrink() {
        let inner: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1024));
        let tracked = Arc::new(TrackedMemoryPool::new(inner));
        let pool: Arc<dyn MemoryPool> = tracked.clone();

        let r = reserve(&pool, "t", 128);
        assert_eq!(tracked.peak(), 128);
        r.grow(256);
        assert_eq!(tracked.peak(), 384);
        r.shrink(128);
        // Peak does not decrease when reservation shrinks.
        assert_eq!(tracked.peak(), 384);
        r.grow(100);
        // Reservation now 356; still below prior peak of 384.
        assert_eq!(tracked.peak(), 384);
        r.grow(100);
        // Reservation now 456; new peak.
        assert_eq!(tracked.peak(), 456);
    }

    #[test]
    fn try_grow_failure_does_not_update_peak() {
        // Pool of size 100. First grow to 80, then try_grow(50) fails (would
        // exceed limit); peak should stay at 80.
        let inner: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(100));
        let tracked = Arc::new(TrackedMemoryPool::new(inner));
        let pool: Arc<dyn MemoryPool> = tracked.clone();

        let r = reserve(&pool, "t", 80);
        assert_eq!(tracked.peak(), 80);
        assert!(r.try_grow(50).is_err());
        assert_eq!(tracked.peak(), 80);
    }

    #[test]
    fn delegates_memory_limit() {
        let inner: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1024));
        let tracked = TrackedMemoryPool::new(inner);
        assert!(matches!(tracked.memory_limit(), MemoryLimit::Finite(1024)));
    }
}
