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

//! A [`MemoryPool`] decorator that gates growth on *real* allocator usage.

use crate::memory_pools::oom_guard;
use datafusion::common::{DataFusionError, resources_datafusion_err};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::sync::Arc;

/// Source of the current process-wide live allocator usage, in bytes. Production
/// wiring uses [`oom_guard::current_balance`]; tests inject a controllable value.
type BalanceSource = Arc<dyn Fn() -> usize + Send + Sync>;

/// A [`MemoryPool`] decorator that, on top of the inner pool's tracked-reservation
/// accounting, rejects growth when *real* allocator usage (untracked Arrow, join, and
/// kernel bytes included) plus the requested amount would exceed a process-global
/// ceiling. Returning `ResourcesExhausted` lets DataFusion spill and retry.
///
/// # What the ceiling means
///
/// The ceiling is the executor's whole `--memory-pool-size`, and it is compared against
/// the live bytes the global allocator has handed out for the **entire process** -- not
/// just query memory. gRPC buffers, tokio's internals, object-store clients and caches,
/// and the shuffle write path all count against it, because they are all real memory the
/// OOM killer would see. So an executor built with the `oom-guard` feature gates somewhat
/// *earlier* than the same `--memory-pool-size` implies in the default build, where the
/// pool only ever sees explicit reservations. No baseline is subtracted: the number
/// budgets the process, not the queries.
///
/// Rejection is first-come: once the process is over the ceiling, any `try_grow` is
/// rejected, so every running task spills and real usage drops. A small task can be
/// the one told to spill, but spilling is cheap and correct, and a consumer that
/// cannot spill falls through to a retriable task failure. Because the tracked balance
/// falls as soon as the spilled memory is freed, the gate re-opens immediately -- it does
/// not stay closed waiting for the OS to reclaim pages.
pub struct RealUsagePool {
    inner: Arc<dyn MemoryPool>,
    /// Process-global real-usage ceiling in bytes; 0 means unset (no gating).
    ceiling: usize,
    balance_source: BalanceSource,
}

impl std::fmt::Debug for RealUsagePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealUsagePool")
            .field("inner", &self.inner)
            .field("ceiling", &self.ceiling)
            .finish_non_exhaustive()
    }
}

impl RealUsagePool {
    /// Wrap `inner` with the real-usage gate, reading the live allocator balance.
    /// A `ceiling` of 0 disables gating.
    pub fn new(inner: Arc<dyn MemoryPool>, ceiling: usize) -> Self {
        Self {
            inner,
            ceiling,
            balance_source: Arc::new(oom_guard::current_balance),
        }
    }

    /// Wrap `inner` with an explicit balance source (test seam).
    #[cfg(test)]
    fn with_balance_source(
        inner: Arc<dyn MemoryPool>,
        ceiling: usize,
        balance_source: BalanceSource,
    ) -> Self {
        Self {
            inner,
            ceiling,
            balance_source,
        }
    }
}

impl std::fmt::Display for RealUsagePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(ceiling: {}, inner: {})",
            self.name(),
            self.ceiling,
            self.inner
        )
    }
}

impl MemoryPool for RealUsagePool {
    fn name(&self) -> &str {
        "real_usage"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink)
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        // Check the real-usage ceiling before delegating, so an over-budget request is
        // rejected without speculatively reserving the inner pool.
        //
        // This rejection is **immediate**: no hysteresis, no debounce, no grace period.
        // That is the crux of how the two layers order themselves. This is the
        // *cooperative* layer, and it is meant to fire first: rejecting a grow is cheap
        // and safe -- DataFusion simply spills the consumer and retries -- so the
        // earliest possible rejection is the best one. The *circuit breaker*
        // (`oom_guard::check_budget`) is the layer that fails whole tasks, and it is the
        // one that debounces, precisely so that the spill this rejection triggers has
        // time to bring the balance back down before any task is killed. Adding
        // hysteresis here would invert that: the guard would start failing tasks before
        // the cheap remedy had even been offered.
        if self.ceiling != 0 && additional != 0 {
            let real = (self.balance_source)();
            if real.saturating_add(additional) > self.ceiling {
                return Err(resources_datafusion_err!(
                    "Ballista real-usage gate: native usage {real} bytes + requested \
                     {additional} bytes exceeds the executor memory budget of {} bytes; \
                     spilling or failing this consumer",
                    self.ceiling
                ));
            }
        }
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fixed_source(bytes: usize) -> BalanceSource {
        let cell = Arc::new(AtomicUsize::new(bytes));
        Arc::new(move || cell.load(Ordering::Relaxed))
    }

    fn pool_with_balance(
        inner: Arc<dyn MemoryPool>,
        ceiling: usize,
        balance: usize,
    ) -> Arc<RealUsagePool> {
        Arc::new(RealUsagePool::with_balance_source(
            inner,
            ceiling,
            fixed_source(balance),
        ))
    }

    #[test]
    fn under_ceiling_delegates_to_inner() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let pool = pool_with_balance(Arc::clone(&inner), 1024 * 1024, 100);
        let reservation =
            MemoryConsumer::new("test").register(&(pool as Arc<dyn MemoryPool>));

        reservation
            .try_grow(1024)
            .expect("under the ceiling should succeed");
        assert_eq!(inner.reserved(), 1024, "the grow must reach the inner pool");
    }

    #[test]
    fn over_ceiling_rejects_without_reserving_inner() {
        // Real usage is already at the ceiling, but the inner pool has plenty of room:
        // only the real-usage gate can reject this.
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let pool = pool_with_balance(Arc::clone(&inner), 1000, 1000);
        let reservation =
            MemoryConsumer::new("test").register(&(pool as Arc<dyn MemoryPool>));

        let err = reservation
            .try_grow(1)
            .expect_err("over the ceiling should be rejected");
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "expected ResourcesExhausted so DataFusion spills, got: {err}"
        );
        assert_eq!(
            inner.reserved(),
            0,
            "a rejected grow must not speculatively reserve the inner pool"
        );
    }

    #[test]
    fn unset_ceiling_never_gates() {
        let inner: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        // ceiling == 0 means unset; even a huge real balance must not gate.
        let pool = pool_with_balance(Arc::clone(&inner), 0, usize::MAX / 2);
        let reservation =
            MemoryConsumer::new("test").register(&(pool as Arc<dyn MemoryPool>));

        reservation
            .try_grow(4096)
            .expect("an unset ceiling must never gate");
        assert_eq!(inner.reserved(), 4096);
    }

    #[test]
    fn zero_sized_grow_is_never_gated() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let pool = pool_with_balance(Arc::clone(&inner), 1000, 5000);
        let reservation =
            MemoryConsumer::new("test").register(&(pool as Arc<dyn MemoryPool>));

        reservation
            .try_grow(0)
            .expect("a zero-byte grow must never be rejected");
    }

    #[test]
    fn grow_exactly_to_the_ceiling_is_allowed_but_one_byte_over_is_not() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let pool = pool_with_balance(Arc::clone(&inner), 1000, 900);
        let reservation = MemoryConsumer::new("test")
            .register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>));

        // real (900) + additional (100) == ceiling (1000): allowed, strictly-greater is required.
        reservation
            .try_grow(100)
            .expect("growing exactly to the ceiling must be allowed");

        // real (900) + additional (101) == 1001 > ceiling (1000): rejected.
        let over =
            pool_with_balance(Arc::new(GreedyMemoryPool::new(1024 * 1024)), 1000, 900);
        let reservation =
            MemoryConsumer::new("test").register(&(over as Arc<dyn MemoryPool>));
        assert!(
            reservation.try_grow(101).is_err(),
            "one byte over the ceiling must be rejected"
        );
    }

    #[test]
    fn a_huge_balance_saturates_rather_than_overflowing() {
        // real + additional would overflow usize; saturating_add must clamp so this is
        // rejected, not panic.
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let pool = pool_with_balance(Arc::clone(&inner), 1000, usize::MAX);
        let reservation =
            MemoryConsumer::new("test").register(&(pool as Arc<dyn MemoryPool>));

        assert!(
            reservation.try_grow(4096).is_err(),
            "a balance near usize::MAX must saturate and reject, not overflow"
        );
        assert_eq!(
            inner.reserved(),
            0,
            "a rejected grow must not reserve the inner pool"
        );
    }

    #[test]
    fn shrink_and_accessors_delegate_to_inner() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let pool = pool_with_balance(Arc::clone(&inner), 1024 * 1024, 0);
        let reservation = MemoryConsumer::new("test")
            .register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>));

        reservation.try_grow(4096).unwrap();
        assert_eq!(pool.reserved(), 4096, "reserved() must delegate");

        reservation.shrink(1024);
        assert_eq!(inner.reserved(), 3072, "shrink() must delegate");
        assert_eq!(pool.reserved(), 3072);

        assert!(
            matches!(pool.memory_limit(), MemoryLimit::Finite(n) if n == 1024 * 1024),
            "memory_limit() must delegate to the inner pool"
        );
    }
}
