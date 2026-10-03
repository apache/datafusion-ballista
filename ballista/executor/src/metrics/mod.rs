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

use crate::execution_engine::QueryStageExecutor;
use ballista_core::JobId;
use datafusion::execution::memory_pool::MemoryPool;
use log::debug;
use parking_lot::Mutex;
use std::{
    fmt::Display,
    sync::{Arc, Weak},
};

/// Shared, observational memory metrics for a single executor process.
///
/// Pools are held weakly and shared pools are counted once. Sampling copies live pools
/// before calling [`MemoryPool::reserved`], so pool callbacks run outside the registry lock.
#[derive(Clone, Default)]
pub struct ExecutorMemoryMetrics {
    inner: Arc<Mutex<MemoryMetricsState>>,
}

#[derive(Default)]
struct MemoryMetricsState {
    initialized: bool,
    pool_size: Option<u64>,
    pools: Vec<Weak<dyn MemoryPool>>,
}

/// A point-in-time view of executor memory pool usage.
#[derive(Debug, PartialEq, Eq)]
pub struct ExecutorMemoryUsage {
    /// Resolved executor-wide pool budget in bytes; `None` means unbounded.
    /// Per-task limits are sized from this budget according to their vcore claims.
    pub pool_size: Option<u64>,
    /// Sum of current reservations across distinct live task pools, in bytes.
    pub reserved: usize,
    /// Number of distinct live pools, including shared pools retained by session runtimes.
    pub pool_count: usize,
}

impl ExecutorMemoryMetrics {
    pub(crate) fn set_pool_size(&self, pool_size: Option<u64>) {
        let mut state = self.inner.lock();
        state.pool_size = pool_size;
        state.initialized = true;
    }

    pub(crate) fn register(&self, pool: &Arc<dyn MemoryPool>) {
        let reference = Arc::downgrade(pool);
        let mut state = self.inner.lock();
        state.pools.retain(|pool| pool.strong_count() != 0);
        if !state.pools.iter().any(|pool| pool.ptr_eq(&reference)) {
            state.pools.push(reference);
        }
    }

    /// Samples the resolved budget and current pool reservations.
    ///
    /// Returns `None` before executor memory pool initialization. Reservation reads are
    /// not atomic across pools and may race with tasks reserving or releasing memory.
    pub fn snapshot(&self) -> Option<ExecutorMemoryUsage> {
        let (pool_size, pools) = {
            let mut state = self.inner.lock();
            if !state.initialized {
                return None;
            }
            let mut pools = Vec::with_capacity(state.pools.len());
            state.pools.retain(|reference| {
                if let Some(pool) = reference.upgrade() {
                    pools.push(pool);
                    true
                } else {
                    false
                }
            });
            (state.pool_size, pools)
        };

        Some(ExecutorMemoryUsage {
            pool_size,
            reserved: pools
                .iter()
                .fold(0_usize, |total, pool| total.saturating_add(pool.reserved())),
            pool_count: pools.len(),
        })
    }
}

/// `ExecutorMetricsCollector` records metrics for `ShuffleWriteExec`
/// after they are executed.
///
/// After each stage completes, `ShuffleWriteExec::record_stage` will be
/// called.
pub trait ExecutorMetricsCollector: Send + Sync {
    /// Record metrics for stage after it is executed
    fn record_stage(
        &self,
        job_id: &JobId,
        stage_id: usize,
        task_id: usize,
        plan: Arc<dyn QueryStageExecutor>,
    );
}

/// Implementation of `ExecutorMetricsCollector` which logs the completed
/// plan to stdout.
#[derive(Default)]
pub struct LoggingMetricsCollector {}

impl ExecutorMetricsCollector for LoggingMetricsCollector {
    fn record_stage(
        &self,
        job_id: &JobId,
        stage_id: usize,
        task_id: usize,
        plan: Arc<dyn QueryStageExecutor>,
    ) {
        debug!(
            "\n=== [{job_id}/{stage_id}/{task_id}] Physical plan with metrics ===\n{plan}\n"
        );
    }
}

/// Configures which executor's metrics should be collected
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum ExecutorMetricCollectionPolicy {
    /// Collect only system-wide metrics
    #[cfg_attr(feature = "build-binary", clap(name = "sys"))]
    SystemOnly,
    /// Collect only current process metrics
    #[cfg_attr(feature = "build-binary", clap(name = "proc"))]
    #[default]
    ProcessOnly,
    /// Collect both system-wide and process metrics
    #[cfg_attr(feature = "build-binary", clap(name = "all"))]
    SystemAndProcess,
    /// No metrics collected
    Off,
}

impl Display for ExecutorMetricCollectionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorMetricCollectionPolicy::SystemOnly => f.write_str("sys"),
            ExecutorMetricCollectionPolicy::ProcessOnly => f.write_str("proc"),
            ExecutorMetricCollectionPolicy::SystemAndProcess => f.write_str("all"),
            ExecutorMetricCollectionPolicy::Off => f.write_str("off"),
        }
    }
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for ExecutorMetricCollectionPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryReservation, UnboundedMemoryPool,
    };

    #[test]
    fn reports_budget_only_after_initialization() {
        let metrics = ExecutorMemoryMetrics::default();
        assert_eq!(metrics.snapshot(), None);
        metrics.set_pool_size(Some(1024));
        assert_eq!(
            metrics.snapshot(),
            Some(ExecutorMemoryUsage {
                pool_size: Some(1024),
                reserved: 0,
                pool_count: 0,
            })
        );
        metrics.set_pool_size(None);
        assert_eq!(metrics.snapshot().unwrap().pool_size, None);
    }

    #[test]
    fn sums_reservations_and_deduplicates_shared_pools() {
        let metrics = ExecutorMemoryMetrics::default();
        metrics.set_pool_size(Some(1024));
        let first: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512));
        let second: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512));
        metrics.register(&first);
        metrics.register(&first.clone());
        metrics.register(&second);
        let first_reservation = MemoryConsumer::new("first").register(&first);
        let second_reservation = MemoryConsumer::new("second").register(&second);
        first_reservation.try_grow(128).unwrap();
        second_reservation.try_grow(256).unwrap();
        assert_eq!(metrics.snapshot().unwrap().reserved, 384);
        assert_eq!(metrics.snapshot().unwrap().pool_count, 2);
        first_reservation.shrink(64);
        assert_eq!(metrics.snapshot().unwrap().reserved, 320);
        drop(first_reservation);
        drop(second_reservation);
        assert_eq!(metrics.snapshot().unwrap().reserved, 0);
        drop(first);
        drop(second);
        assert_eq!(metrics.snapshot().unwrap().pool_count, 0);
    }

    #[test]
    fn reservations_keep_pools_visible_but_registry_does_not() {
        let metrics = ExecutorMemoryMetrics::default();
        metrics.set_pool_size(None);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        metrics.register(&pool);
        let reservation = MemoryConsumer::new("held").register(&pool);
        reservation.try_grow(128).unwrap();
        drop(pool);
        assert_eq!(metrics.snapshot().unwrap().reserved, 128);
        assert_eq!(metrics.snapshot().unwrap().pool_count, 1);
        drop(reservation);
        assert_eq!(metrics.snapshot().unwrap().pool_count, 0);
    }

    #[test]
    fn registration_prunes_expired_pools() {
        let metrics = ExecutorMemoryMetrics::default();
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        metrics.register(&pool);
        drop(pool);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        metrics.register(&pool);
        assert_eq!(metrics.inner.lock().pools.len(), 1);
    }

    struct LockCheckingPool {
        state: Arc<Mutex<MemoryMetricsState>>,
        inner: UnboundedMemoryPool,
    }

    impl std::fmt::Debug for LockCheckingPool {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("LockCheckingPool")
        }
    }

    impl Display for LockCheckingPool {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("LockCheckingPool")
        }
    }

    impl MemoryPool for LockCheckingPool {
        fn name(&self) -> &str {
            "LockCheckingPool"
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.inner.grow(reservation, additional);
        }

        fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
            self.inner.shrink(reservation, shrink);
        }

        fn try_grow(
            &self,
            reservation: &MemoryReservation,
            additional: usize,
        ) -> datafusion::error::Result<()> {
            self.inner.try_grow(reservation, additional)
        }

        fn reserved(&self) -> usize {
            assert!(self.state.try_lock().is_some());
            self.inner.reserved()
        }
    }

    #[test]
    fn samples_pools_outside_registry_lock() {
        let metrics = ExecutorMemoryMetrics::default();
        metrics.set_pool_size(None);
        let pool: Arc<dyn MemoryPool> = Arc::new(LockCheckingPool {
            state: metrics.inner.clone(),
            inner: UnboundedMemoryPool::default(),
        });
        metrics.register(&pool);
        assert_eq!(metrics.snapshot().unwrap().reserved, 0);
    }
}
