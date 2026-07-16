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

//! End-to-end proof that the installed `AccountingAllocator` moves the tracked
//! balance for real heap allocations, that `check_budget` trips on it, and that a
//! [`MemoryGuardExec`] over that balance fails its stream with `ResourcesExhausted`.
//!
//! This lives in its own integration test binary, deliberately separate from the
//! crate's unit test module, for two reasons:
//!
//! - An integration test links `ballista-executor` as a normal dependency, so
//!   `cfg(test)` is *not* set for the library. There is therefore no library-side
//!   test allocator to conflict with, and this file is free to declare its own
//!   `#[global_allocator]`.
//! - Nothing else in this process allocates concurrently, so asserting against the
//!   process-global balance is deterministic here -- unlike in the crate's unit test
//!   binary, where the tracking allocator is not installed at all (the balance is
//!   simply zero), and dozens of unrelated tests run in parallel. The unit tests there
//!   must therefore inject a balance; only here is the real causal chain observable.
//!   The tests in this file take [`SERIAL`] so they do not perturb each other.
//!
//! The whole file is gated on the `oom-guard` feature: `ballista_executor::memory_pools`
//! itself only exists when that feature is enabled.

#![cfg(feature = "oom-guard")]

use ballista_executor::memory_pools::oom_guard::{
    AccountingAllocator, arm, check_budget, current_balance,
};
use ballista_executor::memory_pools::{MemoryGuardExec, RealUsagePool};
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer, MemoryPool};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::common::collect;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Poll `check_budget` until the circuit breaker trips, and return the error it raised.
///
/// The breaker deliberately debounces: it fails a task only once the process has been
/// over budget for several *consecutive* checks and for a minimum interval, so that a
/// spill triggered by the cooperative gate has a chance to rescue the executor before
/// any task is killed. Nothing resets that streak while the memory below is still held,
/// so this converges.
///
/// This is the only test binary that sees the production thresholds -- the library's own
/// unit tests are built with `cfg(test)`, where the debounce is relaxed to trip on the
/// first check -- so this loop is also what proves the debounced global path can trip at
/// all. It panics rather than hanging if it cannot.
fn wait_for_breaker_to_trip() -> DataFusionError {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match check_budget() {
            Err(err) => return err,
            Ok(()) => {
                assert!(
                    Instant::now() < deadline,
                    "the memory is still held and the process is still over budget, so \
                     the breaker must trip: it never did within 10s"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[cfg(feature = "oom-guard")]
#[global_allocator]
static GLOBAL: AccountingAllocator<std::alloc::System> =
    AccountingAllocator::new(std::alloc::System);

/// Serializes the tests in this binary. They share the process-global limit and balance,
/// and `cargo test` runs them on parallel threads, so without this one test's `arm` (or
/// its 64 MiB of held allocations) would perturb the other.
static SERIAL: Mutex<()> = Mutex::new(());

/// Holds [`SERIAL`] and disarms the limit on drop, including on an early return from a
/// failed assertion -- so a failure here cannot leave the limit armed for the next test.
struct Serial(#[allow(dead_code)] MutexGuard<'static, ()>);

impl Serial {
    fn acquire() -> Self {
        Self(SERIAL.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

impl Drop for Serial {
    fn drop(&mut self) {
        arm(0);
    }
}

#[test]
fn real_allocations_move_the_balance_and_trip_check_budget() {
    let _serial = Serial::acquire();
    // 8 MiB of headroom over the current baseline. `Serial` keeps the other test in
    // this binary from running concurrently, so there is no allocator traffic to
    // introduce noise.
    let headroom = 8 * 1024 * 1024;
    arm(current_balance() + headroom);
    assert!(check_budget().is_ok(), "should start under budget");

    // Allocate well past the headroom and hold it, so the balance stays raised.
    let mut held: Vec<Vec<u8>> = Vec::new();
    for _ in 0..64 {
        held.push(vec![0u8; 1024 * 1024]);
    }
    // Touch the data so the allocations cannot be optimized away.
    assert_eq!(
        held.iter().map(|v| v.len()).sum::<usize>(),
        64 * 1024 * 1024
    );

    let err = wait_for_breaker_to_trip();
    assert!(matches!(err, DataFusionError::ResourcesExhausted(_)));

    // Dropping the allocations must bring the balance back under budget -- immediately,
    // with no debounce on the way *out*: the breaker's streak is reset by the very first
    // under-budget observation, which is what makes a successful spill un-arm it.
    drop(held);
    assert!(
        check_budget().is_ok(),
        "freeing the allocations must credit the balance back"
    );

    arm(0);
}

/// The cooperative gate, built the way production builds it.
///
/// Every `RealUsagePool` unit test injects a fake balance through the crate-private
/// `with_balance_source` seam, so the *production* constructor -- `RealUsagePool::new`,
/// the only path that installs `oom_guard::current_balance` as the pool's balance source
/// -- is type-checked but never behaviour-checked there. A `new()` that captured `|| 0`
/// would pass every one of those tests while silently disabling the gate in production.
///
/// This is the test that would catch that: it builds the pool through `new()`, allocates
/// real heap memory until the *real* allocator balance is over the ceiling, and requires
/// the next `try_grow` to be rejected. It can only live here, in the binary where the
/// tracking allocator is genuinely installed.
#[test]
fn the_production_pool_gates_on_the_real_allocator_balance() {
    let _serial = Serial::acquire();

    // A ceiling 8 MiB above wherever the balance happens to sit right now; the inner pool
    // is given a gigabyte, so nothing but the real-usage gate can reject a small grow.
    let ceiling = current_balance() + 8 * 1024 * 1024;
    let inner: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1024 * 1024 * 1024));
    let pool: Arc<dyn MemoryPool> =
        Arc::new(RealUsagePool::new(Arc::clone(&inner), ceiling));
    let reservation = MemoryConsumer::new("real-usage-test").register(&pool);

    // Under the ceiling: the grow goes through to the inner pool.
    reservation
        .try_grow(1024)
        .expect("under the ceiling: the grow must be allowed");
    assert_eq!(inner.reserved(), 1024, "the grow must reach the inner pool");

    // Now allocate 64 MiB for real and hold it, so the balance the *installed* allocator
    // tracks genuinely rises past the 8 MiB of headroom.
    let mut held: Vec<Vec<u8>> = Vec::new();
    for _ in 0..64 {
        held.push(vec![0u8; 1024 * 1024]);
    }
    assert_eq!(
        held.iter().map(|v| v.len()).sum::<usize>(),
        64 * 1024 * 1024,
        "touch the data so the allocations cannot be optimized away"
    );
    assert!(
        current_balance() > ceiling,
        "the real allocator balance must now be over the ceiling"
    );

    let err = reservation
        .try_grow(1024)
        .expect_err("over the ceiling: the production pool must reject the grow");
    assert!(
        matches!(err, DataFusionError::ResourcesExhausted(_)),
        "must be ResourcesExhausted so DataFusion spills and retries, got: {err}"
    );
    assert_eq!(
        inner.reserved(),
        1024,
        "a rejected grow must not reserve the inner pool"
    );

    // Freeing the memory must let the very same pool grow again: the gate reads the live
    // balance, it does not latch.
    drop(held);
    reservation
        .try_grow(1024)
        .expect("back under the ceiling: the grow must be allowed again");
}

/// The single-batch input the guard node is exercised over. Deliberately tiny: the
/// memory that trips the guard is allocated by the test itself, not by the plan.
fn test_input() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

/// The real causal chain, end to end: a genuine heap allocation raises the balance the
/// installed `AccountingAllocator` tracks, and a `MemoryGuardExec` over that balance
/// fails its stream with `ResourcesExhausted`.
///
/// The unit tests in `guard_exec.rs` drive the balance with an injected value, because
/// the crate's unit test binary has no tracking allocator installed. This is the only
/// place the chain from `Vec::new` through the allocator to a failed task can actually
/// be proven, so it is proven here.
#[tokio::test]
async fn a_real_allocation_makes_the_guard_node_fail_its_stream() {
    let _serial = Serial::acquire();

    let ctx = Arc::new(TaskContext::default());
    let guard = Arc::new(MemoryGuardExec::new(test_input()));

    // Under budget to begin with: the node is transparent and the batch passes through.
    arm(current_balance() + 8 * 1024 * 1024);
    let batches = collect(guard.execute(0, Arc::clone(&ctx)).unwrap())
        .await
        .expect("under budget: the batch must pass through");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

    // Now really allocate past the headroom, and hold it so the balance stays raised.
    let mut held: Vec<Vec<u8>> = Vec::new();
    for _ in 0..64 {
        held.push(vec![0u8; 1024 * 1024]);
    }
    assert_eq!(
        held.iter().map(|v| v.len()).sum::<usize>(),
        64 * 1024 * 1024,
        "touch the data so the allocations cannot be optimized away"
    );

    // The guard node's stream checks the budget once per batch, and this input has a
    // single batch -- one check, which the breaker's debounce (deliberately) will not
    // trip on. Ride out the debounce first: the memory is still held, so the streak is
    // never reset, and once the breaker has tripped it keeps tripping for as long as the
    // process stays over budget -- including on the node's own check below.
    let err = wait_for_breaker_to_trip();
    assert!(matches!(err, DataFusionError::ResourcesExhausted(_)));

    let err = collect(guard.execute(0, Arc::clone(&ctx)).unwrap())
        .await
        .expect_err("over budget: the guard must fail the stream");
    assert!(
        matches!(err, DataFusionError::ResourcesExhausted(_)),
        "must be ResourcesExhausted so the task failure is retriable, got: {err}"
    );

    // Freeing the memory must let the very same node stream successfully again -- the
    // guard reflects the live balance, it does not latch.
    drop(held);
    let batches = collect(guard.execute(0, ctx).unwrap())
        .await
        .expect("back under budget: the batch must pass through again");
    assert_eq!(batches.len(), 1);
}
