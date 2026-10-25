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

#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

pub mod alloc_accounting {
    //! Process-wide accounting of outstanding Rust allocator bytes.
    //!
    //! The standalone executor installs [`AccountingAllocator`] around its allocator. Library
    //! users keep their own global allocator unless they explicitly install this wrapper.
    //! Accounting is observational: it never rejects allocations or enforces a memory limit.
    //!
    //! This counts requested `Layout` bytes, not RSS or memory pool reservations. It excludes
    //! allocator fragmentation, retained pages, memory mappings, and allocations made directly
    //! by native libraries. Like Comet, updates are batched per thread to reduce contention.

    use std::alloc::{GlobalAlloc, Layout};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicIsize, Ordering};

    const SETTLE_THRESHOLD: isize = 64 * 1024;
    static BALANCE: AtomicIsize = AtomicIsize::new(0);

    thread_local! {
        static LOCAL_DRIFT: ThreadDrift = const { ThreadDrift(Cell::new(0)) };
    }

    struct ThreadDrift(Cell<isize>);

    impl Drop for ThreadDrift {
        fn drop(&mut self) {
            let drift = self.0.replace(0);
            if drift != 0 {
                BALANCE.fetch_add(drift, Ordering::Relaxed);
            }
        }
    }

    /// Returns the approximate outstanding bytes handed out by the accounting allocator.
    ///
    /// Each live thread can hold less than 64 KiB of unsettled accounting in either direction.
    /// Thread exit settles the remainder. Transient negative balances, caused by cross-thread
    /// allocations and frees settling out of order, are reported as zero.
    pub fn current_balance() -> usize {
        BALANCE.load(Ordering::Relaxed).max(0) as usize
    }

    fn settle(local_drift: &Cell<isize>, delta: isize) {
        let drift = local_drift.get().wrapping_add(delta);
        if drift.unsigned_abs() >= SETTLE_THRESHOLD as usize {
            local_drift.set(0);
            BALANCE.fetch_add(drift, Ordering::Relaxed);
        } else {
            local_drift.set(drift);
        }
    }

    #[inline]
    fn track(delta: isize) {
        if delta == 0 {
            return;
        }

        if LOCAL_DRIFT
            .try_with(|thread_drift| settle(&thread_drift.0, delta))
            .is_err()
        {
            BALANCE.fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Wraps a global allocator with process-wide, thread-batched allocation accounting.
    ///
    /// Allocations are counted only when successful; reallocations count the size difference.
    /// Frees are accounted before delegating, and successful reallocations afterwards.
    /// All instances share the same balance; install the wrapper only once around the backend.
    pub struct AccountingAllocator<Allocator: GlobalAlloc> {
        inner: Allocator,
    }

    impl<Allocator: GlobalAlloc> AccountingAllocator<Allocator> {
        /// Creates a wrapper without allocating or changing the backend's behavior.
        pub const fn new(inner: Allocator) -> Self {
            Self { inner }
        }
    }

    unsafe impl<Allocator: GlobalAlloc> GlobalAlloc for AccountingAllocator<Allocator> {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { self.inner.alloc(layout) };
            if !pointer.is_null() {
                track(layout.size() as isize);
            }
            pointer
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { self.inner.alloc_zeroed(layout) };
            if !pointer.is_null() {
                track(layout.size() as isize);
            }
            pointer
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            track(-(layout.size() as isize));
            unsafe { self.inner.dealloc(pointer, layout) };
        }

        unsafe fn realloc(
            &self,
            pointer: *mut u8,
            layout: Layout,
            new_size: usize,
        ) -> *mut u8 {
            let new_pointer = unsafe { self.inner.realloc(pointer, layout, new_size) };
            if !new_pointer.is_null() {
                track(new_size as isize - layout.size() as isize);
            }
            new_pointer
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::alloc::System;
        use std::ptr;
        use std::sync::Mutex;

        static SERIAL: Mutex<()> = Mutex::new(());
        const SIZE: usize = SETTLE_THRESHOLD as usize * 2;

        struct RecordingAllocator {
            balance_at_dealloc: AtomicIsize,
            balance_at_realloc: AtomicIsize,
            fail_realloc: bool,
        }

        impl RecordingAllocator {
            fn new(fail_realloc: bool) -> Self {
                Self {
                    balance_at_dealloc: AtomicIsize::new(0),
                    balance_at_realloc: AtomicIsize::new(0),
                    fail_realloc,
                }
            }
        }

        unsafe impl GlobalAlloc for RecordingAllocator {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                unsafe { System.alloc(layout) }
            }

            unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
                self.balance_at_dealloc
                    .store(BALANCE.load(Ordering::Relaxed), Ordering::Relaxed);
                unsafe { System.dealloc(pointer, layout) };
            }

            unsafe fn realloc(
                &self,
                pointer: *mut u8,
                layout: Layout,
                new_size: usize,
            ) -> *mut u8 {
                self.balance_at_realloc
                    .store(BALANCE.load(Ordering::Relaxed), Ordering::Relaxed);
                if self.fail_realloc {
                    ptr::null_mut()
                } else {
                    unsafe { System.realloc(pointer, layout, new_size) }
                }
            }
        }

        struct FailingAllocator;

        unsafe impl GlobalAlloc for FailingAllocator {
            unsafe fn alloc(&self, _layout: Layout) -> *mut u8 {
                ptr::null_mut()
            }

            unsafe fn dealloc(&self, _pointer: *mut u8, _layout: Layout) {}
        }

        struct TeardownAllocation;

        impl Drop for TeardownAllocation {
            fn drop(&mut self) {
                let allocator = AccountingAllocator::new(System);
                let layout = Layout::from_size_align(1024, 8).unwrap();
                let pointer = unsafe { allocator.alloc(layout) };
                assert!(!pointer.is_null());
                unsafe { allocator.dealloc(pointer, layout) };
            }
        }

        thread_local! {
            static TEARDOWN_ALLOCATION: TeardownAllocation = const { TeardownAllocation };
        }

        #[test]
        fn batches_positive_and_negative_deltas() {
            let _guard = SERIAL.lock().unwrap();
            let before = BALANCE.load(Ordering::Relaxed);
            let drift = Cell::new(0);
            settle(&drift, SETTLE_THRESHOLD - 1);
            assert_eq!(drift.get(), SETTLE_THRESHOLD - 1);
            assert_eq!(BALANCE.load(Ordering::Relaxed), before);
            settle(&drift, 1);
            assert_eq!(drift.get(), 0);
            assert_eq!(BALANCE.load(Ordering::Relaxed), before + SETTLE_THRESHOLD);
            settle(&drift, -SETTLE_THRESHOLD + 1);
            assert_eq!(drift.get(), -SETTLE_THRESHOLD + 1);
            settle(&drift, -1);
            assert_eq!(drift.get(), 0);
            assert_eq!(BALANCE.load(Ordering::Relaxed), before);
        }

        #[test]
        fn counts_allocations_and_accounts_before_freeing() {
            let _guard = SERIAL.lock().unwrap();
            let allocator = AccountingAllocator::new(RecordingAllocator::new(false));
            let layout = Layout::from_size_align(SIZE, 64).unwrap();
            let before = current_balance();
            let pointer = unsafe { allocator.alloc(layout) };
            assert!(!pointer.is_null());
            assert_eq!(pointer as usize % layout.align(), 0);
            assert_eq!(current_balance(), before + SIZE);
            unsafe { allocator.dealloc(pointer, layout) };
            assert_eq!(current_balance(), before);
            assert_eq!(
                allocator.inner.balance_at_dealloc.load(Ordering::Relaxed),
                before as isize
            );
        }

        #[test]
        fn counts_zeroed_allocations() {
            let _guard = SERIAL.lock().unwrap();
            let allocator = AccountingAllocator::new(System);
            let layout = Layout::from_size_align(SIZE, 8).unwrap();
            let before = current_balance();
            let pointer = unsafe { allocator.alloc_zeroed(layout) };
            assert!(!pointer.is_null());
            assert_eq!(current_balance(), before + SIZE);
            let bytes = unsafe { std::slice::from_raw_parts(pointer, SIZE) };
            assert!(bytes.iter().all(|byte| *byte == 0));
            unsafe { allocator.dealloc(pointer, layout) };
            assert_eq!(current_balance(), before);
        }

        #[test]
        fn failed_allocations_do_not_change_balance() {
            let _guard = SERIAL.lock().unwrap();
            let allocator = AccountingAllocator::new(FailingAllocator);
            let layout = Layout::from_size_align(SIZE, 8).unwrap();
            let before = current_balance();
            assert!(unsafe { allocator.alloc(layout) }.is_null());
            assert!(unsafe { allocator.alloc_zeroed(layout) }.is_null());
            assert_eq!(current_balance(), before);
        }

        #[test]
        fn realloc_counts_size_differences_after_delegating() {
            let _guard = SERIAL.lock().unwrap();
            let allocator = AccountingAllocator::new(RecordingAllocator::new(false));
            let layout = Layout::from_size_align(SIZE, 8).unwrap();
            let before = current_balance();
            let pointer = unsafe { allocator.alloc(layout) };
            assert!(!pointer.is_null());
            let pointer = unsafe { allocator.realloc(pointer, layout, SIZE * 2) };
            assert!(!pointer.is_null());
            assert_eq!(current_balance(), before + SIZE * 2);
            assert_eq!(
                allocator.inner.balance_at_realloc.load(Ordering::Relaxed),
                (before + SIZE) as isize
            );
            let layout = Layout::from_size_align(SIZE * 2, 8).unwrap();
            let pointer = unsafe { allocator.realloc(pointer, layout, SIZE) };
            assert!(!pointer.is_null());
            assert_eq!(current_balance(), before + SIZE);
            assert_eq!(
                allocator.inner.balance_at_realloc.load(Ordering::Relaxed),
                (before + SIZE * 2) as isize
            );
            let layout = Layout::from_size_align(SIZE, 8).unwrap();
            unsafe { allocator.dealloc(pointer, layout) };
            assert_eq!(current_balance(), before);
        }

        #[test]
        fn failed_realloc_keeps_original_allocation_accounted() {
            let _guard = SERIAL.lock().unwrap();
            let allocator = AccountingAllocator::new(RecordingAllocator::new(true));
            let layout = Layout::from_size_align(SIZE, 8).unwrap();
            let before = current_balance();
            let pointer = unsafe { allocator.alloc(layout) };
            assert!(!pointer.is_null());
            assert!(unsafe { allocator.realloc(pointer, layout, SIZE * 2) }.is_null());
            assert_eq!(current_balance(), before + SIZE);
            unsafe { allocator.dealloc(pointer, layout) };
            assert_eq!(current_balance(), before);
        }

        #[test]
        fn thread_exit_settles_small_allocations() {
            let _guard = SERIAL.lock().unwrap();
            let before = current_balance();
            let pointer = std::thread::spawn(|| {
                let allocator = AccountingAllocator::new(System);
                let layout = Layout::from_size_align(1024, 8).unwrap();
                let pointer = unsafe { allocator.alloc(layout) };
                assert!(!pointer.is_null());
                pointer as usize
            })
            .join()
            .unwrap();
            assert_eq!(current_balance(), before + 1024);
            std::thread::spawn(move || {
                let allocator = AccountingAllocator::new(System);
                let layout = Layout::from_size_align(1024, 8).unwrap();
                unsafe { allocator.dealloc(pointer as *mut u8, layout) };
            })
            .join()
            .unwrap();
            assert_eq!(current_balance(), before);
        }

        #[test]
        fn negative_balances_are_reported_as_zero() {
            let _guard = SERIAL.lock().unwrap();
            let before = BALANCE.load(Ordering::Relaxed);
            BALANCE.store(-1, Ordering::Relaxed);
            assert_eq!(current_balance(), 0);
            BALANCE.store(before, Ordering::Relaxed);
        }

        #[test]
        fn allocations_after_accounting_tls_teardown_do_not_panic() {
            let _guard = SERIAL.lock().unwrap();
            let before = current_balance();
            std::thread::spawn(|| {
                TEARDOWN_ALLOCATION.with(|_| {});
                LOCAL_DRIFT.with(|_| {});
            })
            .join()
            .unwrap();
            assert_eq!(current_balance(), before);
        }
    }
}
/// Connection pool for `BallistaClient` instances.
mod client_pool;
/// Execution plan for collecting distributed query results into a single partition.
pub mod collect;
/// Command-line configuration for the executor binary.
#[cfg(feature = "build-binary")]
pub mod config;
/// Extension point for custom query stage execution engines.
pub mod execution_engine;
/// Pull-based task execution loop that polls the scheduler for work.
pub mod execution_loop;
/// Core executor implementation for running distributed query tasks.
pub mod executor;
/// Executor process lifecycle management and configuration.
pub mod executor_process;
/// gRPC server for receiving pushed tasks from the scheduler.
pub mod executor_server;
/// Arrow Flight service for streaming shuffle data between executors.
pub mod flight_service;
/// HTTP server for Kubernetes-style /healthz and /readyz probes.
pub mod health;
/// Metrics collection for executor runtime statistics.
pub mod metrics;
/// Session-scoped cache of shared executor runtime environments.
pub mod runtime_cache;
/// Graceful shutdown coordination for executor components.
pub mod shutdown;
/// Signal handling for process termination.
pub mod terminate;

mod cpu_bound_executor;
mod standalone;

use ballista_core::error::BallistaError;
use log::debug;
use std::net::SocketAddr;

pub use standalone::new_standalone_executor;
pub use standalone::new_standalone_executor_from_builder;
pub use standalone::new_standalone_executor_from_state;

use crate::shutdown::Shutdown;
use ballista_core::serde::protobuf::{
    FailedTask, OperatorMetricsSet, RuntimeStatsReport, ShuffleWritePartition,
    SuccessfulTask, TaskColumnStats, TaskStatus, WindowStateReport, task_status,
};
use ballista_core::serde::scheduler::TaskKey;
use ballista_core::utils::GrpcServerConfig;
use log::info;

/// [ArrowFlightServerProvider] provides a function which creates a new Arrow Flight server.
///
/// The function should take four arguments:
/// [String] - executor work directory
/// [SocketAddr] - the address to bind the server to
/// [Shutdown] - a shutdown signal to gracefully shutdown the server
/// [GrpcServerConfig] - the gRPC server configuration for timeout settings
/// Returns a [tokio::task::JoinHandle] which will be registered as service handler
///
pub type ArrowFlightServerProvider = dyn Fn(
        String,
        SocketAddr,
        Shutdown,
        GrpcServerConfig,
    ) -> tokio::task::JoinHandle<Result<(), BallistaError>>
    + Send
    + Sync;

/// Timestamps capturing the lifecycle of a task execution.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskExecutionTimes {
    /// Timestamp when the task was launched by the scheduler (milliseconds since epoch).
    launch_time: u64,
    /// Timestamp when task execution started on the executor (milliseconds since epoch).
    start_exec_time: u64,
    /// Timestamp when task execution completed (milliseconds since epoch).
    end_exec_time: u64,
}

/// Side-channel data harvested from a task's executed plan, attached to the
/// [`TaskStatus`] reported to the scheduler on success.
///
/// Marked `#[non_exhaustive]` so future additions (e.g. tracing IDs, further
/// runtime reports) are non-breaking for external callers that construct via
/// `TaskCompletionExtras { operator_metrics: …, ..Default::default() }`.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TaskCompletionExtras {
    /// Per-operator metrics collected from the executed plan.
    pub operator_metrics: Option<Vec<OperatorMetricsSet>>,
    /// Runtime-stats reports harvested from `RuntimeStatsExec` taps in the plan.
    pub runtime_stats: Vec<RuntimeStatsReport>,
    /// Finalized window-aggregate state captured by an ever-expanding-frame
    /// window, already stamped with the global partition each entry belongs
    /// to by the stage's `ShuffleWriterExec`.
    pub window_state: Vec<WindowStateReport>,
    /// Per-column statistics folded across this task's shuffle output. Empty
    /// when the executed plan collects none (e.g. non-sort shuffle paths).
    pub column_stats: Vec<TaskColumnStats>,
}

/// Converts a task execution result into a [`TaskStatus`] protobuf message.
///
/// This function wraps the outcome of task execution (success or failure)
/// along with timing and metrics information into a status message that
/// can be sent back to the scheduler.
pub fn as_task_status(
    execution_result: Result<Vec<ShuffleWritePartition>, BallistaError>,
    executor_id: String,
    stage_attempt_num: usize,
    key: TaskKey,
    execution_times: TaskExecutionTimes,
    extras: TaskCompletionExtras,
) -> TaskStatus {
    let TaskCompletionExtras {
        operator_metrics,
        runtime_stats,
        window_state,
        column_stats,
    } = extras;
    let metrics = operator_metrics.unwrap_or_default();
    let task_id = key.task_id;
    match execution_result {
        Ok(partitions) => {
            debug!(
                "Task {task_id} finished with operator_metrics array size {} \
                 and {} runtime-stats report(s), {} window-state report(s)",
                metrics.len(),
                runtime_stats.len(),
                window_state.len(),
            );
            TaskStatus {
                task_id: task_id as u32,
                job_id: key.job_id.clone().into(),
                stage_id: key.stage_id as u32,
                stage_attempt_num: stage_attempt_num as u32,
                launch_time: execution_times.launch_time,
                start_exec_time: execution_times.start_exec_time,
                end_exec_time: execution_times.end_exec_time,
                metrics,
                status: Some(task_status::Status::Successful(SuccessfulTask {
                    executor_id,
                    partitions,
                    runtime_stats,
                    task_column_stats: column_stats,
                    window_state,
                })),
            }
        }
        Err(e) => {
            let error_msg = e.to_string();
            info!("Task {task_id} failed: {error_msg}");

            TaskStatus {
                task_id: task_id as u32,
                job_id: key.job_id.clone().into(),
                stage_id: key.stage_id as u32,
                stage_attempt_num: stage_attempt_num as u32,
                launch_time: execution_times.launch_time,
                start_exec_time: execution_times.start_exec_time,
                end_exec_time: execution_times.end_exec_time,
                metrics,
                status: Some(task_status::Status::Failed(FailedTask::from(e))),
            }
        }
    }
}
