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

//! Allocator-backed OOM protection for the executor.
//!
//! Ballista's memory accounting relies on voluntary `MemoryPool` reservations, which
//! miss allocations made by Arrow buffers, join scratch space, and expression
//! kernels. This module tracks the bytes the global allocator actually hands out and
//! uses that signal in two layers:
//!
//! - a cooperative gate that rejects pool growth (so DataFusion spills and retries)
//!   once real usage plus the request would exceed the budget, and
//! - a last-resort circuit breaker that fails a single task with a retriable error
//!   rather than letting the process be OOM-killed.
//!
//! The allocator itself only *tracks*: unwinding out of a global allocator is
//! undefined behaviour, so enforcement happens at safe points (pool growth and plan
//! poll boundaries) instead.

mod guard_exec;
pub mod oom_guard;
mod real_usage_pool;

pub use guard_exec::MemoryGuardExec;
pub use real_usage_pool::RealUsagePool;

/// Serializes every test in this crate that reads or writes the process-global limit
/// and balance owned by [`oom_guard`], wherever in the module tree it lives.
///
/// Those two atomics are shared mutable state for the whole test binary, so a mutex
/// private to one module would not serialize it against a test in another module
/// writing the same globals. See `oom_guard::test_support::ArmedGuard`, which takes
/// this lock and restores both values on drop.
#[cfg(test)]
pub(crate) static GLOBAL_STATE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
