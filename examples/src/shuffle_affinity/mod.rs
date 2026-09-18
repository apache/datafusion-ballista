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

//! Shuffle-affinity task distribution: place each task on the executor already
//! holding most of its shuffle input, so it reads locally instead of over Flight.

use std::sync::Mutex;

mod locality;
mod policy;
mod scheduler_internals;
mod stats;

#[cfg(test)]
mod tests;

pub use policy::ShuffleAffinityPolicy;
pub use stats::{LocalityObserver, LocalityStats};

/// Locks `mutex`, recovering from poisoning; the guarded memo and counters stay valid.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
