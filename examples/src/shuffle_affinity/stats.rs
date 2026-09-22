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

//! Locality measurements and the observer hook that publishes them.

/// Locality of the policy's placements, summed over its life and logged per round at
/// `debug`. Counted when tasks are bound, so a task bound again after a failed launch
/// or a lost executor counts again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalityStats {
    /// Tasks bound.
    pub tasks: u64,
    /// Input partitions covered by those tasks.
    pub partitions: u64,
    /// Of those, the ones bound to an executor holding some of their input.
    pub local_partitions: u64,
    /// Of `total_bytes`, the bytes on the executor each task was bound to.
    pub local_bytes: u64,
    /// Shuffle bytes those tasks read from their own partitions. Inputs every task reads
    /// in full, such as a broadcast, count only for single-task stages.
    pub total_bytes: u64,
}

impl LocalityStats {
    /// Share of `total_bytes` placed on the executor holding them, in `0.0..=1.0`, or
    /// zero before any are bound.
    pub fn local_byte_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        self.local_bytes as f64 / self.total_bytes as f64
    }
}

impl std::ops::AddAssign for LocalityStats {
    fn add_assign(&mut self, other: Self) {
        self.tasks += other.tasks;
        self.partitions += other.partitions;
        self.local_partitions += other.local_partitions;
        self.local_bytes += other.local_bytes;
        self.total_bytes += other.total_bytes;
    }
}

/// Receives each binding round's measurement, so an embedder can publish it to its
/// own metrics. Any `Fn(&LocalityStats)` is an observer.
pub trait LocalityObserver: Send + Sync {
    /// One binding round, reported as a delta rather than a running total.
    fn observe(&self, round: &LocalityStats);
}

impl<F: Fn(&LocalityStats) + Send + Sync> LocalityObserver for F {
    fn observe(&self, round: &LocalityStats) {
        self(round)
    }
}
