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

//! Release of a task's vcores as its input partitions finish.
//!
//! A task that runs N partitions is charged N vcores and, without this, holds
//! all of them until its slowest partition ends. With skewed partitions most of
//! those vcores sit idle for most of the task. The executor attaches a
//! [`PartitionVcoreReleaser`] to the task's session config, and the shuffle
//! writers report each input partition they finish draining to it.

use datafusion::execution::TaskContext;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Hands back the vcores a running task no longer needs as its input
/// partitions finish.
///
/// A task drives one thread per running partition, so it needs
/// `min(running partitions, vcores)` vcores. It keeps at least one until it
/// ends: the task still has work to do after its last partition, and its
/// terminal status returns whatever is left.
pub struct PartitionVcoreReleaser {
    /// Vcores the task was charged.
    vcores: usize,
    completed: AtomicUsize,
    released: AtomicUsize,
    /// Called with the number of vcores newly released.
    on_release: Box<dyn Fn(usize) + Send + Sync>,
}

impl PartitionVcoreReleaser {
    /// Creates a releaser for a task charged `vcores` vcores.
    pub fn new(
        vcores: usize,
        on_release: impl Fn(usize) + Send + Sync + 'static,
    ) -> Self {
        Self {
            vcores,
            completed: AtomicUsize::new(0),
            released: AtomicUsize::new(0),
            on_release: Box::new(on_release),
        }
    }

    /// Records that one of the task's `partitions` input partitions finished.
    pub fn partition_completed(&self, partitions: usize) {
        let completed = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        let needed = partitions
            .saturating_sub(completed)
            .clamp(1, self.vcores.max(1));
        let cumulative = self.vcores.saturating_sub(needed);
        let previous = self.released.fetch_max(cumulative, Ordering::AcqRel);
        if cumulative > previous {
            (self.on_release)(cumulative - previous);
        }
    }
}

impl std::fmt::Debug for PartitionVcoreReleaser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionVcoreReleaser")
            .field("vcores", &self.vcores)
            .field("completed", &self.completed)
            .field("released", &self.released)
            .finish()
    }
}

/// Reports that one of the task's `partitions` input partitions finished, to
/// the [`PartitionVcoreReleaser`] of `context` if it has one.
pub(crate) fn partition_completed(context: &TaskContext, partitions: usize) {
    if let Some(releaser) = context
        .session_config()
        .get_extension::<PartitionVcoreReleaser>()
    {
        releaser.partition_completed(partitions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionConfig;
    use std::sync::{Arc, Mutex};

    /// The releases of a task of `partitions` partitions charged `vcores`.
    fn releases(partitions: usize, vcores: usize) -> Vec<usize> {
        let released = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&released);
        let releaser = PartitionVcoreReleaser::new(vcores, move |delta| {
            sink.lock().unwrap().push(delta)
        });
        for _ in 0..partitions {
            releaser.partition_completed(partitions);
        }
        let deltas = released.lock().unwrap().clone();
        assert!(deltas.iter().sum::<usize>() < vcores.max(1));
        deltas
    }

    #[test]
    fn releases_one_vcore_per_partition() {
        assert_eq!(releases(4, 4), vec![1, 1, 1]);
    }

    #[test]
    fn keeps_a_vcore_until_the_task_ends() {
        assert_eq!(releases(1, 1), Vec::<usize>::new());
        assert_eq!(releases(4, 1), Vec::<usize>::new());
        // Even once its only partition is done.
        assert_eq!(releases(1, 8), vec![7]);
    }

    #[test]
    fn keeps_a_vcore_for_each_running_partition() {
        // More partitions than vcores: nothing is free until fewer partitions
        // than vcores are left.
        assert_eq!(releases(4, 2), vec![1]);
        // More vcores than partitions: the surplus goes with the first one.
        assert_eq!(releases(2, 5), vec![4]);
    }

    #[test]
    fn task_context_reports_to_its_releaser() {
        let released = Arc::new(AtomicUsize::new(0));
        let sink = Arc::clone(&released);
        let releaser = PartitionVcoreReleaser::new(2, move |vcores| {
            sink.fetch_add(vcores, Ordering::Relaxed);
        });
        let config = SessionConfig::new().with_extension(Arc::new(releaser));
        let context = TaskContext::default().with_session_config(config);

        partition_completed(&context, 2);
        assert_eq!(released.load(Ordering::Relaxed), 1);
        // A task without a releaser ignores the report.
        partition_completed(&TaskContext::default(), 2);
    }
}
