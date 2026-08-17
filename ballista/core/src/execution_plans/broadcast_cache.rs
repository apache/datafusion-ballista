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

//! Executor-wide cache of broadcast-side reads.
//!
//! A broadcast read fetches every upstream partition in every consuming task, so
//! an executor running N tasks of a stage fetches and decodes the same build side
//! N times. Shuffle output is immutable once written, so those tasks can share
//! one read.
//!
//! The key includes a fingerprint of the partition locations: a re-run or
//! replanned stage writes different files, so it never sees a previous attempt's
//! batches.

use crate::JobId;
use crate::serde::scheduler::PartitionLocation;
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, LazyLock};
use tokio::sync::OnceCell;

/// Job, upstream stage, and a fingerprint of that stage's output locations.
type Key = (JobId, usize, u64);

/// The batches of one broadcast side. `OnceCell` makes concurrent tasks share a
/// single fetch instead of racing to fill the entry.
pub(crate) type Slot = Arc<OnceCell<Arc<Vec<RecordBatch>>>>;

static CACHE: LazyLock<Mutex<HashMap<Key, Slot>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Identifies the data behind `locations`, so a stage re-run under the same
/// stage id keys to a different slot.
pub(crate) fn fingerprint(locations: &[PartitionLocation]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for location in locations {
        location.executor_meta.id.hash(&mut hasher);
        location.map_partition_id.hash(&mut hasher);
        location.partition_id.partition_id.hash(&mut hasher);
        location.file_id.hash(&mut hasher);
    }
    hasher.finish()
}

/// The slot for one broadcast side, created empty on first use.
pub(crate) fn slot(job_id: &JobId, stage_id: usize, fingerprint: u64) -> Slot {
    CACHE
        .lock()
        .entry((job_id.clone(), stage_id, fingerprint))
        .or_default()
        .clone()
}

/// Drops cached batches for a job. An empty `stage_ids` drops the whole job,
/// matching the executor's shuffle-file cleanup.
pub fn evict(job_id: &JobId, stage_ids: &[u32]) {
    let mut cache = CACHE.lock();
    cache.retain(|(cached_job, cached_stage, _), _| {
        cached_job != job_id
            || (!stage_ids.is_empty() && !stage_ids.contains(&(*cached_stage as u32)))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::scheduler::{
        ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
        PartitionId,
    };

    fn location(
        job: &str,
        stage: usize,
        partition: usize,
        file_id: Option<u64>,
    ) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: partition,
            partition_id: PartitionId {
                job_id: job.into(),
                stage_id: stage,
                partition_id: partition,
            },
            executor_meta: ExecutorMetadata {
                id: "executor-1".to_string(),
                host: "localhost".to_string(),
                port: 50051,
                grpc_port: 50052,
                specification: ExecutorSpecification::default(),
                os_info: ExecutorOperatingSystemSpecification::default(),
            },
            partition_stats: Default::default(),
            file_id,
            is_sort_shuffle: false,
        }
    }

    #[test]
    fn same_locations_share_a_slot() {
        let locations = vec![location("job-share", 1, 0, Some(7))];
        let f = fingerprint(&locations);
        let first = slot(&"job-share".into(), 1, f);
        let second = slot(&"job-share".into(), 1, f);
        assert!(Arc::ptr_eq(&first, &second));
        evict(&"job-share".into(), &[]);
    }

    #[test]
    fn rerun_stage_gets_a_fresh_slot() {
        // Same job and stage, different shuffle files: the re-run must not read
        // the previous attempt's batches.
        let first_attempt = vec![location("job-rerun", 1, 0, Some(1))];
        let second_attempt = vec![location("job-rerun", 1, 0, Some(2))];
        assert_ne!(fingerprint(&first_attempt), fingerprint(&second_attempt));
        evict(&"job-rerun".into(), &[]);
    }

    #[test]
    fn evict_drops_only_the_named_stages() {
        let job: JobId = "job-evict".into();
        let other: JobId = "job-keep".into();
        let kept = slot(&other, 1, 0);
        let dropped = slot(&job, 1, 0);
        let retained = slot(&job, 2, 0);

        evict(&job, &[1]);

        let cache = CACHE.lock();
        assert!(!cache.contains_key(&(job.clone(), 1, 0)), "stage 1 evicted");
        assert!(cache.contains_key(&(job.clone(), 2, 0)), "stage 2 retained");
        assert!(cache.contains_key(&(other.clone(), 1, 0)), "other job kept");
        drop((kept, dropped, retained, cache));

        evict(&job, &[]);
        evict(&other, &[]);
        let cache = CACHE.lock();
        assert!(!cache.keys().any(|(j, _, _)| j == &job || j == &other));
    }
}
