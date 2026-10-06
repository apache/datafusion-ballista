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

//! Cross-job registry of materialized `DataFrame::cache()` results.
//!
//! A cached dataset is represented in the logical plan by a
//! [`BallistaCacheNode`](ballista_core::extension::BallistaCacheNode). When it
//! is first executed it is materialized as *pinned* shuffle output on the
//! executors (shuffle files that survive normal job cleanup). This registry
//! records, per cache key, the schema and the shuffle [`PartitionLocation`]s of
//! that output so that a later job can read the data back over the existing
//! shuffle path instead of recomputing the subplan.
//!
//! Unlike a job's [`ExecutionGraph`](crate::state::execution_graph::ExecutionGraph),
//! which is garbage-collected shortly after the job completes, entries here
//! outlive the job that produced them — that is the whole point of a cache.
//!
//! # Keying
//!
//! Entries are keyed by [`CacheKey`] = `(session_id, cache_id)`, where
//! `cache_id` is the per-`cache()`-call UUID stamped by
//! `BallistaCacheFactory`. This gives reuse whenever the client holds on to the
//! cached `DataFrame` (its logical plan keeps the same `cache_id`). Canonical
//! plan-based keying for cross-call deduplication is intentionally deferred.
//!
//! # Lifecycle
//!
//! ```text
//!   (absent) --begin_materialization--> Pending
//!   Pending  --complete_materialization--> Materialized
//!   Materialized --invalidate / invalidate_executor--> (absent)
//! ```
//!
//! Invalidation simply removes the entry; the next lookup is therefore a miss
//! and the subplan is re-materialized. This implements the "if an executor
//! holding a cached partition is lost, the cache is invalidated" policy.

use std::collections::HashSet;

use ballista_core::serde::scheduler::PartitionLocation;
use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;

/// Identifies a cached dataset.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// The owning session.
    pub session_id: String,
    /// The per-`cache()`-call identifier carried by `BallistaCacheNode`.
    pub cache_id: String,
}

impl CacheKey {
    /// Creates a new cache key.
    pub fn new(session_id: impl Into<String>, cache_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            cache_id: cache_id.into(),
        }
    }
}

/// The materialized output of a cache: its schema and the shuffle locations of
/// each output partition.
#[derive(Debug, Clone)]
pub struct MaterializedCache {
    /// Schema of the cached data.
    pub schema: SchemaRef,
    /// Shuffle locations indexed by output partition. The inner `Vec` allows a
    /// single output partition to be backed by more than one file (e.g. one per
    /// map task), mirroring [`ExecutionStage`] shuffle output.
    ///
    /// [`ExecutionStage`]: crate::state::execution_stage
    pub locations: Vec<Vec<PartitionLocation>>,
    /// The job that materialized this cache. Used to pin/unpin its shuffle data.
    pub materializing_job: String,
}

/// Internal state of a cache entry.
#[derive(Debug, Clone)]
enum CacheState {
    /// A materialization job has been submitted but has not completed; the
    /// partition locations are not yet known.
    Pending { materializing_job: String },
    /// The cache is fully materialized and readable.
    Materialized(MaterializedCache),
}

/// Cross-job registry of materialized cache entries.
///
/// Cheap to clone-share via `Arc`; all methods take `&self` and are safe to call
/// concurrently.
#[derive(Debug, Default)]
pub struct CacheRegistry {
    entries: DashMap<CacheKey, CacheState>,
}

/// Outcome of [`CacheRegistry::begin_materialization`].
#[derive(Debug, PartialEq, Eq)]
pub enum BeginOutcome {
    /// This caller claimed the key and should submit the materialization job.
    Claimed,
    /// A materialization job is already in flight for this key; do not submit.
    AlreadyPending,
    /// The key is already materialized; the caller should read it back instead.
    AlreadyMaterialized,
}

impl CacheRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the materialized data for `key`, or `None` if the key is absent
    /// (a miss) or still pending.
    pub fn lookup(&self, key: &CacheKey) -> Option<MaterializedCache> {
        match self.entries.get(key)?.value() {
            CacheState::Materialized(cache) => Some(cache.clone()),
            CacheState::Pending { .. } => None,
        }
    }

    /// Attempts to claim `key` for materialization by `job_id`.
    ///
    /// Returns [`BeginOutcome::Claimed`] only when the caller is responsible for
    /// submitting the materialization job. If another job already claimed the
    /// key, or the key is already materialized, the caller must not submit a
    /// duplicate. This races safely: concurrent callers for the same key see
    /// exactly one `Claimed`.
    pub fn begin_materialization(
        &self,
        key: CacheKey,
        job_id: impl Into<String>,
    ) -> BeginOutcome {
        use dashmap::mapref::entry::Entry;
        match self.entries.entry(key) {
            Entry::Occupied(occupied) => match occupied.get() {
                CacheState::Pending { .. } => BeginOutcome::AlreadyPending,
                CacheState::Materialized(_) => BeginOutcome::AlreadyMaterialized,
            },
            Entry::Vacant(vacant) => {
                vacant.insert(CacheState::Pending {
                    materializing_job: job_id.into(),
                });
                BeginOutcome::Claimed
            }
        }
    }

    /// Records the materialized output for `key`, transitioning it to
    /// [`CacheState::Materialized`]. Overwrites any existing state so that a
    /// re-materialization (after invalidation) can refresh in place. `job_id` is
    /// the job that produced the shuffle data and is retained for pin/unpin.
    pub fn complete_materialization(
        &self,
        key: CacheKey,
        job_id: impl Into<String>,
        schema: SchemaRef,
        locations: Vec<Vec<PartitionLocation>>,
    ) {
        self.entries.insert(
            key,
            CacheState::Materialized(MaterializedCache {
                schema,
                locations,
                materializing_job: job_id.into(),
            }),
        );
    }

    /// Removes the entry for `key`. The next [`lookup`](Self::lookup) is a miss.
    /// Returns the removed entry's materialized form, if it was materialized.
    pub fn invalidate(&self, key: &CacheKey) -> Option<MaterializedCache> {
        match self.entries.remove(key) {
            Some((_, CacheState::Materialized(cache))) => Some(cache),
            _ => None,
        }
    }

    /// Invalidates every materialized entry that holds a partition on
    /// `executor_id`. Returns the invalidated entries so the caller can unpin
    /// their shuffle data. Pending entries are left untouched (their job will
    /// fail/retry through the normal task-failure path).
    pub fn invalidate_executor(
        &self,
        executor_id: &str,
    ) -> Vec<(CacheKey, MaterializedCache)> {
        let affected: Vec<CacheKey> = self
            .entries
            .iter()
            .filter(|entry| match entry.value() {
                CacheState::Materialized(cache) => cache
                    .locations
                    .iter()
                    .flatten()
                    .any(|loc| loc.executor_meta.id == executor_id),
                CacheState::Pending { .. } => false,
            })
            .map(|entry| entry.key().clone())
            .collect();

        affected
            .into_iter()
            .filter_map(|key| self.invalidate(&key).map(|cache| (key, cache)))
            .collect()
    }

    /// Removes every entry owned by `session_id` (session teardown). Returns the
    /// removed keys.
    pub fn remove_session(&self, session_id: &str) -> Vec<CacheKey> {
        let keys: Vec<CacheKey> = self
            .entries
            .iter()
            .filter(|entry| entry.key().session_id == session_id)
            .map(|entry| entry.key().clone())
            .collect();
        for key in &keys {
            self.entries.remove(key);
        }
        keys
    }

    /// Returns the set of job ids whose shuffle data must be pinned (kept past
    /// normal job cleanup) because it backs a cache entry. Includes both pending
    /// and materialized entries.
    pub fn pinned_job_ids(&self) -> HashSet<String> {
        self.entries
            .iter()
            .map(|entry| match entry.value() {
                CacheState::Pending { materializing_job } => materializing_job.clone(),
                CacheState::Materialized(cache) => cache.materializing_job.clone(),
            })
            .filter(|id| !id.is_empty())
            .collect()
    }

    /// Number of entries currently tracked (pending + materialized). Primarily
    /// for tests and metrics.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionLocation,
        PartitionStats,
    };
    use ballista_core::JobId;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn executor(id: &str) -> ExecutorMetadata {
        ExecutorMetadata {
            id: id.to_string(),
            host: "localhost".to_string(),
            port: 50051,
            grpc_port: 50052,
            specification: ExecutorSpecification::default(),
            os_info: Default::default(),
        }
    }

    fn location(job: &str, stage: usize, partition: usize, exec: &str) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: partition,
            partition_id: PartitionId::new(&JobId::new(job), stage, partition),
            executor_meta: executor(exec),
            partition_stats: PartitionStats::default(),
            file_id: Some(0),
            is_sort_shuffle: false,
        }
    }

    /// A fresh key is a miss until it is materialized.
    #[test]
    fn lookup_miss_then_hit() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "c1");
        assert!(registry.lookup(&key).is_none());

        assert_eq!(
            registry.begin_materialization(key.clone(), "job-1"),
            BeginOutcome::Claimed
        );
        // Still a miss while pending.
        assert!(registry.lookup(&key).is_none());

        let locs = vec![vec![location("job-1", 1, 0, "e1")]];
        registry.complete_materialization(key.clone(), "job-1", schema(), locs.clone());

        let hit = registry.lookup(&key).expect("should be a hit");
        assert_eq!(hit.locations.len(), 1);
        assert_eq!(hit.materializing_job, "job-1");
        assert_eq!(hit.schema.fields().len(), 1);
    }

    /// Only the first caller claims a key; concurrent callers are told to back off.
    #[test]
    fn begin_materialization_dedupes() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "c1");
        assert_eq!(
            registry.begin_materialization(key.clone(), "job-1"),
            BeginOutcome::Claimed
        );
        assert_eq!(
            registry.begin_materialization(key.clone(), "job-2"),
            BeginOutcome::AlreadyPending
        );

        registry.complete_materialization(key.clone(), "job-1", schema(), vec![]);
        assert_eq!(
            registry.begin_materialization(key.clone(), "job-3"),
            BeginOutcome::AlreadyMaterialized
        );
    }

    /// The same `cache_id` in two different sessions is two different entries.
    #[test]
    fn keyed_by_session_and_cache_id() {
        let registry = CacheRegistry::new();
        let k1 = CacheKey::new("s1", "c1");
        let k2 = CacheKey::new("s2", "c1");
        registry.complete_materialization(k1.clone(), "job-1", schema(), vec![]);
        assert!(registry.lookup(&k1).is_some());
        assert!(registry.lookup(&k2).is_none());
    }

    /// Losing an executor invalidates exactly the entries that hold a partition
    /// on it, and reports them for unpinning.
    #[test]
    fn invalidate_executor_only_affects_holders() {
        let registry = CacheRegistry::new();
        let on_e1 = CacheKey::new("s1", "on-e1");
        let on_e2 = CacheKey::new("s1", "on-e2");
        registry.complete_materialization(
            on_e1.clone(),
            "job-a",
            schema(),
            vec![vec![location("job-a", 1, 0, "e1")]],
        );
        registry.complete_materialization(
            on_e2.clone(),
            "job-b",
            schema(),
            vec![vec![location("job-b", 1, 0, "e2")]],
        );

        let invalidated = registry.invalidate_executor("e1");
        assert_eq!(invalidated.len(), 1);
        assert_eq!(invalidated[0].0, on_e1);
        assert_eq!(invalidated[0].1.materializing_job, "job-a");

        assert!(registry.lookup(&on_e1).is_none(), "e1 holder evicted");
        assert!(registry.lookup(&on_e2).is_some(), "e2 holder retained");
    }

    /// A cache spread across executors is invalidated if any one of them is lost.
    #[test]
    fn invalidate_executor_matches_any_partition() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "spread");
        registry.complete_materialization(
            key.clone(),
            "job-a",
            schema(),
            vec![
                vec![location("job-a", 1, 0, "e1")],
                vec![location("job-a", 1, 1, "e2")],
            ],
        );
        assert_eq!(registry.invalidate_executor("e2").len(), 1);
        assert!(registry.lookup(&key).is_none());
    }

    #[test]
    fn remove_session_drops_all_its_entries() {
        let registry = CacheRegistry::new();
        registry.complete_materialization(CacheKey::new("s1", "a"), "j", schema(), vec![]);
        registry.complete_materialization(CacheKey::new("s1", "b"), "j", schema(), vec![]);
        registry.complete_materialization(CacheKey::new("s2", "a"), "j", schema(), vec![]);

        let removed = registry.remove_session("s1");
        assert_eq!(removed.len(), 2);
        assert_eq!(registry.len(), 1);
        assert!(registry.lookup(&CacheKey::new("s2", "a")).is_some());
    }

    /// Pinned job ids cover both pending and materialized entries, deduped.
    #[test]
    fn pinned_job_ids_tracks_live_entries() {
        let registry = CacheRegistry::new();
        registry.begin_materialization(CacheKey::new("s1", "pending"), "job-1");
        registry.complete_materialization(
            CacheKey::new("s1", "done"),
            "job-2",
            schema(),
            vec![vec![location("job-2", 1, 0, "e1")]],
        );

        let pinned = registry.pinned_job_ids();
        assert_eq!(pinned.len(), 2);
        assert!(pinned.contains("job-1"));
        assert!(pinned.contains("job-2"));

        // Invalidating the materialized entry unpins its job.
        registry.invalidate(&CacheKey::new("s1", "done"));
        let pinned = registry.pinned_job_ids();
        assert_eq!(pinned.len(), 1);
        assert!(pinned.contains("job-1"));
    }

    /// Re-materializing after invalidation refreshes locations in place.
    #[test]
    fn re_materialization_refreshes_locations() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "c1");
        registry.complete_materialization(
            key.clone(),
            "job-1",
            schema(),
            vec![vec![location("job-1", 1, 0, "e1")]],
        );
        registry.invalidate(&key);
        assert!(registry.lookup(&key).is_none());

        registry.begin_materialization(key.clone(), "job-2");
        registry.complete_materialization(
            key.clone(),
            "job-2",
            schema(),
            vec![vec![location("job-2", 1, 0, "e2")]],
        );
        let hit = registry.lookup(&key).unwrap();
        assert_eq!(hit.materializing_job, "job-2");
        assert_eq!(hit.locations[0][0].executor_meta.id, "e2");
    }
}
