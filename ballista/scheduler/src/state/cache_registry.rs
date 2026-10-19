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
//! records, per cache key, the schema and the shuffle
//! [`PartitionLocation`](ballista_core::serde::scheduler::PartitionLocation)s of
//! that output so that a later job can read the data back over the existing
//! shuffle path instead of recomputing the subplan.
//!
//! Unlike a job's [`ExecutionGraph`](crate::state::execution_graph::ExecutionGraph),
//! which is garbage-collected shortly after the job completes, entries here
//! outlive the job that produced them — that is the whole point of a cache.
//!
//! # Keying
//!
//! Entries are keyed by [`CacheKey`](crate::state::cache_registry::CacheKey) =
//! `(session_id, cache_id)`, where `cache_id` is the per-`cache()`-call UUID
//! stamped by `BallistaCacheFactory`. This gives reuse whenever the client
//! holds on to the cached `DataFrame` (its logical plan keeps the same
//! `cache_id`). Canonical plan-based keying for cross-call deduplication is
//! intentionally deferred.
//!
//! # Lifecycle
//!
//! ```text
//!   (absent)      --begin_materialization--------------------> Pending{job}
//!   Pending{job}  --complete_materialization(job) [match]----> Materialized
//!   Pending{job}  --abort_materialization(job)    [match]----> (absent)
//!   Pending{job}  --complete_materialization(other|absent)---> Stale (no change)
//!   Materialized  --invalidate / invalidate_executor / remove_session--> (absent)
//! ```
//!
//! Transitions out of `Pending` are **job-id matched**: a late completion from a
//! job that no longer owns the claim (because the entry was invalidated and
//! re-claimed, or the session was removed) is reported
//! [`CompleteOutcome::Stale`](crate::state::cache_registry::CompleteOutcome::Stale)
//! and changes nothing, so stale output cannot resurrect a removed entry or
//! overwrite a newer claim. Invalidation simply removes the entry; the next
//! lookup is therefore a miss and the subplan is re-materialized. This
//! implements the "if an executor holding a cached partition is lost, the cache
//! is invalidated" policy.

use std::collections::HashSet;

use ballista_core::JobId;
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
    pub materializing_job: JobId,
}

/// Internal state of a cache entry.
#[derive(Debug, Clone)]
enum EntryState {
    /// A materialization job has been submitted but has not completed; the
    /// partition locations are not yet known.
    Pending { materializing_job: JobId },
    /// The cache is fully materialized and readable.
    Materialized(MaterializedCache),
}

/// Cross-job registry of materialized cache entries.
///
/// Cheap to clone-share via `Arc`; all methods take `&self` and are safe to call
/// concurrently. Kept `pub(crate)`: the public surface is the
/// [`CacheState`](crate::cluster::CacheState) trait, so this type can evolve
/// freely while the feature grows.
#[derive(Debug, Default)]
pub(crate) struct CacheRegistry {
    entries: DashMap<CacheKey, EntryState>,
}

/// Outcome of claiming a key for materialization
/// ([`CacheState::begin_materialization`](crate::cluster::CacheState::begin_materialization)).
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BeginOutcome {
    /// This caller claimed the key and should submit the materialization job.
    Claimed,
    /// A materialization job is already in flight for this key; do not submit.
    AlreadyPending,
    /// The key is already materialized; the caller should read it back instead.
    AlreadyMaterialized,
}

/// Outcome of recording a materialization
/// ([`CacheState::complete_materialization`](crate::cluster::CacheState::complete_materialization)).
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompleteOutcome {
    /// The pending claim was promoted to materialized, or this job had already
    /// materialized the entry (idempotent re-delivery of the same completion).
    Completed,
    /// There was no pending claim this job owns — the entry was removed while
    /// materializing, or is owned by a different job. Nothing was recorded; the
    /// shuffle output this call produced is orphaned and the caller should clean
    /// it up. (A duplicate completion of an entry *this* job already
    /// materialized returns [`Completed`](Self::Completed), not `Stale`, so
    /// `Stale` always means the produced output is safe to delete.)
    Stale,
}

/// Returns true if `state` is a materialized entry holding a partition on
/// `executor_id`.
fn holds_executor(state: &EntryState, executor_id: &str) -> bool {
    matches!(
        state,
        EntryState::Materialized(cache)
            if cache
                .locations
                .iter()
                .flatten()
                .any(|loc| loc.executor_meta.id == executor_id)
    )
}

impl CacheRegistry {
    /// Creates an empty registry.
    #[cfg(test)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the materialized data for `key`, or `None` if the key is absent
    /// (a miss) or still pending.
    pub fn lookup(&self, key: &CacheKey) -> Option<MaterializedCache> {
        match self.entries.get(key)?.value() {
            EntryState::Materialized(cache) => Some(cache.clone()),
            EntryState::Pending { .. } => None,
        }
    }

    /// The job materializing `key`, if it is `Pending`, so another submission
    /// waits on it instead of starting a second one.
    pub fn pending_job(&self, key: &CacheKey) -> Option<JobId> {
        match self.entries.get(key)?.value() {
            EntryState::Pending { materializing_job } => Some(materializing_job.clone()),
            EntryState::Materialized(_) => None,
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
        job_id: impl Into<JobId>,
    ) -> BeginOutcome {
        use dashmap::mapref::entry::Entry;
        match self.entries.entry(key) {
            Entry::Occupied(occupied) => match occupied.get() {
                EntryState::Pending { .. } => BeginOutcome::AlreadyPending,
                EntryState::Materialized(_) => BeginOutcome::AlreadyMaterialized,
            },
            Entry::Vacant(vacant) => {
                vacant.insert(EntryState::Pending {
                    materializing_job: job_id.into(),
                });
                BeginOutcome::Claimed
            }
        }
    }

    /// Promotes the pending claim for `key` to [`EntryState::Materialized`],
    /// recording its shuffle output — **only if** `key` is still `Pending` and
    /// owned by `job_id`. A duplicate completion for an entry this same job
    /// already materialized is idempotent and returns
    /// [`CompleteOutcome::Completed`] without touching the live entry. Any other
    /// state (absent, or owned by a different job) is reported
    /// [`CompleteOutcome::Stale`] and the entry is left untouched. The check and
    /// swap are atomic under the entry lock, so a late completion cannot
    /// resurrect a removed entry or overwrite a newer claim.
    ///
    /// Called by the scheduler when a materialization job succeeds (from the
    /// `JobFinished` handler); on `Stale` the caller cleans up the
    /// orphaned shuffle output it produced.
    pub fn complete_materialization(
        &self,
        key: CacheKey,
        job_id: impl Into<JobId>,
        schema: SchemaRef,
        locations: Vec<Vec<PartitionLocation>>,
    ) -> CompleteOutcome {
        use dashmap::mapref::entry::Entry;
        let job_id = job_id.into();
        match self.entries.entry(key) {
            Entry::Occupied(mut occupied) => match occupied.get() {
                // The owning job's pending claim: promote it.
                EntryState::Pending { materializing_job }
                    if *materializing_job == job_id =>
                {
                    occupied.insert(EntryState::Materialized(MaterializedCache {
                        schema,
                        locations,
                        materializing_job: job_id,
                    }));
                    CompleteOutcome::Completed
                }
                // Idempotent re-delivery: this job already materialized the
                // entry. Report success and leave the live entry (and the data
                // it points at) untouched — returning `Stale` here would tell
                // the caller to delete output behind a live cache.
                EntryState::Materialized(cache) if cache.materializing_job == job_id => {
                    CompleteOutcome::Completed
                }
                // Removed-and-reclaimed by a different job, or a stale duplicate
                // from a job that no longer owns the key.
                _ => CompleteOutcome::Stale,
            },
            Entry::Vacant(_) => CompleteOutcome::Stale,
        }
    }

    /// Drops the pending claim for `key` — but **only if** it is still `Pending`
    /// and owned by `job_id`. Returns whether a claim was removed.
    ///
    /// Called when a materialization job fails or is cancelled (from the job
    /// failure and cancel handlers) so a doomed claim does not stay `Pending`
    /// forever (which would make every `lookup` miss and every
    /// `begin_materialization` return `AlreadyPending`). A mismatched `job_id`
    /// (a newer claim re-took the key) is left untouched.
    pub fn abort_materialization(&self, key: &CacheKey, job_id: &JobId) -> bool {
        self.entries
            .remove_if(key, |_, state| {
                matches!(
                    state,
                    EntryState::Pending { materializing_job } if materializing_job == job_id
                )
            })
            .is_some()
    }

    /// Removes the entry for `key`. The next [`lookup`](Self::lookup) is a miss.
    /// Returns the removed entry's materialized form, if it was materialized, so
    /// the caller can unpin its shuffle data.
    pub fn invalidate(&self, key: &CacheKey) -> Option<MaterializedCache> {
        match self.entries.remove(key) {
            Some((_, EntryState::Materialized(cache))) => Some(cache),
            _ => None,
        }
    }

    /// Removes `key`'s entry only while it is still `materializing_job`'s, so a
    /// caller acting on an entry seen earlier never removes what replaced it.
    /// Returns the entry this call removed.
    pub fn invalidate_if(
        &self,
        key: &CacheKey,
        materializing_job: &JobId,
    ) -> Option<MaterializedCache> {
        match self.entries.remove_if(key, |_, state| {
            matches!(state, EntryState::Materialized(cache)
                if &cache.materializing_job == materializing_job)
        }) {
            Some((_, EntryState::Materialized(cache))) => Some(cache),
            _ => None,
        }
    }

    /// Invalidates every materialized entry that holds a partition on
    /// `executor_id`. Returns the invalidated entries so the caller can unpin
    /// their shuffle data. Pending entries are left untouched (their job will
    /// fail/retry through the normal task-failure path).
    ///
    /// Each removal is a `remove_if` guarded by the same predicate, so the
    /// check-and-remove is atomic: a key that is concurrently invalidated and
    /// re-claimed between the scan and the removal is left alone rather than
    /// having its fresh `Pending` claim dropped.
    pub fn invalidate_executor(
        &self,
        executor_id: &str,
    ) -> Vec<(CacheKey, MaterializedCache)> {
        let affected: Vec<CacheKey> = self
            .entries
            .iter()
            .filter(|entry| holds_executor(entry.value(), executor_id))
            .map(|entry| entry.key().clone())
            .collect();

        affected
            .into_iter()
            .filter_map(|key| {
                match self
                    .entries
                    .remove_if(&key, |_, state| holds_executor(state, executor_id))
                {
                    Some((key, EntryState::Materialized(cache))) => Some((key, cache)),
                    _ => None,
                }
            })
            .collect()
    }

    /// Removes every entry owned by `session_id` (session teardown). Returns the
    /// removed materialized entries so the caller can unpin their shuffle data;
    /// pending entries are also removed but not returned (they have no shuffle
    /// output yet).
    pub fn remove_session(&self, session_id: &str) -> Vec<(CacheKey, MaterializedCache)> {
        let keys: Vec<CacheKey> = self
            .entries
            .iter()
            .filter(|entry| entry.key().session_id == session_id)
            .map(|entry| entry.key().clone())
            .collect();
        keys.into_iter()
            .filter_map(|key| match self.entries.remove(&key) {
                Some((key, EntryState::Materialized(cache))) => Some((key, cache)),
                _ => None,
            })
            .collect()
    }

    /// Returns the `(job_id, stage_id)` shuffle stages that must be pinned (kept
    /// past normal job cleanup) because they back a materialized cache entry.
    ///
    /// Stage-keyed because Ballista stores and deletes shuffle data per stage
    /// (`work_dir/{job_id}/{stage_id}/`) — a job-level pin cannot protect a
    /// cached stage that is an intermediate stage of its job. Consumed by
    /// job-data cleanup, which skips these stages.
    /// Pending entries contribute nothing (no locations yet; their job is still
    /// running and not yet subject to success cleanup).
    pub fn pinned_stages(&self) -> HashSet<(JobId, usize)> {
        self.entries
            .iter()
            .filter_map(|entry| match entry.value() {
                EntryState::Materialized(cache) => Some(
                    cache
                        .locations
                        .iter()
                        .flatten()
                        .map(|loc| {
                            (loc.partition_id.job_id.clone(), loc.partition_id.stage_id)
                        })
                        .collect::<Vec<_>>(),
                ),
                EntryState::Pending { .. } => None,
            })
            .flatten()
            .collect()
    }

    /// Number of entries currently tracked (pending + materialized). For tests.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::JobId;
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionLocation,
        PartitionStats,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

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

    fn location(
        job: &str,
        stage: usize,
        partition: usize,
        exec: &str,
    ) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: partition,
            partition_id: PartitionId::new(&JobId::new(job), stage, partition),
            executor_meta: executor(exec),
            partition_stats: PartitionStats::default(),
            file_id: Some(0),
            is_sort_shuffle: false,
        }
    }

    /// Claim then complete a key with a matching job, asserting the promotion
    /// succeeds. The common setup path now that `complete_materialization` only
    /// promotes an existing `Pending` claim.
    fn materialize(
        registry: &CacheRegistry,
        key: CacheKey,
        job: &str,
        locs: Vec<Vec<PartitionLocation>>,
    ) {
        registry.begin_materialization(key.clone(), job);
        assert_eq!(
            registry.complete_materialization(key, job, schema(), locs),
            CompleteOutcome::Completed
        );
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
        assert_eq!(
            registry.complete_materialization(key.clone(), "job-1", schema(), locs),
            CompleteOutcome::Completed
        );

        let hit = registry.lookup(&key).expect("should be a hit");
        assert_eq!(hit.locations.len(), 1);
        assert_eq!(hit.materializing_job.as_str(), "job-1");
        assert_eq!(hit.schema.fields().len(), 1);
    }

    /// `invalidate_if` removes only the materialization the caller saw, never a
    /// claim or materialization that replaced it.
    #[test]
    fn invalidate_if_only_removes_the_materialization_it_names() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "c1");
        materialize(
            &registry,
            key.clone(),
            "old",
            vec![vec![location("old", 1, 0, "e1")]],
        );

        // A different job's materialization is left alone.
        assert!(registry.invalidate_if(&key, &JobId::new("other")).is_none());
        assert!(registry.lookup(&key).is_some());

        // The named one is removed and returned.
        assert!(registry.invalidate_if(&key, &JobId::new("old")).is_some());

        // A newer claim on the key survives a late call naming the old job.
        registry.begin_materialization(key.clone(), "new");
        assert!(registry.invalidate_if(&key, &JobId::new("old")).is_none());
        assert_eq!(registry.pending_job(&key), Some(JobId::new("new")));
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

        assert_eq!(
            registry.complete_materialization(key.clone(), "job-1", schema(), vec![]),
            CompleteOutcome::Completed
        );
        assert_eq!(
            registry.begin_materialization(key.clone(), "job-3"),
            BeginOutcome::AlreadyMaterialized
        );
    }

    /// `complete_materialization` only promotes a matching pending claim; every
    /// other state is reported `Stale` and left unchanged.
    #[test]
    fn complete_requires_matching_pending_claim() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "c1");

        // No pending claim at all -> stale, nothing recorded.
        assert_eq!(
            registry.complete_materialization(key.clone(), "job-1", schema(), vec![]),
            CompleteOutcome::Stale
        );
        assert!(registry.lookup(&key).is_none());

        // A different job cannot complete another job's claim, and does not
        // overwrite it.
        registry.begin_materialization(key.clone(), "job-1");
        assert_eq!(
            registry.complete_materialization(
                key.clone(),
                "job-2",
                schema(),
                vec![vec![location("job-2", 1, 0, "e1")]],
            ),
            CompleteOutcome::Stale
        );
        assert!(
            registry.lookup(&key).is_none(),
            "claim still pending, not overwritten"
        );

        // The owning job completes it.
        assert_eq!(
            registry.complete_materialization(
                key.clone(),
                "job-1",
                schema(),
                vec![vec![location("job-1", 1, 0, "e1")]],
            ),
            CompleteOutcome::Completed
        );
        assert_eq!(
            registry.lookup(&key).unwrap().materializing_job.as_str(),
            "job-1"
        );
    }

    /// A duplicate completion from the job that already materialized the entry
    /// is idempotent (`Completed`), not `Stale` — so the caller never deletes
    /// the data behind a live entry.
    #[test]
    fn duplicate_completion_by_same_job_is_idempotent() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "c1");
        let locs = vec![vec![location("job-1", 1, 0, "e1")]];
        registry.begin_materialization(key.clone(), "job-1");
        assert_eq!(
            registry.complete_materialization(
                key.clone(),
                "job-1",
                schema(),
                locs.clone()
            ),
            CompleteOutcome::Completed
        );
        // Redelivered completion: still Completed, entry still points at the data.
        assert_eq!(
            registry.complete_materialization(key.clone(), "job-1", schema(), locs),
            CompleteOutcome::Completed
        );
        assert!(registry.lookup(&key).is_some());
    }

    /// A completion that arrives after the entry was removed (e.g. the session
    /// was torn down while materializing) must not resurrect it.
    #[test]
    fn remove_session_while_pending_is_not_resurrected() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "c1");

        registry.begin_materialization(key.clone(), "job-1");
        assert!(
            registry.remove_session("s1").is_empty(),
            "pending entry has nothing to unpin"
        );
        assert_eq!(registry.len(), 0);

        // Late completion from the now-orphaned job: stale, no entry recreated.
        assert_eq!(
            registry.complete_materialization(
                key.clone(),
                "job-1",
                schema(),
                vec![vec![location("job-1", 1, 0, "e1")]],
            ),
            CompleteOutcome::Stale
        );
        assert!(registry.lookup(&key).is_none());
        assert_eq!(registry.len(), 0);
    }

    /// Aborting clears only a matching pending claim; a mismatched job is a no-op.
    #[test]
    fn abort_materialization_clears_matching_pending() {
        let registry = CacheRegistry::new();
        let key = CacheKey::new("s1", "c1");
        registry.begin_materialization(key.clone(), "job-1");

        // Wrong job id: nothing removed, claim intact.
        assert!(!registry.abort_materialization(&key, &JobId::new("job-2")));
        assert_eq!(registry.len(), 1);

        // Owning job: claim dropped, key re-claimable.
        assert!(registry.abort_materialization(&key, &JobId::new("job-1")));
        assert_eq!(registry.len(), 0);
        assert_eq!(
            registry.begin_materialization(key.clone(), "job-3"),
            BeginOutcome::Claimed
        );
    }

    /// The same `cache_id` in two different sessions is two different entries.
    #[test]
    fn keyed_by_session_and_cache_id() {
        let registry = CacheRegistry::new();
        let k1 = CacheKey::new("s1", "c1");
        let k2 = CacheKey::new("s2", "c1");
        materialize(&registry, k1.clone(), "job-1", vec![]);
        assert!(registry.lookup(&k1).is_some());
        assert!(registry.lookup(&k2).is_none());
    }

    /// Losing an executor invalidates exactly the entries holding a partition on
    /// it — including one spread across executors (matched via any partition) —
    /// and reports them for unpinning; unaffected entries are retained.
    #[test]
    fn invalidate_executor_affects_only_partition_holders() {
        let registry = CacheRegistry::new();
        let only_e1 = CacheKey::new("s1", "only-e1");
        let spread = CacheKey::new("s1", "spread-e2-e3");
        materialize(
            &registry,
            only_e1.clone(),
            "job-a",
            vec![vec![location("job-a", 1, 0, "e1")]],
        );
        materialize(
            &registry,
            spread.clone(),
            "job-b",
            vec![
                vec![location("job-b", 2, 0, "e2")],
                vec![location("job-b", 2, 1, "e3")],
            ],
        );

        // Losing e2 hits only the spread entry (matched via its e2 partition).
        let invalidated = registry.invalidate_executor("e2");
        assert_eq!(invalidated.len(), 1);
        assert_eq!(invalidated[0].0, spread);
        assert_eq!(invalidated[0].1.materializing_job.as_str(), "job-b");
        assert!(registry.lookup(&spread).is_none(), "holder evicted");
        assert!(
            registry.lookup(&only_e1).is_some(),
            "unaffected entry retained"
        );
    }

    #[test]
    fn remove_session_drops_all_its_entries() {
        let registry = CacheRegistry::new();
        materialize(&registry, CacheKey::new("s1", "a"), "ja", vec![]);
        materialize(&registry, CacheKey::new("s1", "b"), "jb", vec![]);
        materialize(&registry, CacheKey::new("s2", "a"), "jc", vec![]);

        let removed = registry.remove_session("s1");
        assert_eq!(removed.len(), 2);
        assert_eq!(registry.len(), 1);
        assert!(registry.lookup(&CacheKey::new("s2", "a")).is_some());
    }

    /// Pinned stages cover materialized entries at `(job_id, stage_id)`
    /// granularity and drop out when the entry is invalidated.
    #[test]
    fn pinned_stages_covers_materialized_entries() {
        let registry = CacheRegistry::new();
        // A pending entry contributes no stage (no locations yet).
        registry.begin_materialization(CacheKey::new("s1", "pending"), "job-1");
        materialize(
            &registry,
            CacheKey::new("s1", "done"),
            "job-2",
            vec![vec![location("job-2", 3, 0, "e1")]],
        );

        let pinned = registry.pinned_stages();
        assert_eq!(pinned.len(), 1);
        assert!(pinned.contains(&(JobId::new("job-2"), 3)));

        // Invalidating the materialized entry returns it (for unpinning) and
        // drops its stage from the pin set.
        assert!(registry.invalidate(&CacheKey::new("s1", "done")).is_some());
        assert!(registry.pinned_stages().is_empty());
    }

    // --- Concurrency invariants (must hold under any interleaving) ---

    /// Many threads racing to claim the same key: exactly one wins `Claimed`.
    #[test]
    fn concurrent_begin_yields_single_claim() {
        let registry = Arc::new(CacheRegistry::new());
        let key = CacheKey::new("s1", "c1");
        let n = 16;
        let barrier = Arc::new(Barrier::new(n));
        let claims: usize = (0..n)
            .map(|i| {
                let (r, k, b) = (registry.clone(), key.clone(), barrier.clone());
                thread::spawn(move || {
                    b.wait();
                    matches!(
                        r.begin_materialization(k, format!("job-{i}")),
                        BeginOutcome::Claimed
                    ) as usize
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .sum();
        assert_eq!(claims, 1, "exactly one claimant");
    }

    /// Losing two executors that each hold part of the same entry removes and
    /// reports it exactly once — no double unpin.
    #[test]
    fn concurrent_invalidate_executor_removes_entry_once() {
        for _ in 0..100 {
            let registry = Arc::new(CacheRegistry::new());
            let key = CacheKey::new("s1", "spread");
            materialize(
                &registry,
                key.clone(),
                "job-a",
                vec![
                    vec![location("job-a", 1, 0, "e1")],
                    vec![location("job-a", 1, 1, "e2")],
                ],
            );
            let barrier = Arc::new(Barrier::new(2));
            let total: usize = ["e1", "e2"]
                .iter()
                .map(|exec| {
                    let (r, b, e) = (registry.clone(), barrier.clone(), exec.to_string());
                    thread::spawn(move || {
                        b.wait();
                        r.invalidate_executor(&e).len()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .sum();
            assert_eq!(total, 1, "entry removed/returned exactly once");
            assert!(registry.lookup(&key).is_none());
        }
    }

    /// A concurrent complete and abort for the same pending claim are mutually
    /// exclusive: exactly one wins, and the end state matches the winner.
    #[test]
    fn concurrent_complete_and_abort_are_exclusive() {
        for _ in 0..100 {
            let registry = Arc::new(CacheRegistry::new());
            let key = CacheKey::new("s1", "c1");
            registry.begin_materialization(key.clone(), "job-1");
            let barrier = Arc::new(Barrier::new(2));

            let completer = {
                let (r, k, b) = (registry.clone(), key.clone(), barrier.clone());
                thread::spawn(move || {
                    b.wait();
                    r.complete_materialization(k, "job-1", schema(), vec![])
                        == CompleteOutcome::Completed
                })
            };
            let aborter = {
                let (r, k, b) = (registry.clone(), key.clone(), barrier.clone());
                thread::spawn(move || {
                    b.wait();
                    r.abort_materialization(&k, &JobId::new("job-1"))
                })
            };

            let completed = completer.join().unwrap();
            let aborted = aborter.join().unwrap();
            assert!(completed ^ aborted, "exactly one of complete/abort wins");
            assert_eq!(registry.lookup(&key).is_some(), completed);
        }
    }
}
