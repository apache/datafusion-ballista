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

//! Shuffle-affinity task distribution.
//!
//! `Bias` and `RoundRobin` bind a consumer task to whichever executor has free
//! vcores, so a reduce task often reads its input over Arrow Flight even when
//! one executor holds it on local disk. This policy places each task where its
//! bytes already are, reading the `PartitionLocation`s that every resolved
//! shuffle reader carries.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use ballista_core::JobId;
use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{RangeShuffleReaderExec, ShuffleReaderExec};
use ballista_core::serde::protobuf::{AvailableVcores, job_status};
use ballista_core::serde::scheduler::{PartitionLocation, TaskKey};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use log::debug;

use ballista_scheduler::cluster::{BoundTask, DistributionPolicy};
use ballista_scheduler::state::execution_graph::{TaskDescription, create_task_info};
use ballista_scheduler::state::execution_stage::{
    ExecutionStage, PendingPartitions, RunningStage,
};
use ballista_scheduler::state::task_manager::JobInfoCache;

/// Locks `mutex`, taking the value back if a previous holder panicked. What
/// these guard, a memo and a counter, cannot be left inconsistent by one.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Byte counts per executor for one stage input partition. Ids are borrowed
/// from the plan's `PartitionLocation`s to avoid a copy per (partition,
/// producer) pair.
type PartitionBytes<'a> = HashMap<&'a str, u64>;

/// Locality the policy achieved, accumulated over the scheduler's life. Read
/// it with [`ShuffleAffinityPolicy::stats`], or from the per-round `debug!`
/// line logged under the `ballista_examples::shuffle_affinity` target.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalityStats {
    /// Tasks bound.
    pub tasks: u64,
    /// Input partitions covered by those tasks.
    pub partitions: u64,
    /// Of those, the ones bound to an executor holding some of their input.
    pub local_partitions: u64,
    /// Input bytes those tasks will read from the executor running them.
    pub local_bytes: u64,
    /// Input bytes those tasks will read in total. A stage with no shuffle
    /// input contributes to neither byte count, so the ratio is taken over
    /// shuffle reads alone.
    pub total_bytes: u64,
    /// Whether any byte counted above is a placeholder standing in for a
    /// producer that reported no size. Sticky: once set it stays set.
    pub imputed_bytes: bool,
}

impl LocalityStats {
    /// Share of shuffle input read without a network hop, in `0.0..=1.0`; zero
    /// before anything with a known size is bound. Meaningful only while
    /// [`Self::imputed_bytes`] is false, since placeholder sizes turn it into a
    /// ratio of locations rather than of bytes.
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
        self.imputed_bytes |= other.imputed_bytes;
    }
}

/// Sink for each binding round's locality measurement.
///
/// The policy keeps its own metrics rather than adding a hook to the
/// scheduler's [`ballista_scheduler::metrics::SchedulerMetricsCollector`], so an embedder
/// publishes them wherever it already publishes metrics. Any
/// `Fn(&LocalityStats)` is an observer.
pub trait LocalityObserver: Send + Sync {
    /// One binding round, reported as a delta rather than a running total.
    fn observe(&self, round: &LocalityStats);
}

impl<F: Fn(&LocalityStats) + Send + Sync> LocalityObserver for F {
    fn observe(&self, round: &LocalityStats) {
        self(round)
    }
}

/// Task distribution policy that places a consumer task on the executor that
/// already holds most of its shuffle input.
///
/// Per stage, binding runs two passes:
///
/// 1. **Affinity.** Every `(pending partition, holder)` pair is ranked by
///    bytes and taken greedily while the holders have vcores free, so a scarce
///    slot goes to the partition with most to gain and a partition whose best
///    holder is full can settle for its second best. A collapse stage, whose
///    one task reads *every* partition, is placed whole on the executor
///    holding most of the stage.
/// 2. **Fallback.** Whatever is still pending is bound `Bias`-style onto the
///    vcores left over, so a partition never idles waiting for a full holder.
///
/// Locality is advisory: a task bound away from its bytes still reads them,
/// just remotely. [`ShuffleAffinityPolicy::stats`] reports how often.
///
/// Only an *uneven* split of a partition's bytes is exploitable, whether from
/// heterogeneous executors, skewed inputs, or a hot key. Where every producer
/// writes every partition at equal size, any placement reads `1/E` locally.
#[derive(Clone, Default)]
pub struct ShuffleAffinityPolicy {
    /// Memoized scans, by job then stage. Clones of a policy share one cache,
    /// so handing the same policy to several schedulers is safe.
    cache: Arc<Mutex<HashMap<JobId, HashMap<usize, CachedLocality>>>>,
    /// Cumulative locality achieved, shared by clones like the cache.
    stats: Arc<Mutex<LocalityStats>>,
    /// Sink for each round's measurement, set once by the embedder (see
    /// [`Self::attach_observer`]) and shared by clones.
    observer: Arc<OnceLock<Arc<dyn LocalityObserver>>>,
}

impl std::fmt::Debug for ShuffleAffinityPolicy {
    /// Hand-written because a metrics collector is not [`Debug`].
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShuffleAffinityPolicy")
            .field("cached_stages", &self.cache_len())
            .field("stats", &self.stats())
            .field("observer_attached", &self.observer.get().is_some())
            .finish()
    }
}

/// A stage's scan, tagged with the attempt it was taken from.
#[derive(Debug)]
struct CachedLocality {
    stage_attempt_num: usize,
    locality: Arc<StageLocality>,
}

impl ShuffleAffinityPolicy {
    /// Creates a policy with an empty cache and zeroed stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// How much locality the policy has actually achieved so far. See
    /// [`LocalityStats`].
    pub fn stats(&self) -> LocalityStats {
        *lock(&self.stats)
    }

    /// Publish every later round's measurement to `observer`, which is how an
    /// embedder turns the achieved locality into its own metrics.
    ///
    /// Set once: a second call is ignored, so two schedulers sharing a policy
    /// cannot steal each other's observer. Returns whether this call took.
    pub fn attach_observer(&self, observer: Arc<dyn LocalityObserver>) -> bool {
        self.observer.set(observer).is_ok()
    }

    /// The stage's locality scan, computed once per stage attempt.
    ///
    /// Binding re-runs on every task-status batch, and the scan walks one
    /// `PartitionLocation` per (partition, producer) pair. Both paths that can
    /// replace a stage's plan (`RunningStage::to_unresolved`,
    /// `SuccessfulStage::to_running`) bump `stage_attempt_num`, so the attempt
    /// is a sound cache tag.
    fn locality_for(
        &self,
        job_id: &JobId,
        running_stage: &RunningStage,
    ) -> Arc<StageLocality> {
        let mut cache = lock(&self.cache);
        let stages = cache.entry(job_id.clone()).or_default();
        if let Some(cached) = stages.get(&running_stage.stage_id)
            && cached.stage_attempt_num == running_stage.stage_attempt_num
        {
            return cached.locality.clone();
        }
        let locality = Arc::new(StageLocality::of(&running_stage.plan));
        stages.insert(
            running_stage.stage_id,
            CachedLocality {
                stage_attempt_num: running_stage.stage_attempt_num,
                locality: locality.clone(),
            },
        );
        locality
    }

    /// Bind one stage's pending partitions across the available budgets:
    /// first the partitions an executor already holds, then everything left
    /// over onto whatever vcores remain.
    ///
    /// Returns whether the cluster ran out of vcores with work still pending,
    /// which ends the round.
    fn bind_stage(
        &self,
        running_stage: &mut RunningStage,
        session_id: &str,
        job_id: &JobId,
        budgets: &mut [&mut AvailableVcores],
        bound_tasks: &mut Vec<BoundTask>,
        round: &mut LocalityStats,
    ) -> bool {
        let locality = self.locality_for(job_id, running_stage);
        let binding = Binding {
            session_id,
            job_id,
            locality: &locality,
            whole_stage: stage_has_input_collapse(&running_stage.plan),
        };

        // Affinity pass: hand each executor the partitions assigned to it.
        if binding.whole_stage {
            // The one task reads every byte of the stage, so it goes to the
            // biggest holder with room, settling for a smaller one rather than
            // letting the fallback pass choose on free vcores alone. No
            // `MIN_HOLDER_SHARE` floor here: a single task has no spread to
            // protect, so the largest holder always wins.
            for home in locality.ranked_executors() {
                let budget = budgets
                    .iter_mut()
                    .find(|budget| budget.executor_id == home && budget.vcores > 0);
                if let Some(budget) = budget {
                    binding.drain_onto(running_stage, budget, None, bound_tasks, round);
                    break;
                }
            }
        } else {
            // `capacity` borrows the budgets, so it is dropped before they are
            // spent below; the assignment it returns borrows the scan instead.
            let assignment = {
                let mut capacity: HashMap<&str, u32> = budgets
                    .iter()
                    .filter(|budget| budget.vcores > 0)
                    .map(|budget| (budget.executor_id.as_str(), budget.vcores))
                    .collect();
                locality.assign(
                    pending_snapshot(&mut running_stage.pending).into_iter(),
                    &mut capacity,
                )
            };
            let homes: HashSet<&str> = assignment.values().copied().collect();

            for budget in budgets.iter_mut() {
                let Some(&home) = homes.get(budget.executor_id.as_str()) else {
                    continue;
                };
                let keep = |partition: usize| assignment.get(&partition) == Some(&home);
                binding.drain_onto(
                    running_stage,
                    budget,
                    Some(&keep),
                    bound_tasks,
                    round,
                );
            }
        }

        // Fallback: partitions with no recorded holder, or whose holders were
        // all full, take whatever vcores remain. Not restricted to the
        // executors that won an assignment above, since a stage with no
        // shuffle input has none.
        for budget in budgets.iter_mut() {
            if running_stage.pending.is_empty() {
                break;
            }
            binding.drain_onto(running_stage, budget, None, bound_tasks, round);
        }

        // Work still pending means the cluster ran out of vcores: end the round.
        !running_stage.pending.is_empty()
    }

    /// Folds a round's measurement into the cumulative counters and logs it.
    fn record(&self, round: LocalityStats) {
        if round.tasks == 0 {
            return;
        }
        let cumulative = {
            let mut stats = lock(&self.stats);
            *stats += round;
            *stats
        };
        if let Some(observer) = self.observer.get() {
            observer.observe(&round);
        }
        debug!(
            "shuffle-affinity: bound {} tasks / {} partitions ({} with local input); \
             {} of {} input bytes local ({:.1}%); cumulative {:.1}% of {} bytes{}",
            round.tasks,
            round.partitions,
            round.local_partitions,
            round.local_bytes,
            round.total_bytes,
            round.local_byte_ratio() * 100.0,
            cumulative.local_byte_ratio() * 100.0,
            cumulative.total_bytes,
            if cumulative.imputed_bytes {
                " (some producers reported no size, so the byte counts are padded)"
            } else {
                ""
            },
        );
    }

    /// The shared stats handle, for tests asserting two policies share state.
    #[cfg(test)]
    pub(crate) fn stats_handle(&self) -> Arc<Mutex<LocalityStats>> {
        self.stats.clone()
    }

    /// Number of memoized stage scans currently held.
    fn cache_len(&self) -> usize {
        lock(&self.cache).values().map(HashMap::len).sum()
    }

    /// Drops scans for jobs that are no longer running, bounding the cache to
    /// the stages of live jobs.
    fn prune_cache(&self, running_jobs: &HashMap<JobId, JobInfoCache>) {
        lock(&self.cache).retain(|job_id, _| running_jobs.contains_key(job_id));
    }

    /// Drops `job_id`'s scans for stages that are no longer running.
    ///
    /// A stage is bound only while it runs, so without this a long job holds
    /// every stage it has ever run, tens of megabytes for a wide one.
    fn prune_finished_stages(
        &self,
        job_id: &JobId,
        stages: &HashMap<usize, ExecutionStage>,
    ) {
        if let Some(cached) = lock(&self.cache).get_mut(job_id) {
            cached.retain(|stage_id, _| {
                matches!(stages.get(stage_id), Some(ExecutionStage::Running(_)))
            });
        }
    }
}

#[async_trait::async_trait]
impl DistributionPolicy for ShuffleAffinityPolicy {
    async fn bind_tasks(
        &self,
        mut budgets: Vec<&mut AvailableVcores>,
        running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    ) -> datafusion::error::Result<Vec<BoundTask>> {
        let mut schedulable_tasks: Vec<BoundTask> = vec![];
        let mut round = LocalityStats::default();

        self.prune_cache(&running_jobs);

        if budgets.iter().all(|b| b.vcores == 0) {
            debug!("No executor vcores available for task binding");
            return Ok(schedulable_tasks);
        }

        // Largest executor first, as `Bias` does, so the fallback pass keeps
        // packing onto the biggest budget. Ties break on id to keep repeated
        // binding rounds deterministic.
        budgets.sort_by(|a, b| {
            Ord::cmp(&b.vcores, &a.vcores)
                .then_with(|| Ord::cmp(&a.executor_id, &b.executor_id))
        });

        // A labelled break rather than an early return, so the round is
        // recorded on exactly one path.
        'jobs: for (job_id, job_info) in running_jobs.iter() {
            if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
                debug!("Job {job_id} is not in running status and will be skipped");
                continue;
            }
            let mut graph = job_info.execution_graph.write().await;
            self.prune_finished_stages(job_id, graph.stages());
            let session_id = graph.session_id().to_string();
            // `DistributionPolicy` is handed no `if_skip` predicate, so no
            // stage is blacklisted, the same `|_| false` the built-in
            // policies are called with today.
            while let Some(running_stage) = graph.fetch_running_stage(&[]) {
                let cluster_exhausted = self.bind_stage(
                    running_stage,
                    &session_id,
                    job_id,
                    &mut budgets,
                    &mut schedulable_tasks,
                    &mut round,
                );
                if cluster_exhausted {
                    break 'jobs;
                }
            }
        }

        self.record(round);
        Ok(schedulable_tasks)
    }

    fn name(&self) -> &str {
        "shuffle-affinity"
    }
}

/// One stage's binding context, shared by the affinity and fallback passes.
struct Binding<'a> {
    session_id: &'a str,
    job_id: &'a JobId,
    locality: &'a StageLocality,
    /// Whether one task of this stage reads *every* partition (a collapse).
    /// It changes both where the task goes and how its locality is measured.
    whole_stage: bool,
}

impl Binding<'_> {
    /// Bind as much of `stage` onto one executor as its budget and `keep`
    /// allow, measuring every task bound.
    fn drain_onto(
        &self,
        stage: &mut RunningStage,
        budget: &mut AvailableVcores,
        keep: Option<&dyn Fn(usize) -> bool>,
        bound_tasks: &mut Vec<BoundTask>,
        round: &mut LocalityStats,
    ) {
        let executor_id = budget.executor_id.clone();
        while budget.vcores > 0 {
            let Some(bound) = bind_one_where(
                stage,
                self.session_id,
                self.job_id,
                budget,
                self.whole_stage,
                keep,
            ) else {
                break; // the (filtered) pending queue is drained
            };
            *round += self.measure(&executor_id, &bound);
            bound_tasks.push(bound);
        }
    }

    /// Of `partitions`, how many the executor holds any bytes of.
    fn local_partitions(&self, executor_id: &str, partitions: &[usize]) -> u64 {
        partitions
            .iter()
            .filter(|&&partition| {
                self.locality
                    .partitions
                    .get(&partition)
                    .is_some_and(|locality| locality.bytes_on(executor_id) > 0)
            })
            .count() as u64
    }

    /// What a bound task achieved: bytes it will read from the executor it
    /// landed on, against every byte it will read.
    fn measure(&self, executor_id: &str, bound: &BoundTask) -> LocalityStats {
        let partitions = &bound.1.global_input_partition_ids;
        if self.whole_stage {
            // A collapse task reads the entire stage, so its bytes are the
            // stage's, broadcast included. Its partitions are not: holding
            // stage bytes is not holding bytes of every partition.
            return LocalityStats {
                tasks: 1,
                partitions: partitions.len() as u64,
                local_partitions: self.local_partitions(executor_id, partitions),
                local_bytes: self.locality.bytes_on(executor_id),
                total_bytes: self.locality.total_bytes(),
                imputed_bytes: !self.locality.measured,
            };
        }
        let mut stats = LocalityStats {
            tasks: 1,
            imputed_bytes: !self.locality.measured,
            ..Default::default()
        };
        for partition in partitions {
            stats.partitions += 1;
            let Some(locality) = self.locality.partitions.get(partition) else {
                continue;
            };
            let local = locality.bytes_on(executor_id);
            stats.local_bytes += local;
            stats.total_bytes += locality.total;
            if local > 0 {
                stats.local_partitions += 1;
            }
        }
        stats
    }
}

/// Where a stage's input bytes live, as recorded by its completed producers in
/// the shuffle readers of its plan.
#[derive(Debug, Default)]
struct StageLocality {
    /// Stage-global input partition id -> who holds its bytes.
    partitions: HashMap<usize, PartitionLocality>,
    /// Executor -> bytes it holds across every input partition of the stage,
    /// broadcast included.
    totals: HashMap<String, u64>,
    /// Whether every producer feeding the stage reported a real size.
    measured: bool,
}

/// Who holds one partition's input, best first.
#[derive(Debug, Default)]
struct PartitionLocality {
    /// Every byte a task for this partition will read.
    total: u64,
    /// `(executor, bytes it holds)`, largest share first, ties by executor id.
    holders: Vec<(String, u64)>,
}

impl PartitionLocality {
    /// `bytes` as a share of everything a task for this partition will read.
    fn share(&self, bytes: u64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        bytes as f64 / self.total as f64
    }

    /// Bytes `executor_id` holds of this partition, or zero if it holds none.
    fn bytes_on(&self, executor_id: &str) -> u64 {
        self.holders
            .iter()
            .find(|(id, _)| id == executor_id)
            .map(|(_, bytes)| *bytes)
            .unwrap_or(0)
    }
}

/// The share of a partition an executor must hold to count as a home for it.
///
/// In an even shuffle each of `E` executors holds `1/E`, so past five nothing
/// qualifies, the partition has no home, and it is bound like any other. That
/// is the honest answer, since no placement is better there.
///
/// Filters preference only: the scan keeps every holder, so
/// [`PartitionLocality::bytes_on`] still measures what a task reads locally.
///
/// Spark uses the same 0.2 in `MapOutputTracker.getLocationsWithLargestOutputs`.
const MIN_HOLDER_SHARE: f64 = 0.2;

impl StageLocality {
    fn of(plan: &Arc<dyn ExecutionPlan>) -> Self {
        let mut acc = LocalityAcc::default();
        collect_locations(plan, 0, &mut acc);

        let mut partitions = HashMap::with_capacity(acc.per_partition.len());
        for (partition, bytes) in acc.per_partition {
            let total: u64 = bytes.values().sum();
            if total == 0 {
                continue;
            }
            let mut holders: Vec<(String, u64)> = bytes
                .into_iter()
                .map(|(executor_id, held)| (executor_id.to_string(), held))
                .collect();
            // Largest share first; ties by executor id so the same plan always
            // yields the same ranking, whatever order the locations arrived in.
            holders.sort_by(|(a_id, a), (b_id, b)| {
                Ord::cmp(b, a).then_with(|| Ord::cmp(a_id, b_id))
            });
            partitions.insert(partition, PartitionLocality { total, holders });
        }

        let totals = acc
            .totals
            .into_iter()
            .map(|(executor_id, bytes)| (executor_id.to_string(), bytes))
            .collect();

        Self {
            partitions,
            totals,
            measured: !acc.imputed,
        }
    }

    /// Executors holding stage input, most bytes first.
    fn ranked_executors(&self) -> Vec<&str> {
        let mut ranked: Vec<(&str, u64)> = self
            .totals
            .iter()
            .map(|(executor_id, bytes)| (executor_id.as_str(), *bytes))
            .collect();
        // Bytes descending, ties by id ascending, as the per-partition holder
        // ranking sorts.
        ranked.sort_by(|(a_id, a), (b_id, b)| {
            Ord::cmp(b, a).then_with(|| Ord::cmp(a_id, b_id))
        });
        ranked
            .into_iter()
            .map(|(executor_id, _)| executor_id)
            .collect()
    }

    /// The executor holding the most of the stage's input bytes overall.
    #[cfg(test)]
    fn dominant_executor(&self) -> Option<&str> {
        self.ranked_executors().first().copied()
    }

    /// Every input byte of the stage, wherever it lives.
    fn total_bytes(&self) -> u64 {
        self.totals.values().sum()
    }

    /// Bytes `executor_id` holds across the whole stage.
    fn bytes_on(&self, executor_id: &str) -> u64 {
        self.totals.get(executor_id).copied().unwrap_or(0)
    }

    /// Assign pending partitions to executors, strongest locality first. A
    /// scarce vcore then goes to the partition with most to gain, and a
    /// partition whose best holder is full can settle for its second best.
    ///
    /// `capacity` is consumed as partitions are assigned. Partitions left out,
    /// having no holder worth the name (see [`MIN_HOLDER_SHARE`]) or none with
    /// room, are absent from the result and get bound by the fallback pass.
    fn assign<'a>(
        &'a self,
        pending: impl Iterator<Item = usize>,
        capacity: &mut HashMap<&str, u32>,
    ) -> HashMap<usize, &'a str> {
        let mut candidates: Vec<(u64, usize, &'a str)> = vec![];
        for partition in pending {
            let Some(locality) = self.partitions.get(&partition) else {
                continue;
            };
            for (executor_id, held) in &locality.holders {
                if locality.share(*held) < MIN_HOLDER_SHARE {
                    // Holders are ordered by size, so nothing after this one
                    // qualifies either.
                    break;
                }
                if capacity.contains_key(executor_id.as_str()) {
                    candidates.push((*held, partition, executor_id.as_str()));
                }
            }
        }
        // Bytes descending; partition then executor ascending so a tie always
        // resolves the same way.
        candidates.sort_by(|(a_bytes, a_part, a_id), (b_bytes, b_part, b_id)| {
            Ord::cmp(b_bytes, a_bytes)
                .then_with(|| Ord::cmp(a_part, b_part))
                .then_with(|| Ord::cmp(a_id, b_id))
        });

        let mut assignment = HashMap::new();
        for (_, partition, executor_id) in candidates {
            if assignment.contains_key(&partition) {
                continue;
            }
            let Some(free) = capacity.get_mut(executor_id) else {
                continue;
            };
            if *free == 0 {
                continue;
            }
            *free -= 1;
            assignment.insert(partition, executor_id);
        }
        assignment
    }
}

/// Running totals for one stage plan.
#[derive(Debug, Default)]
struct LocalityAcc<'a> {
    /// Stage-global input partition id -> executor -> bytes. Drives
    /// per-partition placement, so it holds only what a *single* task reads.
    per_partition: HashMap<usize, PartitionBytes<'a>>,
    /// Executor -> every input byte of the stage, broadcast included. Drives
    /// whole-stage placement, where a task really does read all of it.
    totals: HashMap<&'a str, u64>,
    /// Whether any producer reported no size, leaving the bytes above partly
    /// placeholders. Default-false means fully measured.
    imputed: bool,
}

/// Accumulate the stage's shuffle input, from every reader in the plan.
///
/// `offset` maps a reader's local partition index onto the stage-global index
/// a task slice is expressed in, the same mapping
/// [`ballista_scheduler::state::task_builder`] applies. It is zero except below a
/// `UnionExec`, whose output partition belongs to exactly one child;
/// co-partitioned fan-ins (a join's two sides) share one index space, so their
/// bytes sum per partition. An operator that changes the partition count in
/// between makes the attribution approximate, costing a misplaced task at
/// worst.
fn collect_locations<'a>(
    node: &'a Arc<dyn ExecutionPlan>,
    offset: usize,
    acc: &mut LocalityAcc<'a>,
) {
    if let Some(reader) = node.downcast_ref::<ShuffleReaderExec>() {
        // A broadcast reader hands every task the same locations, so it says
        // nothing about which partition goes where, but its bytes are read by
        // whichever task runs and so belong to the stage's totals.
        if reader.broadcast {
            acc.add_stage_bytes(&reader.partition);
        } else {
            acc.add_partition_bytes(&reader.partition, offset);
        }
        return;
    }
    if let Some(reader) = node.downcast_ref::<RangeShuffleReaderExec>() {
        acc.add_partition_bytes(&reader.partition, offset);
        return;
    }
    if node.is::<UnionExec>() {
        let mut child_offset = offset;
        for child in node.children() {
            collect_locations(child, child_offset, acc);
            child_offset += child.properties().output_partitioning().partition_count();
        }
        return;
    }
    for child in node.children() {
        collect_locations(child, offset, acc);
    }
}

impl<'a> LocalityAcc<'a> {
    /// Count a reader's bytes towards both the stage totals and the individual
    /// partitions that will read them.
    fn add_partition_bytes(
        &mut self,
        partitions: &'a [Vec<PartitionLocation>],
        offset: usize,
    ) {
        for (partition, locations) in partitions.iter().enumerate() {
            for (executor_id, bytes, measured) in held(locations) {
                *self.totals.entry(executor_id).or_insert(0) += bytes;
                *self
                    .per_partition
                    .entry(offset + partition)
                    .or_default()
                    .entry(executor_id)
                    .or_insert(0) += bytes;
                self.imputed |= !measured;
            }
        }
    }

    /// Count a reader's bytes towards the stage totals only.
    fn add_stage_bytes(&mut self, partitions: &'a [Vec<PartitionLocation>]) {
        for locations in partitions {
            for (executor_id, bytes, measured) in held(locations) {
                *self.totals.entry(executor_id).or_insert(0) += bytes;
                self.imputed |= !measured;
            }
        }
    }
}

/// `(executor, bytes, measured)` for each location. A writer that reported no
/// size still proves the file is there, so it counts as one placeholder byte.
/// That is enough to rank executors, hence `measured: false`.
fn held(locations: &[PartitionLocation]) -> impl Iterator<Item = (&str, u64, bool)> {
    locations.iter().map(|location| {
        let bytes = location.partition_stats.num_bytes();
        (
            location.executor_meta.id.as_str(),
            bytes.unwrap_or(1),
            bytes.is_some(),
        )
    })
}

// ---------------------------------------------------------------------------
// Copies of scheduler internals.
//
// #2319 asks for this to be prototyped without touching the core, and it can
// be: every field these need is already public. Rather than widen the
// scheduler's API for a policy nobody has measured yet, the three pieces
// binding needs are copied here, pinned to the version of Ballista this crate
// builds against. If the policy is upstreamed, they give way to shared
// functions.
//
// A copy can drift from its original. One rule matters more than the rest when
// it does: see `stage_has_input_collapse`.
// ---------------------------------------------------------------------------

/// Whether one task of this stage must read *every* input partition.
///
/// Copied from `ballista_scheduler::cluster`. A stage "collapses" when
/// something above its shuffle readers folds every input partition into a
/// single output partition, such as a global aggregate or `ORDER BY ... LIMIT`.
/// Such a stage must be bound as one task over the whole pending queue:
/// splitting it yields partial results that nothing downstream merges, which
/// is a wrong answer rather than a slow query.
///
/// The upstream version enumerates the stage-boundary operators
/// (`ShuffleReaderExec`, `RangeShuffleReaderExec`); if it learns about a new
/// one, this copy must follow.
fn stage_has_input_collapse(plan_root: &Arc<dyn ExecutionPlan>) -> bool {
    fn walk(node: &Arc<dyn ExecutionPlan>) -> bool {
        if node.downcast_ref::<ShuffleReaderExec>().is_some()
            || node.downcast_ref::<RangeShuffleReaderExec>().is_some()
        {
            return false;
        }
        if node.properties().output_partitioning().partition_count() == 1 {
            return true;
        }
        match node.children().as_slice() {
            [child] => walk(child),
            _ => false,
        }
    }
    match plan_root.children().as_slice() {
        [child] => walk(child),
        _ => false,
    }
}

/// The partitions still waiting, front first, leaving the queue untouched.
///
/// `PendingPartitions` keeps its queue private and exposes no read-only view,
/// so the snapshot is taken by draining and putting everything straight back.
/// `reschedule` pushes to the front in the order given, and the queue is empty
/// at that point, so the original order is restored exactly.
fn pending_snapshot(pending: &mut PendingPartitions) -> Vec<usize> {
    let queued = pending.next_slice(usize::MAX);
    pending.reschedule(queued.iter().copied());
    queued
}

/// Take up to `max` partitions matching `keep`, front first and in queue
/// order, leaving the rest queued where they were.
fn next_slice_where(
    pending: &mut PendingPartitions,
    max: usize,
    keep: &dyn Fn(usize) -> bool,
) -> Vec<usize> {
    if max == 0 {
        return vec![];
    }
    let queued = pending.next_slice(usize::MAX);
    let mut taken = Vec::with_capacity(max.min(queued.len()));
    let mut rest = Vec::with_capacity(queued.len());
    for partition in queued {
        if taken.len() < max && keep(partition) {
            taken.push(partition);
        } else {
            rest.push(partition);
        }
    }
    pending.reschedule(rest);
    taken
}

/// Bind one task off `running_stage`'s pending queue onto `budget`, taking
/// only the partitions `keep` accepts.
///
/// Copied from `bind_one` in `ballista_scheduler::cluster`, with the `keep`
/// filter added so a placement decision can pull a *subset* of the queue
/// instead of the front slice. Two invariants come with the copy:
///
/// - A collapse stage ignores `keep` and the `max_partitions_per_task` cap,
///   because its single task must consume the whole queue.
/// - A collapse task reserves 1 vcore however many partitions it packs (its
///   plan root has one output partition, so one thread runs the pipeline);
///   every other task reserves one per partition.
fn bind_one_where(
    running_stage: &mut RunningStage,
    session_id: &str,
    job_id: &JobId,
    budget: &mut AvailableVcores,
    is_collapse: bool,
    keep: Option<&dyn Fn(usize) -> bool>,
) -> Option<BoundTask> {
    let cap = running_stage
        .session_config
        .options()
        .extensions
        .get::<BallistaConfig>()
        .map(|bc| bc.max_partitions_per_task())
        .filter(|&n| n > 0)
        .unwrap_or(usize::MAX);
    let max_partitions = if is_collapse {
        usize::MAX
    } else {
        (budget.vcores as usize).min(cap)
    };
    let input_partition_ids = match keep {
        Some(keep) if !is_collapse => {
            next_slice_where(&mut running_stage.pending, max_partitions, keep)
        }
        _ => running_stage.pending.next_slice(max_partitions),
    };
    if input_partition_ids.is_empty() {
        return None;
    }
    let vcores_consumed = if is_collapse {
        1
    } else {
        input_partition_ids.len() as u32
    };
    let executor_id = budget.executor_id.clone();
    // task_id is the append-order slot in `task_infos`. Since we are about to
    // push, that is `task_infos.len()`.
    let task_id = running_stage.task_infos.len();
    let task_attempt = input_partition_ids
        .iter()
        .map(|pid| running_stage.task_failure_numbers[*pid])
        .max()
        .unwrap_or(0);
    let mut task_info = create_task_info(executor_id.clone(), task_id);
    task_info.global_input_partition_ids = input_partition_ids.clone();
    task_info.vcores_consumed = vcores_consumed;
    running_stage.task_infos.push(task_info);
    let task_desc = TaskDescription {
        session_id: session_id.to_string(),
        key: TaskKey {
            job_id: job_id.clone(),
            stage_id: running_stage.stage_id,
            task_id,
        },
        stage_attempt_num: running_stage.stage_attempt_num,
        task_attempt,
        global_input_partition_ids: input_partition_ids,
        vcores_consumed,
        plan: running_stage.plan.clone(),
        session_config: running_stage.session_config.clone(),
    };
    budget.vcores -= vcores_consumed;
    Some((executor_id, task_desc))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use ballista_core::JobId;
    use ballista_core::config::BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK;
    use ballista_core::error::Result;
    use ballista_core::execution_plans::ShuffleReaderExec;
    use ballista_core::extension::SessionConfigExt;
    use ballista_core::serde::protobuf::AvailableVcores;
    use ballista_core::serde::scheduler::{
        PartitionId, PartitionLocation, PartitionStats, TaskKey,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::joins::CrossJoinExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};
    use datafusion::prelude::SessionConfig;

    use super::{
        Binding, ExecutionStage, LocalityObserver, LocalityStats, PendingPartitions,
        ShuffleAffinityPolicy, StageLocality, next_slice_where, pending_snapshot,
    };
    use ballista_core::serde::protobuf::{self, TaskStatus, task_status};
    use ballista_core::serde::scheduler::ExecutorMetadata;
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    };
    use ballista_scheduler::cluster::ClusterState;
    use ballista_scheduler::cluster::memory::InMemoryClusterState;
    use ballista_scheduler::cluster::{BoundTask, DistributionPolicy};
    use ballista_scheduler::config::TaskDistributionPolicy;
    use ballista_scheduler::planner::DefaultDistributedPlanner;
    use ballista_scheduler::state::execution_graph::StaticExecutionGraph;
    use ballista_scheduler::state::execution_graph::{ExecutionGraph, TaskDescription};
    use ballista_scheduler::state::execution_stage::RunningStage;
    use ballista_scheduler::state::task_manager::JobInfoCache;
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::col;
    use datafusion::prelude::SessionContext;
    use datafusion::test_util::scan_empty_with_partitions;
    use mock_locality_executor as executor;
    use std::collections::HashSet;

    /// Creates a test execution graph whose consumer stage *collapses*: an
    /// ungrouped aggregate puts a `CoalescePartitionsExec` above the shuffle
    /// reader, so one task must drain every input partition.
    async fn test_collapse_plan_with_config(
        job_id: &JobId,
        session_config: Arc<SessionConfig>,
    ) -> StaticExecutionGraph {
        let config = SessionConfig::new().with_target_partitions(2);
        let ctx = Arc::new(SessionContext::new_with_config(config));
        let session_state = ctx.state();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        // Two input partitions, no GROUP BY: the final aggregate consumes every
        // partial through a collapse.
        let logical_plan = scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), 2)
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap();

        let optimized_plan = session_state.optimize(&logical_plan).unwrap();
        let plan = session_state
            .create_physical_plan(&optimized_plan)
            .await
            .unwrap();

        let mut planner = DefaultDistributedPlanner::new();
        StaticExecutionGraph::new(
            "localhost:50050",
            job_id,
            "",
            "session",
            plan,
            0,
            session_config,
            &mut planner,
            None,
        )
        .unwrap()
    }

    /// An executor with enough vcores that a placement decision is never masked
    /// by its budget running out.
    fn mock_locality_executor(executor_id: &str) -> ExecutorMetadata {
        ExecutorMetadata {
            id: executor_id.to_string(),
            host: "localhost".to_string(),
            port: 50051,
            grpc_port: 50052,
            specification: ExecutorSpecification::default().with_vcores(8),
            os_info: ExecutorOperatingSystemSpecification::default(),
        }
    }

    /// Available tasks in the first running stage, the map stage, before any
    /// consumer stage has been revived.
    fn first_running_stage_tasks(graph: &StaticExecutionGraph) -> usize {
        graph
            .stages()
            .values()
            .filter_map(|stage| match stage {
                ExecutionStage::Running(stage) => Some(stage.available_tasks()),
                _ => None,
            })
            .find(|available| *available > 0)
            .unwrap()
    }

    /// An aggregation graph whose map stage has completed, leaving the consumer
    /// stage pending with real `PartitionLocation`s to place.
    ///
    /// Map task `n` runs on `executor_1` when `n` is even and `executor_2` when
    /// odd, and `bytes(n, partition)` sizes what it wrote per output partition, so
    /// a test can state its locality layout as a function.
    async fn mock_shuffle_graph(
        job_id: &JobId,
        num_partitions: usize,
        bytes: &(dyn Fn(usize, usize) -> u64 + Sync),
    ) -> Result<StaticExecutionGraph> {
        let session_config = Arc::new(
            SessionConfig::new_with_ballista()
                .set_str(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, "0"),
        );
        let mut graph =
            test_aggregation_plan_with_config(num_partitions, job_id, session_config)
                .await;
        graph.revive();

        // Complete exactly the map stage: popping past it would drain the
        // consumer stage the caller wants left pending.
        let map_tasks = first_running_stage_tasks(&graph);

        for map_task in 0..map_tasks {
            let Some(task) = pop_map_task(&mut graph, "executor_0") else {
                break;
            };
            let executor = mock_locality_executor(if map_task % 2 == 0 {
                "executor_1"
            } else {
                "executor_2"
            });
            let status =
                mock_completed_task_with_partition_bytes(task, &executor.id, |p| {
                    bytes(map_task, p)
                });
            graph.update_task_status(&executor, vec![status], 1, 1)?;
        }

        Ok(graph)
    }

    /// [`mock_shuffle_graph`] wrapped as the running-job map a distribution policy
    /// binds against.
    async fn mock_shuffle_jobs(
        job_id: &JobId,
        num_partitions: usize,
        bytes: &(dyn Fn(usize, usize) -> u64 + Sync),
    ) -> Result<HashMap<JobId, JobInfoCache>> {
        let graph = mock_shuffle_graph(job_id, num_partitions, bytes).await?;
        let mut jobs = HashMap::new();
        jobs.insert(job_id.clone(), JobInfoCache::new(Box::new(graph)));
        Ok(jobs)
    }

    /// Copied from `ballista_scheduler::test_utils`, which is `cfg(test)` and
    /// so unreachable from here. A two-stage aggregation: the map stage writes
    /// a shuffle the consumer stage reads.
    async fn test_aggregation_plan_with_config(
        partition: usize,
        job_id: &JobId,
        session_config: Arc<SessionConfig>,
    ) -> StaticExecutionGraph {
        let config = SessionConfig::new().with_target_partitions(partition);
        let ctx = Arc::new(SessionContext::new_with_config(config));
        let session_state = ctx.state();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        let logical_plan = scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), 2)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap();

        let optimized_plan = session_state.optimize(&logical_plan).unwrap();
        let plan = session_state
            .create_physical_plan(&optimized_plan)
            .await
            .unwrap();

        let mut planner = DefaultDistributedPlanner::new();
        StaticExecutionGraph::new(
            "localhost:50050",
            job_id,
            "",
            "session",
            plan,
            0,
            session_config,
            &mut planner,
            None,
        )
        .unwrap()
    }

    /// Copied from `ballista_scheduler::test_utils`, sized so each shuffle
    /// output partition reports `num_bytes(partition_id)`, the counts the
    /// policy reads back off the consumer stage's `PartitionLocation`s.
    fn mock_completed_task_with_partition_bytes(
        task: TaskDescription,
        executor_id: &str,
        num_bytes: impl Fn(usize) -> u64,
    ) -> TaskStatus {
        let partitions = (0..task.get_output_partition_number())
            .map(|partition_id| protobuf::ShuffleWritePartition {
                partition_id: partition_id as u64,
                num_batches: 1,
                num_rows: 1,
                num_bytes: num_bytes(partition_id),
                file_id: None,
                is_sort_shuffle: false,
            })
            .collect();

        TaskStatus {
            task_id: task.key.task_id as u32,
            job_id: task.key.job_id.clone().into(),
            stage_id: task.key.stage_id as u32,
            stage_attempt_num: task.stage_attempt_num as u32,
            launch_time: 0,
            start_exec_time: 0,
            end_exec_time: 0,
            metrics: vec![],
            status: Some(task_status::Status::Successful(protobuf::SuccessfulTask {
                executor_id: executor_id.to_owned(),
                partitions,
                runtime_stats: vec![],
                window_state: vec![],
            })),
        }
    }

    /// One single-partition task off the graph's running stage.
    ///
    /// `ExecutionGraph::pop_next_task` is `cfg(test)` inside the scheduler, so
    /// the map stage is driven with the policy's own binding copy instead. A
    /// one-vcore budget takes exactly one partition, as `pop_next_task` does.
    fn pop_map_task(
        graph: &mut StaticExecutionGraph,
        executor_id: &str,
    ) -> Option<TaskDescription> {
        let session_id = graph.session_id().to_string();
        let job_id = graph.job_id().clone();
        let stage = graph.fetch_running_stage(&[])?;
        let mut budget = AvailableVcores {
            executor_id: executor_id.to_string(),
            vcores: 1,
        };
        // `is_collapse: false` unconditionally, as `pop_next_task` does: it
        // always hands out one partition. Passing the real collapse flag would
        // pack a whole stage into one task and walk the loop into the next
        // stage.
        super::bind_one_where(stage, &session_id, &job_id, &mut budget, false, None)
            .map(|(_, task)| task)
    }

    /// Stands in for the embedder's metrics backend, keeping each round it is
    /// handed so a test can assert on the deltas the policy published.
    #[derive(Default)]
    struct RecordingObserver {
        rounds: std::sync::Mutex<Vec<LocalityStats>>,
    }

    impl RecordingObserver {
        fn rounds(&self) -> Vec<LocalityStats> {
            super::lock(&self.rounds).clone()
        }

        /// Every round summed, as a counter-based backend would.
        fn total(&self) -> LocalityStats {
            let mut total = LocalityStats::default();
            for round in self.rounds() {
                total += round;
            }
            total
        }
    }

    impl LocalityObserver for RecordingObserver {
        fn observe(&self, round: &LocalityStats) {
            super::lock(&self.rounds).push(*round);
        }
    }

    fn location(executor_id: &str, partition: usize, bytes: u64) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: 0,
            partition_id: PartitionId {
                job_id: "job".into(),
                stage_id: 1,
                partition_id: partition,
            },
            executor_meta: executor(executor_id),
            partition_stats: PartitionStats::new(None, None, Some(bytes)),
            file_id: None,
            is_sort_shuffle: false,
        }
    }

    /// A location whose producer reported no size.
    fn unsized_location(executor_id: &str, partition: usize) -> PartitionLocation {
        PartitionLocation {
            partition_stats: PartitionStats::new(None, None, None),
            ..location(executor_id, partition, 0)
        }
    }

    fn reader_over(partitions: Vec<Vec<PartitionLocation>>) -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, false)]));
        let n = partitions.len();
        Arc::new(
            ShuffleReaderExec::try_new(
                1,
                partitions,
                schema,
                Partitioning::UnknownPartitioning(n),
            )
            .unwrap(),
        )
    }

    /// A reader whose partition `p` is held by the executors in `layout[p]`.
    fn reader(layout: Vec<Vec<(&str, u64)>>) -> Arc<dyn ExecutionPlan> {
        reader_over(
            layout
                .into_iter()
                .enumerate()
                .map(|(p, holders)| {
                    holders
                        .into_iter()
                        .map(|(id, bytes)| location(id, p, bytes))
                        .collect()
                })
                .collect(),
        )
    }

    /// The same, for producers that reported no sizes at all.
    fn reader_without_sizes(layout: Vec<Vec<&str>>) -> Arc<dyn ExecutionPlan> {
        reader_over(
            layout
                .into_iter()
                .enumerate()
                .map(|(p, holders)| {
                    holders
                        .into_iter()
                        .map(|id| unsized_location(id, p))
                        .collect()
                })
                .collect(),
        )
    }

    /// A broadcast reader: one logical partition holding every location.
    fn broadcast_reader(holders: Vec<(&str, u64)>) -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, false)]));
        let locations = holders
            .into_iter()
            .map(|(id, bytes)| location(id, 0, bytes))
            .collect();
        Arc::new(ShuffleReaderExec::try_new_broadcast(1, locations, schema, 1).unwrap())
    }

    /// An embedder hands one clone to the scheduler and keeps another, so the
    /// stats it reads must be the ones the scheduler is accumulating.
    #[test]
    fn clones_of_a_policy_share_their_state() {
        let policy = ShuffleAffinityPolicy::new();
        let clone = policy.clone();

        assert!(Arc::ptr_eq(&policy.stats_handle(), &clone.stats_handle()));

        clone.record(LocalityStats {
            tasks: 1,
            local_bytes: 10,
            total_bytes: 10,
            ..Default::default()
        });
        assert_eq!(
            1,
            policy.stats().tasks,
            "the clone's round is the original's"
        );
    }

    #[test]
    fn preferred_executor_is_the_largest_byte_holder() {
        let plan = reader(vec![
            vec![("executor_1", 900), ("executor_2", 100)],
            vec![("executor_1", 100), ("executor_2", 900)],
            vec![("executor_2", 5)],
        ]);
        let locality = StageLocality::of(&plan);

        assert_eq!(Some("executor_1"), best_holder(&locality, 0));
        assert_eq!(Some("executor_2"), best_holder(&locality, 1));
        assert_eq!(Some("executor_2"), best_holder(&locality, 2));
        // executor_2 holds 1005 bytes against executor_1's 1000.
        assert_eq!(locality.dominant_executor(), Some("executor_2"));
    }

    /// A holder too small to be a home is still a holder: the scan keeps it,
    /// so a task the fallback pass lands there has its local bytes counted.
    #[test]
    fn a_holder_below_the_threshold_is_still_measured() {
        let holders: Vec<(&str, u64)> = vec![
            ("executor_1", 600),
            ("executor_2", 500),
            ("executor_3", 400),
            ("executor_4", 300),
            ("executor_5", 200),
        ];
        let plan = reader(vec![holders]);
        let locality = StageLocality::of(&plan);
        let partition = &locality.partitions[&0];

        assert_eq!(200, partition.bytes_on("executor_5"));
        assert_eq!(0, partition.bytes_on("executor_9"));

        // 200 of 2000 is a tenth of the partition, under the bar, so it is
        // never offered the slot and falls through to the fallback pass.
        let mut capacity = HashMap::from([("executor_5", 1)]);
        assert!(locality.assign(0..1, &mut capacity).is_empty());
    }

    /// An evenly spread partition has no home worth the name. This is the
    /// canonical shuffle, where every producer writes every partition at about
    /// the same size, and expressing a preference there is a guess. Measured,
    /// all three distribution policies read the same share locally.
    #[test]
    fn an_evenly_spread_partition_has_no_home() {
        // Eight executors, an eighth of the partition each.
        let locality = StageLocality::of(&reader(vec![vec![
            ("executor_1", 125),
            ("executor_2", 125),
            ("executor_3", 125),
            ("executor_4", 125),
            ("executor_5", 125),
            ("executor_6", 125),
            ("executor_7", 125),
            ("executor_8", 125),
        ]]));

        let mut capacity = HashMap::from([("executor_1", 1), ("executor_2", 1)]);
        assert!(
            locality.assign(0..1, &mut capacity).is_empty(),
            "no executor holds enough to be worth preferring",
        );
        assert_eq!(
            Some(&1),
            capacity.get("executor_1"),
            "and no capacity was spent pretending otherwise",
        );

        // The bytes are still recorded; only the preference is withheld.
        assert_eq!(125, locality.partitions[&0].bytes_on("executor_1"));
    }

    /// Five executors hold exactly [`MIN_HOLDER_SHARE`] each. The bar is
    /// inclusive, so the last one that can qualify still does.
    #[test]
    fn an_exact_threshold_share_counts_as_a_home() {
        let locality = StageLocality::of(&reader(vec![vec![
            ("executor_1", 200),
            ("executor_2", 200),
            ("executor_3", 200),
            ("executor_4", 200),
            ("executor_5", 200),
        ]]));

        let mut capacity = HashMap::from([("executor_5", 1)]);
        assert_eq!(
            HashMap::from([(0, "executor_5")]),
            locality.assign(0..1, &mut capacity),
        );
    }

    #[test]
    fn a_stage_without_shuffle_input_has_no_locality() {
        let plan = reader(vec![]);
        let locality = StageLocality::of(&plan);

        assert!(locality.partitions.is_empty());
        assert!(locality.dominant_executor().is_none());
    }

    /// Every partition prefers the executor that wrote its bigger half.
    #[tokio::test]
    async fn affinity_binds_each_partition_to_its_home_executor() -> Result<()> {
        let bound = bind(
            &ShuffleAffinityPolicy::new(),
            mock_jobs(8).await?,
            &[("executor_1", 4), ("executor_2", 4)],
        )
        .await;

        // Even partitions were written large by executor_1, odd by executor_2.
        for (executor_id, task) in &bound {
            for partition in &task.global_input_partition_ids {
                let expected = if partition % 2 == 0 {
                    "executor_1"
                } else {
                    "executor_2"
                };
                assert_eq!(
                    executor_id, expected,
                    "partition {partition} should run where its bytes are",
                );
            }
        }
        assert_eq!(8, partitions_covered(&bound));

        Ok(())
    }

    /// A partition no live executor holds is still bound, just away from its
    /// bytes: affinity never idles a vcore waiting for a holder. A zero ratio
    /// is measured too, since that is an answer rather than a gap.
    #[tokio::test]
    async fn affinity_falls_back_when_no_executor_holds_the_input() -> Result<()> {
        let policy = ShuffleAffinityPolicy::new();
        let bound = bind(&policy, mock_jobs(8).await?, &[("executor_9", 8)]).await;

        assert_eq!(8, partitions_covered(&bound));
        assert!(bound.iter().all(|(id, _)| id == "executor_9"));

        let stats = policy.stats();
        assert_eq!(0, stats.local_bytes);
        assert_eq!(0, stats.local_partitions);
        assert_eq!(8, stats.partitions);
        assert_eq!(8_000, stats.total_bytes, "all of it read over the network");
        assert_eq!(0.0, stats.local_byte_ratio());

        Ok(())
    }

    /// A broadcast reader serves every task the same locations, so it must not
    /// steer any single partition, but a whole-stage placement still counts
    /// its bytes.
    #[test]
    fn broadcast_bytes_count_for_the_stage_but_not_for_a_partition() {
        let broadcast = broadcast_reader(vec![("executor_2", 5_000)]);
        let locality = StageLocality::of(&broadcast);

        assert!(
            locality.partitions.is_empty(),
            "a broadcast reader says nothing about which partition goes where",
        );
        assert_eq!(
            locality.dominant_executor(),
            Some("executor_2"),
            "its bytes still belong to the stage's totals",
        );
    }

    /// A `UnionExec` splits its output partition space across its children, so
    /// a reader's local partition `k` is stage-global partition `k + offset`.
    #[test]
    fn union_children_occupy_disjoint_partition_ranges() {
        let left = reader(vec![vec![("executor_1", 100)], vec![("executor_1", 100)]]);
        let right = reader(vec![vec![("executor_2", 100)], vec![("executor_2", 100)]]);
        let union = UnionExec::try_new(vec![left, right]).unwrap();

        let locality = StageLocality::of(&union);

        assert_eq!(
            (0..4)
                .map(|p| best_holder(&locality, p))
                .collect::<Vec<_>>(),
            vec![
                Some("executor_1"),
                Some("executor_1"),
                Some("executor_2"),
                Some("executor_2"),
            ],
        );
    }

    /// A co-partitioned fan-in (a join) reads partition `k` of *both* sides in
    /// one task, so the two readers share an index space and their bytes add.
    #[test]
    fn co_partitioned_readers_sum_into_the_same_partition() {
        let build = reader(vec![vec![("executor_1", 60)]]);
        let probe = reader(vec![vec![("executor_2", 100)]]);
        let join: Arc<dyn ExecutionPlan> = Arc::new(CrossJoinExec::new(build, probe));

        let locality = StageLocality::of(&join);

        assert_eq!(
            Some("executor_2"),
            best_holder(&locality, 0),
            "partition 0 holds 60 bytes on executor_1 and 100 on executor_2",
        );
        assert_eq!(locality.totals.get("executor_1"), Some(&60));
        assert_eq!(locality.totals.get("executor_2"), Some(&100));
    }

    /// A collapse stage's single task reads *every* upstream partition, so it
    /// is placed on the executor holding most of the stage as a whole.
    ///
    /// The mock separates that from the two placements it could be confused
    /// with: `executor_2` has eight times the room (bias would take it) and
    /// holds the only pending partition's first location (the per-partition
    /// pass would take it), while `executor_1` holds nine tenths of the bytes.
    #[tokio::test]
    async fn a_collapse_stage_is_placed_where_its_bytes_are() -> Result<()> {
        let bound = bind(
            &ShuffleAffinityPolicy::new(),
            mock_collapse_job().await?,
            &[("executor_1", 1), ("executor_2", 8)],
        )
        .await;

        assert_eq!(1, bound.len(), "a collapse stage binds exactly one task");
        assert_eq!(
            "executor_1", bound[0].0,
            "placed on the executor holding the stage's bytes, not the one \
             with the most room or the one holding its first partition",
        );

        Ok(())
    }

    /// A full first choice must not hand the task to the fallback pass, which
    /// picks on free vcores alone. `executor_1` holds the bytes but has no
    /// room; `executor_3` has the most room but none of the bytes.
    #[tokio::test]
    async fn a_full_collapse_home_settles_for_the_next_biggest_holder() -> Result<()> {
        let bound = bind(
            &ShuffleAffinityPolicy::new(),
            mock_collapse_job().await?,
            &[("executor_1", 0), ("executor_2", 1), ("executor_3", 8)],
        )
        .await;

        assert_eq!(1, bound.len(), "a collapse stage binds exactly one task");
        assert_eq!(
            "executor_2", bound[0].0,
            "should settle for the second-biggest holder, not the emptiest executor",
        );

        Ok(())
    }

    /// The copies read the pending queue by draining and putting it back, so
    /// the round trip has to be order-preserving or binding silently reorders
    /// the stage's partitions.
    #[test]
    fn snapshotting_and_filtering_leave_the_queue_in_order() {
        let mut pending = PendingPartitions::new(6);

        assert_eq!(vec![0, 1, 2, 3, 4, 5], pending_snapshot(&mut pending));
        assert_eq!(6, pending.remaining(), "a snapshot must not consume");

        // Take the even partitions, capped at two.
        assert_eq!(
            vec![0, 2],
            next_slice_where(&mut pending, 2, &|p| p % 2 == 0)
        );
        // The odd ones the filter passed over kept their place in the queue.
        assert_eq!(vec![1, 3, 4, 5], pending_snapshot(&mut pending));
        assert_eq!(vec![1, 3, 4], pending.next_slice(3));
        assert_eq!(vec![5], next_slice_where(&mut pending, 4, &|_| true));
        assert!(pending.is_empty());
    }

    #[test]
    fn filtering_takes_nothing_when_nothing_matches() {
        let mut pending = PendingPartitions::new(3);

        assert!(next_slice_where(&mut pending, 3, &|_| false).is_empty());
        assert!(next_slice_where(&mut pending, 0, &|_| true).is_empty());
        assert_eq!(3, pending.remaining());
    }

    /// Repeated binding rounds reuse the scan; a new attempt invalidates it.
    #[test]
    fn locality_is_scanned_once_per_stage_attempt() {
        let policy = ShuffleAffinityPolicy::new();
        let job_id: JobId = "job_a".into();
        let plan = reader(vec![vec![("executor_1", 100)]]);

        let first = policy.locality_for(&job_id, &stage(plan.clone(), 0));
        let second = policy.locality_for(&job_id, &stage(plan.clone(), 0));
        assert!(
            Arc::ptr_eq(&first, &second),
            "the second bind of the same stage attempt reuses the scan",
        );

        let retried = policy.locality_for(&job_id, &stage(plan, 1));
        assert!(
            !Arc::ptr_eq(&first, &retried),
            "a new stage attempt can carry new locations, so it rescans",
        );
        assert_eq!(1, policy.cache_len(), "the retry replaces the old entry");
    }

    #[tokio::test]
    async fn the_cache_drops_jobs_that_stopped_running() -> Result<()> {
        let policy = ShuffleAffinityPolicy::new();

        bind(&policy, mock_jobs(8).await?, &[("executor_1", 8)]).await;
        assert_eq!(1, policy.cache_len());

        // The job is gone from the running set on the next round.
        bind(&policy, HashMap::new(), &[]).await;
        assert_eq!(0, policy.cache_len());

        Ok(())
    }

    /// A stage's scan is dead weight once the stage succeeds, and a long job
    /// would otherwise hold every stage it ever ran until the job itself ends.
    #[test]
    fn the_cache_drops_stages_that_stopped_running() {
        let policy = ShuffleAffinityPolicy::new();
        let job_id: JobId = "job_a".into();
        let other_job: JobId = "job_b".into();
        let running = stage(reader(vec![vec![("executor_1", 100)]]), 0);

        policy.locality_for(&job_id, &running);
        policy.locality_for(&other_job, &running);
        assert_eq!(2, policy.cache_len());

        // Still running: the scan is still needed.
        let stages = HashMap::from([(
            running.stage_id,
            ExecutionStage::Running(stage(reader(vec![vec![("executor_1", 100)]]), 0)),
        )]);
        policy.prune_finished_stages(&job_id, &stages);
        assert_eq!(2, policy.cache_len());

        // Succeeded: dropped, and only for the job it belongs to.
        let stages = HashMap::from([(
            running.stage_id,
            ExecutionStage::Successful(running.to_successful()),
        )]);
        policy.prune_finished_stages(&job_id, &stages);
        assert_eq!(
            1,
            policy.cache_len(),
            "job_b's scan of the same stage id must survive",
        );
    }

    /// An executor's last free vcore goes to the partition that gains most by
    /// running there, not to whichever is at the front of the queue.
    #[test]
    fn a_scarce_slot_goes_to_the_strongest_locality() {
        let plan = reader(vec![
            vec![("executor_1", 10)],
            vec![("executor_1", 1_000)],
            vec![("executor_1", 100)],
        ]);
        let locality = StageLocality::of(&plan);
        let mut capacity = HashMap::from([("executor_1", 1)]);

        let assignment = locality.assign(0..3, &mut capacity);

        assert_eq!(
            HashMap::from([(1, "executor_1")]),
            assignment,
            "partition 1 has 100x the bytes on executor_1 that partition 0 has",
        );
        assert_eq!(Some(&0), capacity.get("executor_1"));
    }

    /// A partition whose best holder is full lands on its second best.
    #[test]
    fn a_full_first_choice_falls_to_the_next_best_holder() {
        let plan = reader(vec![
            vec![("executor_1", 1_000), ("executor_2", 500)],
            vec![("executor_1", 1_000), ("executor_2", 500)],
        ]);
        let locality = StageLocality::of(&plan);
        let mut capacity = HashMap::from([("executor_1", 1), ("executor_2", 1)]);

        let assignment = locality.assign(0..2, &mut capacity);

        assert_eq!(
            HashMap::from([(0, "executor_1"), (1, "executor_2"),]),
            assignment,
            "both partitions prefer executor_1, which only has room for one",
        );
    }

    /// End to end: the binding path, not just the assignment, picks the
    /// heaviest partition when vcores are scarce.
    #[tokio::test]
    async fn binding_spends_a_scarce_vcore_on_the_heaviest_partition() -> Result<()> {
        // executor_1 (map task 0) holds 100 bytes per partition index, rising
        // with the index; executor_2 holds a token byte of everything.
        let jobs = mock_shuffle_jobs(&"job_a".into(), 4, &|map_task, partition| {
            if map_task % 2 == 0 {
                100 * (partition as u64 + 1)
            } else {
                1
            }
        })
        .await?;

        let bound = bind(&ShuffleAffinityPolicy::new(), jobs, &[("executor_1", 2)]).await;

        let mut covered: Vec<usize> = bound
            .iter()
            .flat_map(|(_, task)| task.global_input_partition_ids.clone())
            .collect();
        covered.sort();
        assert_eq!(
            vec![2, 3],
            covered,
            "the two heaviest partitions, not the two at the front of the queue",
        );

        Ok(())
    }

    #[tokio::test]
    async fn stats_report_the_locality_achieved() -> Result<()> {
        let policy = ShuffleAffinityPolicy::new();
        assert_eq!(LocalityStats::default(), policy.stats());

        bind(
            &policy,
            mock_jobs(8).await?,
            &[("executor_1", 4), ("executor_2", 4)],
        )
        .await;

        // Every partition holds 900 bytes on its home and 100 on the other,
        // and every one was bound home: 8 x 900 local of 8 x 1000 read.
        let stats = policy.stats();
        assert_eq!(
            LocalityStats {
                tasks: 2,
                partitions: 8,
                local_partitions: 8,
                local_bytes: 7_200,
                total_bytes: 8_000,
                imputed_bytes: false,
            },
            stats,
        );
        assert!((stats.local_byte_ratio() - 0.9).abs() < f64::EPSILON);

        Ok(())
    }

    /// An attached observer sees every round, so the locality reaches the
    /// embedder's metrics and not just the debug log.
    #[tokio::test]
    async fn an_attached_observer_receives_each_round() -> Result<()> {
        let policy = ShuffleAffinityPolicy::new();
        let observer = Arc::new(RecordingObserver::default());
        assert!(policy.attach_observer(observer.clone()));

        for _ in 0..2 {
            bind(
                &policy,
                mock_jobs(8).await?,
                &[("executor_1", 4), ("executor_2", 4)],
            )
            .await;
        }

        // A round is reported as its own delta, not a running total, so a
        // counter-based backend can simply add each one.
        let rounds = observer.rounds();
        assert_eq!(2, rounds.len());
        for round in &rounds {
            assert_eq!(
                LocalityStats {
                    tasks: 2,
                    partitions: 8,
                    local_partitions: 8,
                    local_bytes: 7_200,
                    total_bytes: 8_000,
                    imputed_bytes: false,
                },
                *round,
            );
        }
        // Summing the deltas is what the policy's own cumulative stats say.
        assert_eq!(policy.stats(), observer.total());

        Ok(())
    }

    /// An empty delta would only add zeroes and make "rounds seen"
    /// meaningless.
    #[test]
    fn an_empty_round_is_not_reported() {
        let policy = ShuffleAffinityPolicy::new();
        let observer = Arc::new(RecordingObserver::default());
        policy.attach_observer(observer.clone());

        policy.record(LocalityStats::default());

        assert!(observer.rounds().is_empty());
        assert_eq!(
            LocalityStats::default(),
            policy.stats(),
            "an empty round should not disturb the cumulative total either",
        );
    }

    /// The sink is set once, so a second scheduler sharing the policy cannot
    /// take over the first's metrics.
    #[tokio::test]
    async fn the_observer_is_attached_only_once() {
        let policy = ShuffleAffinityPolicy::new();
        let first = Arc::new(RecordingObserver::default());
        let second = Arc::new(RecordingObserver::default());

        assert!(policy.attach_observer(first.clone()));
        assert!(!policy.attach_observer(second.clone()));

        policy.record(LocalityStats {
            tasks: 1,
            partitions: 1,
            local_partitions: 1,
            local_bytes: 10,
            total_bytes: 10,
            imputed_bytes: false,
        });

        assert_eq!(1, first.rounds().len());
        assert!(second.rounds().is_empty());
    }

    /// A stage reading files rather than shuffle output has no executor with
    /// an affinity assignment, so gating the fallback pass on them would leave
    /// every job's first stage unbound.
    #[tokio::test]
    async fn a_stage_with_no_shuffle_input_is_still_bound() -> Result<()> {
        let job_id: JobId = "job_a".into();
        let session_config = Arc::new(
            SessionConfig::new_with_ballista()
                .set_str(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, "0"),
        );
        // The map stage is left running: it reads the scan, so there is no
        // `PartitionLocation` anywhere in its plan.
        let mut graph =
            test_aggregation_plan_with_config(4, &job_id, session_config).await;
        graph.revive();
        let expected = first_running_stage_tasks(&graph);
        assert!(expected > 0, "the map stage should have work to bind");

        let mut jobs = HashMap::new();
        jobs.insert(job_id, JobInfoCache::new(Box::new(graph)));

        let bound = bind(&ShuffleAffinityPolicy::new(), jobs, &[("executor_1", 4)]).await;

        assert_eq!(
            expected,
            partitions_covered(&bound),
            "a stage with no locality to exploit still has to be scheduled",
        );

        Ok(())
    }

    /// With no producer reporting a size, ranking degrades to counting
    /// locations, so the stage is flagged unmeasured.
    #[test]
    fn unsized_producers_still_rank_but_leave_the_stage_unmeasured() {
        let plan = reader_without_sizes(vec![
            vec!["executor_1", "executor_1", "executor_2"],
            vec!["executor_2"],
        ]);
        let locality = StageLocality::of(&plan);

        // Two placeholder bytes against one: the order is still right.
        assert_eq!(Some("executor_1"), best_holder(&locality, 0));
        assert_eq!(Some("executor_2"), best_holder(&locality, 1));
        // But nothing here is a byte count.
        assert!(!locality.measured);
    }

    /// One producer without a size pads its partition's total, and the flag is
    /// stage-wide.
    #[test]
    fn one_unsized_producer_marks_the_whole_stage_unmeasured() {
        let plan = reader_over(vec![
            vec![
                location("executor_1", 0, 900),
                unsized_location("executor_2", 0),
            ],
            vec![
                location("executor_1", 1, 100),
                location("executor_2", 1, 900),
            ],
        ]);
        let locality = StageLocality::of(&plan);

        assert_eq!(901, locality.partitions[&0].total, "900 plus a placeholder");
        assert_eq!(1_000, locality.partitions[&1].total, "fully reported");
        assert!(!locality.measured);
    }

    /// The flag is sticky: one padded round taints the cumulative total.
    #[test]
    fn imputed_bytes_survives_a_later_measured_round() {
        assert!(!LocalityStats::default().imputed_bytes);

        let mut stats = LocalityStats {
            tasks: 1,
            partitions: 2,
            local_partitions: 2,
            local_bytes: 900,
            total_bytes: 901,
            imputed_bytes: true,
        };

        stats += LocalityStats {
            tasks: 1,
            partitions: 2,
            local_partitions: 0,
            local_bytes: 0,
            total_bytes: 1_000,
            imputed_bytes: false,
        };
        assert!(stats.imputed_bytes);
        assert_eq!(4, stats.partitions);
    }

    /// A collapse task's local bytes include broadcast input; its local
    /// partitions must not, since a broadcast belongs to no partition.
    #[test]
    fn a_collapse_task_counts_only_partitions_it_holds() {
        let plan = UnionExec::try_new(vec![
            reader(vec![vec![("executor_1", 900)], vec![("executor_1", 900)]]),
            broadcast_reader(vec![("executor_2", 5_000)]),
        ])
        .unwrap();
        let locality = StageLocality::of(&plan);
        let job_id: JobId = "job_a".into();
        let binding = Binding {
            session_id: "session",
            job_id: &job_id,
            locality: &locality,
            whole_stage: true,
        };
        let bound = bound_task("executor_2", vec![0, 1]);

        let stats = binding.measure("executor_2", &bound);
        assert_eq!(5_000, stats.local_bytes, "the broadcast is read locally");
        assert_eq!(
            0, stats.local_partitions,
            "executor_2 holds no bytes of either partition",
        );
        assert_eq!(2, stats.partitions);

        // The executor that wrote them holds both.
        let bound = bound_task("executor_1", vec![0, 1]);
        let stats = binding.measure("executor_1", &bound);
        assert_eq!(2, stats.local_partitions);
        assert_eq!(1_800, stats.local_bytes);
    }

    /// A bound task covering `partitions`, as `bind_one_where` would return.
    fn bound_task(executor_id: &str, partitions: Vec<usize>) -> BoundTask {
        let task = TaskDescription {
            session_id: "session".to_string(),
            key: TaskKey {
                job_id: "job_a".into(),
                stage_id: 2,
                task_id: 0,
            },
            stage_attempt_num: 0,
            task_attempt: 0,
            vcores_consumed: 1,
            global_input_partition_ids: partitions,
            plan: reader(vec![]),
            session_config: Arc::new(SessionConfig::default()),
        };
        (executor_id.to_string(), task)
    }

    fn stage(plan: Arc<dyn ExecutionPlan>, stage_attempt_num: usize) -> RunningStage {
        RunningStage::new(
            2,
            stage_attempt_num,
            plan,
            1,
            vec![],
            HashMap::new(),
            Arc::new(SessionConfig::default()),
        )
    }

    /// The executor holding the largest share of `partition`.
    fn best_holder(locality: &StageLocality, partition: usize) -> Option<&str> {
        locality
            .partitions
            .get(&partition)
            .and_then(|partition| partition.holders.first())
            .map(|(executor_id, _)| executor_id.as_str())
    }

    /// Binds `jobs` with `policy` over executors given as `(id, vcores)`.
    async fn bind(
        policy: &ShuffleAffinityPolicy,
        jobs: HashMap<JobId, JobInfoCache>,
        executors: &[(&str, u32)],
    ) -> Vec<BoundTask> {
        let mut budgets: Vec<AvailableVcores> = executors
            .iter()
            .map(|(executor_id, vcores)| AvailableVcores {
                executor_id: executor_id.to_string(),
                vcores: *vcores,
            })
            .collect();
        policy
            .bind_tasks(budgets.iter_mut().collect(), Arc::new(jobs))
            .await
            .unwrap()
    }

    fn partitions_covered(bound: &[BoundTask]) -> usize {
        bound
            .iter()
            .map(|(_, task)| task.global_input_partition_ids.len())
            .sum()
    }

    /// A job whose consumer stage collapses. `executor_2` wrote the first
    /// upstream partition but only a tenth of the bytes; `executor_1` the rest.
    async fn mock_collapse_job() -> Result<HashMap<JobId, JobInfoCache>> {
        let job_id: JobId = "job_collapse".into();
        let session_config = Arc::new(
            SessionConfig::new_with_ballista()
                .set_str(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, "0"),
        );
        let mut graph = test_collapse_plan_with_config(&job_id, session_config).await;
        graph.revive();

        let map_tasks = first_running_stage_tasks(&graph);
        for map_task in 0..map_tasks {
            let Some(task) = pop_map_task(&mut graph, "executor_0") else {
                break;
            };
            let (executor, bytes) = if map_task == 0 {
                (executor("executor_2"), 100)
            } else {
                (executor("executor_1"), 900)
            };
            let status =
                mock_completed_task_with_partition_bytes(task, &executor.id, |_| bytes);
            graph.update_task_status(&executor, vec![status], 1, 1)?;
        }

        let mut jobs = HashMap::new();
        jobs.insert(job_id, JobInfoCache::new(Box::new(graph)));
        Ok(jobs)
    }

    /// Even partitions are written large by `executor_1`, odd by `executor_2`.
    async fn mock_jobs(num_partitions: usize) -> Result<HashMap<JobId, JobInfoCache>> {
        mock_shuffle_jobs(&"job_a".into(), num_partitions, &|map_task, partition| {
            if partition % 2 == map_task % 2 {
                900
            } else {
                100
            }
        })
        .await
    }

    /// Registers `executor_id` with `vcores` free.
    async fn register(
        cluster_state: &InMemoryClusterState,
        executor_id: &str,
        vcores: u32,
    ) -> Result<()> {
        cluster_state
            .register_executor(
                executor(executor_id),
                ExecutorData {
                    executor_id: executor_id.to_string(),
                    total_vcores: vcores,
                    available_vcores: vcores,
                },
            )
            .await
    }

    /// Which executor each bound partition landed on.
    fn placements(bound: &[BoundTask]) -> HashMap<usize, String> {
        bound
            .iter()
            .flat_map(|(executor_id, task)| {
                task.global_input_partition_ids
                    .iter()
                    .map(move |p| (*p, executor_id.clone()))
            })
            .collect()
    }

    /// Binds one fresh cluster with `policy` and reports where each partition
    /// landed.
    async fn place_with(
        policy: TaskDistributionPolicy,
    ) -> Result<HashMap<usize, String>> {
        let cluster_state = InMemoryClusterState::default();
        register(&cluster_state, "executor_1", 4).await?;
        register(&cluster_state, "executor_2", 4).await?;

        let bound = cluster_state
            .bind_schedulable_tasks(policy, Arc::new(mock_jobs(4).await?), None)
            .await?;
        Ok(placements(&bound))
    }

    /// The push path reaches the policy through
    /// `ClusterState::bind_schedulable_tasks`, which must dispatch to the
    /// *configured* instance. Otherwise another policy binds, or the locality
    /// this one measures is lost. Bias on the same input packs onto one
    /// executor, so the placements tell the two arms apart.
    #[tokio::test]
    async fn the_push_path_dispatches_to_the_configured_policy() -> Result<()> {
        let policy = ShuffleAffinityPolicy::new();
        let affinity =
            place_with(TaskDistributionPolicy::Custom(Arc::new(policy.clone()))).await?;
        let bias = place_with(TaskDistributionPolicy::Bias).await?;

        assert_eq!(4, affinity.len(), "every partition should be bound");
        for (partition, executor_id) in &affinity {
            let expected = if partition % 2 == 0 {
                "executor_1"
            } else {
                "executor_2"
            };
            assert_eq!(
                expected, executor_id,
                "partition {partition} did not land on its home",
            );
        }
        assert_eq!(
            1,
            bias.values().collect::<HashSet<_>>().len(),
            "bias should pack every partition onto the first budget",
        );

        let stats = policy.stats();
        assert_eq!(4, stats.local_partitions);
        assert_eq!(3_600, stats.local_bytes, "4 partitions x 900 bytes local");
        assert_eq!(4_000, stats.total_bytes);

        Ok(())
    }

    /// A cluster with no room binds nothing and must not error.
    #[tokio::test]
    async fn the_push_path_with_no_vcores_binds_nothing() -> Result<()> {
        let cluster_state = InMemoryClusterState::default();
        register(&cluster_state, "executor_1", 0).await?;

        let jobs = mock_shuffle_jobs(&"job_a".into(), 4, &|_, _| 900).await?;
        let bound = cluster_state
            .bind_schedulable_tasks(
                TaskDistributionPolicy::Custom(Arc::new(ShuffleAffinityPolicy::new())),
                Arc::new(jobs),
                None,
            )
            .await?;

        assert!(bound.is_empty());

        Ok(())
    }
}
