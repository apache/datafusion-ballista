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

//! The binding policy: an affinity pass, then a bias-style fallback.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ballista_core::JobId;
use ballista_core::serde::protobuf::{AvailableVcores, job_status};
use log::debug;

use ballista_scheduler::cluster::{BoundTask, DistributionPolicy};
use ballista_scheduler::state::execution_stage::{ExecutionStage, RunningStage};
use ballista_scheduler::state::task_manager::JobInfoCache;

use super::locality::StageLocality;
use super::lock;
use super::scheduler_internals::{bind_one_from, max_partitions_per_task};
use super::stats::{LocalityObserver, LocalityStats};

/// Places each task on the executor already holding most of its shuffle input.
///
/// Each partition is offered to its three largest holders with free vcores, and the
/// largest holdings are filled first; a collapse stage goes whole to its largest
/// holder. The remainder is bound bias-style, so no vcore idles. Only uneven splits
/// help: an even shuffle over `E` executors reads `1/E` locally under any placement.
#[derive(Clone, Default)]
pub struct ShuffleAffinityPolicy {
    /// Memoized scans by job, then stage. Shared by clones, but a policy shared across
    /// schedulers loses its memo because each prunes the other's jobs.
    cache: Arc<Mutex<HashMap<JobId, HashMap<usize, CachedLocality>>>>,
    /// Cumulative locality, shared by clones.
    stats: Arc<Mutex<LocalityStats>>,
    /// Set once by [`Self::attach_observer`], shared by clones.
    observer: Arc<OnceLock<Arc<dyn LocalityObserver>>>,
}

impl std::fmt::Debug for ShuffleAffinityPolicy {
    /// Hand-written: the observer is not `Debug`, and the cache prints as its size.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShuffleAffinityPolicy")
            .field("cached_stages", &self.cache_len())
            .field("stats", &self.stats())
            .field("observer_attached", &self.observer.get().is_some())
            .finish()
    }
}

/// A stage's scan, tagged with the attempt it came from.
struct CachedLocality {
    stage_attempt_num: usize,
    locality: Arc<StageLocality>,
}

impl ShuffleAffinityPolicy {
    /// Creates a policy with an empty cache and zeroed stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cumulative locality of the policy's placements so far; see [`LocalityStats`].
    pub fn stats(&self) -> LocalityStats {
        *lock(&self.stats)
    }

    /// Publishes every later round to `observer`. Set once: returns false and changes
    /// nothing if one is already attached.
    pub fn attach_observer(&self, observer: Arc<dyn LocalityObserver>) -> bool {
        self.observer.set(observer).is_ok()
    }

    /// The stage's scan, memoized per stage attempt. Every path that replaces a
    /// stage's plan bumps the attempt, so it is a sound cache key.
    pub(super) fn locality_for(
        &self,
        job_id: &JobId,
        running_stage: &RunningStage,
    ) -> Arc<StageLocality> {
        let mut cache = lock(&self.cache);
        if let Some(cached) = cache
            .get(job_id)
            .and_then(|stages| stages.get(&running_stage.stage_id))
            && cached.stage_attempt_num == running_stage.stage_attempt_num
        {
            return cached.locality.clone();
        }
        let locality = Arc::new(StageLocality::of(&running_stage.plan));
        cache.entry(job_id.clone()).or_default().insert(
            running_stage.stage_id,
            CachedLocality {
                stage_attempt_num: running_stage.stage_attempt_num,
                locality: locality.clone(),
            },
        );
        locality
    }

    /// Binds one stage: affinity first, then a fallback over the remaining vcores.
    /// Returns whether vcores ran out with work still pending.
    fn bind_stage(
        &self,
        running_stage: &mut RunningStage,
        session_id: &str,
        job_id: &JobId,
        budgets: &mut [&mut AvailableVcores],
        round: &mut Round,
    ) -> bool {
        let locality = self.locality_for(job_id, running_stage);
        let binding = Binding {
            session_id,
            job_id,
            locality: &locality,
            cap: max_partitions_per_task(running_stage),
        };

        // Pending partitions can only be read by draining, so drain once and requeue
        // the remainder at the end.
        let mut queued = running_stage.pending.next_slice(usize::MAX);

        // Affinity pass.
        if locality.whole_stage {
            // A collapse task reads the whole stage: bind it to the largest holder with room.
            for home in locality.ranked_executors() {
                let budget = budgets
                    .iter_mut()
                    .find(|budget| budget.executor_id == home && budget.vcores > 0);
                if let Some(budget) = budget {
                    binding.bind_all(running_stage, budget, &mut queued, round);
                    break;
                }
            }
        } else {
            // Scoped so the borrow of `budgets` ends before they are spent.
            let assignment = {
                let mut capacity: HashMap<&str, u32> = budgets
                    .iter()
                    .filter(|budget| budget.vcores > 0)
                    .map(|budget| (budget.executor_id.as_str(), budget.vcores))
                    .collect();
                locality.assign(queued.iter().copied(), &mut capacity)
            };

            // Group assigned partitions by executor, keeping queue order.
            let mut by_home: HashMap<&str, Vec<usize>> = HashMap::new();
            queued.retain(|&partition| match assignment.get(&partition) {
                Some(home) => {
                    by_home.entry(home).or_default().push(partition);
                    false
                }
                None => true,
            });

            for budget in budgets.iter_mut() {
                let Some(mut mine) = by_home.remove(budget.executor_id.as_str()) else {
                    continue;
                };
                binding.bind_all(running_stage, budget, &mut mine, round);
                // Assignment never exceeds an executor's free vcores, so all of it binds.
                debug_assert!(
                    mine.is_empty(),
                    "assigned partitions left unbound: {mine:?}"
                );
            }
        }

        // Fallback: anything unassigned or unplaced takes the remaining vcores on any
        // executor, since a stage without shuffle input has no assignments.
        for budget in budgets.iter_mut() {
            if queued.is_empty() {
                break;
            }
            binding.bind_all(running_stage, budget, &mut queued, round);
        }

        let exhausted = !queued.is_empty();
        running_stage.pending.reschedule(queued);
        exhausted
    }

    /// Folds a round's measurement into the cumulative counters and logs it.
    pub(super) fn record(&self, round: LocalityStats) {
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
             {} of {} input bytes local ({:.1}%); cumulative {:.1}% of {} bytes",
            round.tasks,
            round.partitions,
            round.local_partitions,
            round.local_bytes,
            round.total_bytes,
            round.local_byte_ratio() * 100.0,
            cumulative.local_byte_ratio() * 100.0,
            cumulative.total_bytes,
        );
    }

    /// Number of memoized stage scans currently held.
    pub(super) fn cache_len(&self) -> usize {
        lock(&self.cache).values().map(HashMap::len).sum()
    }

    /// Drops scans for jobs the caller no longer runs. A policy shared by two
    /// schedulers therefore evicts the other's scans, which are rescanned next bind.
    pub(super) fn prune_cache(&self, running_jobs: &HashMap<JobId, JobInfoCache>) {
        lock(&self.cache).retain(|job_id, _| running_jobs.contains_key(job_id));
    }

    /// Drops `job_id`'s scans for stages no longer running, so long jobs don't
    /// accumulate them.
    pub(super) fn prune_finished_stages(
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
        let mut round = Round::default();

        self.prune_cache(&running_jobs);

        if budgets.iter().all(|budget| budget.vcores == 0) {
            debug!("No executor vcores available for task binding");
            return Ok(vec![]);
        }

        // Largest budget first, as `Bias` does; ties on id keep rounds deterministic.
        budgets.sort_by(|a, b| {
            Ord::cmp(&b.vcores, &a.vcores)
                .then_with(|| Ord::cmp(&a.executor_id, &b.executor_id))
        });

        // Break rather than return, so the round is recorded on one path.
        'jobs: for (job_id, job_info) in running_jobs.iter() {
            if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
                debug!("Job {job_id} is not in running status and will be skipped");
                continue;
            }
            let mut graph = job_info.execution_graph.write().await;
            self.prune_finished_stages(job_id, graph.stages());
            let session_id = graph.session_id().to_string();
            // No stages skipped: the trait passes no `if_skip`, like the built-ins.
            while let Some(running_stage) = graph.fetch_running_stage(&[]) {
                let cluster_exhausted = self.bind_stage(
                    running_stage,
                    &session_id,
                    job_id,
                    &mut budgets,
                    &mut round,
                );
                if cluster_exhausted {
                    break 'jobs;
                }
            }
        }

        self.record(round.stats);
        Ok(round.tasks)
    }

    fn name(&self) -> &str {
        "shuffle-affinity"
    }
}

/// What one binding round has produced so far.
#[derive(Default)]
struct Round {
    tasks: Vec<BoundTask>,
    stats: LocalityStats,
}

/// One stage's binding context, shared by the affinity and fallback passes.
struct Binding<'a> {
    session_id: &'a str,
    job_id: &'a JobId,
    locality: &'a StageLocality,
    /// Most partitions a non-collapse task may take.
    cap: usize,
}

impl Binding<'_> {
    /// Binds as much of `partitions` onto one executor as its budget allows,
    /// leaving the rest in order.
    fn bind_all(
        &self,
        stage: &mut RunningStage,
        budget: &mut AvailableVcores,
        partitions: &mut Vec<usize>,
        round: &mut Round,
    ) {
        // Bound partitions are removed once at the end, not shifted out per task.
        let mut bound = 0;
        while budget.vcores > 0 && bound < partitions.len() {
            let remaining = partitions.len() - bound;
            let take = if self.locality.whole_stage {
                remaining
            } else {
                (budget.vcores as usize).min(self.cap).min(remaining)
            };
            let slice = partitions[bound..bound + take].to_vec();
            bound += take;
            let (executor_id, task) = bind_one_from(
                stage,
                self.session_id,
                self.job_id,
                budget,
                slice,
                self.locality.whole_stage,
            );
            round.stats += self
                .locality
                .measure(&executor_id, &task.global_input_partition_ids);
            round.tasks.push((executor_id, task));
        }
        partitions.drain(..bound);
    }
}
