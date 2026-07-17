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

use crate::cluster::memory::{InMemoryClusterState, InMemoryJobState};
use crate::config::{SchedulerConfig, TaskDistributionPolicy};
use crate::scheduler_server::SessionBuilder;
use crate::state::execution_graph::{
    ExecutionGraphBox, TaskDescription, create_task_info,
};
use crate::state::task_manager::JobInfoCache;
use ballista_core::error::Result;
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::serde::protobuf::{
    AvailableVcores, ExecutorHeartbeat, JobStatus, job_status,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata, TaskKey};
use ballista_core::utils::{default_config_producer, default_session_builder};
use ballista_core::{ConfigProducer, JobId, JobStatusSubscriber};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::Stream;
use log::{debug, info};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

/// Event broadcasting and subscription for cluster state changes.
pub mod event;
/// In-memory cluster state implementation.
pub mod memory;

/// Test utilities for cluster state testing.
#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
pub mod test_util;

/// Enum to configure the cluster state storage backend.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum ClusterStorage {
    /// In-memory storage (non-persistent).
    Memory,
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for ClusterStorage {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

/// Manages the distributed state of a Ballista cluster.
///
/// Combines cluster state (executor registration, heartbeats) with job state
/// (job submissions, execution graphs).
#[derive(Clone)]
pub struct BallistaCluster {
    /// State for tracking executors and their resources.
    cluster_state: Arc<dyn ClusterState>,
    /// State for tracking jobs and their execution progress.
    job_state: Arc<dyn JobState>,
}

impl BallistaCluster {
    /// Creates a new `BallistaCluster` with the given state backends.
    pub fn new(
        cluster_state: Arc<dyn ClusterState>,
        job_state: Arc<dyn JobState>,
    ) -> Self {
        Self {
            cluster_state,
            job_state,
        }
    }

    /// Creates a new `BallistaCluster` with in-memory state backends.
    pub fn new_memory(
        scheduler: impl Into<String>,
        session_builder: SessionBuilder,
        config_producer: ConfigProducer,
    ) -> Self {
        Self {
            cluster_state: Arc::new(InMemoryClusterState::default()),
            job_state: Arc::new(InMemoryJobState::new(
                scheduler,
                session_builder,
                config_producer,
            )),
        }
    }

    /// Creates a new `BallistaCluster` from scheduler configuration.
    pub async fn new_from_config(config: &SchedulerConfig) -> Result<Self> {
        let scheduler = config.scheduler_name();

        let session_builder = config
            .override_session_builder
            .clone()
            .unwrap_or_else(|| Arc::new(default_session_builder));

        let config_producer = config
            .override_config_producer
            .clone()
            .unwrap_or_else(|| Arc::new(default_config_producer));

        Ok(BallistaCluster::new_memory(
            scheduler,
            session_builder,
            config_producer,
        ))
    }

    /// Returns the cluster state backend.
    pub fn cluster_state(&self) -> Arc<dyn ClusterState> {
        self.cluster_state.clone()
    }

    /// Returns the job state backend.
    pub fn job_state(&self) -> Arc<dyn JobState> {
        self.job_state.clone()
    }
}

/// Stream of `ExecutorHeartbeat` messages.
///
/// This stream contains all heartbeats received by any schedulers sharing a `ClusterState`.
pub type ExecutorHeartbeatStream = Pin<Box<dyn Stream<Item = ExecutorHeartbeat> + Send>>;

/// A task bound to an executor for execution.
///
/// Tuple of (executor_id, task_description).
pub type BoundTask = (String, TaskDescription);

/// An executor slot representing available task capacity.
///
/// Tuple of (executor_id, slot_count).
pub type ExecutorSlot = (String, u32);

/// Trait for maintaining a globally consistent view of cluster resources.
///
/// Implementations track executor registration, heartbeats, and free vcores.
#[async_trait::async_trait]
pub trait ClusterState: Send + Sync + 'static {
    /// Initializes the cluster state backend.
    ///
    /// This is particularly important for backends with external storage.
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    /// Binds ready-to-run tasks from active jobs to executor vcores.
    ///
    /// If `executors` is provided, only bind on the specified executor IDs.
    async fn bind_schedulable_tasks(
        &self,
        distribution: TaskDistributionPolicy,
        active_jobs: Arc<HashMap<JobId, JobInfoCache>>,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<BoundTask>>;

    /// Releases reserved vcores when tasks finish or fail.
    ///
    /// This operation is atomic: either all vcores are released or none are.
    async fn unbind_tasks(&self, executor_slots: Vec<ExecutorSlot>) -> Result<()>;

    /// Registers a new executor in the cluster.
    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
    ) -> Result<()>;

    /// Saves executor metadata, overwriting any existing metadata for the executor ID.
    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()>;

    /// Returns executor metadata for the given executor ID.
    ///
    /// Returns an error if the executor does not exist.
    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata>;

    /// Returns a list of all registered executor metadata.
    async fn registered_executor_metadata(&self) -> Vec<ExecutorMetadata>;

    /// Saves an executor heartbeat.
    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()>;

    /// Removes an executor from the cluster.
    async fn remove_executor(&self, executor_id: &str) -> Result<()>;

    /// Returns the last seen heartbeat for all active executors.
    fn executor_heartbeats(&self) -> HashMap<String, ExecutorHeartbeat>;

    /// Returns the executor heartbeat for the given executor ID, or None if not found.
    fn get_executor_heartbeat(&self, executor_id: &str) -> Option<ExecutorHeartbeat>;

    /// Returns a stream of cluster state events.
    ///
    /// Events are published whenever the cluster state changes (e.g., executor registration/removal).
    async fn cluster_state_events(&self) -> Result<ClusterStateEventStream>;
}

/// Events related to the state of jobs. Implementations may or may not support all event types.
#[derive(Debug, Clone, PartialEq)]
pub enum JobStateEvent {
    /// Event when a job status has been updated
    JobUpdated {
        /// Job ID of updated job
        job_id: JobId,
        /// New job status
        status: JobStatus,
    },
    /// Event when a scheduler acquires ownership of the job. This happens
    /// either when a scheduler submits a job (in which case ownership is implied)
    /// or when a scheduler acquires ownership of a running job release by a
    /// different scheduler
    JobAcquired {
        /// Job ID of the acquired job
        job_id: JobId,
        /// The scheduler which acquired ownership of the job
        owner: String,
    },
    /// Event when a scheduler releases ownership of a still active job
    JobReleased {
        /// Job ID of the released job
        job_id: JobId,
    },
    /// Event when a new session has been created.
    SessionAccessed {
        /// Session ID that was accessed.
        session_id: String,
    },
    /// Event when a session configuration has been removed.
    SessionRemoved {
        /// Session ID that was removed.
        session_id: String,
    },
}

/// Events related to the state of the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterStateEvent {
    /// An executor has been registered with the cluster.
    RegisteredExecutor {
        /// ID of the registered executor.
        executor_id: String,
    },
    /// An executor has been removed from the cluster.
    RemovedExecutor {
        /// ID of the removed executor.
        executor_id: String,
    },
}

/// Stream of cluster state events.
pub type ClusterStateEventStream = Pin<Box<dyn Stream<Item = ClusterStateEvent> + Send>>;

/// Stream of job state events.
///
/// This stream contains all events received by schedulers sharing a `ClusterState`.
pub type JobStateEventStream = Pin<Box<dyn Stream<Item = JobStateEvent> + Send>>;

/// Trait for persisting state related to executing jobs.
///
/// Implementations handle job lifecycle, execution graphs, and session management.
#[async_trait::async_trait]
pub trait JobState: Send + Sync {
    /// Accepts a job into the scheduler's queue.
    ///
    /// Called when a job is received but before it is planned.
    fn accept_job(&self, job_id: &JobId, job_name: &str, queued_at: u64) -> Result<()>;

    /// Returns the number of queued jobs waiting to be scheduled.
    fn pending_job_number(&self) -> usize;

    /// Submits a new job to the job state.
    ///
    /// The submitter is assumed to own the job.
    async fn submit_job(
        &self,
        job_id: JobId,
        graph: &ExecutionGraphBox,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()>;

    /// Returns the set of all active job IDs.
    async fn get_jobs(&self) -> Result<HashSet<JobId>>;

    /// Returns the set of all job IDs including running, queued, and completed jobs.
    async fn get_all_jobs(&self) -> Result<HashSet<JobId>>;

    /// Returns the status of the specified job.
    async fn get_job_status(&self, job_id: &JobId) -> Result<Option<JobStatus>>;

    /// Returns the execution graph for a job.
    ///
    /// The job may not belong to the caller, and the graph may be updated
    /// by another scheduler after this call returns.
    async fn get_execution_graph(
        &self,
        job_id: &JobId,
    ) -> Result<Option<ExecutionGraphBox>>;

    /// Persists the current state of an owned job.
    ///
    /// Implementations are responsible for applying any backend-specific retry policy.
    /// Callers must not assume that this operation is safe to retry.
    ///
    /// Returns an error if the job is not owned by the caller.
    async fn save_job(&self, job_id: &JobId, graph: &ExecutionGraphBox) -> Result<()>;

    /// Marks an unscheduled job as failed.
    ///
    /// Called when a job fails during planning before an execution graph is created.
    async fn fail_unscheduled_job(&self, job_id: &JobId, reason: String) -> Result<()>;

    /// Deletes a job from the state.
    async fn remove_job(&self, job_id: &JobId) -> Result<()>;

    /// Attempts to acquire ownership of a job.
    ///
    /// Returns the execution graph if the job is still running and successfully acquired,
    /// otherwise returns None.
    async fn try_acquire_job(&self, job_id: &JobId) -> Result<Option<ExecutionGraphBox>>;

    /// Returns a stream of job state events.
    async fn job_state_events(&self) -> Result<JobStateEventStream>;

    /// Creates a new session or updates an existing one.
    async fn create_or_update_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>>;

    /// Removes a session from the state.
    async fn remove_session(&self, session_id: &str) -> Result<()>;

    /// Produces a session configuration for new sessions.
    fn produce_config(&self) -> SessionConfig;
}

/// Whether this stage's plan contains a collapse — any operator whose
/// `output_partitioning().partition_count() == 1` (`CoalescePartitionsExec`,
/// `SortPreservingMergeExec`, `AggregateExec(Final, gby=[])`,
/// `SortExec(preserve_partitioning=false)`, …). Downstream of a collapse
/// expects the *combined* input; splitting across tasks would give each
/// task's local collapse only a slice, producing partial results.
///
/// Walks past *any* single-child operator: today's writers that bake
/// partitioning into the shuffle write itself (`SortShuffleWriterExec::Hash`),
/// and future partition operators that separate partitioning from writing
/// (e.g. `UnorderedRangeRepartitionExec`). Stops at leaves, multi-child
/// operators (fan-in / joins), and stage boundaries.
///
/// The stage-boundary stop is currently a `ShuffleReaderExec` downcast — the
/// only kind of stage-boundary leaf that appears in a resolved stage plan.
/// The *general* rule is "stop at any stage boundary"; if new stage-boundary
/// operators appear, add them here (or, better, get `ExecutionPlan` upstream
/// to expose an `is_stage_boundary()` property so we don't keep
/// enumerating).
fn stage_has_input_collapse(plan_root: &Arc<dyn ExecutionPlan>) -> bool {
    fn walk(node: &Arc<dyn ExecutionPlan>) -> bool {
        if node.downcast_ref::<ShuffleReaderExec>().is_some() {
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
    let result = match plan_root.children().as_slice() {
        [child] => walk(child),
        _ => false,
    };
    info!(
        "DIAG stage_has_input_collapse: root={} root_partitions={} → {result}",
        plan_root.name(),
        plan_root
            .properties()
            .output_partitioning()
            .partition_count(),
    );
    result
}

/// Bind a task to an executor: draw a partition slice, append the resulting
/// `TaskInfo` to the stage, and produce a `TaskDescription` for dispatch.
///
/// Vcore accounting under DataFusion's volcano/pull model — the plan is
/// driven by one tokio task per root output partition, so a task consumes
/// vcores equal to the number of threads it will actually keep busy:
///
/// - **Non-collapse stage**: the root's output partitioning matches the
///   number of input partitions bundled into this task, so consumption =
///   `slice.len()`. Leftover budget stays available for another bind on
///   the same executor in the same scheduling round.
///
/// - **Collapse stage** (see [`stage_has_input_collapse`]): the plan's
///   root has a single output partition, so only 1 thread is ever active
///   driving the whole pipeline — reserve 1 vcore regardless of how many
///   input partitions are packed into the task, and let other stages'
///   tasks run in parallel on the remaining vcores. Correctness still
///   requires packing the entire pending queue into one bind (a split
///   collapse would produce partial results downstream can't merge).
fn bind_one(
    running_stage: &mut crate::state::execution_stage::RunningStage,
    session_id: &str,
    job_id: &JobId,
    budget: &mut AvailableVcores,
) -> Option<BoundTask> {
    let is_collapse = stage_has_input_collapse(&running_stage.plan);
    let max_partitions = if is_collapse {
        usize::MAX
    } else {
        budget.vcores as usize
    };
    let input_partition_ids = running_stage.pending.next_slice(max_partitions);
    if input_partition_ids.is_empty() {
        return None;
    }
    // Non-collapse: DataFusion's volcano/pull model drives one tokio task per
    // root output partition, so N input partitions bundled into a task need
    // N vcores of concurrent execution. Collapse: the plan's root has a
    // single output partition, so only 1 thread is ever active for the
    // whole pipeline no matter how many inputs the task packs — reserve
    // one vcore and leave the rest for other stages' tasks on this executor.
    let vcores_consumed = if is_collapse {
        1
    } else {
        input_partition_ids.len() as u32
    };
    info!(
        "DIAG bind_one: job={} stage={} exec={} vcores={} max={} slice_len={} consumed={} partitions={:?} collapse={}",
        job_id,
        running_stage.stage_id,
        budget.executor_id,
        budget.vcores,
        if max_partitions == usize::MAX {
            -1i64
        } else {
            max_partitions as i64
        },
        input_partition_ids.len(),
        vcores_consumed,
        input_partition_ids,
        is_collapse,
    );
    let executor_id = budget.executor_id.clone();
    // task_id is the append-order slot in `task_infos` — since we're
    // about to push, that's `task_infos.len()`. `(job_id, stage_id,
    // task_id)` is globally unique.
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
    let key = TaskKey {
        job_id: job_id.clone(),
        stage_id: running_stage.stage_id,
        task_id,
    };
    let task_desc = TaskDescription {
        session_id: session_id.to_string(),
        key,
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

pub(crate) async fn bind_task_bias(
    mut budgets: Vec<&mut AvailableVcores>,
    running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    if_skip: fn(Arc<dyn ExecutionPlan>) -> bool,
) -> Vec<BoundTask> {
    let mut schedulable_tasks: Vec<BoundTask> = vec![];

    if budgets.iter().all(|b| b.vcores == 0) {
        debug!("No executor vcores available for task binding");
        return schedulable_tasks;
    }

    // Bias: give each stage the biggest exec available. Sort descending
    // so bind_one keeps packing tasks onto the largest executor until its
    // vcore budget is drained.
    budgets.sort_by(|a, b| Ord::cmp(&b.vcores, &a.vcores));

    let mut idx = 0usize;
    for (job_id, job_info) in running_jobs.iter() {
        if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
            debug!("Job {job_id} is not in running status and will be skipped");
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        let mut black_list = vec![];
        while let Some(running_stage) = graph.fetch_running_stage(&black_list) {
            if if_skip(running_stage.plan.clone()) {
                debug!(
                    "Will skip stage {}/{} for bias task binding",
                    job_id, running_stage.stage_id
                );
                black_list.push(running_stage.stage_id);
                continue;
            }
            while idx < budgets.len() {
                while idx < budgets.len() && budgets[idx].vcores == 0 {
                    idx += 1;
                }
                if idx >= budgets.len() {
                    return schedulable_tasks;
                }
                match bind_one(running_stage, &session_id, job_id, &mut *budgets[idx]) {
                    Some(bound) => schedulable_tasks.push(bound),
                    None => break, // stage's pending is drained
                }
            }
        }
    }

    schedulable_tasks
}

pub(crate) async fn bind_task_round_robin(
    mut budgets: Vec<&mut AvailableVcores>,
    running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    if_skip: fn(Arc<dyn ExecutionPlan>) -> bool,
) -> Vec<BoundTask> {
    let mut schedulable_tasks: Vec<BoundTask> = vec![];

    if budgets.iter().all(|b| b.vcores == 0) {
        debug!("No executor vcores available for task binding");
        return schedulable_tasks;
    }

    // Round-robin across execs so multiple running stages each get one
    // exec per rotation. Order by vcores desc so the largest exec goes
    // first in the rotation.
    budgets.sort_by(|a, b| Ord::cmp(&b.vcores, &a.vcores));

    let mut idx = 0usize;
    for (job_id, job_info) in running_jobs.iter() {
        if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
            debug!("Job {job_id} is not in running status and will be skipped");
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        let mut black_list = vec![];
        while let Some(running_stage) = graph.fetch_running_stage(&black_list) {
            if if_skip(running_stage.plan.clone()) {
                debug!(
                    "Will skip stage {}/{} for round robin task binding",
                    job_id, running_stage.stage_id
                );
                black_list.push(running_stage.stage_id);
                continue;
            }
            loop {
                let mut scanned = 0usize;
                while budgets[idx].vcores == 0 {
                    idx = (idx + 1) % budgets.len();
                    scanned += 1;
                    if scanned >= budgets.len() {
                        return schedulable_tasks;
                    }
                }
                match bind_one(running_stage, &session_id, job_id, &mut *budgets[idx]) {
                    Some(bound) => {
                        schedulable_tasks.push(bound);
                        idx = (idx + 1) % budgets.len();
                    }
                    None => break, // stage's pending is drained
                }
            }
        }
    }

    schedulable_tasks
}

/// User provided task distribution policy
#[async_trait::async_trait]
pub trait DistributionPolicy: std::fmt::Debug + Send + Sync {
    // few open questions for later:
    //
    // - should scheduling policy type be a parameter
    //   as we see in the consistent hash, it does not work in
    //   pull based. Or we find another way to address this concern
    // - should we add `ClusterState` as method parameter
    //

    /// User provided custom task distribution policy
    ///
    /// # Parameters
    ///
    /// * `budgets` - per-executor free-vcore budgets (may be empty)
    /// * `running_jobs` - (JobId -> JobInfoCache) cache must contain only running jobs
    ///
    /// # Returns
    ///
    /// vector of task, executor bounding
    ///
    async fn bind_tasks(
        &self,
        mut budgets: Vec<&mut AvailableVcores>,
        running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    ) -> datafusion::error::Result<Vec<BoundTask>>;

    /// Name of [DistributionPolicy]
    fn name(&self) -> &str;
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use ballista_core::JobId;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::AvailableVcores;
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    };

    use crate::cluster::{BoundTask, bind_task_bias, bind_task_round_robin};
    use crate::state::execution_graph::{ExecutionGraph, StaticExecutionGraph};
    use crate::state::task_manager::JobInfoCache;
    use crate::test_utils::{
        mock_completed_task, revive_graph_and_complete_next_stage,
        test_aggregation_plan_with_job_id,
    };

    #[tokio::test]
    async fn test_bind_task_bias() -> Result<()> {
        let num_partition = 8usize;
        let active_jobs = mock_active_jobs(num_partition).await?;
        let mut budgets = mock_budgets();
        let budgets_ref: Vec<&mut AvailableVcores> = budgets.iter_mut().collect();
        let bound_tasks =
            bind_task_bias(budgets_ref, Arc::new(active_jobs), |_| false).await;
        // 9 total pending partitions (job_a: 2, job_b: 7) — verify all were
        // covered. Task count is emergent under multi-partition binding.
        assert_eq!(9, total_partitions_covered(&bound_tasks));

        let result = get_result(bound_tasks);

        // Multi-partition-task binding: each bind consumes slice.len() vcores,
        // so an executor with leftover vcores keeps taking work under the bias
        // policy (largest exec stays hot). Distribution depends on HashMap
        // iteration order across jobs.
        let mut expected = Vec::new();
        {
            // job_a iterated first: exec_3(7) takes job_a's 2 partitions
            // (exec_3 now has 5 vcores left), then bias keeps exec_3 hot so
            // it takes 5 of job_b's 7, and exec_2(5) takes the remaining 2.
            let mut expected0: HashMap<JobId, HashMap<String, usize>> = HashMap::new();
            let mut entry_a = HashMap::new();
            entry_a.insert("executor_3".to_string(), 2);
            let mut entry_b = HashMap::new();
            entry_b.insert("executor_3".to_string(), 5);
            entry_b.insert("executor_2".to_string(), 2);
            expected0.insert("job_a".into(), entry_a);
            expected0.insert("job_b".into(), entry_b);
            expected.push(expected0);
        }
        {
            // job_b iterated first: exec_3(7) takes job_b's full 7 partitions,
            // then exec_2(5) takes job_a's 2.
            let mut expected0: HashMap<JobId, HashMap<String, usize>> = HashMap::new();
            let mut entry_b = HashMap::new();
            entry_b.insert("executor_3".to_string(), 7);
            let mut entry_a = HashMap::new();
            entry_a.insert("executor_2".to_string(), 2);
            expected0.insert("job_a".into(), entry_a);
            expected0.insert("job_b".into(), entry_b);
            expected.push(expected0);
        }

        assert!(
            expected.contains(&result),
            "The result {result:?} is not as expected {expected:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_bind_task_round_robin() -> Result<()> {
        let num_partition = 8usize;
        let active_jobs = mock_active_jobs(num_partition).await?;
        let mut budgets = mock_budgets();
        let budgets_ref: Vec<&mut AvailableVcores> = budgets.iter_mut().collect();
        let bound_tasks =
            bind_task_round_robin(budgets_ref, Arc::new(active_jobs), |_| false).await;
        // 9 total pending partitions (job_a: 2, job_b: 7) — verify all were
        // covered. Task count is emergent under multi-partition binding.
        assert_eq!(9, total_partitions_covered(&bound_tasks));

        let result = get_result(bound_tasks);

        // Multi-partition-task binding: same shape as bias variant under this
        // mock — each executor consumes its full vcore budget in one task per
        // (job, stage) and round-robin's idx rotation gives the same
        // ordering. Two variants depending on job HashMap iteration order.
        let mut expected: Vec<HashMap<JobId, HashMap<String, usize>>> = Vec::new();
        {
            // job_a iterated first: exec_3(7) takes job_a's 2 partitions,
            // then exec_2(5) + exec_1(3) split job_b's 7 as 5/2.
            let mut expected0: HashMap<JobId, HashMap<String, usize>> = HashMap::new();
            let mut entry_a = HashMap::new();
            entry_a.insert("executor_3".to_string(), 2);
            let mut entry_b = HashMap::new();
            entry_b.insert("executor_2".to_string(), 5);
            entry_b.insert("executor_1".to_string(), 2);
            expected0.insert("job_a".into(), entry_a);
            expected0.insert("job_b".into(), entry_b);
            expected.push(expected0);
        }
        {
            // job_b iterated first: exec_3(7) takes job_b's full 7,
            // then exec_2(5) takes job_a's 2.
            let mut expected0: HashMap<JobId, HashMap<String, usize>> = HashMap::new();
            let mut entry_b = HashMap::new();
            entry_b.insert("executor_3".to_string(), 7);
            let mut entry_a = HashMap::new();
            entry_a.insert("executor_2".to_string(), 2);
            expected0.insert("job_a".into(), entry_a);
            expected0.insert("job_b".into(), entry_b);
            expected.push(expected0);
        }

        assert!(
            expected.contains(&result),
            "The result {result:?} is not as expected {expected:?}"
        );

        Ok(())
    }

    /// Sum partitions covered per (job, executor). Under multi-partition
    /// tasks one task covers a slice of partitions rather than exactly one,
    /// so counting bound tasks would understate the distribution. Summing
    /// `global_input_partition_ids.len()` preserves the "N partitions distributed as
    /// X/Y/Z across executors" invariant the assertions actually care about.
    fn get_result(bound_tasks: Vec<BoundTask>) -> HashMap<JobId, HashMap<String, usize>> {
        let mut result = HashMap::new();

        for bound_task in bound_tasks {
            let entry = result
                .entry(bound_task.1.key.job_id)
                .or_insert_with(HashMap::new);
            let n = entry.entry(bound_task.0).or_insert_with(|| 0);
            *n += bound_task.1.global_input_partition_ids.len();
        }

        result
    }

    /// Total partitions covered across every bound task — the multi-partition
    /// analogue of "how many tasks did we bind" for tests that used to assert
    /// on `bound_tasks.len()`.
    fn total_partitions_covered(bound_tasks: &[BoundTask]) -> usize {
        bound_tasks
            .iter()
            .map(|(_, task)| task.global_input_partition_ids.len())
            .sum()
    }

    async fn mock_active_jobs(
        num_partition: usize,
    ) -> Result<HashMap<JobId, JobInfoCache>> {
        let graph_a = mock_graph(&"job_a".into(), num_partition, 2).await?;

        let graph_b = mock_graph(&"job_b".into(), num_partition, 7).await?;

        let mut active_jobs = HashMap::new();
        active_jobs.insert(
            graph_a.job_id().to_owned(),
            JobInfoCache::new(Box::new(graph_a)),
        );
        active_jobs.insert(
            graph_b.job_id().to_owned(),
            JobInfoCache::new(Box::new(graph_b)),
        );

        Ok(active_jobs)
    }

    async fn mock_graph(
        job_id: &JobId,
        num_target_partitions: usize,
        num_pending_task: usize,
    ) -> Result<StaticExecutionGraph> {
        let mut graph =
            test_aggregation_plan_with_job_id(num_target_partitions, job_id).await;
        let executor = ExecutorMetadata {
            id: "executor_0".to_string(),
            host: "localhost".to_string(),
            port: 50051,
            grpc_port: 50052,
            specification: ExecutorSpecification::default().with_vcores(32),
            os_info: ExecutorOperatingSystemSpecification::default(),
        };

        // complete first stage
        revive_graph_and_complete_next_stage(&mut graph)?;

        for _ in 0..num_target_partitions - num_pending_task {
            if let Some(task) = graph.pop_next_task(&executor.id)? {
                let task_status = mock_completed_task(task, &executor.id);
                graph.update_task_status(&executor, vec![task_status], 1, 1)?;
            }
        }

        Ok(graph)
    }

    fn mock_budgets() -> Vec<AvailableVcores> {
        vec![
            AvailableVcores {
                executor_id: "executor_1".to_string(),
                vcores: 3,
            },
            AvailableVcores {
                executor_id: "executor_2".to_string(),
                vcores: 5,
            },
            AvailableVcores {
                executor_id: "executor_3".to_string(),
                vcores: 7,
            },
        ]
    }
}
