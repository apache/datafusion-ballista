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

use crate::scheduler_server::event::QueryStageSchedulerEvent;

use crate::state::execution_graph::{
    ExecutionGraph, ExecutionStage, RunningTaskInfo, TaskDescription,
};
use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};

use ballista_core::error::BallistaError;
use ballista_core::error::Result;
use datafusion::config::{ConfigEntry, ConfigOptions};
use futures::future::try_join_all;

use crate::cluster::JobState;
use ballista_core::serde::protobuf::{
    self, JobStatus, KeyValuePair, TaskDefinition, TaskStatus,
};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::BallistaCodec;
use dashmap::DashMap;
use datafusion::physical_plan::ExecutionPlan;

use crossbeam_queue::SegQueue;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;

use datafusion::prelude::SessionContext;
use itertools::Itertools;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use tokio::sync::{watch, RwLock, RwLockWriteGuard};

use crate::scheduler_server::timestamp_millis;
use ballista_core::physical_optimizer::OptimizeTaskGroup;
use tracing::trace;

type ActiveJobCache = Arc<DashMap<String, JobInfoCache>>;

#[derive(Default)]
struct ActiveJobQueue {
    queue: SegQueue<String>,
    jobs: ActiveJobCache,
}

impl ActiveJobQueue {
    pub fn pop(&self) -> Option<ActiveJobRef> {
        loop {
            if let Some(job_id) = self.queue.pop() {
                if let Some(job_info) = self.jobs.get(&job_id) {
                    return Some(ActiveJobRef {
                        queue: &self.queue,
                        job: job_info.clone(),
                        job_id,
                    });
                } else {
                    continue;
                }
            } else {
                return None;
            }
        }
    }

    pub fn pending_tasks(&self) -> usize {
        let mut count = 0;
        for job in self.jobs.iter() {
            count += job.pending_tasks.load(Ordering::Acquire);
        }

        count
    }

    pub fn push(&self, job_id: String, graph: ExecutionGraph) {
        self.jobs.insert(job_id.clone(), JobInfoCache::new(graph));
        self.queue.push(job_id);
    }

    pub fn jobs(&self) -> &ActiveJobCache {
        &self.jobs
    }

    pub fn get_job(&self, job_id: &str) -> Option<JobInfoCache> {
        self.jobs.get(job_id).map(|info| info.clone())
    }

    pub fn remove(&self, job_id: &str) -> Option<JobInfoCache> {
        self.jobs.remove(job_id).map(|(_, job)| job)
    }

    pub fn size(&self) -> usize {
        self.jobs.len()
    }
}

struct ActiveJobRef<'a> {
    queue: &'a SegQueue<String>,
    job: JobInfoCache,
    job_id: String,
}

impl<'a> Deref for ActiveJobRef<'a> {
    type Target = JobInfoCache;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

impl<'a> Drop for ActiveJobRef<'a> {
    fn drop(&mut self) {
        self.queue.push(std::mem::take(&mut self.job_id));
    }
}

// TODO move to configuration file
/// Default max failure attempts for task level retry
pub const TASK_MAX_FAILURES: usize = 4;
/// Default max failure attempts for stage level retry
pub const STAGE_MAX_FAILURES: usize = 4;

#[async_trait::async_trait]
pub trait TaskLauncher<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>:
    Send + Sync + 'static
{
    fn prepare_task_definition(
        &self,
        ctx: Arc<SessionContext>,
        task: TaskDescription,
    ) -> Result<TaskDefinition>;

    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<TaskDescription>,
        executor_manager: &ExecutorManager,
    ) -> Result<()>;
}

struct DefaultTaskLauncher<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    scheduler_id: String,
    state: Arc<dyn JobState>,
    codec: BallistaCodec<T, U>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> DefaultTaskLauncher<T, U> {
    pub fn new(
        scheduler_id: String,
        state: Arc<dyn JobState>,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            scheduler_id,
            state,
            codec,
        }
    }
}

#[async_trait::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskLauncher<T, U>
    for DefaultTaskLauncher<T, U>
{
    fn prepare_task_definition(
        &self,
        ctx: Arc<SessionContext>,
        task: TaskDescription,
    ) -> Result<TaskDefinition> {
        let job_id = task.partitions.job_id.clone();
        let stage_id = task.partitions.stage_id;

        debug!(job_id, stage_id, "Preparing task definition for {:?}", task);

        let props = ctx
            .state()
            .config_options()
            .entries()
            .into_iter()
            .filter_map(|ConfigEntry { key, value, .. }| {
                value.map(|value| KeyValuePair { key, value })
            })
            .collect();

        let optimizer = OptimizeTaskGroup::new(task.partitions.partitions.clone());

        let group_plan =
            optimizer.optimize(task.plan.clone(), &ConfigOptions::default())?;

        let mut plan: Vec<u8> = vec![];
        let plan_proto =
            U::try_from_physical_plan(group_plan, self.codec.physical_extension_codec())?;
        plan_proto.try_encode(&mut plan)?;

        let output_partitioning =
            hash_partitioning_to_proto(task.output_partitioning.as_ref())?;

        Ok(TaskDefinition {
            task_id: task.task_id as u32,
            job_id,
            stage_id: stage_id as u32,
            stage_attempt_num: task.stage_attempt_num as u32,
            partitions: task
                .partitions
                .partitions
                .iter()
                .map(|p| *p as u32)
                .collect(),
            plan,
            output_partitioning,
            session_id: task.session_id,
            launch_time: timestamp_millis(),
            props,
        })
    }

    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<TaskDescription>,
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        if log::max_level() >= log::Level::Info {
            let tasks_ids: Vec<String> = tasks
                .iter()
                .map(|task| {
                    format!(
                        "{}/{}/{:?}",
                        task.partitions.job_id,
                        task.partitions.stage_id,
                        task.partitions.partitions
                    )
                })
                .collect();
            info!(
                "Launching tasks on executor {:?} for {:?}",
                executor.id, tasks_ids
            );
        }

        let tasks = tasks.into_iter().map(|task_def| async {
            let ctx = self.state.get_session(&task_def.session_id).await?;
            self.prepare_task_definition(ctx, task_def)
        });

        let tasks: Result<Vec<TaskDefinition>> = try_join_all(tasks).await;

        let mut client = executor_manager.get_client(&executor.id).await?;
        client
            .launch_task(protobuf::LaunchTaskParams {
                tasks: tasks?,
                scheduler_id: self.scheduler_id.clone(),
            })
            .await
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Failed to connect to executor {}: {:?}",
                    executor.id, e
                ))
            })?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct TaskManager<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<dyn JobState>,
    scheduler_id: String,
    // Cache for active jobs curated by this scheduler
    active_job_queue: Arc<ActiveJobQueue>,
    launcher: Arc<dyn TaskLauncher<T, U>>,
    drained: Arc<watch::Sender<()>>,
    check_drained: watch::Receiver<()>,
}

struct ExecutionGraphWriteGuard<'a> {
    inner: RwLockWriteGuard<'a, ExecutionGraph>,
    pending_tasks: Arc<AtomicUsize>,
}

impl<'a> Deref for ExecutionGraphWriteGuard<'a> {
    type Target = ExecutionGraph;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<'a> DerefMut for ExecutionGraphWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

impl<'a> Drop for ExecutionGraphWriteGuard<'a> {
    fn drop(&mut self) {
        let tasks = self.inner.available_tasks();
        self.pending_tasks.store(tasks, Ordering::Release);
    }
}

#[derive(Clone)]
struct JobInfoCache {
    // Cache for active execution graphs curated by this scheduler
    execution_graph: Arc<RwLock<ExecutionGraph>>,
    // Cache for encoded execution stage plan to avoid duplicated encoding for multiple tasks
    #[allow(dead_code)]
    encoded_stage_plans: HashMap<usize, Vec<u8>>,
    // Number of current pending tasks for this job
    pending_tasks: Arc<AtomicUsize>,
}

impl JobInfoCache {
    fn new(graph: ExecutionGraph) -> Self {
        let pending_tasks = Arc::new(AtomicUsize::new(graph.available_tasks()));

        Self {
            execution_graph: Arc::new(RwLock::new(graph)),
            encoded_stage_plans: HashMap::new(),
            pending_tasks,
        }
    }

    pub async fn graph_mut(&self) -> ExecutionGraphWriteGuard {
        let guard = self.execution_graph.write().await;

        ExecutionGraphWriteGuard {
            inner: guard,
            pending_tasks: self.pending_tasks.clone(),
        }
    }
}

#[derive(Clone)]
pub struct UpdatedStages {
    pub resolved_stages: HashSet<usize>,
    pub successful_stages: HashSet<usize>,
    pub failed_stages: HashMap<usize, String>,
    pub rollback_running_stages: HashMap<usize, HashSet<String>>,
    pub resubmit_successful_stages: HashSet<usize>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskManager<T, U> {
    pub fn new(
        state: Arc<dyn JobState>,
        codec: BallistaCodec<T, U>,
        scheduler_id: String,
    ) -> Self {
        let launcher =
            DefaultTaskLauncher::new(scheduler_id.clone(), state.clone(), codec);

        Self::with_launcher(state, scheduler_id, Arc::new(launcher))
    }

    #[allow(dead_code)]
    pub(crate) fn with_launcher(
        state: Arc<dyn JobState>,
        scheduler_id: String,
        launcher: Arc<dyn TaskLauncher<T, U>>,
    ) -> Self {
        let (drained, check_drained) = watch::channel(());

        Self {
            state,
            scheduler_id,
            active_job_queue: Arc::new(ActiveJobQueue::default()),
            launcher,
            drained: Arc::new(drained),
            check_drained,
        }
    }

    /// Return the number of current pending tasks for active jobs
    /// on this scheduler
    pub fn get_pending_task_count(&self) -> usize {
        self.active_job_queue.pending_tasks()
    }

    /// Return the count of current active jobs on this scheduler instance.
    pub fn get_active_job_count(&self) -> usize {
        self.active_job_queue.size()
    }

    /// Enqueue a job for scheduling
    pub async fn queue_job(
        &self,
        job_id: &str,
        job_name: &str,
        queued_at: u64,
    ) -> Result<()> {
        self.state.accept_job(job_id, job_name, queued_at).await
    }

    /// Generate an ExecutionGraph for the job and save it to the persistent state.
    /// By default, this job will be curated by the scheduler which receives it.
    /// Then we will also save it to the active execution graph
    pub async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
    ) -> Result<()> {
        let mut graph = ExecutionGraph::new(
            &self.scheduler_id,
            job_id,
            job_name,
            session_id,
            plan,
            queued_at,
        )?;
        info!("Submitting execution graph: {:?}", graph);

        self.state.submit_job(job_id.to_string(), &graph).await?;

        graph.revive();
        self.active_job_queue.push(job_id.to_owned(), graph);

        Ok(())
    }

    /// Get a list of active job ids
    pub async fn get_jobs(&self) -> Result<Vec<JobOverview>> {
        let job_ids = self.state.get_jobs().await?;

        let mut jobs = vec![];
        for job_id in &job_ids {
            if let Some(cached) = self.get_active_execution_graph(job_id) {
                let graph = cached.read().await;
                jobs.push(graph.deref().into());
            } else {
                let graph = self.state
                    .get_execution_graph(job_id)
                    .await?
                    .ok_or_else(|| BallistaError::Internal(format!("Error getting job overview, no execution graph found for job {job_id}")))?;
                jobs.push((&graph).into());
            }
        }
        Ok(jobs)
    }

    /// Get the status of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs, and then in Failed jobs
    pub async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some(graph) = self.get_active_execution_graph(job_id) {
            let guard = graph.read().await;

            Ok(Some(guard.status()))
        } else {
            self.state.get_job_status(job_id).await
        }
    }

    /// Get the execution graph of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs.
    pub(crate) async fn get_job_execution_graph(
        &self,
        job_id: &str,
    ) -> Result<Option<Arc<ExecutionGraph>>> {
        if let Some(cached) = self.get_active_execution_graph(job_id) {
            let guard = cached.read().await;

            Ok(Some(Arc::new(guard.deref().clone())))
        } else {
            let graph = self.state.get_execution_graph(job_id).await?;

            Ok(graph.map(Arc::new))
        }
    }

    /// Update given task statuses in the respective job and return a tuple containing:
    /// 1. A list of QueryStageSchedulerEvent to publish.
    /// 2. A list of reservations that can now be offered.
    pub(crate) async fn update_task_statuses(
        &self,
        executor: &ExecutorMetadata,
        task_status: Vec<TaskStatus>,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let mut job_updates: HashMap<String, Vec<TaskStatus>> = HashMap::new();
        for status in task_status {
            trace!("Task Update\n{:?}", status);
            let job_id = status.job_id.clone();
            let job_task_statuses = job_updates.entry(job_id).or_insert_with(Vec::new);
            job_task_statuses.push(status);
        }

        let mut events: Vec<QueryStageSchedulerEvent> = vec![];
        for (job_id, statuses) in job_updates {
            let num_tasks = statuses.len();
            debug!("Updating {} tasks in job {}", num_tasks, job_id);

            let job_events = if let Some(job) = self.active_job_queue.get_job(&job_id) {
                let mut graph = job.graph_mut().await;

                graph.update_task_status(
                    executor,
                    statuses,
                    TASK_MAX_FAILURES,
                    STAGE_MAX_FAILURES,
                )?
            } else {
                // TODO Deal with curator changed case
                error!("Fail to find job {} in the active cache and it may not be curated by this scheduler", job_id);
                vec![]
            };

            for event in job_events {
                events.push(event);
            }
        }

        Ok(events)
    }

    /// Take a list of executor reservations and fill them with tasks that are ready
    /// to be scheduled.
    ///
    /// Here we use the following  algorithm:
    ///
    /// 1. For each free reservation, try to assign a task from one of the active jobs
    /// 2. If we cannot find a task in all active jobs, then add the reservation to the list of unassigned reservations
    ///
    /// Finally, we return:
    /// 1. A list of assignments which is a (Executor ID, Task) tuple
    /// 2. A list of unassigned reservations which we could not find tasks for
    /// 3. The number of pending tasks across active jobs
    pub async fn fill_reservations(
        &self,
        reservations: &[ExecutorReservation],
    ) -> Result<(
        Vec<(String, TaskDescription)>,
        Vec<ExecutorReservation>,
        usize,
    )> {
        let num_reservations = reservations.len();

        let mut free_reservations: HashMap<&String, Vec<&ExecutorReservation>> =
            reservations
                .iter()
                .group_by(|res| &res.executor_id)
                .into_iter()
                .map(|(executor_id, group)| (executor_id, group.collect()))
                .collect();

        let mut assignments: Vec<(String, TaskDescription)> = vec![];
        let mut pending_tasks = 0usize;
        let mut assign_tasks = 0usize;

        for _ in 0..self.get_active_job_count() {
            if let Some(job_info) = self.active_job_queue.pop() {
                let mut graph = job_info.graph_mut().await;
                for (exec_id, slots) in free_reservations.iter_mut() {
                    if slots.is_empty() {
                        continue;
                    }

                    if let Some(task) = graph.pop_next_task(exec_id, slots.len())? {
                        assign_tasks += task.concurrency();
                        slots.truncate(slots.len() - task.concurrency());
                        assignments.push(((*exec_id).clone(), task));
                    } else {
                        break;
                    }
                }

                if assign_tasks >= num_reservations {
                    pending_tasks += graph.available_tasks();
                    break;
                }
            } else {
                break;
            }
        }

        let mut unassigned = vec![];
        for (_, slots) in free_reservations {
            unassigned.extend(slots.into_iter().cloned());
        }

        Ok((assignments, unassigned, pending_tasks))
    }

    /// Mark a job to success. This will create a key under the CompletedJobs keyspace
    /// and remove the job from ActiveJobs
    pub(crate) async fn succeed_job(&self, job_id: &str) -> Result<()> {
        debug!("Moving job {} from Active to Success", job_id);

        if let Some(graph) = self.remove_active_execution_graph(job_id) {
            let graph = graph.read().await.clone();
            if graph.is_successful() {
                self.state.save_job(job_id, &graph).await?;
            } else {
                error!("Job {} has not finished and cannot be completed", job_id);
                return Ok(());
            }
        } else {
            warn!("Fail to find job {} in the cache", job_id);
        }

        Ok(())
    }

    /// Cancel the job and return a Vec of running tasks need to cancel
    pub(crate) async fn cancel_job(
        &self,
        job_id: &str,
    ) -> Result<(Vec<RunningTaskInfo>, usize)> {
        self.abort_job(job_id, "Cancelled".to_owned()).await
    }

    /// Abort the job and return a Vec of running tasks need to cancel
    pub(crate) async fn abort_job(
        &self,
        job_id: &str,
        failure_reason: String,
    ) -> Result<(Vec<RunningTaskInfo>, usize)> {
        let (tasks_to_cancel, pending_tasks) = if let Some(job) =
            self.active_job_queue.get_job(job_id)
        {
            let mut guard = job.graph_mut().await;

            let pending_tasks = guard.available_tasks();
            let running_tasks = guard.running_tasks();

            info!(
                "Cancelling {} running tasks for job {}",
                running_tasks.len(),
                job_id
            );

            guard.fail_job(failure_reason);

            self.state.save_job(job_id, &guard).await?;

            // After state is saved, remove job from active cache
            let _ = self.remove_active_execution_graph(job_id);

            (running_tasks, pending_tasks)
        } else {
            // TODO listen the job state update event and fix task cancelling
            warn!("Fail to find job {} in the cache, unable to cancel tasks for job, fail the job state only.", job_id);
            (vec![], 0)
        };

        Ok((tasks_to_cancel, pending_tasks))
    }

    /// Mark a unscheduled job as failed. This will create a key under the FailedJobs keyspace
    /// and remove the job from ActiveJobs or QueuedJobs
    pub async fn fail_unscheduled_job(
        &self,
        job_id: &str,
        failure_reason: String,
    ) -> Result<()> {
        self.state
            .fail_unscheduled_job(job_id, failure_reason)
            .await
    }

    pub async fn update_job(&self, job_id: &str) -> Result<usize> {
        debug!("Update active job {job_id}");
        if let Some(job) = self.active_job_queue.get_job(job_id) {
            let mut graph = job.graph_mut().await;

            let curr_available_tasks = graph.available_tasks();

            graph.revive();

            debug!("Saving job with status {:?}", graph.status());

            self.state.save_job(job_id, &graph).await?;

            let new_tasks = graph.available_tasks() - curr_available_tasks;

            Ok(new_tasks)
        } else {
            warn!("Fail to find job {} in the cache", job_id);

            Ok(0)
        }
    }

    /// return a Vec of running tasks need to cancel
    pub async fn executor_lost(&self, executor_id: &str) -> Result<Vec<RunningTaskInfo>> {
        // Collect all the running task need to cancel when there are running stages rolled back.
        let mut running_tasks_to_cancel: Vec<RunningTaskInfo> = vec![];
        // Collect graphs we update so we can update them in storage
        let updated_graphs: DashMap<String, ExecutionGraph> = DashMap::new();
        {
            for pairs in self.active_job_queue.jobs().iter() {
                let (job_id, job_info) = pairs.pair();
                let mut graph = job_info.graph_mut().await;
                let reset = graph.reset_stages_on_lost_executor(executor_id)?;
                if !reset.0.is_empty() {
                    updated_graphs.insert(job_id.to_owned(), graph.clone());
                    running_tasks_to_cancel.extend(reset.1);
                }
            }
        }

        Ok(running_tasks_to_cancel)
    }

    /// Retrieve the number of available tasks for the given job. The value returned
    /// is strictly a point-in-time snapshot
    pub async fn get_available_task_count(&self, job_id: &str) -> Result<usize> {
        if let Some(graph) = self.get_active_execution_graph(job_id) {
            let available_tasks = graph.read().await.available_tasks();
            Ok(available_tasks)
        } else {
            warn!("Fail to find job {} in the cache", job_id);
            Ok(0)
        }
    }

    pub async fn prepare_task_definition(
        &self,
        task: TaskDescription,
    ) -> Result<TaskDefinition> {
        let ctx = self.state.get_session(&task.session_id).await?;
        self.launcher.prepare_task_definition(ctx, task)
    }

    /// Launch the given tasks on the specified executor
    pub(crate) async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<TaskDescription>,
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        self.launcher
            .launch_tasks(executor, tasks, executor_manager)
            .await
    }

    /// Get the `ExecutionGraph` for the given job ID from cache
    pub(crate) fn get_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraph>>> {
        self.active_job_queue
            .get_job(job_id)
            .map(|cached| cached.execution_graph)
    }

    /// Remove the `ExecutionGraph` for the given job ID from cache
    pub(crate) fn remove_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraph>>> {
        let removed = self
            .active_job_queue
            .remove(job_id)
            .map(|value| value.execution_graph);

        if self.get_active_job_count() == 0 {
            self.drained.send_replace(());
        }

        removed
    }

    /// Generate a new random Job ID
    pub fn generate_job_id(&self) -> String {
        let mut rng = thread_rng();
        std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(7)
            .collect()
    }

    /// Clean up a failed job in FailedJobs Keyspace by delayed clean_up_interval seconds
    pub(crate) fn clean_up_job_delayed(&self, job_id: String, clean_up_interval: u64) {
        if clean_up_interval == 0 {
            info!("The interval is 0 and the clean up for the failed job state {} will not triggered", job_id);
            return;
        }

        let state = self.state.clone();
        tokio::spawn(async move {
            let job_id = job_id;
            tokio::time::sleep(Duration::from_secs(clean_up_interval)).await;
            if let Err(err) = state.remove_job(&job_id).await {
                error!("Failed to delete job {job_id}: {err:?}");
            }
        });
    }

    pub async fn wait_drained(&self) {
        let mut check_drained = self.check_drained.clone();

        loop {
            if self.get_active_job_count() == 0 {
                break;
            }

            if check_drained.changed().await.is_err() {
                break;
            };
        }
    }
}

pub struct JobOverview {
    pub job_id: String,
    pub job_name: String,
    pub status: JobStatus,
    pub start_time: u64,
    pub end_time: u64,
    pub num_stages: usize,
    pub completed_stages: usize,
}

impl From<&ExecutionGraph> for JobOverview {
    fn from(value: &ExecutionGraph) -> Self {
        let mut completed_stages = 0;
        for stage in value.stages().values() {
            if let ExecutionStage::Successful(_) = stage {
                completed_stages += 1;
            }
        }

        Self {
            job_id: value.job_id().to_string(),
            job_name: value.job_name().to_string(),
            status: value.status(),
            start_time: value.start_time(),
            end_time: value.end_time(),
            num_stages: value.stage_count(),
            completed_stages,
        }
    }
}
