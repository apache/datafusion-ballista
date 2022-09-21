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

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{
    accept, ExecutionPlan, ExecutionPlanVisitor, Partitioning,
};
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use log::{error, info, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{ShuffleWriterExec, UnresolvedShuffleExec};
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::{
    self, execution_graph_stage::StageType, FailedTask, JobStatus, QueuedJob, ResultLost,
    SuccessfulJob, TaskStatus,
};
use ballista_core::serde::protobuf::{job_status, FailedJob, ShuffleWritePartition};
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, PartitionId, PartitionLocation, PartitionStats,
};
use ballista_core::serde::{AsExecutionPlan, BallistaCodec};

use crate::display::print_stage_metrics;
use crate::planner::DistributedPlanner;
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::execution_graph::execution_stage::{
    ExecutionStage, FailedStage, ResolvedStage, StageOutput, SuccessfulStage, TaskInfo,
    UnresolvedStage,
};
use crate::state::task_manager::UpdatedStages;

mod execution_stage;

/// Represents the DAG for a distributed query plan.
///
/// A distributed query plan consists of a set of stages which must be executed sequentially.
///
/// Each stage consists of a set of partitions which can be executed in parallel, where each partition
/// represents a `Task`, which is the basic unit of scheduling in Ballista.
///
/// As an example, consider a SQL query which performs a simple aggregation:
///
/// `SELECT id, SUM(gmv) FROM some_table GROUP BY id`
///
/// This will produce a DataFusion execution plan that looks something like
///
///
///   CoalesceBatchesExec: target_batch_size=4096
///     RepartitionExec: partitioning=Hash([Column { name: "id", index: 0 }], 4)
///       AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[SUM(some_table.gmv)]
///         TableScan: some_table
///
/// The Ballista `DistributedPlanner` will turn this into a distributed plan by creating a shuffle
/// boundary (called a "Stage") whenever the underlying plan needs to perform a repartition.
/// In this case we end up with a distributed plan with two stages:
///
///
/// ExecutionGraph[job_id=job, session_id=session, available_tasks=1, complete=false]
/// =========UnResolvedStage[id=2, children=1]=========
/// Inputs{1: StageOutput { partition_locations: {}, complete: false }}
/// ShuffleWriterExec: None
///   AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     CoalesceBatchesExec: target_batch_size=4096
///       UnresolvedShuffleExec
/// =========ResolvedStage[id=1, partitions=1]=========
/// ShuffleWriterExec: Some(Hash([Column { name: "id", index: 0 }], 4))
///   AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     TableScan: some_table
///
///
/// The DAG structure of this `ExecutionGraph` is encoded in the stages. Each stage's `input` field
/// will indicate which stages it depends on, and each stage's `output_links` will indicate which
/// stage it needs to publish its output to.
///
/// If a stage has `output_links` is empty then it is the final stage in this query, and it should
/// publish its outputs to the `ExecutionGraph`s `output_locations` representing the final query results.
#[derive(Clone)]
pub struct ExecutionGraph {
    /// Curator scheduler name
    scheduler_id: String,
    /// ID for this job
    job_id: String,
    /// Session ID for this job
    session_id: String,
    /// Status of this job
    status: JobStatus,
    /// Map from Stage ID -> ExecutionStage
    stages: HashMap<usize, ExecutionStage>,
    /// Total number fo output partitions
    output_partitions: usize,
    /// Locations of this `ExecutionGraph` final output locations
    output_locations: Vec<PartitionLocation>,
    /// Task ID generator, generate unique TID in the execution graph
    tid_generator: usize,
    /// Failed stage attempts, record the failed stage attempts to limit the retry times.
    /// Map from Stage ID -> Set<Stage_ATTPMPT_NUM>
    failed_stage_attempts: HashMap<usize, HashSet<usize>>,
}

#[derive(Clone)]
pub struct RunningTaskInfo {
    pub task_id: usize,
    pub job_id: String,
    pub stage_id: usize,
    pub partition_id: usize,
    pub executor_id: String,
}

impl ExecutionGraph {
    pub fn new(
        scheduler_id: &str,
        job_id: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let mut planner = DistributedPlanner::new();

        let output_partitions = plan.output_partitioning().partition_count();

        let shuffle_stages = planner.plan_query_stages(job_id, plan)?;

        let builder = ExecutionStageBuilder::new();
        let stages = builder.build(shuffle_stages)?;

        Ok(Self {
            scheduler_id: scheduler_id.to_string(),
            job_id: job_id.to_string(),
            session_id: session_id.to_string(),
            status: JobStatus {
                status: Some(job_status::Status::Queued(QueuedJob {})),
            },
            stages,
            output_partitions,
            output_locations: vec![],
            tid_generator: 0,
            failed_stage_attempts: HashMap::new(),
        })
    }

    pub fn job_id(&self) -> &str {
        self.job_id.as_str()
    }

    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    pub fn status(&self) -> JobStatus {
        self.status.clone()
    }

    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    pub fn next_task_id(&mut self) -> usize {
        let new_tid = self.tid_generator;
        self.tid_generator += 1;
        new_tid
    }

    /// An ExecutionGraph is successful if all its stages are successful
    pub fn is_successful(&self) -> bool {
        self.stages
            .values()
            .all(|s| matches!(s, ExecutionStage::Successful(_)))
    }

    /// Revive the execution graph by converting the resolved stages to running stages
    /// If any stages are converted, return true; else false.
    pub fn revive(&mut self) -> bool {
        let running_stages = self
            .stages
            .values()
            .filter_map(|stage| {
                if let ExecutionStage::Resolved(resolved_stage) = stage {
                    Some(resolved_stage.to_running())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if running_stages.is_empty() {
            false
        } else {
            for running_stage in running_stages {
                self.stages.insert(
                    running_stage.stage_id,
                    ExecutionStage::Running(running_stage),
                );
            }
            true
        }
    }

    /// Update task statuses and task metrics in the graph.
    /// This will also push shuffle partitions to their respective shuffle read stages.
    pub fn update_task_status(
        &mut self,
        executor: &ExecutorMetadata,
        task_statuses: Vec<TaskStatus>,
        max_task_failures: usize,
        max_stage_failures: usize,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();
        // First of all, classify the statuses by stages
        let mut job_task_statuses: HashMap<usize, Vec<TaskStatus>> = HashMap::new();
        for task_status in task_statuses {
            let stage_id = task_status.stage_id as usize;
            let stage_task_statuses =
                job_task_statuses.entry(stage_id).or_insert_with(Vec::new);
            stage_task_statuses.push(task_status);
        }

        // Revive before updating due to some updates not saved
        // It will be refined later
        self.revive();

        // Copy the failed stage attempts from self
        let mut failed_stage_attempts: HashMap<usize, HashSet<usize>> = HashMap::new();
        for (stage_id, attempts) in self.failed_stage_attempts.iter() {
            failed_stage_attempts
                .insert(*stage_id, HashSet::from_iter(attempts.iter().copied()));
        }

        let mut resolved_stages = vec![];
        let mut successful_stages = vec![];

        let mut failed_stages = HashMap::new();

        let mut rollback_running_stages = HashSet::new();
        let mut resubmit_successful_stages: HashMap<usize, HashSet<usize>> =
            HashMap::new();

        for (stage_id, stage_task_statuses) in job_task_statuses {
            if let Some(stage) = self.stages.get_mut(&stage_id) {
                if let ExecutionStage::Running(running_stage) = stage {
                    let mut locations = vec![];
                    for task_status in stage_task_statuses.into_iter() {
                        {
                            let stage_id = stage_id as usize;
                            let task_stage_attempt_num =
                                task_status.stage_attempt_num as usize;
                            if task_stage_attempt_num < running_stage.stage_attempt_num {
                                warn!("Ignore TaskStatus update with TID {} as it's from Stage {}.{} and there is a more recent stage attempt {}.{} running",
                                    task_status.task_id, stage_id, task_stage_attempt_num, stage_id, running_stage.stage_attempt_num);
                                continue;
                            }
                            let partition_id = task_status.clone().partition_id as usize;
                            let operator_metrics = task_status.metrics.clone();

                            if !running_stage
                                .update_task_info(partition_id, task_status.clone())
                            {
                                continue;
                            }

                            if let Some(task_status::Status::Failed(failed_task)) =
                                task_status.status
                            {
                                let failed_reason = failed_task.failed_reason;

                                match failed_reason {
                                    Some(FailedReason::FetchPartitionError(
                                        fetch_partiton_error,
                                    )) => {
                                        let failed_attempts = failed_stage_attempts
                                            .entry(stage_id)
                                            .or_insert_with(HashSet::new);
                                        failed_attempts.insert(task_stage_attempt_num);
                                        if failed_attempts.len() < max_stage_failures {
                                            let map_stage_id =
                                                fetch_partiton_error.map_stage_id;
                                            let map_partition_id =
                                                fetch_partiton_error.map_partition_id;

                                            if failed_stages.contains_key(&stage_id) {
                                                let error_msg = format!(
                                                        "Stage {} was marked failed, ignore FetchPartitionError from task with TID {}",stage_id, task_status.task_id);
                                                warn!("{}", error_msg);
                                            } else {
                                                running_stage.remove_input_partition(
                                                    map_stage_id as usize,
                                                    map_partition_id as usize,
                                                )?;

                                                rollback_running_stages.insert(stage_id);
                                                let missing_inputs =
                                                    resubmit_successful_stages
                                                        .entry(map_stage_id as usize)
                                                        .or_insert_with(HashSet::new);
                                                missing_inputs
                                                    .insert(map_partition_id as usize);
                                            }
                                        } else {
                                            let error_msg = format!(
                                                "Stage {} has failed {} times, \
                                            most recent failure reason: {:?}",
                                                stage_id,
                                                max_stage_failures,
                                                failed_task.error
                                            );
                                            error!("{}", error_msg);
                                            failed_stages.insert(stage_id, error_msg);
                                        }
                                    }
                                    Some(FailedReason::ExecutionError(_)) => {
                                        failed_stages.insert(stage_id, failed_task.error);
                                    }
                                    Some(_) => {
                                        if failed_task.retryable
                                            && failed_task.count_to_failures
                                        {
                                            if running_stage
                                                .task_failure_number(partition_id)
                                                < max_task_failures
                                            {
                                                // TODO add new struct to track all the failed task infos
                                                // The failure TaskInfo is ignored and set to None here
                                                running_stage
                                                    .reset_task_info(partition_id);
                                            } else {
                                                let error_msg = format!(
                        "Task {} in Stage {} failed {} times, fail the stage, most recent failure reason: {:?}",
                        partition_id, stage_id, max_task_failures, failed_task.error
                    );
                                                error!("{}", error_msg);
                                                failed_stages.insert(stage_id, error_msg);
                                            }
                                        } else if failed_task.retryable {
                                            // TODO add new struct to track all the failed task infos
                                            // The failure TaskInfo is ignored and set to None here
                                            running_stage.reset_task_info(partition_id);
                                        }
                                    }
                                    None => {
                                        let error_msg = format!(
                                            "Task {} in Stage {} failed with unknown failure reasons, fail the stage",
                                            partition_id, stage_id);
                                        error!("{}", error_msg);
                                        failed_stages.insert(stage_id, error_msg);
                                    }
                                }
                            } else if let Some(task_status::Status::Successful(
                                successful_task,
                            )) = task_status.status
                            {
                                // update task metrics for successfu task
                                running_stage.update_task_metrics(
                                    partition_id,
                                    operator_metrics,
                                )?;

                                locations.append(&mut partition_to_location(
                                    &job_id,
                                    stage_id,
                                    executor,
                                    successful_task.partitions,
                                ));
                            } else {
                                warn!(
                                    "The task {}/{}/{}'s status is invalid for updating",
                                    job_id, stage_id, partition_id
                                );
                            }
                        }
                    }
                    let is_successful = running_stage.is_successful();
                    if is_successful {
                        successful_stages.push(stage_id);
                        // if this stage is successful, we want to combine the stage metrics to plan's metric set and print out the plan
                        if let Some(stage_metrics) = running_stage.stage_metrics.as_ref()
                        {
                            print_stage_metrics(
                                &job_id,
                                stage_id,
                                running_stage.plan.as_ref(),
                                stage_metrics,
                            );
                        }
                    }

                    let output_links = running_stage.output_links.clone();
                    resolved_stages.append(&mut self.update_stage_output_links(
                        stage_id,
                        is_successful,
                        locations,
                        output_links,
                    )?);
                } else {
                    warn!(
                        "Stage {}/{} is not in running when updating the status of tasks {:?}",
                        job_id,
                        stage_id,
                        stage_task_statuses.into_iter().map(|task_status| task_status.partition_id).collect::<Vec<_>>(),
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {} for job {}",
                    stage_id, job_id
                )));
            }
        }

        // Update failed stage attempts back to self
        for (stage_id, attempts) in failed_stage_attempts.iter() {
            self.failed_stage_attempts
                .insert(*stage_id, HashSet::from_iter(attempts.iter().copied()));
        }

        for (stage_id, missing_parts) in &resubmit_successful_stages {
            if let Some(stage) = self.stages.get_mut(stage_id) {
                if let ExecutionStage::Successful(success_stage) = stage {
                    for partition in missing_parts {
                        let task_info = &mut success_stage.task_infos[*partition];
                        // Update the task info to failed
                        task_info.task_status = task_status::Status::Failed(FailedTask {
                            error: "FetchPartitionError in parent stage".to_owned(),
                            retryable: true,
                            count_to_failures: false,
                            failed_reason: Some(FailedReason::ResultLost(ResultLost {})),
                        });
                    }
                } else {
                    warn!(
                        "Stage {}/{} is not in Successful state when try to resubmit this stage. ",
                        job_id,
                        stage_id);
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {} for job {}",
                    stage_id, job_id
                )));
            }
        }

        self.processing_stages_update(UpdatedStages {
            resolved_stages,
            successful_stages,
            failed_stages,
            rollback_running_stages,
            resubmit_successful_stages: resubmit_successful_stages
                .keys()
                .cloned()
                .collect(),
        })
    }

    /// Processing stage status update after task status changing
    fn processing_stages_update(
        &mut self,
        updated_stages: UpdatedStages,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();
        let mut has_resolved = false;
        let mut job_err_msg = "".to_owned();

        for stage_id in updated_stages.resolved_stages {
            self.resolve_stage(stage_id)?;
            has_resolved = true;
        }

        for stage_id in updated_stages.successful_stages {
            self.succeed_stage(stage_id);
        }

        // Fail the stage and also abort the job
        for (stage_id, err_msg) in &updated_stages.failed_stages {
            job_err_msg =
                format!("Job failed due to stage {} failed: {}\n", stage_id, err_msg);
        }

        let mut events = vec![];
        // Only handle the rollback logic when there are no failed stages
        if updated_stages.failed_stages.is_empty() {
            let mut running_tasks_to_cancel = vec![];
            for stage_id in updated_stages.rollback_running_stages {
                let tasks = self.rollback_running_stage(stage_id)?;
                running_tasks_to_cancel.extend(tasks);
            }

            for stage_id in updated_stages.resubmit_successful_stages {
                self.rerun_successful_stage(stage_id);
            }

            if !running_tasks_to_cancel.is_empty() {
                events.push(QueryStageSchedulerEvent::CancelTasks(
                    running_tasks_to_cancel,
                ));
            }
        }

        if !updated_stages.failed_stages.is_empty() {
            info!("Job {} is failed", job_id);
            self.fail_job(job_err_msg.clone());
            events.push(QueryStageSchedulerEvent::JobRunningFailed(
                job_id,
                job_err_msg,
            ));
        } else if self.is_successful() {
            // If this ExecutionGraph is successful, finish it
            info!("Job {} is success, finalizing output partitions", job_id);
            self.succeed_job()?;
            events.push(QueryStageSchedulerEvent::JobFinished(job_id));
        } else if has_resolved {
            events.push(QueryStageSchedulerEvent::JobUpdated(job_id))
        }
        Ok(events)
    }

    /// Return a Vec of resolvable stage ids
    fn update_stage_output_links(
        &mut self,
        stage_id: usize,
        is_completed: bool,
        locations: Vec<PartitionLocation>,
        output_links: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let mut resolved_stages = vec![];
        let job_id = &self.job_id;
        if output_links.is_empty() {
            // If `output_links` is empty, then this is a final stage
            self.output_locations.extend(locations);
        } else {
            for link in output_links.iter() {
                // If this is an intermediate stage, we need to push its `PartitionLocation`s to the parent stage
                if let Some(linked_stage) = self.stages.get_mut(link) {
                    if let ExecutionStage::UnResolved(linked_unresolved_stage) =
                        linked_stage
                    {
                        linked_unresolved_stage
                            .add_input_partitions(stage_id, locations.clone())?;

                        // If all tasks for this stage are complete, mark the input complete in the parent stage
                        if is_completed {
                            linked_unresolved_stage.complete_input(stage_id);
                        }

                        // If all input partitions are ready, we can resolve any UnresolvedShuffleExec in the parent stage plan
                        if linked_unresolved_stage.resolvable() {
                            resolved_stages.push(linked_unresolved_stage.stage_id);
                        }
                    } else {
                        return Err(BallistaError::Internal(format!(
                            "Error updating job {}: The stage {} as the output link of stage {}  should be unresolved",
                            job_id, link, stage_id
                        )));
                    }
                } else {
                    return Err(BallistaError::Internal(format!(
                        "Error updating job {}: Invalid output link {} for stage {}",
                        job_id, stage_id, link
                    )));
                }
            }
        }
        Ok(resolved_stages)
    }

    /// Return all the currently running stage ids
    pub fn running_stages(&self) -> Vec<usize> {
        self.stages
            .iter()
            .filter_map(|(stage_id, stage)| {
                if let ExecutionStage::Running(_running) = stage {
                    Some(*stage_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    /// Return all currently running tasks along with the executor ID on which they are assigned
    pub fn running_tasks(&self) -> Vec<RunningTaskInfo> {
        self.stages
            .iter()
            .flat_map(|(_, stage)| {
                if let ExecutionStage::Running(stage) = stage {
                    stage
                        .running_tasks()
                        .into_iter()
                        .map(|(task_id, stage_id, partition_id, executor_id)| {
                            RunningTaskInfo {
                                task_id,
                                job_id: self.job_id.clone(),
                                stage_id,
                                partition_id,
                                executor_id,
                            }
                        })
                        .collect::<Vec<RunningTaskInfo>>()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<RunningTaskInfo>>()
    }

    /// Total number of tasks in this plan that are ready for scheduling
    pub fn available_tasks(&self) -> usize {
        self.stages
            .iter()
            .map(|(_, stage)| {
                if let ExecutionStage::Running(stage) = stage {
                    stage.available_tasks()
                } else {
                    0
                }
            })
            .sum()
    }

    /// Get next task that can be assigned to the given executor.
    /// This method should only be called when the resulting task is immediately
    /// being launched as the status will be set to Running and it will not be
    /// available to the scheduler.
    /// If the task is not launched the status must be reset to allow the task to
    /// be scheduled elsewhere.
    pub fn pop_next_task(&mut self, executor_id: &str) -> Result<Option<TaskDefinition>> {
        let job_id = self.job_id.clone();
        let session_id = self.session_id.clone();

        let find_candidate = self.stages.iter().any(|(_stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                stage.available_tasks() > 0
            } else {
                false
            }
        });
        let next_task_id = if find_candidate {
            Some(self.next_task_id())
        } else {
            None
        };

        let mut next_task = self.stages.iter_mut().find(|(_stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                stage.available_tasks() > 0
            } else {
                false
            }
        }).map(|(stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                let (partition_id, _) = stage
                    .task_infos
                    .iter()
                    .enumerate()
                    .find(|(_partition, info)| info.is_none())
                    .ok_or_else(|| {
                        BallistaError::Internal(format!("Error getting next task for job {}: Stage {} is ready but has no pending tasks", job_id, stage_id))
                    })?;

                let partition = PartitionId {
                    job_id,
                    stage_id: *stage_id,
                    partition_id,
                };

                let task_id = next_task_id.unwrap();
                let task_attempt = stage.task_failure_numbers[partition_id];
                let task_info = TaskInfo {
                    task_id,
                    scheduled_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                    // Those times will be updated when the task finish
                    launch_time: 0,
                    start_exec_time: 0,
                    end_exec_time: 0,
                    finish_time: 0,
                    task_status: task_status::Status::Running(RunningTask {
                        executor_id: executor_id.to_owned()
                    }),
                };

                // Set the task info to Running for new task
                stage.task_infos[partition_id] = Some(task_info);

                Ok(TaskDefinition {
                    session_id,
                    partition,
                    stage_attempt_num: stage.stage_attempt_num,
                    task_id,
                    task_attempt,
                    plan: stage.plan.clone(),
                    output_partitioning: stage.output_partitioning.clone(),
                })
            } else {
                Err(BallistaError::General(format!("Stage {} is not a running stage", stage_id)))
            }
        }).transpose()?;

        // If no available tasks found in the running stage,
        // try to find a resolved stage and convert it to the running stage
        if next_task.is_none() {
            if self.revive() {
                next_task = self.pop_next_task(executor_id)?;
            } else {
                next_task = None;
            }
        }

        Ok(next_task)
    }

    pub fn update_status(&mut self, status: JobStatus) {
        self.status = status;
    }

    pub fn output_locations(&self) -> Vec<PartitionLocation> {
        self.output_locations.clone()
    }

    /// Reset running and successful stages on a given executor
    /// This will first check the unresolved/resolved/running stages and reset the running tasks and successful tasks.
    /// Then it will check the successful stage and whether there are running parent stages need to read shuffle from it.
    /// If yes, reset the successful tasks and roll back the resolved shuffle recursively.
    ///
    /// Returns the reset stage ids and running tasks should be killed
    pub fn reset_stages_on_lost_executor(
        &mut self,
        executor_id: &str,
    ) -> Result<(HashSet<usize>, Vec<RunningTaskInfo>)> {
        let mut reset = HashSet::new();
        let mut tasks_to_cancel = vec![];
        loop {
            let reset_stage = self.reset_stages_internal(executor_id)?;
            if !reset_stage.0.is_empty() {
                reset.extend(reset_stage.0.iter());
                tasks_to_cancel.extend(reset_stage.1)
            } else {
                return Ok((reset, tasks_to_cancel));
            }
        }
    }

    fn reset_stages_internal(
        &mut self,
        executor_id: &str,
    ) -> Result<(HashSet<usize>, Vec<RunningTaskInfo>)> {
        let job_id = self.job_id.clone();
        // collect the input stages that need to resubmit
        let mut resubmit_inputs: HashSet<usize> = HashSet::new();

        let mut reset_running_stage = HashSet::new();
        let mut rollback_resolved_stages = HashSet::new();
        let mut rollback_running_stages = HashSet::new();
        let mut resubmit_successful_stages = HashSet::new();

        let mut empty_inputs: HashMap<usize, StageOutput> = HashMap::new();
        // check the unresolved, resolved and running stages
        self.stages
            .iter_mut()
            .for_each(|(stage_id, stage)| {
                let stage_inputs = match stage {
                    ExecutionStage::UnResolved(stage) => {
                        &mut stage.inputs
                    }
                    ExecutionStage::Resolved(stage) => {
                        &mut stage.inputs
                    }
                    ExecutionStage::Running(stage) => {
                        let reset = stage.reset_tasks(executor_id);
                        if reset > 0 {
                            warn!(
                        "Reset {} tasks for running job/stage {}/{} on lost Executor {}",
                        reset, job_id, stage_id, executor_id
                        );
                            reset_running_stage.insert(*stage_id);
                        }
                        &mut stage.inputs
                    }
                    _ => &mut empty_inputs
                };

                // For each stage input, check whether there are input locations match that executor
                // and calculate the resubmit input stages if the input stages are successful.
                let mut rollback_stage = false;
                stage_inputs.iter_mut().for_each(|(input_stage_id, stage_output)| {
                    let mut match_found = false;
                    stage_output.partition_locations.iter_mut().for_each(
                        |(_partition, locs)| {
                            let indexes = locs
                                .iter()
                                .enumerate()
                                .filter_map(|(idx, loc)| {
                                    (loc.executor_meta.id == executor_id).then(|| idx)
                                })
                                .collect::<Vec<_>>();

                            // remove the matched partition locations
                            if !indexes.is_empty() {
                                for idx in &indexes {
                                    locs.remove(*idx);
                                }
                                match_found = true;
                            }
                        },
                    );
                    if match_found {
                        stage_output.complete = false;
                        rollback_stage = true;
                        resubmit_inputs.insert(*input_stage_id);
                    }
                });

                if rollback_stage {
                    match stage {
                        ExecutionStage::Resolved(_) => {
                            rollback_resolved_stages.insert(*stage_id);
                            warn!(
                            "Roll back resolved job/stage {}/{} and change ShuffleReaderExec back to UnresolvedShuffleExec",
                            job_id, stage_id);

                        },
                        ExecutionStage::Running(_) => {
                            rollback_running_stages.insert(*stage_id);
                            warn!(
                            "Roll back running job/stage {}/{} and change ShuffleReaderExec back to UnresolvedShuffleExec",
                            job_id, stage_id);
                        },
                        _ => {},
                    }
                }
            });

        // check and reset the successful stages
        if !resubmit_inputs.is_empty() {
            self.stages
                .iter_mut()
                .filter(|(stage_id, _stage)| resubmit_inputs.contains(stage_id))
                .filter_map(|(_stage_id, stage)| {
                    if let ExecutionStage::Successful(success) = stage {
                        Some(success)
                    } else {
                        None
                    }
                })
                .for_each(|stage| {
                    let reset = stage.reset_tasks(executor_id);
                    if reset > 0 {
                        resubmit_successful_stages.insert(stage.stage_id);
                        warn!(
                            "Reset {} tasks for successful job/stage {}/{} on lost Executor {}",
                            reset, job_id, stage.stage_id, executor_id
                        )
                    }
                });
        }

        for stage_id in rollback_resolved_stages.iter() {
            self.rollback_resolved_stage(*stage_id)?;
        }

        let mut all_running_tasks = vec![];
        for stage_id in rollback_running_stages.iter() {
            let tasks = self.rollback_running_stage(*stage_id)?;
            all_running_tasks.extend(tasks);
        }

        for stage_id in resubmit_successful_stages.iter() {
            self.rerun_successful_stage(*stage_id);
        }

        let mut reset_stage = HashSet::new();
        reset_stage.extend(reset_running_stage);
        reset_stage.extend(rollback_resolved_stages);
        reset_stage.extend(rollback_running_stages);
        reset_stage.extend(resubmit_successful_stages);
        Ok((reset_stage, all_running_tasks))
    }

    /// Convert unresolved stage to be resolved
    pub fn resolve_stage(&mut self, stage_id: usize) -> Result<bool> {
        if let Some(ExecutionStage::UnResolved(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Resolved(stage.to_resolved()?));
            Ok(true)
        } else {
            warn!(
                "Fail to find a unresolved stage {}/{} to resolve",
                self.job_id(),
                stage_id
            );
            Ok(false)
        }
    }

    /// Convert running stage to be successful
    pub fn succeed_stage(&mut self, stage_id: usize) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Successful(stage.to_successful()));
            self.clear_stage_failure(stage_id);
            true
        } else {
            warn!(
                "Fail to find a running stage {}/{} to make it success",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// Convert running stage to be failed
    pub fn fail_stage(&mut self, stage_id: usize, err_msg: String) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Failed(stage.to_failed(err_msg)));
            true
        } else {
            info!(
                "Fail to find a running stage {}/{} to fail",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// Convert running stage to be unresolved,
    /// Returns a Vec of RunningTaskInfo for running tasks in this stage.
    pub fn rollback_running_stage(
        &mut self,
        stage_id: usize,
    ) -> Result<Vec<RunningTaskInfo>> {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            let running_tasks = stage
                .running_tasks()
                .into_iter()
                .map(
                    |(task_id, stage_id, partition_id, executor_id)| RunningTaskInfo {
                        task_id,
                        job_id: self.job_id.clone(),
                        stage_id,
                        partition_id,
                        executor_id,
                    },
                )
                .collect();
            self.stages
                .insert(stage_id, ExecutionStage::UnResolved(stage.to_unresolved()?));
            Ok(running_tasks)
        } else {
            warn!(
                "Fail to find a running stage {}/{} to rollback",
                self.job_id(),
                stage_id
            );
            Ok(vec![])
        }
    }

    /// Convert resolved stage to be unresolved
    pub fn rollback_resolved_stage(&mut self, stage_id: usize) -> Result<bool> {
        if let Some(ExecutionStage::Resolved(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::UnResolved(stage.to_unresolved()?));
            Ok(true)
        } else {
            warn!(
                "Fail to find a resolved stage {}/{} to rollback",
                self.job_id(),
                stage_id
            );
            Ok(false)
        }
    }

    /// Convert successful stage to be running
    pub fn rerun_successful_stage(&mut self, stage_id: usize) -> bool {
        if let Some(ExecutionStage::Successful(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Running(stage.to_running()));
            true
        } else {
            warn!(
                "Fail to find a successful stage {}/{} to rerun",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// fail job with error message
    pub fn fail_job(&mut self, error: String) {
        self.status = JobStatus {
            status: Some(job_status::Status::Failed(FailedJob { error })),
        };
    }

    /// Mark the job success
    pub fn succeed_job(&mut self) -> Result<()> {
        if !self.is_successful() {
            return Err(BallistaError::Internal(format!(
                "Attempt to finalize an incomplete job {}",
                self.job_id()
            )));
        }

        let partition_location = self
            .output_locations()
            .into_iter()
            .map(|l| l.try_into())
            .collect::<Result<Vec<_>>>()?;

        self.status = JobStatus {
            status: Some(job_status::Status::Successful(SuccessfulJob {
                partition_location,
            })),
        };

        Ok(())
    }

    /// Clear the stage failure count for this stage if the stage is finally success
    fn clear_stage_failure(&mut self, stage_id: usize) {
        self.failed_stage_attempts.remove(&stage_id);
    }

    pub(crate) async fn decode_execution_graph<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        proto: protobuf::ExecutionGraph,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<ExecutionGraph> {
        let mut stages: HashMap<usize, ExecutionStage> = HashMap::new();
        for graph_stage in proto.stages {
            let stage_type = graph_stage.stage_type.expect("Unexpected empty stage");

            let execution_stage = match stage_type {
                StageType::UnresolvedStage(stage) => {
                    let stage: UnresolvedStage =
                        UnresolvedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::UnResolved(stage))
                }
                StageType::ResolvedStage(stage) => {
                    let stage: ResolvedStage =
                        ResolvedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Resolved(stage))
                }
                StageType::SuccessfulStage(stage) => {
                    let stage: SuccessfulStage =
                        SuccessfulStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Successful(stage))
                }
                StageType::FailedStage(stage) => {
                    let stage: FailedStage =
                        FailedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Failed(stage))
                }
            };

            stages.insert(execution_stage.0, execution_stage.1);
        }

        let output_locations: Vec<PartitionLocation> = proto
            .output_locations
            .into_iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        let failed_stage_attempts = proto
            .failed_attempts
            .into_iter()
            .map(|attempt| {
                (
                    attempt.stage_id as usize,
                    HashSet::from_iter(
                        attempt
                            .stage_attempt_num
                            .into_iter()
                            .map(|num| num as usize),
                    ),
                )
            })
            .collect();

        Ok(ExecutionGraph {
            scheduler_id: proto.scheduler_id,
            job_id: proto.job_id,
            session_id: proto.session_id,
            status: proto.status.ok_or_else(|| {
                BallistaError::Internal(
                    "Invalid Execution Graph: missing job status".to_owned(),
                )
            })?,
            stages,
            output_partitions: proto.output_partitions as usize,
            output_locations,
            tid_generator: proto.tid_gen as usize,
            failed_stage_attempts,
        })
    }

    /// Running stages will not be persisted so that will not be encoded.
    /// Running stages will be convert back to the resolved stages to be encoded and persisted
    pub(crate) fn encode_execution_graph<
        T: 'static + AsLogicalPlan,
        U: 'static + AsExecutionPlan,
    >(
        graph: ExecutionGraph,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::ExecutionGraph> {
        let job_id = graph.job_id().to_owned();

        let stages = graph
            .stages
            .into_values()
            .map(|stage| {
                let stage_type = match stage {
                    ExecutionStage::UnResolved(stage) => {
                        StageType::UnresolvedStage(UnresolvedStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Resolved(stage) => {
                        StageType::ResolvedStage(ResolvedStage::encode(stage, codec)?)
                    }
                    ExecutionStage::Running(stage) => StageType::ResolvedStage(
                        ResolvedStage::encode(stage.to_resolved(), codec)?,
                    ),
                    ExecutionStage::Successful(stage) => StageType::SuccessfulStage(
                        SuccessfulStage::encode(job_id.clone(), stage, codec)?,
                    ),
                    ExecutionStage::Failed(stage) => StageType::FailedStage(
                        FailedStage::encode(job_id.clone(), stage, codec)?,
                    ),
                };
                Ok(protobuf::ExecutionGraphStage {
                    stage_type: Some(stage_type),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let output_locations: Vec<protobuf::PartitionLocation> = graph
            .output_locations
            .into_iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        let failed_attempts: Vec<protobuf::StageAttempts> = graph
            .failed_stage_attempts
            .into_iter()
            .map(|(stage_id, attempts)| {
                let stage_attempt_num = attempts
                    .into_iter()
                    .map(|num| num as u32)
                    .collect::<Vec<_>>();
                protobuf::StageAttempts {
                    stage_id: stage_id as u32,
                    stage_attempt_num,
                }
            })
            .collect::<Vec<_>>();

        Ok(protobuf::ExecutionGraph {
            job_id: graph.job_id,
            session_id: graph.session_id,
            status: Some(graph.status),
            stages,
            output_partitions: graph.output_partitions as u64,
            output_locations,
            scheduler_id: graph.scheduler_id,
            tid_gen: graph.tid_generator as u32,
            failed_attempts,
        })
    }
}

impl Debug for ExecutionGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let stages = self
            .stages
            .iter()
            .map(|(_, stage)| format!("{:?}", stage))
            .collect::<Vec<String>>()
            .join("");
        write!(f, "ExecutionGraph[job_id={}, session_id={}, available_tasks={}, is_successful={}]\n{}",
               self.job_id, self.session_id, self.available_tasks(), self.is_successful(), stages)
    }
}

/// Utility for building a set of `ExecutionStage`s from
/// a list of `ShuffleWriterExec`.
///
/// This will infer the dependency structure for the stages
/// so that we can construct a DAG from the stages.
struct ExecutionStageBuilder {
    /// Stage ID which is currently being visited
    current_stage_id: usize,
    /// Map from stage ID -> List of child stage IDs
    stage_dependencies: HashMap<usize, Vec<usize>>,
    /// Map from Stage ID -> output link
    output_links: HashMap<usize, Vec<usize>>,
}

impl ExecutionStageBuilder {
    pub fn new() -> Self {
        Self {
            current_stage_id: 0,
            stage_dependencies: HashMap::new(),
            output_links: HashMap::new(),
        }
    }

    pub fn build(
        mut self,
        stages: Vec<Arc<ShuffleWriterExec>>,
    ) -> Result<HashMap<usize, ExecutionStage>> {
        let mut execution_stages: HashMap<usize, ExecutionStage> = HashMap::new();
        // First, build the dependency graph
        for stage in &stages {
            accept(stage.as_ref(), &mut self)?;
        }

        // Now, create the execution stages
        for stage in stages {
            let partitioning = stage.shuffle_output_partitioning().cloned();
            let stage_id = stage.stage_id();
            let output_links = self.output_links.remove(&stage_id).unwrap_or_default();

            let child_stages = self
                .stage_dependencies
                .remove(&stage_id)
                .unwrap_or_default();

            let stage = if child_stages.is_empty() {
                ExecutionStage::Resolved(ResolvedStage::new(
                    stage_id,
                    0,
                    stage,
                    partitioning,
                    output_links,
                    HashMap::new(),
                ))
            } else {
                ExecutionStage::UnResolved(UnresolvedStage::new(
                    stage_id,
                    stage,
                    partitioning,
                    output_links,
                    child_stages,
                ))
            };
            execution_stages.insert(stage_id, stage);
        }

        Ok(execution_stages)
    }
}

impl ExecutionPlanVisitor for ExecutionStageBuilder {
    type Error = BallistaError;

    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        if let Some(shuffle_write) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
            self.current_stage_id = shuffle_write.stage_id();
        } else if let Some(unresolved_shuffle) =
            plan.as_any().downcast_ref::<UnresolvedShuffleExec>()
        {
            if let Some(output_links) =
                self.output_links.get_mut(&unresolved_shuffle.stage_id)
            {
                if !output_links.contains(&self.current_stage_id) {
                    output_links.push(self.current_stage_id);
                }
            } else {
                self.output_links
                    .insert(unresolved_shuffle.stage_id, vec![self.current_stage_id]);
            }

            if let Some(deps) = self.stage_dependencies.get_mut(&self.current_stage_id) {
                if !deps.contains(&unresolved_shuffle.stage_id) {
                    deps.push(unresolved_shuffle.stage_id);
                }
            } else {
                self.stage_dependencies
                    .insert(self.current_stage_id, vec![unresolved_shuffle.stage_id]);
            }
        }
        Ok(true)
    }
}

/// Represents the basic unit of work for the Ballista executor. Will execute
/// one partition of one stage on one task slot.
#[derive(Clone)]
pub struct TaskDefinition {
    pub session_id: String,
    pub partition: PartitionId,
    pub stage_attempt_num: usize,
    pub task_id: usize,
    pub task_attempt: usize,
    pub plan: Arc<dyn ExecutionPlan>,
    pub output_partitioning: Option<Partitioning>,
}

impl Debug for TaskDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent();
        write!(
            f,
            "TaskDefinition[session_id: {},job: {}, stage: {}.{}, partition: {} task_id {}, task attempt {}]\n{}",
            self.session_id,
            self.partition.job_id,
            self.partition.stage_id,
            self.stage_attempt_num,
            self.partition.partition_id,
            self.task_id,
            self.task_attempt,
            plan
        )
    }
}

fn partition_to_location(
    job_id: &str,
    stage_id: usize,
    executor: &ExecutorMetadata,
    shuffles: Vec<ShuffleWritePartition>,
) -> Vec<PartitionLocation> {
    shuffles
        .into_iter()
        .map(|shuffle| PartitionLocation {
            partition_id: PartitionId {
                job_id: job_id.to_owned(),
                stage_id,
                partition_id: shuffle.partition_id as usize,
            },
            executor_meta: executor.clone(),
            partition_stats: PartitionStats::new(
                Some(shuffle.num_rows),
                Some(shuffle.num_batches),
                Some(shuffle.num_bytes),
            ),
            path: shuffle.path,
        })
        .collect()
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, sum, Expr};
    use datafusion::logical_plan::JoinType;
    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::test_util::scan_empty;

    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{self, job_status, task_status, TaskStatus};
    use ballista_core::serde::scheduler::{ExecutorMetadata, ExecutorSpecification};

    use crate::state::execution_graph::{ExecutionGraph, TaskDefinition};

    #[tokio::test]
    async fn test_drain_tasks() -> Result<()> {
        let mut agg_graph = test_aggregation_plan(4).await;

        println!("Graph: {:?}", agg_graph);

        drain_tasks(&mut agg_graph)?;

        assert!(
            agg_graph.is_successful(),
            "Failed to complete aggregation plan"
        );

        let mut coalesce_graph = test_coalesce_plan(4).await;

        drain_tasks(&mut coalesce_graph)?;

        assert!(
            coalesce_graph.is_successful(),
            "Failed to complete coalesce plan"
        );

        let mut join_graph = test_join_plan(4).await;

        drain_tasks(&mut join_graph)?;

        println!("{:?}", join_graph);

        assert!(join_graph.is_successful(), "Failed to complete join plan");

        let mut union_all_graph = test_union_all_plan(4).await;

        drain_tasks(&mut union_all_graph)?;

        println!("{:?}", union_all_graph);

        assert!(
            union_all_graph.is_successful(),
            "Failed to complete union plan"
        );

        let mut union_graph = test_union_plan(4).await;

        drain_tasks(&mut union_graph)?;

        println!("{:?}", union_graph);

        assert!(union_graph.is_successful(), "Failed to complete union plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_finalize() -> Result<()> {
        let mut agg_graph = test_aggregation_plan(4).await;

        drain_tasks(&mut agg_graph)?;

        let status = agg_graph.status();

        assert!(matches!(
            status,
            protobuf::JobStatus {
                status: Some(job_status::Status::Successful(_))
            }
        ));

        let outputs = agg_graph.output_locations();

        assert_eq!(outputs.len(), agg_graph.output_partitions);

        for location in outputs {
            assert_eq!(location.executor_meta.host, "localhost2".to_owned());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_reset_completed_stage() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut join_graph = test_join_plan(4).await;

        assert_eq!(join_graph.stage_count(), 5);
        assert_eq!(join_graph.available_tasks(), 0);

        // Call revive to move the two leaf Resolved stages to Running
        join_graph.revive();

        assert_eq!(join_graph.stage_count(), 5);
        assert_eq!(join_graph.available_tasks(), 2);

        // Complete the first stage
        if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            join_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // Complete the second stage
        if let Some(task) = join_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            join_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
        }

        join_graph.revive();
        // There are 4 tasks pending schedule for the 3rd stage
        assert_eq!(join_graph.available_tasks(), 4);

        // Complete 1 task
        if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            join_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }
        // Mock 1 running task
        let _task = join_graph.pop_next_task(&executor1.id)?;

        let reset = join_graph.reset_stages_on_lost_executor(&executor1.id)?;

        // Two stages were reset, 1 Running stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.0.len(), 2);
        assert_eq!(join_graph.available_tasks(), 1);

        drain_tasks(&mut join_graph)?;
        assert!(join_graph.is_successful(), "Failed to complete join plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_reset_resolved_stage() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut join_graph = test_join_plan(4).await;

        assert_eq!(join_graph.stage_count(), 5);
        assert_eq!(join_graph.available_tasks(), 0);

        // Call revive to move the two leaf Resolved stages to Running
        join_graph.revive();

        assert_eq!(join_graph.stage_count(), 5);
        assert_eq!(join_graph.available_tasks(), 2);

        // Complete the first stage
        if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            join_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // Complete the second stage
        if let Some(task) = join_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            join_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
        }

        // There are 0 tasks pending schedule now
        assert_eq!(join_graph.available_tasks(), 0);

        let reset = join_graph.reset_stages_on_lost_executor(&executor1.id)?;

        // Two stages were reset, 1 Resolved stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.0.len(), 2);
        assert_eq!(join_graph.available_tasks(), 1);

        drain_tasks(&mut join_graph)?;
        assert!(join_graph.is_successful(), "Failed to complete join plan");

        Ok(())
    }

    #[tokio::test]
    async fn test_task_update_after_reset_stage() -> Result<()> {
        let executor1 = mock_executor("executor-id1".to_string());
        let executor2 = mock_executor("executor-id2".to_string());
        let mut agg_graph = test_aggregation_plan(4).await;

        assert_eq!(agg_graph.stage_count(), 2);
        assert_eq!(agg_graph.available_tasks(), 0);

        // Call revive to move the leaf Resolved stages to Running
        agg_graph.revive();

        assert_eq!(agg_graph.stage_count(), 2);
        assert_eq!(agg_graph.available_tasks(), 1);

        // Complete the first stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // 1st task in the second stage
        if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            agg_graph.update_task_status(&executor2, vec![task_status], 1, 1)?;
        }

        // 2rd task in the second stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;
        }

        // 3rd task in the second stage, scheduled but not completed
        let task = agg_graph.pop_next_task(&executor1.id)?;

        // There is 1 task pending schedule now
        assert_eq!(agg_graph.available_tasks(), 1);

        let reset = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;

        // 3rd task status update comes later.
        let task_status = mock_completed_task(task.unwrap(), &executor1.id);
        agg_graph.update_task_status(&executor1, vec![task_status], 1, 1)?;

        // Two stages were reset, 1 Running stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.0.len(), 2);
        assert_eq!(agg_graph.available_tasks(), 1);

        // Call the reset again
        let reset = agg_graph.reset_stages_on_lost_executor(&executor1.id)?;
        assert_eq!(reset.0.len(), 0);
        assert_eq!(agg_graph.available_tasks(), 1);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.is_successful(), "Failed to complete agg plan");

        Ok(())
    }

    fn drain_tasks(graph: &mut ExecutionGraph) -> Result<()> {
        let executor = mock_executor("executor-id1".to_string());
        while let Some(task) = graph.pop_next_task(&executor.id)? {
            let task_status = mock_completed_task(task, &executor.id);
            graph.update_task_status(&executor, vec![task_status], 1, 1)?;
        }

        Ok(())
    }

    async fn test_aggregation_plan(partition: usize) -> ExecutionGraph {
        let config = SessionConfig::new().with_target_partitions(partition);
        let ctx = Arc::new(SessionContext::with_config(config));

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        let logical_plan = scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap();

        let optimized_plan = ctx.optimize(&logical_plan).unwrap();

        let plan = ctx.create_physical_plan(&optimized_plan).await.unwrap();

        println!("{}", DisplayableExecutionPlan::new(plan.as_ref()).indent());

        ExecutionGraph::new("localhost:50050", "job", "session", plan).unwrap()
    }

    async fn test_coalesce_plan(partition: usize) -> ExecutionGraph {
        let config = SessionConfig::new().with_target_partitions(partition);
        let ctx = Arc::new(SessionContext::with_config(config));

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        let logical_plan = scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let optimized_plan = ctx.optimize(&logical_plan).unwrap();

        let plan = ctx.create_physical_plan(&optimized_plan).await.unwrap();

        ExecutionGraph::new("localhost:50050", "job", "session", plan).unwrap()
    }

    async fn test_join_plan(partition: usize) -> ExecutionGraph {
        let config = SessionConfig::new().with_target_partitions(partition);
        let ctx = Arc::new(SessionContext::with_config(config));

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);

        let left_plan = scan_empty(Some("left"), &schema, None).unwrap();

        let right_plan = scan_empty(Some("right"), &schema, None)
            .unwrap()
            .build()
            .unwrap();

        let sort_expr = Expr::Sort {
            expr: Box::new(col("id")),
            asc: false,
            nulls_first: false,
        };

        let logical_plan = left_plan
            .join(&right_plan, JoinType::Inner, (vec!["id"], vec!["id"]), None)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .sort(vec![sort_expr])
            .unwrap()
            .build()
            .unwrap();

        let optimized_plan = ctx.optimize(&logical_plan).unwrap();

        let plan = ctx.create_physical_plan(&optimized_plan).await.unwrap();

        println!("{}", DisplayableExecutionPlan::new(plan.as_ref()).indent());

        let graph =
            ExecutionGraph::new("localhost:50050", "job", "session", plan).unwrap();

        println!("{:?}", graph);

        graph
    }

    async fn test_union_all_plan(partition: usize) -> ExecutionGraph {
        let config = SessionConfig::new().with_target_partitions(partition);
        let ctx = Arc::new(SessionContext::with_config(config));

        let logical_plan = ctx
            .sql("SELECT 1 as NUMBER union all SELECT 1 as NUMBER;")
            .await
            .unwrap()
            .to_logical_plan()
            .unwrap();

        let optimized_plan = ctx.optimize(&logical_plan).unwrap();

        let plan = ctx.create_physical_plan(&optimized_plan).await.unwrap();

        println!("{}", DisplayableExecutionPlan::new(plan.as_ref()).indent());

        let graph =
            ExecutionGraph::new("localhost:50050", "job", "session", plan).unwrap();

        println!("{:?}", graph);

        graph
    }

    async fn test_union_plan(partition: usize) -> ExecutionGraph {
        let config = SessionConfig::new().with_target_partitions(partition);
        let ctx = Arc::new(SessionContext::with_config(config));

        let logical_plan = ctx
            .sql("SELECT 1 as NUMBER union SELECT 1 as NUMBER;")
            .await
            .unwrap()
            .to_logical_plan()
            .unwrap();

        let optimized_plan = ctx.optimize(&logical_plan).unwrap();

        let plan = ctx.create_physical_plan(&optimized_plan).await.unwrap();

        println!("{}", DisplayableExecutionPlan::new(plan.as_ref()).indent());

        let graph =
            ExecutionGraph::new("localhost:50050", "job", "session", plan).unwrap();

        println!("{:?}", graph);

        graph
    }

    fn mock_executor(executor_id: String) -> ExecutorMetadata {
        ExecutorMetadata {
            id: executor_id,
            host: "localhost2".to_string(),
            port: 8080,
            grpc_port: 9090,
            specification: ExecutorSpecification { task_slots: 1 },
        }
    }

    fn mock_completed_task(task: TaskDefinition, executor_id: &str) -> TaskStatus {
        let mut partitions: Vec<protobuf::ShuffleWritePartition> = vec![];

        let num_partitions = task
            .output_partitioning
            .map(|p| p.partition_count())
            .unwrap_or(1);

        for partition_id in 0..num_partitions {
            partitions.push(protobuf::ShuffleWritePartition {
                partition_id: partition_id as u64,
                path: format!(
                    "/{}/{}/{}",
                    task.partition.job_id,
                    task.partition.stage_id,
                    task.partition.partition_id
                ),
                num_batches: 1,
                num_rows: 1,
                num_bytes: 1,
            })
        }

        // Complete the task
        protobuf::TaskStatus {
            task_id: task.task_id as u32,
            job_id: task.partition.job_id.clone(),
            stage_id: task.partition.stage_id as u32,
            stage_attempt_num: task.stage_attempt_num as u32,
            partition_id: task.partition.partition_id as u32,
            launch_time: 0,
            start_exec_time: 0,
            end_exec_time: 0,
            metrics: vec![],
            status: Some(task_status::Status::Successful(protobuf::SuccessfulTask {
                executor_id: executor_id.to_owned(),
                partitions,
            })),
        }
    }
}
