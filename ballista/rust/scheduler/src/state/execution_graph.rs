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
use std::sync::Arc;

use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{
    accept, ExecutionPlan, ExecutionPlanVisitor, Partitioning,
};
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use log::{error, info, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{ShuffleWriterExec, UnresolvedShuffleExec};
use ballista_core::serde::protobuf::{
    self, execution_graph_stage::StageType, CompletedJob, JobStatus, QueuedJob,
    TaskStatus,
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
    CompletedStage, ExecutionStage, FailedStage, ResolvedStage, StageOutput,
    UnresolvedStage,
};

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

    /// An ExecutionGraph is complete if all its stages are complete
    pub fn complete(&self) -> bool {
        self.stages
            .values()
            .all(|s| matches!(s, ExecutionStage::Completed(_)))
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
    ) -> Result<Option<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();
        // First of all, classify the statuses by stages
        let mut job_task_statuses: HashMap<usize, Vec<TaskStatus>> = HashMap::new();
        for task_status in task_statuses {
            if let Some(task_id) = task_status.task_id.as_ref() {
                if task_id.job_id != job_id {
                    return Err(BallistaError::Internal(format!(
                        "Error updating job {}: Invalid task status job ID {}",
                        job_id, task_id.job_id
                    )));
                }
                let stage_task_statuses = job_task_statuses
                    .entry(task_id.stage_id as usize)
                    .or_insert_with(Vec::new);
                stage_task_statuses.push(task_status);
            } else {
                error!("There's no task id when updating status");
            }
        }

        // Revive before updating due to some updates not saved
        // It will be refined later
        self.revive();

        let mut events = vec![];
        for (stage_id, stage_task_statuses) in job_task_statuses {
            if let Some(stage) = self.stages.get_mut(&stage_id) {
                if let ExecutionStage::Running(running_stage) = stage {
                    let mut locations = vec![];
                    for task_status in stage_task_statuses.into_iter() {
                        if let TaskStatus {
                            task_id:
                                Some(protobuf::PartitionId {
                                    job_id,
                                    stage_id,
                                    partition_id,
                                }),
                            metrics: operator_metrics,
                            status: Some(status),
                        } = task_status
                        {
                            let stage_id = stage_id as usize;
                            let partition_id = partition_id as usize;

                            running_stage
                                .update_task_status(partition_id, status.clone());

                            // TODO Should be able to reschedule this task.
                            if let task_status::Status::Failed(failed_task) = status {
                                events.push(StageEvent::StageFailed(
                                    stage_id,
                                    format!(
                                        "Task {}/{}/{} failed: {}",
                                        job_id, stage_id, partition_id, failed_task.error
                                    ),
                                ));
                                break;
                            } else if let task_status::Status::Completed(completed_task) =
                                status
                            {
                                // update task metrics for completed task
                                running_stage.update_task_metrics(
                                    partition_id,
                                    operator_metrics,
                                )?;

                                locations.append(&mut partition_to_location(
                                    &job_id,
                                    stage_id,
                                    executor,
                                    completed_task.partitions,
                                ));
                            } else {
                                warn!("The task {}/{}/{} with status {:?} is invalid for updating", job_id, stage_id, partition_id, status);
                            }
                        }
                    }
                    let is_completed = running_stage.is_completed();
                    if is_completed {
                        events.push(StageEvent::StageCompleted(stage_id));
                        // if this stage is completed, we want to combine the stage metrics to plan's metric set and print out the plan
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
                    events.append(&mut self.update_stage_output_links(
                        stage_id,
                        is_completed,
                        locations,
                        output_links,
                    )?);
                } else {
                    warn!(
                        "Stage {}/{} is not in running when updating the status of tasks {:?}",
                        job_id,
                        stage_id,
                        stage_task_statuses.into_iter().map(|task_status| task_status.task_id.map(|task_id| task_id.partition_id)).collect::<Vec<_>>(),
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {} for job {}",
                    stage_id, job_id
                )));
            }
        }

        self.processing_stage_events(events)
    }

    fn update_stage_output_links(
        &mut self,
        stage_id: usize,
        is_completed: bool,
        locations: Vec<PartitionLocation>,
        output_links: Vec<usize>,
    ) -> Result<Vec<StageEvent>> {
        let mut ret = vec![];
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
                            ret.push(StageEvent::StageResolved(
                                linked_unresolved_stage.stage_id,
                            ));
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

        Ok(ret)
    }

    /// Return all currently running tasks along with the executor ID on which they are assigned
    pub fn running_tasks(&self) -> Vec<(PartitionId, String)> {
        self.stages
            .iter()
            .flat_map(|(_, stage)| {
                if let ExecutionStage::Running(stage) = stage {
                    stage
                        .running_tasks()
                        .into_iter()
                        .map(|(stage_id, partition_id, executor_id)| {
                            (
                                PartitionId {
                                    job_id: self.job_id.clone(),
                                    stage_id,
                                    partition_id,
                                },
                                executor_id,
                            )
                        })
                        .collect::<Vec<(PartitionId, String)>>()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<(PartitionId, String)>>()
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
    pub fn pop_next_task(&mut self, executor_id: &str) -> Result<Option<Task>> {
        let job_id = self.job_id.clone();
        let session_id = self.session_id.clone();
        let mut next_task = self.stages.iter_mut().find(|(_stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                stage.available_tasks() > 0
            } else {
                false
            }
        }).map(|(stage_id, stage)| {
            if let ExecutionStage::Running(stage) = stage {
                let (partition_id, _) = stage
                    .task_statuses
                    .iter()
                    .enumerate()
                    .find(|(_partition, status)| status.is_none())
                    .ok_or_else(|| {
                        BallistaError::Internal(format!("Error getting next task for job {}: Stage {} is ready but has no pending tasks", job_id, stage_id))
                    })?;

                let partition = PartitionId {
                    job_id,
                    stage_id: *stage_id,
                    partition_id,
                };

                // Set the status to Running
                stage.task_statuses[partition_id] = Some(task_status::Status::Running(RunningTask {
                    executor_id: executor_id.to_owned()
                }));

                Ok(Task {
                    session_id,
                    partition,
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

    /// Reset the status for the given task. This should be called is a task failed to
    /// launch and it needs to be returned to the set of available tasks and be
    /// re-scheduled.
    pub fn reset_task_status(&mut self, task: Task) {
        let stage_id = task.partition.stage_id;
        let partition = task.partition.partition_id;

        if let Some(ExecutionStage::Running(stage)) = self.stages.get_mut(&stage_id) {
            stage.task_statuses[partition] = None;
        }
    }

    pub fn output_locations(&self) -> Vec<PartitionLocation> {
        self.output_locations.clone()
    }

    /// Reset running and completed stages on a given executor
    /// This will first check the unresolved/resolved/running stages and reset the running tasks and completed tasks.
    /// Then it will check the completed stage and whether there are running parent stages need to read shuffle from it.
    /// If yes, reset the complete tasks and roll back the resolved shuffle recursively.
    ///
    /// Returns the reset stage ids
    pub fn reset_stages(&mut self, executor_id: &str) -> Result<HashSet<usize>> {
        let mut reset = HashSet::new();
        loop {
            let reset_stage = self.reset_stages_internal(executor_id)?;
            if !reset_stage.is_empty() {
                reset.extend(reset_stage.iter());
            } else {
                return Ok(reset);
            }
        }
    }

    fn reset_stages_internal(&mut self, executor_id: &str) -> Result<HashSet<usize>> {
        let mut reset_stage = HashSet::new();
        let job_id = self.job_id.clone();
        let mut stage_events = vec![];
        let mut resubmit_inputs: HashSet<usize> = HashSet::new();
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
                            reset_stage.insert(*stage_id);
                        }
                        &mut stage.inputs
                    }
                    _ => &mut empty_inputs
                };

                // For each stage input, check whether there are input locations match that executor
                // and calculate the resubmit input stages if the input stages are completed.
                let mut rollback_stage = false;
                stage_inputs.iter_mut().for_each(|(input_stage_id, stage_output)| {
                    let mut match_found = false;
                    stage_output.partition_locations.iter_mut().for_each(
                        |(_partition, locs)| {
                            let indexes = locs
                                .iter()
                                .enumerate()
                                .filter_map(|(idx, loc)| {
                                    (loc.executor_meta.id == executor_id).then_some(idx)
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
                            stage_events.push(StageEvent::RollBackResolvedStage(*stage_id));
                            warn!(
                            "Roll back resolved job/stage {}/{} and change ShuffleReaderExec back to UnresolvedShuffleExec",
                            job_id, stage_id);
                            reset_stage.insert(*stage_id);
                        },
                        ExecutionStage::Running(_) => {
                            stage_events.push(StageEvent::RollBackRunningStage(*stage_id));
                            warn!(
                            "Roll back running job/stage {}/{} and change ShuffleReaderExec back to UnresolvedShuffleExec",
                            job_id, stage_id);
                            reset_stage.insert(*stage_id);
                        },
                        _ => {},
                    }
                }
            });

        // check and reset the complete stages
        if !resubmit_inputs.is_empty() {
            self.stages
                .iter_mut()
                .filter(|(stage_id, _stage)| resubmit_inputs.contains(stage_id))
                .filter_map(|(_stage_id, stage)| {
                    if let ExecutionStage::Completed(completed) = stage {
                        Some(completed)
                    } else {
                        None
                    }
                })
                .for_each(|stage| {
                    let reset = stage.reset_tasks(executor_id);
                    if reset > 0 {
                        stage_events
                            .push(StageEvent::ReRunCompletedStage(stage.stage_id));
                        reset_stage.insert(stage.stage_id);
                        warn!(
                            "Reset {} tasks for completed job/stage {}/{} on lost Executor {}",
                            reset, job_id, stage.stage_id, executor_id
                        )
                    }
                });
        }
        self.processing_stage_events(stage_events)?;
        Ok(reset_stage)
    }

    /// Processing stage events for stage state changing
    pub fn processing_stage_events(
        &mut self,
        events: Vec<StageEvent>,
    ) -> Result<Option<QueryStageSchedulerEvent>> {
        let mut has_resolved = false;
        let mut job_err_msg = "".to_owned();
        for event in events {
            match event {
                StageEvent::StageResolved(stage_id) => {
                    self.resolve_stage(stage_id)?;
                    has_resolved = true;
                }
                StageEvent::StageCompleted(stage_id) => {
                    self.complete_stage(stage_id);
                }
                StageEvent::StageFailed(stage_id, err_msg) => {
                    job_err_msg = format!("{}{}\n", job_err_msg, &err_msg);
                    self.fail_stage(stage_id, err_msg);
                }
                StageEvent::RollBackRunningStage(stage_id) => {
                    self.rollback_running_stage(stage_id)?;
                }
                StageEvent::RollBackResolvedStage(stage_id) => {
                    self.rollback_resolved_stage(stage_id)?;
                }
                StageEvent::ReRunCompletedStage(stage_id) => {
                    self.rerun_completed_stage(stage_id);
                }
            }
        }

        let event = if !job_err_msg.is_empty() {
            // If this ExecutionGraph is complete, fail it
            info!("Job {} is failed", self.job_id());
            self.fail_job(job_err_msg);

            Some(QueryStageSchedulerEvent::JobRunningFailed(
                self.job_id.clone(),
            ))
        } else if self.complete() {
            // If this ExecutionGraph is complete, finalize it
            info!(
                "Job {} is complete, finalizing output partitions",
                self.job_id()
            );
            self.complete_job()?;
            Some(QueryStageSchedulerEvent::JobFinished(self.job_id.clone()))
        } else if has_resolved {
            Some(QueryStageSchedulerEvent::JobUpdated(self.job_id.clone()))
        } else {
            None
        };

        Ok(event)
    }

    /// Convert unresolved stage to be resolved
    fn resolve_stage(&mut self, stage_id: usize) -> Result<bool> {
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

    /// Convert running stage to be completed
    fn complete_stage(&mut self, stage_id: usize) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Completed(stage.to_completed()));
            true
        } else {
            warn!(
                "Fail to find a running stage {}/{} to complete",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// Convert running stage to be failed
    fn fail_stage(&mut self, stage_id: usize, err_msg: String) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Failed(stage.to_failed(err_msg)));
            true
        } else {
            warn!(
                "Fail to find a running stage {}/{} to fail",
                self.job_id(),
                stage_id
            );
            false
        }
    }

    /// Convert running stage to be unresolved
    fn rollback_running_stage(&mut self, stage_id: usize) -> Result<bool> {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::UnResolved(stage.to_unresolved()?));
            Ok(true)
        } else {
            warn!(
                "Fail to find a running stage {}/{} to rollback",
                self.job_id(),
                stage_id
            );
            Ok(false)
        }
    }

    /// Convert resolved stage to be unresolved
    fn rollback_resolved_stage(&mut self, stage_id: usize) -> Result<bool> {
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

    /// Convert completed stage to be running
    fn rerun_completed_stage(&mut self, stage_id: usize) -> bool {
        if let Some(ExecutionStage::Completed(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Running(stage.to_running()));
            true
        } else {
            warn!(
                "Fail to find a completed stage {}/{} to rerun",
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

    /// finalize job as completed
    fn complete_job(&mut self) -> Result<()> {
        if !self.complete() {
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
            status: Some(job_status::Status::Completed(CompletedJob {
                partition_location,
            })),
        };

        Ok(())
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
                StageType::CompletedStage(stage) => {
                    let stage: CompletedStage =
                        CompletedStage::decode(stage, codec, session_ctx)?;
                    (stage.stage_id, ExecutionStage::Completed(stage))
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
                    ExecutionStage::Completed(stage) => StageType::CompletedStage(
                        CompletedStage::encode(job_id.clone(), stage, codec)?,
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

        Ok(protobuf::ExecutionGraph {
            job_id: graph.job_id,
            session_id: graph.session_id,
            status: Some(graph.status),
            stages,
            output_partitions: graph.output_partitions as u64,
            output_locations,
            scheduler_id: graph.scheduler_id,
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
        write!(f, "ExecutionGraph[job_id={}, session_id={}, available_tasks={}, complete={}]\n{}",
               self.job_id, self.session_id, self.available_tasks(), self.complete(), stages)
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

#[derive(Clone)]
pub enum StageEvent {
    StageResolved(usize),
    StageCompleted(usize),
    StageFailed(usize, String),
    RollBackRunningStage(usize),
    RollBackResolvedStage(usize),
    ReRunCompletedStage(usize),
}

/// Represents the basic unit of work for the Ballista executor. Will execute
/// one partition of one stage on one task slot.
#[derive(Clone)]
pub struct Task {
    pub session_id: String,
    pub partition: PartitionId,
    pub plan: Arc<dyn ExecutionPlan>,
    pub output_partitioning: Option<Partitioning>,
}

impl Debug for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent();
        write!(
            f,
            "Task[session_id: {}, job: {}, stage: {}, partition: {}]\n{}",
            self.session_id,
            self.partition.job_id,
            self.partition.stage_id,
            self.partition.partition_id,
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

    use crate::state::execution_graph::{ExecutionGraph, Task};

    #[tokio::test]
    async fn test_drain_tasks() -> Result<()> {
        let mut agg_graph = test_aggregation_plan(4).await;

        println!("Graph: {:?}", agg_graph);

        drain_tasks(&mut agg_graph)?;

        assert!(agg_graph.complete(), "Failed to complete aggregation plan");

        let mut coalesce_graph = test_coalesce_plan(4).await;

        drain_tasks(&mut coalesce_graph)?;

        assert!(
            coalesce_graph.complete(),
            "Failed to complete coalesce plan"
        );

        let mut join_graph = test_join_plan(4).await;

        drain_tasks(&mut join_graph)?;

        println!("{:?}", join_graph);

        assert!(join_graph.complete(), "Failed to complete join plan");

        let mut union_all_graph = test_union_all_plan(4).await;

        drain_tasks(&mut union_all_graph)?;

        println!("{:?}", union_all_graph);

        assert!(union_all_graph.complete(), "Failed to complete union plan");

        let mut union_graph = test_union_plan(4).await;

        drain_tasks(&mut union_graph)?;

        println!("{:?}", union_graph);

        assert!(union_graph.complete(), "Failed to complete union plan");

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
                status: Some(job_status::Status::Completed(_))
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
            join_graph.update_task_status(&executor1, vec![task_status])?;
        }

        // Complete the second stage
        if let Some(task) = join_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            join_graph.update_task_status(&executor2, vec![task_status])?;
        }

        join_graph.revive();
        // There are 4 tasks pending schedule for the 3rd stage
        assert_eq!(join_graph.available_tasks(), 4);

        // Complete 1 task
        if let Some(task) = join_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            join_graph.update_task_status(&executor1, vec![task_status])?;
        }
        // Mock 1 running task
        let _task = join_graph.pop_next_task(&executor1.id)?;

        let reset = join_graph.reset_stages(&executor1.id)?;

        // Two stages were reset, 1 Running stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.len(), 2);
        assert_eq!(join_graph.available_tasks(), 1);

        drain_tasks(&mut join_graph)?;
        assert!(join_graph.complete(), "Failed to complete join plan");

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
            join_graph.update_task_status(&executor1, vec![task_status])?;
        }

        // Complete the second stage
        if let Some(task) = join_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            join_graph.update_task_status(&executor2, vec![task_status])?;
        }

        // There are 0 tasks pending schedule now
        assert_eq!(join_graph.available_tasks(), 0);

        let reset = join_graph.reset_stages(&executor1.id)?;

        // Two stages were reset, 1 Resolved stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.len(), 2);
        assert_eq!(join_graph.available_tasks(), 1);

        drain_tasks(&mut join_graph)?;
        assert!(join_graph.complete(), "Failed to complete join plan");

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
            agg_graph.update_task_status(&executor1, vec![task_status])?;
        }

        // 1st task in the second stage
        if let Some(task) = agg_graph.pop_next_task(&executor2.id)? {
            let task_status = mock_completed_task(task, &executor2.id);
            agg_graph.update_task_status(&executor2, vec![task_status])?;
        }

        // 2rd task in the second stage
        if let Some(task) = agg_graph.pop_next_task(&executor1.id)? {
            let task_status = mock_completed_task(task, &executor1.id);
            agg_graph.update_task_status(&executor1, vec![task_status])?;
        }

        // 3rd task in the second stage, scheduled but not completed
        let task = agg_graph.pop_next_task(&executor1.id)?;

        // There is 1 task pending schedule now
        assert_eq!(agg_graph.available_tasks(), 1);

        let reset = agg_graph.reset_stages(&executor1.id)?;

        // 3rd task status update comes later.
        let task_status = mock_completed_task(task.unwrap(), &executor1.id);
        agg_graph.update_task_status(&executor1, vec![task_status])?;

        // Two stages were reset, 1 Running stage rollback to Unresolved and 1 Completed stage move to Running
        assert_eq!(reset.len(), 2);
        assert_eq!(agg_graph.available_tasks(), 1);

        // Call the reset again
        let reset = agg_graph.reset_stages(&executor1.id)?;
        assert_eq!(reset.len(), 0);
        assert_eq!(agg_graph.available_tasks(), 1);

        drain_tasks(&mut agg_graph)?;
        assert!(agg_graph.complete(), "Failed to complete agg plan");

        Ok(())
    }

    fn drain_tasks(graph: &mut ExecutionGraph) -> Result<()> {
        let executor = mock_executor("executor-id1".to_string());
        while let Some(task) = graph.pop_next_task(&executor.id)? {
            let task_status = mock_completed_task(task, &executor.id);
            graph.update_task_status(&executor, vec![task_status])?;
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

    fn mock_completed_task(task: Task, executor_id: &str) -> TaskStatus {
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
            status: Some(task_status::Status::Completed(protobuf::CompletedTask {
                executor_id: executor_id.to_owned(),
                partitions,
            })),
            metrics: vec![],
            task_id: Some(protobuf::PartitionId {
                job_id: task.partition.job_id.clone(),
                stage_id: task.partition.stage_id as u32,
                partition_id: task.partition.partition_id as u32,
            }),
        }
    }
}
