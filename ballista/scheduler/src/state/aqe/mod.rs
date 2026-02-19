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

use crate::display::print_stage_metrics;
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::timestamp_millis;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::execution_graph::{
    ExecutionGraph, ExecutionGraphBox, ExecutionStage, ResolvedStage, RunningTaskInfo,
    StageOutput,
};
use crate::state::execution_stage::RunningStage;
use crate::state::task_manager::UpdatedStages;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    FailedJob, FailedTask, JobStatus, ResultLost, RunningJob, SuccessfulJob, TaskStatus,
    job_status, task_status,
};
use ballista_core::serde::scheduler::{ExecutorMetadata, PartitionLocation};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::vec;

mod adapter;
mod execution_plan;
pub mod optimizer_rule;
pub mod planner;
#[cfg(test)]
mod test;

/// Adaptive Execution Graph
///
/// Implementation of [ExecutionGraph] which computes DAG in runtime, hance
/// it can add, remove or change stages based on collected statistics.
///
/// It has been implemented on top of [AdaptivePlanner] which runs set of predefined
/// optimizers.
///
/// Current implementation covers happy day scenarios, such as:
/// - join reordering
/// - elimination of stages which do not produce data (initial implementation)
/// - ...
///
/// with many limitations, such as:
///
/// - it does not cover executor failure
/// - dynamically coalescing shuffle partitions, not supported yet
/// - does not switch from hash join to sort merge join
/// - does not switch from streaming aggregation to hash aggregation
/// - ...
///
/// Those functionalities are yet to be implemented.
///
/// Note, this [AdaptiveExecutionGraph] is a fork of [crate::state::execution_graph::StaticExecutionGraph],
/// with changes needed to support adaptive execution. Notable change that at the moment
/// there is not need for [ExecutionStage::UnResolved] as stages are [ExecutionStage::Resolved]
/// when they are returned from [AdaptivePlanner].
#[derive(Debug, Clone)]
pub(crate) struct AdaptiveExecutionGraph {
    /// Curator scheduler name. Can be `None` is `ExecutionGraph` is not currently curated by any scheduler
    #[allow(dead_code)] // not used at the moment, will be used later
    scheduler_id: Option<String>,
    /// Adaptive Planner to be used with this execution graph
    planner: AdaptivePlanner,
    /// ID for this job
    job_id: String,
    /// Job name, can be empty string
    job_name: String,
    /// Session ID for this job
    session_id: String,
    /// Status of this job
    status: JobStatus,
    /// Timestamp of when this job was submitted
    queued_at: u64,
    /// Job start time
    start_time: u64,
    /// Job end time
    end_time: u64,
    /// Map from Stage ID -> ExecutionStage
    stages: HashMap<usize, ExecutionStage>,

    /// Locations of this `ExecutionGraph` final output locations
    output_locations: Vec<PartitionLocation>,
    /// Task ID generator, generate unique TID in the execution graph
    task_id_gen: usize,
    /// Failed stage attempts, record the failed stage attempts to limit the retry times.
    /// Map from Stage ID -> Set<Stage_ATTPMPT_NUM>
    failed_stage_attempts: HashMap<usize, HashSet<usize>>,
    /// Session config for this job
    session_config: Arc<SessionConfig>,
}

impl AdaptiveExecutionGraph {
    /// Creates a new `ExecutionGraph` from a physical execution plan.
    ///
    /// This will use the `DistributedPlanner` to break the plan into stages
    /// and build the DAG structure needed for distributed execution.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        scheduler_id: &str,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
        session_config: Arc<SessionConfig>,
    ) -> ballista_core::error::Result<Self> {
        let mut planner =
            AdaptivePlanner::try_new(&session_config, plan, job_name.to_owned())?;

        //let stages = HashMap::new();
        let started_at = timestamp_millis();

        // initial plans should have at least one stage to run,
        // also, there should be no stages to cancel at this point
        let (stages_to_run, stages_to_cancel) = planner.actionable_stages()?;
        let runnable = stages_to_run.ok_or(BallistaError::General(format!(
            "provided plan does not have any stages to run! job_id: {}",
            job_id
        )))?;

        if !stages_to_cancel.is_empty() {
            Err(BallistaError::General(format!(
                "provided plan should not have stages to cancel at this point! job_id: {}",
                job_id
            )))?
        }

        let stages: ballista_core::error::Result<HashMap<usize, ExecutionStage>> =
            runnable
                .into_iter()
                .map(|s| Self::create_resolved_stage(session_config.clone(), s.plan))
                .collect();
        let stages = stages?;

        Ok(Self {
            planner,
            scheduler_id: Some(scheduler_id.to_string()),
            job_id: job_id.to_string(),
            job_name: job_name.to_string(),
            session_id: session_id.to_string(),

            status: JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.to_string(),
                status: Some(Status::Running(RunningJob {
                    queued_at,
                    started_at,
                    scheduler: scheduler_id.to_string(),
                })),
            },
            queued_at,
            start_time: started_at,
            end_time: 0,
            stages,
            output_locations: vec![],
            task_id_gen: 0,
            failed_stage_attempts: HashMap::new(),
            session_config,
        })
    }
}

impl AdaptiveExecutionGraph {
    fn create_resolved_stage(
        session_config: Arc<SessionConfig>,
        stage: Arc<ShuffleWriterExec>,
    ) -> ballista_core::error::Result<(usize, ExecutionStage)> {
        let stage_id = stage.stage_id();
        let stage = ExecutionStage::Resolved(ResolvedStage::new(
            stage_id,
            0,
            stage,
            vec![],         // we do not know output links at this moment
            HashMap::new(), // we do not keep inputs at the moment
            HashSet::new(),
            session_config,
        ));

        Ok((stage_id, stage))
    }

    #[cfg(test)]
    fn next_task_id(&mut self) -> usize {
        let new_tid = self.task_id_gen;
        self.task_id_gen += 1;
        new_tid
    }

    /// Processing stage status update after task status changing
    fn processing_stages_update(
        &mut self,
        updated_stages: UpdatedStages,
    ) -> ballista_core::error::Result<Vec<QueryStageSchedulerEvent>> {
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
                format!("Job failed due to stage {stage_id} failed: {err_msg}\n");
        }

        let mut events = vec![];
        // Only handle the rollback logic when there are no failed stages
        if updated_stages.failed_stages.is_empty() {
            let mut running_tasks_to_cancel = vec![];
            for (stage_id, failure_reasons) in updated_stages.rollback_running_stages {
                let tasks = self.rollback_running_stage(stage_id, failure_reasons)?;
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
            info!("Job {job_id} is failed");
            self.fail_job(job_err_msg.clone());
            events.push(QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message: job_err_msg,
                queued_at: self.queued_at,
                failed_at: timestamp_millis(),
            });
        } else if self.is_successful() {
            // If this ExecutionGraph is successful, finish it
            info!("Job {job_id} is success, finalizing output partitions");
            self.succeed_job()?;
            events.push(QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at: self.queued_at,
                completed_at: timestamp_millis(),
            });
        } else if has_resolved {
            events.push(QueryStageSchedulerEvent::JobUpdated(job_id))
        }
        Ok(events)
    }

    /// Return a Vec of stages to cancel
    fn update_stage_progress(
        &mut self,
        stage_id: usize,
        is_completed: bool,
        locations: Vec<PartitionLocation>,
    ) -> ballista_core::error::Result<HashSet<usize>> {
        self.planner
            .update_exchange_locations(stage_id, locations)?;

        if is_completed {
            let locations = self.planner.finalise_stage(stage_id)?;

            let (runnable, stages_to_cancel) = self.planner.actionable_stages()?;

            if let Some(runnable_stages) = runnable {
                // at the moment planner returns all runnable stages
                // but some of them could already be running, so we need to
                // filter them at the moment
                //
                // TODO: mark stage as running to avoid returning it back
                for stage in runnable_stages {
                    if !self.stages.contains_key(&stage.plan.stage_id()) {
                        let (stage_id, stage) = Self::create_resolved_stage(
                            self.session_config.clone(),
                            stage.plan,
                        )?;
                        self.stages.insert(stage_id, stage);
                    }
                }
            } else {
                // There is no more tasks to run
                // we update output locations
                self.output_locations = locations.into_iter().flatten().collect();
            }
            // marking stages which need cancelling as canceled.
            // stage ids are returned for task cancellation action
            for stage_id in stages_to_cancel.iter() {
                self.planner.cancel_stage(*stage_id)?;
            }

            Ok(stages_to_cancel)
        } else {
            Ok(HashSet::new())
        }
    }

    fn get_running_stage_id(&mut self, black_list: &[usize]) -> Option<usize> {
        let mut running_stage_id = self.stages.iter().find_map(|(stage_id, stage)| {
            if black_list.contains(stage_id) {
                None
            } else if let ExecutionStage::Running(stage) = stage {
                if stage.available_tasks() > 0 {
                    Some(*stage_id)
                } else {
                    None
                }
            } else {
                None
            }
        });

        // If no available tasks found in the running stage,
        // try to find a resolved stage and convert it to the running stage
        if running_stage_id.is_none() {
            if self.revive() {
                running_stage_id = self.get_running_stage_id(black_list);
            } else {
                running_stage_id = None;
            }
        }

        running_stage_id
    }

    fn reset_stages_internal(
        &mut self,
        executor_id: &str,
    ) -> ballista_core::error::Result<(HashSet<usize>, Vec<RunningTaskInfo>)> {
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
                        "Reset {reset} tasks for running job/stage {job_id}/{stage_id} on lost Executor {executor_id}"
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
                            let before_len = locs.len();
                            locs.retain(|loc| loc.executor_meta.id != executor_id);
                            if locs.len() < before_len {
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
                            "Roll back resolved job/stage {job_id}/{stage_id} and change ShuffleReaderExec back to UnresolvedShuffleExec");
                        }
                        ExecutionStage::Running(_) => {
                            rollback_running_stages.insert(*stage_id);
                            warn!(
                            "Roll back running job/stage {job_id}/{stage_id} and change ShuffleReaderExec back to UnresolvedShuffleExec");
                        }
                        _ => {}
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
            let tasks = self.rollback_running_stage(
                *stage_id,
                HashSet::from([executor_id.to_owned()]),
            )?;
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

    /// Clear the stage failure count for this stage if the stage is finally success
    fn clear_stage_failure(&mut self, stage_id: usize) {
        self.failed_stage_attempts.remove(&stage_id);
    }
}

impl ExecutionGraph for AdaptiveExecutionGraph {
    fn cloned(&self) -> ExecutionGraphBox {
        Box::new(self.clone())
    }

    fn job_id(&self) -> &str {
        self.job_id.as_str()
    }

    fn job_name(&self) -> &str {
        self.job_name.as_str()
    }

    fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    fn status(&self) -> &JobStatus {
        &self.status
    }

    fn start_time(&self) -> u64 {
        self.start_time
    }

    fn end_time(&self) -> u64 {
        self.end_time
    }

    fn completed_stages(&self) -> usize {
        let mut completed_stages = 0;
        for stage in self.stages.values() {
            if let ExecutionStage::Successful(_) = stage {
                completed_stages += 1;
            }
        }
        completed_stages
    }
    /// An ExecutionGraph is successful if all its stages are successful
    fn is_successful(&self) -> bool {
        self.stages
            .values()
            .all(|s| matches!(s, ExecutionStage::Successful(_)))
    }

    // pub fn is_complete(&self) -> bool {
    //     self.stages
    //         .values()
    //         .all(|s| matches!(s, ExecutionStage::Successful(_)))
    // }

    /// Revive the execution graph by converting the resolved stages to running stages
    /// If any stages are converted, return true; else false.
    fn revive(&mut self) -> bool {
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
    fn update_task_status(
        &mut self,
        executor: &ExecutorMetadata,
        task_statuses: Vec<TaskStatus>,
        max_task_failures: usize,
        max_stage_failures: usize,
    ) -> ballista_core::error::Result<Vec<QueryStageSchedulerEvent>> {
        let job_id = self.job_id().to_owned();

        // First of all, classify the statuses by stages
        let mut job_task_statuses: HashMap<usize, Vec<TaskStatus>> = HashMap::new();
        for task_status in task_statuses {
            let stage_id = task_status.stage_id as usize;
            let stage_task_statuses = job_task_statuses.entry(stage_id).or_default();
            stage_task_statuses.push(task_status);
        }

        // Revive before updating due to some updates not saved
        // It will be refined later
        self.revive();

        // let current_running_stages: HashSet<usize> =
        //     HashSet::from_iter(self.running_stages());

        // Copy the failed stage attempts from self
        let mut failed_stage_attempts: HashMap<usize, HashSet<usize>> = HashMap::new();
        for (stage_id, attempts) in self.failed_stage_attempts.iter() {
            failed_stage_attempts
                .insert(*stage_id, HashSet::from_iter(attempts.iter().copied()));
        }

        let mut successful_stages = HashSet::new();
        let mut failed_stages = HashMap::new();
        let mut rollback_running_stages = HashMap::new();
        let mut resubmit_successful_stages: HashMap<usize, HashSet<usize>> =
            HashMap::new();
        let reset_running_stages: HashMap<usize, HashSet<usize>> = HashMap::new();

        //
        //
        //

        for (stage_id, stage_task_statuses) in job_task_statuses {
            if let Some(stage) = self.stages.get_mut(&stage_id) {
                //
                //
                //

                if let ExecutionStage::Running(running_stage) = stage {
                    let mut locations = vec![];
                    for task_status in stage_task_statuses.into_iter() {
                        let task_stage_attempt_num =
                            task_status.stage_attempt_num as usize;
                        if task_stage_attempt_num < running_stage.stage_attempt_num {
                            warn!(
                                "Ignore TaskStatus update with TID {} as it's from Stage {}.{} and there is a more recent stage attempt {}.{} running",
                                task_status.task_id,
                                stage_id,
                                task_stage_attempt_num,
                                stage_id,
                                running_stage.stage_attempt_num
                            );
                            continue;
                        }
                        let partition_id = task_status.clone().partition_id as usize;
                        let task_identity = format!(
                            "TID {} {}/{}.{}/{}",
                            task_status.task_id,
                            job_id,
                            stage_id,
                            task_stage_attempt_num,
                            partition_id
                        );
                        let operator_metrics = task_status.metrics.clone();

                        if !running_stage
                            .update_task_info(partition_id, task_status.clone())
                        {
                            continue;
                        }
                        //
                        // handle task failure
                        //
                        if let Some(task_status::Status::Failed(failed_task)) =
                            task_status.status
                        {
                            let failed_reason = failed_task.failed_reason;

                            match failed_reason {
                                Some(FailedReason::FetchPartitionError(
                                    fetch_partition_error,
                                )) => {
                                    let failed_attempts = failed_stage_attempts
                                        .entry(stage_id)
                                        .or_default();
                                    failed_attempts.insert(task_stage_attempt_num);
                                    if failed_attempts.len() < max_stage_failures {
                                        let map_stage_id =
                                            fetch_partition_error.map_stage_id as usize;
                                        let map_partition_id = fetch_partition_error
                                            .map_partition_id
                                            as usize;
                                        let executor_id =
                                            fetch_partition_error.executor_id;

                                        if !failed_stages.is_empty() {
                                            let error_msg = format!(
                                                "Stages was marked failed, ignore FetchPartitionError from task {task_identity}"
                                            );
                                            warn!("{error_msg}");
                                        } else {
                                            // There are different removal strategies here.
                                            // We can choose just remove the map_partition_id in the FetchPartitionError, when resubmit the input stage, there are less tasks
                                            // need to rerun, but this might miss many more bad input partitions, lead to more stage level retries in following.
                                            // Here we choose remove all the bad input partitions which match the same executor id in this single input stage.
                                            // There are other more aggressive approaches, like considering the executor is lost and check all the running stages in this graph.
                                            // Or count the fetch failure number on executor and mark the executor lost globally.
                                            let removed_map_partitions = running_stage
                                                .remove_input_partitions(
                                                    map_stage_id,
                                                    map_partition_id,
                                                    &executor_id,
                                                )?;

                                            let failure_reasons = rollback_running_stages
                                                .entry(stage_id)
                                                .or_insert_with(HashSet::new);
                                            failure_reasons.insert(executor_id);

                                            let missing_inputs =
                                                resubmit_successful_stages
                                                    .entry(map_stage_id)
                                                    .or_default();
                                            missing_inputs.extend(removed_map_partitions);
                                            warn!(
                                                "Need to resubmit the current running Stage {stage_id} and its map Stage {map_stage_id} due to FetchPartitionError from task {task_identity}"
                                            )
                                        }
                                    } else {
                                        let error_msg = format!(
                                            "Stage {} has failed {} times, \
                                            most recent failure reason: {:?}",
                                            stage_id,
                                            max_stage_failures,
                                            failed_task.error
                                        );
                                        error!("{error_msg}");
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
                                        if running_stage.task_failure_number(partition_id)
                                            < max_task_failures
                                        {
                                            // TODO add new struct to track all the failed task infos
                                            // The failure TaskInfo is ignored and set to None here
                                            running_stage.reset_task_info(partition_id);
                                        } else {
                                            let error_msg = format!(
                                                "Task {} in Stage {} failed {} times, fail the stage, most recent failure reason: {:?}",
                                                partition_id,
                                                stage_id,
                                                max_task_failures,
                                                failed_task.error
                                            );
                                            error!("{error_msg}");
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
                                        "Task {partition_id} in Stage {stage_id} failed with unknown failure reasons, fail the stage"
                                    );
                                    error!("{error_msg}");
                                    failed_stages.insert(stage_id, error_msg);
                                }
                            }
                        }
                        //
                        // handle successful task
                        //
                        else if let Some(task_status::Status::Successful(
                            successful_task,
                        )) = task_status.status
                        {
                            // update task metrics for successfu task
                            running_stage
                                .update_task_metrics(partition_id, operator_metrics)?;

                            locations.append(
                                &mut crate::state::execution_graph::partition_to_location(
                                    &job_id,
                                    partition_id,
                                    stage_id,
                                    executor,
                                    successful_task.partitions,
                                ),
                            );
                        } else {
                            warn!(
                                "The task {task_identity}'s status is invalid for updating"
                            );
                        }
                    }

                    //
                    // all task updates have been handled
                    //

                    let is_final_successful = running_stage.is_successful()
                        && !reset_running_stages.contains_key(&stage_id);

                    if is_final_successful {
                        successful_stages.insert(stage_id);
                        // if this stage is final successful, we want to combine
                        // the stage metrics to plan's metric set and print out the plan
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
                    let stages_to_cancel = self.update_stage_progress(
                        stage_id,
                        is_final_successful,
                        locations,
                    )?;

                    if !stages_to_cancel.is_empty() {
                        warn!(
                            "there are stages to be cancelled but its not implemented. stages to cancel: {:?}",
                            stages_to_cancel
                        );
                    }
                } else {
                    warn!(
                        "Stage {}/{} is not in running when updating the status of tasks {:?}",
                        job_id,
                        stage_id,
                        stage_task_statuses
                            .into_iter()
                            .map(|task_status| task_status.partition_id)
                            .collect::<Vec<_>>(),
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}"
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
                        if *partition > success_stage.partitions {
                            return Err(BallistaError::Internal(format!(
                                "Invalid partition ID {} in map stage {}",
                                *partition, stage_id
                            )));
                        }
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
                        "Stage {job_id}/{stage_id} is not in Successful state when try to resubmit this stage. "
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}"
                )));
            }
        }

        for (stage_id, missing_parts) in &reset_running_stages {
            if let Some(stage) = self.stages.get_mut(stage_id) {
                if let ExecutionStage::Running(running_stage) = stage {
                    for partition in missing_parts {
                        if *partition > running_stage.partitions {
                            return Err(BallistaError::Internal(format!(
                                "Invalid partition ID {} in map stage {}",
                                *partition, stage_id
                            )));
                        }
                        running_stage.reset_task_info(*partition);
                    }
                } else {
                    warn!(
                        "Stage {job_id}/{stage_id} is not in Running state when try to reset the running task. "
                    );
                }
            } else {
                return Err(BallistaError::Internal(format!(
                    "Invalid stage ID {stage_id} for job {job_id}"
                )));
            }
        }

        self.processing_stages_update(UpdatedStages {
            resolved_stages: Default::default(),
            successful_stages,
            failed_stages,
            rollback_running_stages,
            resubmit_successful_stages: resubmit_successful_stages
                .keys()
                .cloned()
                .collect(),
        })
    }

    /// Return all the currently running stage ids
    fn running_stages(&self) -> Vec<usize> {
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
    fn running_tasks(&self) -> Vec<RunningTaskInfo> {
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
    fn available_tasks(&self) -> usize {
        self.stages
            .values()
            .map(|stage| {
                if let ExecutionStage::Running(stage) = stage {
                    stage.available_tasks()
                } else {
                    0
                }
            })
            .sum()
    }

    fn fetch_running_stage(
        &mut self,
        black_list: &[usize],
    ) -> Option<(&mut RunningStage, &mut usize)> {
        if matches!(
            self.status,
            JobStatus {
                status: Some(job_status::Status::Failed(_)),
                ..
            }
        ) {
            debug!("Call fetch_runnable_stage on failed Job");
            return None;
        }

        let running_stage_id = self.get_running_stage_id(black_list);
        if let Some(running_stage_id) = running_stage_id {
            if let Some(ExecutionStage::Running(running_stage)) =
                self.stages.get_mut(&running_stage_id)
            {
                Some((running_stage, &mut self.task_id_gen))
            } else {
                warn!("Fail to find running stage with id {running_stage_id}");
                None
            }
        } else {
            None
        }
    }

    fn update_status(&mut self, status: JobStatus) {
        self.status = status;
    }

    fn output_locations(&self) -> Vec<PartitionLocation> {
        self.output_locations.clone()
    }

    /// Reset running and successful stages on a given executor
    /// This will first check the unresolved/resolved/running stages and reset the running tasks and successful tasks.
    /// Then it will check the successful stage and whether there are running parent stages need to read shuffle from it.
    /// If yes, reset the successful tasks and roll back the resolved shuffle recursively.
    ///
    /// Returns the reset stage ids and running tasks should be killed
    fn reset_stages_on_lost_executor(
        &mut self,
        executor_id: &str,
    ) -> ballista_core::error::Result<(HashSet<usize>, Vec<RunningTaskInfo>)> {
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

    /// Convert unresolved stage to be resolved
    fn resolve_stage(&mut self, stage_id: usize) -> ballista_core::error::Result<bool> {
        if let Some(ExecutionStage::UnResolved(stage)) = self.stages.remove(&stage_id) {
            self.stages.insert(
                stage_id,
                ExecutionStage::Resolved(
                    stage.to_resolved(self.session_config.options())?,
                ),
            );
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
    fn succeed_stage(&mut self, stage_id: usize) -> bool {
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
    fn fail_stage(&mut self, stage_id: usize, err_msg: String) -> bool {
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
    fn rollback_running_stage(
        &mut self,
        stage_id: usize,
        failure_reasons: HashSet<String>,
    ) -> ballista_core::error::Result<Vec<RunningTaskInfo>> {
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
            self.stages.insert(
                stage_id,
                ExecutionStage::UnResolved(stage.to_unresolved(failure_reasons)?),
            );
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
    fn rollback_resolved_stage(
        &mut self,
        stage_id: usize,
    ) -> ballista_core::error::Result<bool> {
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
    fn rerun_successful_stage(&mut self, stage_id: usize) -> bool {
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
    fn fail_job(&mut self, error: String) {
        self.status = JobStatus {
            job_id: self.job_id.clone(),
            job_name: self.job_name.clone(),
            status: Some(Status::Failed(FailedJob {
                error,
                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
            })),
        };
    }

    /// Mark the job success
    fn succeed_job(&mut self) -> ballista_core::error::Result<()> {
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
            .collect::<ballista_core::error::Result<Vec<_>>>()?;

        self.end_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.status = JobStatus {
            job_id: self.job_id.clone(),
            job_name: self.job_name.clone(),
            status: Some(job_status::Status::Successful(SuccessfulJob {
                partition_location,

                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
            })),
        };

        Ok(())
    }

    fn stages(&self) -> &HashMap<usize, ExecutionStage> {
        &self.stages
    }

    fn stage_count(&self) -> usize {
        self.stages.len()
    }

    /// Get next task that can be assigned to the given executor.
    /// This method should only be called when the resulting task is immediately
    /// being launched as the status will be set to Running and it will not be
    /// available to the scheduler.
    /// If the task is not launched the status must be reset to allow the task to
    /// be scheduled elsewhere.
    #[cfg(test)]
    fn pop_next_task(
        &mut self,
        executor_id: &str,
    ) -> ballista_core::error::Result<Option<super::execution_graph::TaskDescription>>
    {
        if matches!(
            self.status,
            JobStatus {
                status: Some(job_status::Status::Failed(_)),
                ..
            }
        ) {
            warn!("Call pop_next_task on failed Job");
            return Ok(None);
        }

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
                use ballista_core::serde::scheduler::PartitionId;

                let (partition_id, _) = stage
                    .task_infos
                    .iter()
                    .enumerate()
                    .find(|(_partition, info)| info.is_none())
                    .ok_or_else(|| {
                        BallistaError::Internal(format!("Error getting next task for job {job_id}: Stage {stage_id} is ready but has no pending tasks"))
                    })?;

                let partition = PartitionId {
                    job_id,
                    stage_id: *stage_id,
                    partition_id,
                };

                let task_id = next_task_id.unwrap();
                let task_attempt = stage.task_failure_numbers[partition_id];
                let task_info = crate::state::execution_graph::TaskInfo {
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
                    task_status: task_status::Status::Running(ballista_core::serde::protobuf::RunningTask {
                        executor_id: executor_id.to_owned()
                    }),
                };

                // Set the task info to Running for new task
                stage.task_infos[partition_id] = Some(task_info);

                Ok(crate::state::execution_graph::TaskDescription {
                    session_id,
                    partition,
                    stage_attempt_num: stage.stage_attempt_num,
                    task_id,
                    task_attempt,
                    plan: stage.plan.clone(),
                    session_config: self.session_config.clone()
                })
            } else {
                Err(BallistaError::General(format!("Stage {stage_id} is not a running stage")))
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
}

/// Checks is the plan same as expected string representation
#[cfg(test)]
#[macro_export]
macro_rules! assert_plan {
    ($PLAN: expr, @ $EXPECTED_LINES: literal $(,)?) => {
        let plan = datafusion::physical_plan::displayable($PLAN).indent(true).to_string();
        let actual_lines = plan.trim();

        insta::assert_snapshot!(actual_lines, @ $EXPECTED_LINES);
    };
}
