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
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, Metric};
use datafusion::prelude::SessionConfig;
use log::{debug, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::protobuf::{
    FailedTask, OperatorMetricsSet, ResultLost, SuccessfulTask, TaskStatus,
};
use ballista_core::serde::scheduler::PartitionLocation;

use crate::display::DisplayableBallistaExecutionPlan;

/// A stage in the ExecutionGraph,
/// represents a set of tasks (one per each `partition`) which can be executed concurrently.
/// For a stage, there are five states. And the state machine is as follows:
///
/// UnResolvedStage           FailedStage
///       ↓            ↙           ↑
///  ResolvedStage     →     RunningStage
///                                ↓
///                         SuccessfulStage
#[derive(Clone)]
pub(crate) enum ExecutionStage {
    UnResolved(UnresolvedStage),
    Resolved(ResolvedStage),
    Running(RunningStage),
    Successful(SuccessfulStage),
    Failed(FailedStage),
}

impl Debug for ExecutionStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStage::UnResolved(unresolved_stage) => unresolved_stage.fmt(f),
            ExecutionStage::Resolved(resolved_stage) => resolved_stage.fmt(f),
            ExecutionStage::Running(running_stage) => running_stage.fmt(f),
            ExecutionStage::Successful(successful_stage) => successful_stage.fmt(f),
            ExecutionStage::Failed(failed_stage) => failed_stage.fmt(f),
        }
    }
}

impl ExecutionStage {
    /// Get the name of the variant
    pub(crate) fn variant_name(&self) -> &str {
        match self {
            ExecutionStage::UnResolved(_) => "Unresolved",
            ExecutionStage::Resolved(_) => "Resolved",
            ExecutionStage::Running(_) => "Running",
            ExecutionStage::Successful(_) => "Successful",
            ExecutionStage::Failed(_) => "Failed",
        }
    }

    /// Get the query plan for this query stage
    pub(crate) fn plan(&self) -> &dyn ExecutionPlan {
        match self {
            ExecutionStage::UnResolved(stage) => stage.plan.as_ref(),
            ExecutionStage::Resolved(stage) => stage.plan.as_ref(),
            ExecutionStage::Running(stage) => stage.plan.as_ref(),
            ExecutionStage::Successful(stage) => stage.plan.as_ref(),
            ExecutionStage::Failed(stage) => stage.plan.as_ref(),
        }
    }
}

/// For a stage whose input stages are not all completed, we say it's a unresolved stage
#[derive(Clone)]
pub(crate) struct UnresolvedStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    /// This stage can only be resolved an executed once all child stages are completed.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Record last attempt's failure reasons to avoid duplicate resubmits
    pub(crate) last_attempt_failure_reasons: HashSet<String>,

    pub(crate) session_config: Arc<SessionConfig>,
}

/// For a stage, if it has no inputs or all of its input stages are completed,
/// then we call it as a resolved stage
#[derive(Clone)]
pub(crate) struct ResolvedStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Record last attempt's failure reasons to avoid duplicate resubmits
    pub(crate) last_attempt_failure_reasons: HashSet<String>,

    pub(crate) session_config: Arc<SessionConfig>,
}

/// Different from the resolved stage, a running stage will
/// 1. save the execution plan as encoded one to avoid serialization cost for creating task definition
/// 2. manage the task statuses
/// 3. manage the stage-level combined metrics
///    Running stages will only be maintained in memory and will not saved to the backend storage
#[derive(Clone)]
pub(crate) struct RunningStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already scheduled task. If info is None, the partition has not yet been scheduled.
    /// The index of the Vec is the task's partition id
    pub(crate) task_infos: Vec<Option<TaskInfo>>,
    /// Track the number of failures for each partition's task attempts.
    /// The index of the Vec is the task's partition id.
    pub(crate) task_failure_numbers: Vec<usize>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    pub(crate) stage_metrics: Option<Vec<MetricsSet>>,

    pub(crate) session_config: Arc<SessionConfig>,
}

/// If a stage finishes successfully, its task statuses and metrics will be finalized
#[derive(Clone)]
pub(crate) struct SuccessfulStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already successful task.
    /// The index of the Vec is the task's partition id
    pub(crate) task_infos: Vec<TaskInfo>,
    /// Combined metrics of the already finished tasks in the stage.
    pub(crate) stage_metrics: Vec<MetricsSet>,

    pub(crate) session_config: Arc<SessionConfig>,
}

/// If a stage fails, it will be with an error message
#[derive(Clone)]
pub(crate) struct FailedStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Stage Attempt number
    pub(crate) stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    #[allow(dead_code)] // not used at the moment, will be used later
    pub(crate) output_links: Vec<usize>,
    /// `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already scheduled tasks. If info is None, the partition has not yet been scheduled
    /// The index of the Vec is the task's partition id
    pub(crate) task_infos: Vec<Option<TaskInfo>>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    #[allow(dead_code)] // not used at the moment, will be used later
    pub(crate) stage_metrics: Option<Vec<MetricsSet>>,
    /// Error message
    pub(crate) error_message: String,
}

#[derive(Clone)]
#[allow(dead_code)] // we may use the fields later
pub(crate) struct TaskInfo {
    /// Task ID
    pub(super) task_id: usize,
    /// Task scheduled time
    pub(super) scheduled_time: u128,
    /// Task launch time
    pub(super) launch_time: u128,
    /// Start execution time
    pub(super) start_exec_time: u128,
    /// Finish execution time
    pub(super) end_exec_time: u128,
    /// Task finish time
    pub(super) finish_time: u128,
    /// Task Status
    pub(super) task_status: task_status::Status,
    //pub(crate) session_config: Arc<SessionConfig>,
}

impl UnresolvedStage {
    pub(super) fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_links: Vec<usize>,
        child_stage_ids: Vec<usize>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        let mut inputs: HashMap<usize, StageOutput> = HashMap::new();
        for input_stage_id in child_stage_ids {
            inputs.insert(input_stage_id, StageOutput::new());
        }

        Self {
            stage_id,
            stage_attempt_num: 0,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons: Default::default(),
            session_config,
        }
    }

    pub(super) fn new_with_inputs(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        last_attempt_failure_reasons: HashSet<String>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        Self {
            stage_id,
            stage_attempt_num,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons,
            session_config,
        }
    }

    /// Add input partitions published from an input stage.
    pub(super) fn add_input_partitions(
        &mut self,
        stage_id: usize,
        locations: Vec<PartitionLocation>,
    ) -> Result<()> {
        if let Some(stage_inputs) = self.inputs.get_mut(&stage_id) {
            for partition in locations {
                stage_inputs.add_partition(partition);
            }
        } else {
            return Err(BallistaError::Internal(format!("Error adding input partitions to stage {}, {} is not a valid child stage ID", self.stage_id, stage_id)));
        }

        Ok(())
    }

    /// Remove input partitions from an input stage on a given executor.
    /// Return the HashSet of removed map partition ids
    pub(super) fn remove_input_partitions(
        &mut self,
        input_stage_id: usize,
        _input_partition_id: usize,
        executor_id: &str,
    ) -> Result<HashSet<usize>> {
        if let Some(stage_output) = self.inputs.get_mut(&input_stage_id) {
            let mut bad_map_partitions = HashSet::new();
            stage_output
                .partition_locations
                .iter_mut()
                .for_each(|(_partition, locs)| {
                    locs.iter().for_each(|loc| {
                        if loc.executor_meta.id == executor_id {
                            bad_map_partitions.insert(loc.map_partition_id);
                        }
                    });

                    locs.retain(|loc| loc.executor_meta.id != executor_id);
                });
            stage_output.complete = false;
            Ok(bad_map_partitions)
        } else {
            Err(BallistaError::Internal(format!("Error remove input partition for Stage {}, {} is not a valid child stage ID", self.stage_id, input_stage_id)))
        }
    }

    /// Marks the input stage ID as complete.
    pub(super) fn complete_input(&mut self, stage_id: usize) {
        if let Some(input) = self.inputs.get_mut(&stage_id) {
            input.complete = true;
        }
    }

    /// Returns true if all inputs are complete and we can resolve all
    /// UnresolvedShuffleExec operators to ShuffleReadExec
    pub(super) fn resolvable(&self) -> bool {
        self.inputs.iter().all(|(_, input)| input.is_complete())
    }

    /// Change to the resolved state
    pub(super) fn to_resolved(&self) -> Result<ResolvedStage> {
        let input_locations = self
            .inputs
            .iter()
            .map(|(stage, input)| (*stage, input.partition_locations.clone()))
            .collect();
        let plan = crate::planner::remove_unresolved_shuffles(
            self.plan.clone(),
            &input_locations,
        )?;

        // TODO reinstate this logic once https://github.com/apache/datafusion/issues/10978
        // is fixed
        // Optimize join order and statistics based on new resolved statistics
        // let optimize_join = JoinSelection::new();
        // let config = SessionConfig::default();
        // let plan = optimize_join.optimize(plan, config.options())?;

        let optimize_aggregate = AggregateStatistics::new();
        let plan =
            optimize_aggregate.optimize(plan, SessionConfig::default().options())?;

        Ok(ResolvedStage::new(
            self.stage_id,
            self.stage_attempt_num,
            plan,
            self.output_links.clone(),
            self.inputs.clone(),
            self.last_attempt_failure_reasons.clone(),
            self.session_config.clone(),
        ))
    }
}

impl Debug for UnresolvedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========UnResolvedStage[stage_id={}.{}, children={}]=========\nInputs{:?}\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.inputs.len(),
            self.inputs,
            plan
        )
    }
}

impl ResolvedStage {
    pub(super) fn new(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        last_attempt_failure_reasons: HashSet<String>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        let partitions = get_stage_partitions(plan.clone());

        Self {
            stage_id,
            stage_attempt_num,
            partitions,
            output_links,
            inputs,
            plan,
            last_attempt_failure_reasons,
            session_config,
        }
    }

    /// Change to the running state
    pub(super) fn to_running(&self) -> RunningStage {
        RunningStage::new(
            self.stage_id,
            self.stage_attempt_num,
            self.plan.clone(),
            self.partitions,
            self.output_links.clone(),
            self.inputs.clone(),
            self.session_config.clone(),
        )
    }

    /// Change to the unresolved state
    pub(super) fn to_unresolved(&self) -> Result<UnresolvedStage> {
        let new_plan = crate::planner::rollback_resolved_shuffles(self.plan.clone())?;

        let unresolved = UnresolvedStage::new_with_inputs(
            self.stage_id,
            self.stage_attempt_num,
            new_plan,
            self.output_links.clone(),
            self.inputs.clone(),
            self.last_attempt_failure_reasons.clone(),
            self.session_config.clone(),
        );
        Ok(unresolved)
    }
}

impl Debug for ResolvedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========ResolvedStage[stage_id={}.{}, partitions={}]=========\n{}",
            self.stage_id, self.stage_attempt_num, self.partitions, plan
        )
    }
}

impl RunningStage {
    pub(super) fn new(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitions: usize,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        Self {
            stage_id,
            stage_attempt_num,
            partitions,
            output_links,
            inputs,
            plan,
            task_infos: vec![None; partitions],
            task_failure_numbers: vec![0; partitions],
            stage_metrics: None,
            session_config,
        }
    }

    pub(super) fn to_successful(&self) -> SuccessfulStage {
        let task_infos = self
            .task_infos
            .iter()
            .enumerate()
            .map(|(partition_id, info)| {
                info.clone().unwrap_or_else(|| {
                    panic!(
                        "TaskInfo for task {}.{}/{} should not be none",
                        self.stage_id, self.stage_attempt_num, partition_id
                    )
                })
            })
            .collect();
        let stage_metrics = self.stage_metrics.clone().unwrap_or_else(|| {
            warn!("The metrics for stage {} should not be none", self.stage_id);
            vec![]
        });
        SuccessfulStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num,
            partitions: self.partitions,
            output_links: self.output_links.clone(),
            inputs: self.inputs.clone(),
            plan: self.plan.clone(),
            task_infos,
            stage_metrics,
            session_config: self.session_config.clone(),
        }
    }

    pub(super) fn to_failed(&self, error_message: String) -> FailedStage {
        FailedStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num,
            partitions: self.partitions,
            output_links: self.output_links.clone(),
            plan: self.plan.clone(),
            task_infos: self.task_infos.clone(),
            stage_metrics: self.stage_metrics.clone(),
            error_message,
        }
    }

    /// Change to the unresolved state and bump the stage attempt number
    pub(super) fn to_unresolved(
        &self,
        failure_reasons: HashSet<String>,
    ) -> Result<UnresolvedStage> {
        let new_plan = crate::planner::rollback_resolved_shuffles(self.plan.clone())?;

        let unresolved = UnresolvedStage::new_with_inputs(
            self.stage_id,
            self.stage_attempt_num + 1,
            new_plan,
            self.output_links.clone(),
            self.inputs.clone(),
            failure_reasons,
            self.session_config.clone(),
        );
        Ok(unresolved)
    }

    /// Returns `true` if all tasks for this stage are successful
    pub(super) fn is_successful(&self) -> bool {
        self.task_infos.iter().all(|info| {
            matches!(
                info,
                Some(TaskInfo {
                    task_status: task_status::Status::Successful(_),
                    ..
                })
            )
        })
    }

    /// Returns the number of successful tasks
    pub(super) fn successful_tasks(&self) -> usize {
        self.task_infos
            .iter()
            .filter(|info| {
                matches!(
                    info,
                    Some(TaskInfo {
                        task_status: task_status::Status::Successful(_),
                        ..
                    })
                )
            })
            .count()
    }

    /// Returns the number of scheduled tasks
    pub(super) fn scheduled_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_some()).count()
    }

    /// Returns a vector of currently running tasks in this stage
    pub(super) fn running_tasks(&self) -> Vec<(usize, usize, usize, String)> {
        self.task_infos
            .iter()
            .enumerate()
            .filter_map(|(partition, info)| match info {
                Some(TaskInfo {task_id,
                         task_status: task_status::Status::Running(RunningTask { executor_id }), ..}) => {
                    Some((*task_id, self.stage_id, partition, executor_id.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task info is not yet set.
    pub(super) fn available_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_none()).count()
    }

    /// Update the TaskInfo for task partition
    pub(super) fn update_task_info(
        &mut self,
        partition_id: usize,
        status: TaskStatus,
    ) -> bool {
        debug!("Updating TaskInfo for partition {}", partition_id);
        let task_info = self.task_infos[partition_id].as_ref().unwrap();
        let task_id = task_info.task_id;
        if (status.task_id as usize) < task_id {
            warn!("Ignore TaskStatus update with TID {} because there is more recent task attempt with TID {} running for partition {}",
                status.task_id, task_id, partition_id);
            return false;
        }
        let scheduled_time = task_info.scheduled_time;
        let task_status = status.status.unwrap();
        let updated_task_info = TaskInfo {
            task_id,
            scheduled_time,
            launch_time: status.launch_time as u128,
            start_exec_time: status.start_exec_time as u128,
            end_exec_time: status.end_exec_time as u128,
            finish_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            task_status: task_status.clone(),
        };
        self.task_infos[partition_id] = Some(updated_task_info);

        if let task_status::Status::Failed(failed_task) = task_status {
            // if the failed task is retryable, increase the task failure count for this partition
            if failed_task.retryable {
                self.task_failure_numbers[partition_id] += 1;
            }
        } else {
            self.task_failure_numbers[partition_id] = 0;
        }
        true
    }

    /// update and combine the task metrics to the stage metrics
    pub(super) fn update_task_metrics(
        &mut self,
        partition: usize,
        metrics: Vec<OperatorMetricsSet>,
    ) -> Result<()> {
        // For some cases, task metrics not set, especially for testings.
        if metrics.is_empty() {
            return Ok(());
        }

        let new_metrics_set = if let Some(combined_metrics) = &mut self.stage_metrics {
            if metrics.len() != combined_metrics.len() {
                return Err(BallistaError::Internal(format!("Error updating task metrics to stage {}, task metrics array size {} does not equal \
                with the stage metrics array size {} for task {}", self.stage_id, metrics.len(), combined_metrics.len(), partition)));
            }
            let metrics_values_array = metrics
                .into_iter()
                .map(|ms| {
                    ms.metrics
                        .into_iter()
                        .map(|m| m.try_into())
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            combined_metrics
                .iter_mut()
                .zip(metrics_values_array)
                .map(|(first, second)| {
                    Self::combine_metrics_set(first, second, partition)
                })
                .collect()
        } else {
            metrics
                .into_iter()
                .map(|ms| ms.try_into())
                .collect::<Result<Vec<_>>>()?
        };
        self.stage_metrics = Some(new_metrics_set);

        Ok(())
    }

    pub(super) fn combine_metrics_set(
        first: &mut MetricsSet,
        second: Vec<MetricValue>,
        partition: usize,
    ) -> MetricsSet {
        for metric_value in second {
            // TODO recheck the lable logic
            let new_metric = Arc::new(Metric::new(metric_value, Some(partition)));
            first.push(new_metric);
        }
        first.aggregate_by_name()
    }

    pub(super) fn task_failure_number(&self, partition_id: usize) -> usize {
        self.task_failure_numbers[partition_id]
    }

    /// Reset the task info for the given task partition. This should be called when a task failed and need to be
    /// re-scheduled.
    pub fn reset_task_info(&mut self, partition_id: usize) {
        self.task_infos[partition_id] = None;
    }

    /// Reset the running and completed tasks on a given executor
    /// Returns the number of running tasks that were reset
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        for task in self.task_infos.iter_mut() {
            match task {
                Some(TaskInfo {
                    task_status: task_status::Status::Running(RunningTask { executor_id }),
                    ..
                }) if *executor == *executor_id => {
                    *task = None;
                    reset += 1;
                }
                Some(TaskInfo {
                    task_status:
                        task_status::Status::Successful(SuccessfulTask {
                            executor_id,
                            partitions: _,
                        }),
                    ..
                }) if *executor == *executor_id => {
                    *task = None;
                    reset += 1;
                }
                _ => {}
            }
        }
        reset
    }

    /// Remove input partitions from an input stage on a given executor.
    /// Return the HashSet of removed map partition ids
    pub(super) fn remove_input_partitions(
        &mut self,
        input_stage_id: usize,
        _input_partition_id: usize,
        executor_id: &str,
    ) -> Result<HashSet<usize>> {
        if let Some(stage_output) = self.inputs.get_mut(&input_stage_id) {
            let mut bad_map_partitions = HashSet::new();
            stage_output
                .partition_locations
                .iter_mut()
                .for_each(|(_partition, locs)| {
                    locs.iter().for_each(|loc| {
                        if loc.executor_meta.id == executor_id {
                            bad_map_partitions.insert(loc.map_partition_id);
                        }
                    });

                    locs.retain(|loc| loc.executor_meta.id != executor_id);
                });
            stage_output.complete = false;
            Ok(bad_map_partitions)
        } else {
            Err(BallistaError::Internal(format!("Error remove input partition for Stage {}, {} is not a valid child stage ID", self.stage_id, input_stage_id)))
        }
    }
}

impl Debug for RunningStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========RunningStage[stage_id={}.{}, partitions={}, successful_tasks={}, scheduled_tasks={}, available_tasks={}]=========\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.partitions,
            self.successful_tasks(),
            self.scheduled_tasks(),
            self.available_tasks(),
            plan
        )
    }
}

impl SuccessfulStage {
    /// Change to the running state and bump the stage attempt number
    pub fn to_running(&self) -> RunningStage {
        let mut task_infos: Vec<Option<TaskInfo>> = Vec::new();
        for task in self.task_infos.iter() {
            match task {
                TaskInfo {
                    task_status: task_status::Status::Successful(_),
                    ..
                } => task_infos.push(Some(task.clone())),
                _ => task_infos.push(None),
            }
        }
        let stage_metrics = if self.stage_metrics.is_empty() {
            None
        } else {
            Some(self.stage_metrics.clone())
        };
        RunningStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num + 1,
            partitions: self.partitions,
            output_links: self.output_links.clone(),
            inputs: self.inputs.clone(),
            plan: self.plan.clone(),
            task_infos,
            // It is Ok to forget the previous task failure attempts
            task_failure_numbers: vec![0; self.partitions],
            stage_metrics,
            session_config: self.session_config.clone(),
        }
    }

    /// Reset the successful tasks on a given executor
    /// Returns the number of running tasks that were reset
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        let failure_reason = format!("Task failure due to Executor {executor} lost");
        for task in self.task_infos.iter_mut() {
            match task {
                TaskInfo {
                    task_id,
                    scheduled_time,
                    task_status:
                        task_status::Status::Successful(SuccessfulTask {
                            executor_id, ..
                        }),
                    ..
                } if *executor == *executor_id => {
                    *task = TaskInfo {
                        task_id: *task_id,
                        scheduled_time: *scheduled_time,
                        launch_time: 0,
                        start_exec_time: 0,
                        end_exec_time: 0,
                        finish_time: 0,
                        task_status: task_status::Status::Failed(FailedTask {
                            error: failure_reason.clone(),
                            retryable: true,
                            count_to_failures: false,
                            failed_reason: Some(FailedReason::ResultLost(ResultLost {})),
                        }),
                    };
                    reset += 1;
                }
                _ => {}
            }
        }
        reset
    }
}

impl Debug for SuccessfulStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableBallistaExecutionPlan::new(
            self.plan.as_ref(),
            &self.stage_metrics,
        )
        .indent();

        write!(
            f,
            "=========SuccessfulStage[stage_id={}.{}, partitions={}]=========\n{}",
            self.stage_id, self.stage_attempt_num, self.partitions, plan
        )
    }
}

impl FailedStage {
    /// Returns the number of successful tasks
    pub(super) fn successful_tasks(&self) -> usize {
        self.task_infos
            .iter()
            .filter(|info| {
                matches!(
                    info,
                    Some(TaskInfo {
                        task_status: task_status::Status::Successful(_),
                        ..
                    })
                )
            })
            .count()
    }
    /// Returns the number of scheduled tasks
    pub(super) fn scheduled_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_some()).count()
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task status is not yet set.
    pub(super) fn available_tasks(&self) -> usize {
        self.task_infos.iter().filter(|s| s.is_none()).count()
    }
}

impl Debug for FailedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent(false);

        write!(
            f,
            "=========FailedStage[stage_id={}.{}, partitions={}, successful_tasks={}, scheduled_tasks={}, available_tasks={}, error_message={}]=========\n{}",
            self.stage_id,
            self.stage_attempt_num,
            self.partitions,
            self.successful_tasks(),
            self.scheduled_tasks(),
            self.available_tasks(),
            self.error_message,
            plan
        )
    }
}

/// Get the total number of partitions for a stage with plan.
/// Only for [`ShuffleWriterExec`], the input partition count and the output partition count
/// will be different. Here, we should use the input partition count.
fn get_stage_partitions(plan: Arc<dyn ExecutionPlan>) -> usize {
    plan.as_any()
        .downcast_ref::<ShuffleWriterExec>()
        .map(|shuffle_writer| shuffle_writer.input_partition_count())
        .unwrap_or_else(|| plan.properties().output_partitioning().partition_count())
}

/// This data structure collects the partition locations for an `ExecutionStage`.
/// Each `ExecutionStage` will hold a `StageOutput`s for each of its child stages.
/// When all tasks for the child stage are complete, it will mark the `StageOutput`
/// as complete.
#[derive(Clone, Debug, Default)]
pub(crate) struct StageOutput {
    /// Map from partition -> partition locations
    pub partition_locations: HashMap<usize, Vec<PartitionLocation>>,
    /// Flag indicating whether all tasks are complete
    pub complete: bool,
}

impl StageOutput {
    pub(super) fn new() -> Self {
        Self {
            partition_locations: HashMap::new(),
            complete: false,
        }
    }

    /// Add a `PartitionLocation` to the `StageOutput`
    pub(super) fn add_partition(&mut self, partition_location: PartitionLocation) {
        if let Some(parts) = self
            .partition_locations
            .get_mut(&partition_location.partition_id.partition_id)
        {
            parts.push(partition_location)
        } else {
            self.partition_locations.insert(
                partition_location.partition_id.partition_id,
                vec![partition_location],
            );
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete
    }
}
