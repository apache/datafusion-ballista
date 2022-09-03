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

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, Metric, Partitioning};
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use log::{debug, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use ballista_core::serde::protobuf::{
    self, CompletedTask, FailedTask, GraphStageInput, OperatorMetricsSet,
};
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::PartitionLocation;
use ballista_core::serde::{AsExecutionPlan, BallistaCodec};

use crate::display::DisplayableBallistaExecutionPlan;

/// A stage in the ExecutionGraph,
/// represents a set of tasks (one per each `partition`) which can be executed concurrently.
/// For a stage, there are five states. And the state machine is as follows:
///
/// UnResolvedStage           FailedStage
///       ↓            ↙           ↑
///  ResolvedStage     →     RunningStage
///                                ↓
///                         CompletedStage
#[derive(Clone)]
pub(super) enum ExecutionStage {
    UnResolved(UnresolvedStage),
    Resolved(ResolvedStage),
    Running(RunningStage),
    Completed(CompletedStage),
    Failed(FailedStage),
}

impl Debug for ExecutionStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStage::UnResolved(unresolved_stage) => unresolved_stage.fmt(f),
            ExecutionStage::Resolved(resolved_stage) => resolved_stage.fmt(f),
            ExecutionStage::Running(running_stage) => running_stage.fmt(f),
            ExecutionStage::Completed(completed_stage) => completed_stage.fmt(f),
            ExecutionStage::Failed(failed_stage) => failed_stage.fmt(f),
        }
    }
}

/// For a stage whose input stages are not all completed, we say it's a unresolved stage
#[derive(Clone)]
pub(super) struct UnresolvedStage {
    /// Stage ID
    pub(super) stage_id: usize,
    /// Output partitioning for this stage.
    pub(super) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(super) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    /// This stage can only be resolved an executed once all child stages are completed.
    pub(super) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(super) plan: Arc<dyn ExecutionPlan>,
}

/// For a stage, if it has no inputs or all of its input stages are completed,
/// then we call it as a resolved stage
#[derive(Clone)]
pub(super) struct ResolvedStage {
    /// Stage ID
    pub(super) stage_id: usize,
    /// Total number of output partitions for this stage.
    /// This stage will produce on task for partition.
    pub(super) partitions: usize,
    /// Output partitioning for this stage.
    pub(super) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(super) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(super) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(super) plan: Arc<dyn ExecutionPlan>,
}

/// Different from the resolved stage, a running stage will
/// 1. save the execution plan as encoded one to avoid serialization cost for creating task definition
/// 2. manage the task statuses
/// 3. manage the stage-level combined metrics
/// Running stages will only be maintained in memory and will not saved to the backend storage
#[derive(Clone)]
pub(super) struct RunningStage {
    /// Stage ID
    pub(super) stage_id: usize,
    /// Total number of output partitions for this stage.
    /// This stage will produce on task for partition.
    pub(super) partitions: usize,
    /// Output partitioning for this stage.
    pub(super) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(super) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(super) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(super) plan: Arc<dyn ExecutionPlan>,
    /// Status of each already scheduled task. If status is None, the partition has not yet been scheduled
    pub(super) task_statuses: Vec<Option<task_status::Status>>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    pub(super) stage_metrics: Option<Vec<MetricsSet>>,
}

/// If a stage finishes successfully, its task statuses and metrics will be finalized
#[derive(Clone)]
pub(super) struct CompletedStage {
    /// Stage ID
    pub(super) stage_id: usize,
    /// Total number of output partitions for this stage.
    /// This stage will produce on task for partition.
    pub(super) partitions: usize,
    /// Output partitioning for this stage.
    pub(super) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(super) output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub(super) inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub(super) plan: Arc<dyn ExecutionPlan>,
    /// Status of each already scheduled task.
    pub(super) task_statuses: Vec<task_status::Status>,
    /// Combined metrics of the already finished tasks in the stage.
    pub(super) stage_metrics: Vec<MetricsSet>,
}

/// If a stage fails, it will be with an error message
#[derive(Clone)]
pub(super) struct FailedStage {
    /// Stage ID
    pub(super) stage_id: usize,
    /// Total number of output partitions for this stage.
    /// This stage will produce on task for partition.
    pub(super) partitions: usize,
    /// Output partitioning for this stage.
    pub(super) output_partitioning: Option<Partitioning>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(super) output_links: Vec<usize>,
    /// `ExecutionPlan` for this stage
    pub(super) plan: Arc<dyn ExecutionPlan>,
    /// Status of each already scheduled task. If status is None, the partition has not yet been scheduled
    pub(super) task_statuses: Vec<Option<task_status::Status>>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    pub(super) stage_metrics: Option<Vec<MetricsSet>>,
    /// Error message
    pub(super) error_message: String,
}

impl UnresolvedStage {
    pub(super) fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        child_stage_ids: Vec<usize>,
    ) -> Self {
        let mut inputs: HashMap<usize, StageOutput> = HashMap::new();
        for input_stage_id in child_stage_ids {
            inputs.insert(input_stage_id, StageOutput::new());
        }

        Self {
            stage_id,
            output_partitioning,
            output_links,
            inputs,
            plan,
        }
    }

    pub(super) fn new_with_inputs(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
    ) -> Self {
        Self {
            stage_id,
            output_partitioning,
            output_links,
            inputs,
            plan,
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
        Ok(ResolvedStage::new(
            self.stage_id,
            plan,
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
        ))
    }

    pub(super) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::UnResolvedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<UnresolvedStage> {
        let plan_proto = U::try_decode(&stage.plan)?;
        let plan = plan_proto.try_into_physical_plan(
            session_ctx,
            session_ctx.runtime_env().as_ref(),
            codec.physical_extension_codec(),
        )?;

        let output_partitioning: Option<Partitioning> = parse_protobuf_hash_partitioning(
            stage.output_partitioning.as_ref(),
            session_ctx,
            plan.schema().as_ref(),
        )?;

        let inputs = decode_inputs(stage.inputs)?;

        Ok(UnresolvedStage {
            stage_id: stage.stage_id as usize,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            plan,
            inputs,
        })
    }

    pub(super) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: UnresolvedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::UnResolvedStage> {
        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(stage.plan, codec.physical_extension_codec())
            .and_then(|proto| proto.try_encode(&mut plan))?;

        let inputs = encode_inputs(stage.inputs)?;

        let output_partitioning =
            hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

        Ok(protobuf::UnResolvedStage {
            stage_id: stage.stage_id as u64,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as u32).collect(),
            inputs,
            plan,
        })
    }
}

impl Debug for UnresolvedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent();

        write!(
            f,
            "=========UnResolvedStage[id={}, children={}]=========\nInputs{:?}\n{}",
            self.stage_id,
            self.inputs.len(),
            self.inputs,
            plan
        )
    }
}

impl ResolvedStage {
    pub(super) fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
    ) -> Self {
        let partitions = plan.output_partitioning().partition_count();

        Self {
            stage_id,
            partitions,
            output_partitioning,
            output_links,
            inputs,
            plan,
        }
    }

    /// Change to the running state
    pub(super) fn to_running(&self) -> RunningStage {
        RunningStage::new(
            self.stage_id,
            self.plan.clone(),
            self.partitions,
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
        )
    }

    /// Change to the unresolved state
    pub(super) fn to_unresolved(&self) -> Result<UnresolvedStage> {
        let new_plan = crate::planner::rollback_resolved_shuffles(self.plan.clone())?;

        let unresolved = UnresolvedStage::new_with_inputs(
            self.stage_id,
            new_plan,
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
        );
        Ok(unresolved)
    }

    pub(super) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::ResolvedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<ResolvedStage> {
        let plan_proto = U::try_decode(&stage.plan)?;
        let plan = plan_proto.try_into_physical_plan(
            session_ctx,
            session_ctx.runtime_env().as_ref(),
            codec.physical_extension_codec(),
        )?;

        let output_partitioning: Option<Partitioning> = parse_protobuf_hash_partitioning(
            stage.output_partitioning.as_ref(),
            session_ctx,
            plan.schema().as_ref(),
        )?;

        let inputs = decode_inputs(stage.inputs)?;

        Ok(ResolvedStage {
            stage_id: stage.stage_id as usize,
            partitions: stage.partitions as usize,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            inputs,
            plan,
        })
    }

    pub(super) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: ResolvedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::ResolvedStage> {
        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(stage.plan, codec.physical_extension_codec())
            .and_then(|proto| proto.try_encode(&mut plan))?;

        let output_partitioning =
            hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

        let inputs = encode_inputs(stage.inputs)?;

        Ok(protobuf::ResolvedStage {
            stage_id: stage.stage_id as u64,
            partitions: stage.partitions as u32,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as u32).collect(),
            inputs,
            plan,
        })
    }
}

impl Debug for ResolvedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent();

        write!(
            f,
            "=========ResolvedStage[id={}, partitions={}]=========\n{}",
            self.stage_id, self.partitions, plan
        )
    }
}

impl RunningStage {
    pub(super) fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitions: usize,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
    ) -> Self {
        Self {
            stage_id,
            partitions,
            output_partitioning,
            output_links,
            inputs,
            plan,
            task_statuses: vec![None; partitions],
            stage_metrics: None,
        }
    }

    pub(super) fn to_completed(&self) -> CompletedStage {
        let task_statuses = self
            .task_statuses
            .iter()
            .enumerate()
            .map(|(task_id, status)| {
                status.clone().unwrap_or_else(|| {
                    panic!(
                        "The status of task {}/{} should not be none",
                        self.stage_id, task_id
                    )
                })
            })
            .collect();
        let stage_metrics = self.stage_metrics.clone().unwrap_or_else(|| {
            warn!("The metrics for stage {} should not be none", self.stage_id);
            vec![]
        });
        CompletedStage {
            stage_id: self.stage_id,
            partitions: self.partitions,
            output_partitioning: self.output_partitioning.clone(),
            output_links: self.output_links.clone(),
            inputs: self.inputs.clone(),
            plan: self.plan.clone(),
            task_statuses,
            stage_metrics,
        }
    }

    pub(super) fn to_failed(&self, error_message: String) -> FailedStage {
        FailedStage {
            stage_id: self.stage_id,
            partitions: self.partitions,
            output_partitioning: self.output_partitioning.clone(),
            output_links: self.output_links.clone(),
            plan: self.plan.clone(),
            task_statuses: self.task_statuses.clone(),
            stage_metrics: self.stage_metrics.clone(),
            error_message,
        }
    }

    pub(super) fn to_resolved(&self) -> ResolvedStage {
        ResolvedStage::new(
            self.stage_id,
            self.plan.clone(),
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
        )
    }

    /// Change to the unresolved state
    pub(super) fn to_unresolved(&self) -> Result<UnresolvedStage> {
        let new_plan = crate::planner::rollback_resolved_shuffles(self.plan.clone())?;

        let unresolved = UnresolvedStage::new_with_inputs(
            self.stage_id,
            new_plan,
            self.output_partitioning.clone(),
            self.output_links.clone(),
            self.inputs.clone(),
        );
        Ok(unresolved)
    }

    /// Returns `true` if all tasks for this stage are complete
    pub(super) fn is_completed(&self) -> bool {
        self.task_statuses
            .iter()
            .all(|status| matches!(status, Some(task_status::Status::Completed(_))))
    }

    /// Returns the number of completed tasks
    pub(super) fn completed_tasks(&self) -> usize {
        self.task_statuses
            .iter()
            .filter(|status| matches!(status, Some(task_status::Status::Completed(_))))
            .count()
    }

    /// Returns the number of scheduled tasks
    pub(super) fn scheduled_tasks(&self) -> usize {
        self.task_statuses.iter().filter(|s| s.is_some()).count()
    }

    /// Returns a vector of currently running tasks in this stage
    pub(super) fn running_tasks(&self) -> Vec<(usize, usize, String)> {
        self.task_statuses
            .iter()
            .enumerate()
            .filter_map(|(partition, status)| match status {
                Some(task_status::Status::Running(RunningTask { executor_id })) => {
                    Some((self.stage_id, partition, executor_id.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task status is not yet set.
    pub(super) fn available_tasks(&self) -> usize {
        self.task_statuses.iter().filter(|s| s.is_none()).count()
    }

    /// Update the status for task partition
    pub(super) fn update_task_status(
        &mut self,
        partition_id: usize,
        status: task_status::Status,
    ) {
        debug!("Updating task status for partition {}", partition_id);
        self.task_statuses[partition_id] = Some(status);
    }

    /// update and combine the task metrics to the stage metrics
    pub(super) fn update_task_metrics(
        &mut self,
        partition: usize,
        metrics: Vec<OperatorMetricsSet>,
    ) -> Result<()> {
        if let Some(combined_metrics) = &mut self.stage_metrics {
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

            let new_metrics_set = combined_metrics
                .iter_mut()
                .zip(metrics_values_array)
                .map(|(first, second)| {
                    Self::combine_metrics_set(first, second, partition)
                })
                .collect();
            self.stage_metrics = Some(new_metrics_set)
        } else {
            let new_metrics_set = metrics
                .into_iter()
                .map(|ms| ms.try_into())
                .collect::<Result<Vec<_>>>()?;
            if !new_metrics_set.is_empty() {
                self.stage_metrics = Some(new_metrics_set)
            }
        }
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
        first.aggregate_by_partition()
    }

    /// Reset the running and completed tasks on a given executor
    /// Returns the number of running tasks that were reset
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        for task in self.task_statuses.iter_mut() {
            match task {
                Some(task_status::Status::Running(RunningTask { executor_id }))
                    if *executor == *executor_id =>
                {
                    *task = None;
                    reset += 1;
                }
                Some(task_status::Status::Completed(CompletedTask {
                    executor_id,
                    partitions: _,
                })) if *executor == *executor_id => {
                    *task = None;
                    reset += 1;
                }
                _ => {}
            }
        }
        reset
    }
}

impl Debug for RunningStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent();

        write!(
            f,
            "=========RunningStage[id={}, partitions={}, completed_tasks={}, scheduled_tasks={}, available_tasks={}]=========\n{}",
            self.stage_id,
            self.partitions,
            self.completed_tasks(),
            self.scheduled_tasks(),
            self.available_tasks(),
            plan
        )
    }
}

impl CompletedStage {
    pub fn to_running(&self) -> RunningStage {
        let mut task_status: Vec<Option<task_status::Status>> = Vec::new();
        for task in self.task_statuses.iter() {
            match task {
                task_status::Status::Completed(_) => task_status.push(Some(task.clone())),
                _ => task_status.push(None),
            }
        }
        RunningStage {
            stage_id: self.stage_id,
            partitions: self.partitions,
            output_partitioning: self.output_partitioning.clone(),
            output_links: self.output_links.clone(),
            inputs: self.inputs.clone(),
            plan: self.plan.clone(),
            task_statuses: task_status,
            stage_metrics: Some(self.stage_metrics.clone()),
        }
    }

    /// Reset the completed tasks on a given executor
    /// Returns the number of running tasks that were reset
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        let failure_reason = format!("Task failure due to Executor {} lost", executor);
        for task in self.task_statuses.iter_mut() {
            match task {
                task_status::Status::Completed(CompletedTask {
                    executor_id,
                    partitions: _,
                }) if *executor == *executor_id => {
                    *task = task_status::Status::Failed(FailedTask {
                        error: failure_reason.clone(),
                    });
                    reset += 1;
                }
                _ => {}
            }
        }
        reset
    }

    pub(super) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::CompletedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<CompletedStage> {
        let plan_proto = U::try_decode(&stage.plan)?;
        let plan = plan_proto.try_into_physical_plan(
            session_ctx,
            session_ctx.runtime_env().as_ref(),
            codec.physical_extension_codec(),
        )?;

        let output_partitioning: Option<Partitioning> = parse_protobuf_hash_partitioning(
            stage.output_partitioning.as_ref(),
            session_ctx,
            plan.schema().as_ref(),
        )?;

        let inputs = decode_inputs(stage.inputs)?;

        let task_statuses = stage
            .task_statuses
            .into_iter()
            .enumerate()
            .map(|(task_id, status)| {
                status.status.unwrap_or_else(|| {
                    panic!("Status for task {} should not be none", task_id)
                })
            })
            .collect();

        let stage_metrics = stage
            .stage_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(CompletedStage {
            stage_id: stage.stage_id as usize,
            partitions: stage.partitions as usize,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            inputs,
            plan,
            task_statuses,
            stage_metrics,
        })
    }

    pub(super) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        job_id: String,
        stage: CompletedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::CompletedStage> {
        let stage_id = stage.stage_id;

        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(stage.plan, codec.physical_extension_codec())
            .and_then(|proto| proto.try_encode(&mut plan))?;

        let output_partitioning =
            hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

        let inputs = encode_inputs(stage.inputs)?;

        let task_statuses: Vec<protobuf::TaskStatus> = stage
            .task_statuses
            .into_iter()
            .enumerate()
            .map(|(partition, status)| {
                protobuf::TaskStatus {
                    task_id: Some(protobuf::PartitionId {
                        job_id: job_id.clone(),
                        stage_id: stage_id as u32,
                        partition_id: partition as u32,
                    }),
                    // task metrics should not persist.
                    metrics: vec![],
                    status: Some(status),
                }
            })
            .collect();

        let stage_metrics = stage
            .stage_metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(protobuf::CompletedStage {
            stage_id: stage_id as u64,
            partitions: stage.partitions as u32,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as u32).collect(),
            inputs,
            plan,
            task_statuses,
            stage_metrics,
        })
    }
}

impl Debug for CompletedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableBallistaExecutionPlan::new(
            self.plan.as_ref(),
            &self.stage_metrics,
        )
        .indent();

        write!(
            f,
            "=========CompletedStage[id={}, partitions={}]=========\n{}",
            self.stage_id, self.partitions, plan
        )
    }
}

impl FailedStage {
    /// Returns the number of completed tasks
    pub(super) fn completed_tasks(&self) -> usize {
        self.task_statuses
            .iter()
            .filter(|status| matches!(status, Some(task_status::Status::Completed(_))))
            .count()
    }

    /// Returns the number of scheduled tasks
    pub(super) fn scheduled_tasks(&self) -> usize {
        self.task_statuses.iter().filter(|s| s.is_some()).count()
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task status is not yet set.
    pub(super) fn available_tasks(&self) -> usize {
        self.task_statuses.iter().filter(|s| s.is_none()).count()
    }

    pub(super) fn decode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        stage: protobuf::FailedStage,
        codec: &BallistaCodec<T, U>,
        session_ctx: &SessionContext,
    ) -> Result<FailedStage> {
        let plan_proto = U::try_decode(&stage.plan)?;
        let plan = plan_proto.try_into_physical_plan(
            session_ctx,
            session_ctx.runtime_env().as_ref(),
            codec.physical_extension_codec(),
        )?;

        let output_partitioning: Option<Partitioning> = parse_protobuf_hash_partitioning(
            stage.output_partitioning.as_ref(),
            session_ctx,
            plan.schema().as_ref(),
        )?;

        let mut task_statuses: Vec<Option<task_status::Status>> =
            vec![None; stage.partitions as usize];
        for status in stage.task_statuses {
            if let Some(task_id) = status.task_id.as_ref() {
                task_statuses[task_id.partition_id as usize] = status.status
            }
        }

        let stage_metrics = if stage.stage_metrics.is_empty() {
            None
        } else {
            let ms = stage
                .stage_metrics
                .into_iter()
                .map(|m| m.try_into())
                .collect::<Result<Vec<_>>>()?;
            Some(ms)
        };

        Ok(FailedStage {
            stage_id: stage.stage_id as usize,
            partitions: stage.partitions as usize,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as usize).collect(),
            plan,
            task_statuses,
            stage_metrics,
            error_message: stage.error_message,
        })
    }

    pub(super) fn encode<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
        job_id: String,
        stage: FailedStage,
        codec: &BallistaCodec<T, U>,
    ) -> Result<protobuf::FailedStage> {
        let stage_id = stage.stage_id;

        let mut plan: Vec<u8> = vec![];
        U::try_from_physical_plan(stage.plan, codec.physical_extension_codec())
            .and_then(|proto| proto.try_encode(&mut plan))?;

        let output_partitioning =
            hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

        let task_statuses: Vec<protobuf::TaskStatus> = stage
            .task_statuses
            .into_iter()
            .enumerate()
            .filter_map(|(partition, status)| {
                status.map(|status| protobuf::TaskStatus {
                    task_id: Some(protobuf::PartitionId {
                        job_id: job_id.clone(),
                        stage_id: stage_id as u32,
                        partition_id: partition as u32,
                    }),
                    // task metrics should not persist.
                    metrics: vec![],
                    status: Some(status),
                })
            })
            .collect();

        let stage_metrics = stage
            .stage_metrics
            .unwrap_or_default()
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(protobuf::FailedStage {
            stage_id: stage_id as u64,
            partitions: stage.partitions as u32,
            output_partitioning,
            output_links: stage.output_links.into_iter().map(|l| l as u32).collect(),
            plan,
            task_statuses,
            stage_metrics,
            error_message: stage.error_message,
        })
    }
}

impl Debug for FailedStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent();

        write!(
            f,
            "=========FailedStage[id={}, partitions={}, completed_tasks={}, scheduled_tasks={}, available_tasks={}, error_message={}]=========\n{}",
            self.stage_id,
            self.partitions,
            self.completed_tasks(),
            self.scheduled_tasks(),
            self.available_tasks(),
            self.error_message,
            plan
        )
    }
}

/// This data structure collects the partition locations for an `ExecutionStage`.
/// Each `ExecutionStage` will hold a `StageOutput`s for each of its child stages.
/// When all tasks for the child stage are complete, it will mark the `StageOutput`
#[derive(Clone, Debug, Default)]
pub(super) struct StageOutput {
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

fn decode_inputs(
    stage_inputs: Vec<GraphStageInput>,
) -> Result<HashMap<usize, StageOutput>> {
    let mut inputs: HashMap<usize, StageOutput> = HashMap::new();
    for input in stage_inputs {
        let stage_id = input.stage_id as usize;

        let outputs = input
            .partition_locations
            .into_iter()
            .map(|loc| {
                let partition = loc.partition as usize;
                let locations = loc
                    .partition_location
                    .into_iter()
                    .map(|l| l.try_into())
                    .collect::<Result<Vec<_>>>()?;
                Ok((partition, locations))
            })
            .collect::<Result<HashMap<usize, Vec<PartitionLocation>>>>()?;

        inputs.insert(
            stage_id,
            StageOutput {
                partition_locations: outputs,
                complete: input.complete,
            },
        );
    }
    Ok(inputs)
}

fn encode_inputs(
    stage_inputs: HashMap<usize, StageOutput>,
) -> Result<Vec<GraphStageInput>> {
    let mut inputs: Vec<protobuf::GraphStageInput> = vec![];
    for (stage_id, output) in stage_inputs.into_iter() {
        inputs.push(protobuf::GraphStageInput {
            stage_id: stage_id as u32,
            partition_locations: output
                .partition_locations
                .into_iter()
                .map(|(partition, locations)| {
                    Ok(protobuf::TaskInputPartitions {
                        partition: partition as u32,
                        partition_location: locations
                            .into_iter()
                            .map(|l| l.try_into())
                            .collect::<Result<Vec<_>>>()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            complete: output.complete,
        });
    }
    Ok(inputs)
}
