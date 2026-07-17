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

use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
//use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, Metric};
use datafusion::prelude::SessionConfig;
use log::{debug, warn};

use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{ShuffleWriterExec, SortShuffleWriterExec};
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::{
    FailedTask, OperatorMetricsSet, ResultLost, SuccessfulTask, TaskKilled, TaskStatus,
};
use ballista_core::serde::protobuf::{RunningTask, task_status};
use ballista_core::serde::scheduler::PartitionLocation;

use crate::display::DisplayableBallistaExecutionPlan;

/// A stage in the ExecutionGraph representing a set of tasks that can be executed concurrently.
///
/// Each stage contains one task per partition. The stage progresses through a state machine:
///
/// ```text
/// UnResolvedStage           FailedStage
///       ↓            ↙           ↑
///  ResolvedStage     →     RunningStage
///                                ↓
///                         SuccessfulStage
/// ```
///
/// - `UnResolved`: Input stages are not yet complete
/// - `Resolved`: All inputs are ready, stage can be scheduled
/// - `Running`: Tasks are being executed
/// - `Successful`: All tasks completed successfully
/// - `Failed`: Stage execution failed
#[derive(Clone)]
pub enum ExecutionStage {
    /// Stage whose input stages are not all completed.
    UnResolved(UnresolvedStage),
    /// Stage with all inputs ready, waiting to be scheduled.
    Resolved(ResolvedStage),
    /// Stage with tasks currently being executed.
    Running(RunningStage),
    /// Stage that completed all tasks successfully.
    Successful(SuccessfulStage),
    /// Stage that failed during execution.
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
    pub fn variant_name(&self) -> &str {
        match self {
            ExecutionStage::UnResolved(_) => "Unresolved",
            ExecutionStage::Resolved(_) => "Resolved",
            ExecutionStage::Running(_) => "Running",
            ExecutionStage::Successful(_) => "Successful",
            ExecutionStage::Failed(_) => "Failed",
        }
    }

    /// Get the query plan for this query stage
    pub fn plan(&self) -> &dyn ExecutionPlan {
        match self {
            ExecutionStage::UnResolved(stage) => stage.plan.as_ref(),
            ExecutionStage::Resolved(stage) => stage.plan.as_ref(),
            ExecutionStage::Running(stage) => stage.plan.as_ref(),
            ExecutionStage::Successful(stage) => stage.plan.as_ref(),
            ExecutionStage::Failed(stage) => stage.plan.as_ref(),
        }
    }

    /// Get the output links for this stage. An empty slice means this is a
    /// final stage in the `ExecutionGraph`.
    pub fn output_links(&self) -> &[usize] {
        match self {
            ExecutionStage::UnResolved(stage) => &stage.output_links,
            ExecutionStage::Resolved(stage) => &stage.output_links,
            ExecutionStage::Running(stage) => &stage.output_links,
            ExecutionStage::Successful(stage) => &stage.output_links,
            ExecutionStage::Failed(stage) => &stage.output_links,
        }
    }

    /// TaskInfos indexed by task_id (the append-order slot) for stages
    /// that have started binding tasks. Returns `None` for
    /// UnResolved/Resolved stages that haven't bound anything yet.
    pub fn task_infos(&self) -> Option<&[TaskInfo]> {
        match self {
            ExecutionStage::Running(stage) => Some(&stage.task_infos),
            ExecutionStage::Successful(stage) => Some(&stage.task_infos),
            ExecutionStage::Failed(stage) => Some(&stage.task_infos),
            ExecutionStage::UnResolved(_) | ExecutionStage::Resolved(_) => None,
        }
    }
}

/// For a stage whose input stages are not all completed, we say it's a unresolved stage
#[derive(Clone)]
pub struct UnresolvedStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    /// This stage can only be resolved an executed once all child stages are completed.
    pub inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// Record last attempt's failure reasons to avoid duplicate resubmits
    pub last_attempt_failure_reasons: HashSet<String>,
    /// [SessionConfig] used for this stage
    pub session_config: Arc<SessionConfig>,
}

/// For a stage, if it has no inputs or all of its input stages are completed,
/// then we call it as a resolved stage
#[derive(Clone)]
pub struct ResolvedStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// Record last attempt's failure reasons to avoid duplicate resubmits
    pub last_attempt_failure_reasons: HashSet<String>,
    /// [SessionConfig] used for this stage
    pub session_config: Arc<SessionConfig>,
}

/// Different from the resolved stage, a running stage will
/// 1. save the execution plan as encoded one to avoid serialization cost for creating task definition
/// 2. manage the task statuses
/// 3. manage the stage-level combined metrics
///    Running stages will only be maintained in memory and will not saved to the backend storage
#[derive(Clone)]
pub struct RunningStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Stage activation time (when was stage become running) in millis
    pub stage_running_time: u128,
    /// Total plan input partitions for this stage (frozen at resolve).
    /// Num tasks is emergent from `pending` / `task_infos.len()`, not this.
    pub partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// Cursor over partitions still waiting to be scheduled. Bind time
    /// pulls a chunk sized to the executor's free vcores; failed
    /// partitions are pushed back to the front for re-attempt.
    pub pending: PendingPartitions,
    /// TaskInfo of every task ever started for this stage (append-only).
    /// Indexed by task_id (the bind-order slot). Retries append new
    /// entries with fresh task_ids rather than reusing slots.
    pub task_infos: Vec<TaskInfo>,
    /// Number of times each plan input partition has been tried and
    /// failed. Indexed by partition id (real plan input index), not by
    /// task_id. When any partition exceeds `stage_max_failures`, the
    /// stage is failed.
    pub task_failure_numbers: Vec<usize>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    pub stage_metrics: Option<Vec<MetricsSet>>,
    /// [SessionConfig] used for this stage
    pub session_config: Arc<SessionConfig>,
}

/// If a stage finishes successfully, its task statuses and metrics will be finalized
#[derive(Clone)]
pub struct SuccessfulStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub output_links: Vec<usize>,
    /// Represents the outputs from this stage's child stages.
    pub inputs: HashMap<usize, StageOutput>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of each already successful task.
    /// The index of the Vec is the task's partition id
    pub task_infos: Vec<TaskInfo>,
    /// Combined metrics of the already finished tasks in the stage.
    pub stage_metrics: Vec<MetricsSet>,
    /// [SessionConfig] used for this stage
    pub session_config: Arc<SessionConfig>,
}

/// If a stage fails, it will be with an error message
#[derive(Clone)]
pub struct FailedStage {
    /// Stage ID
    pub stage_id: usize,
    /// Stage Attempt number
    pub stage_attempt_num: usize,
    /// Total number of partitions for this stage.
    /// This stage will produce on task for partition.
    pub partitions: usize,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    #[allow(dead_code)] // not used at the moment, will be used later
    pub output_links: Vec<usize>,
    /// `ExecutionPlan` for this stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// TaskInfo of every task ever started before the stage failed.
    /// Append-only, indexed by task_id (bind order).
    pub task_infos: Vec<TaskInfo>,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    #[allow(dead_code)] // not used at the moment, will be used later
    pub stage_metrics: Option<Vec<MetricsSet>>,
    /// Error message
    pub error_message: String,
}

/// Cursor over the partitions remaining to be scheduled for a stage.
///
/// One task processes a slice of partitions (up to `exec.vcores`). Rather
/// than pre-computing num_tasks at resolve time and pre-allocating
/// `Vec<Option<TaskInfo>>`, the number of tasks is emergent: at bind time
/// each executor's free-vcore budget pulls a chunk of partitions off the
/// queue. Retries push failed partitions to the front so they get
/// re-attempted before any fresh partition.
#[derive(Clone, Debug)]
pub struct PendingPartitions {
    /// Total plan input partitions this stage must process (frozen).
    total: usize,
    /// Partitions waiting to be scheduled. Front = next out.
    queue: VecDeque<usize>,
}

impl PendingPartitions {
    /// Create a cursor covering partitions `[0..total)`.
    pub fn new(total: usize) -> Self {
        Self {
            total,
            queue: (0..total).collect(),
        }
    }

    /// Create an empty cursor sized to `total` partitions. Callers populate
    /// via `reschedule` (used by `SuccessfulStage::to_running` where only
    /// the failed/lost subset needs re-processing).
    pub fn empty(total: usize) -> Self {
        Self {
            total,
            queue: VecDeque::new(),
        }
    }

    /// Take up to `max` partitions for the next task. Returns an empty
    /// vec when the stage has no more work.
    pub fn next_slice(&mut self, max: usize) -> Vec<usize> {
        let take = max.min(self.queue.len());
        self.queue.drain(..take).collect()
    }

    /// Push failed partitions to the front of the queue so the next bind
    /// picks them up before any fresh partition.
    pub fn reschedule(&mut self, partitions: impl IntoIterator<Item = usize>) {
        for p in partitions.into_iter().collect::<Vec<_>>().into_iter().rev() {
            self.queue.push_front(p);
        }
    }

    /// True when there are no more partitions to hand out.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Total partitions still unassigned.
    pub fn remaining(&self) -> usize {
        self.queue.len()
    }

    /// Total plan input partition count (frozen at construction).
    pub fn total_partitions(&self) -> usize {
        self.total
    }
}

/// Information about a task's execution lifecycle and current status.
#[derive(Clone)]
#[allow(dead_code)] // we may use the fields later
pub struct TaskInfo {
    /// Unique task identifier within the execution graph.
    pub task_id: usize,
    /// Timestamp when the task was scheduled (in milliseconds since epoch).
    pub scheduled_time: u128,
    /// Timestamp when the task was launched on an executor (in milliseconds since epoch).
    pub launch_time: u128,
    /// Timestamp when actual execution started (in milliseconds since epoch).
    pub start_exec_time: u128,
    /// Timestamp when execution finished (in milliseconds since epoch).
    pub end_exec_time: u128,
    /// Timestamp when the task result was received (in milliseconds since epoch).
    pub finish_time: u128,
    /// Current status of the task (Running, Successful, Failed).
    pub task_status: task_status::Status,
    /// Plan input partitions this task is (or was) processing. Mirrors the
    /// `global_input_partition_ids` on `TaskDescription`; needed here so executor-loss
    /// and retry paths know which partitions to push back to `pending`,
    /// and per-partition failure bookkeeping can locate entries in
    /// `task_failure_numbers`.
    pub global_input_partition_ids: Vec<usize>,
    /// Vcores this task consumed from the executor's budget at bind time.
    /// Usually equals `global_input_partition_ids.len()` (one vcore per
    /// packed partition), but for collapse-input tasks that consume the
    /// executor's entire remaining budget it is `min(slice_len, budget)`.
    /// Used to refund the exact amount when the task completes so the
    /// executor's vcore budget stays consistent across bind/refund.
    pub vcores_consumed: u32,
}

impl UnresolvedStage {
    /// Creates a new unresolved stage with the given child stage dependencies.
    pub fn new(
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

    /// Creates a new unresolved stage with pre-populated inputs (used for stage rollback).
    pub fn new_with_inputs(
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
    pub fn add_input_partitions(
        &mut self,
        stage_id: usize,
        locations: Vec<PartitionLocation>,
    ) -> Result<()> {
        if let Some(stage_inputs) = self.inputs.get_mut(&stage_id) {
            for partition in locations {
                stage_inputs.add_partition(partition);
            }
        } else {
            return Err(BallistaError::Internal(format!(
                "Error adding input partitions to stage {}, {} is not a valid child stage ID",
                self.stage_id, stage_id
            )));
        }

        Ok(())
    }

    /// Remove input partitions from an input stage on a given executor.
    /// Return the HashSet of removed map partition ids
    pub fn remove_input_partitions(
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
            Err(BallistaError::Internal(format!(
                "Error remove input partition for Stage {}, {} is not a valid child stage ID",
                self.stage_id, input_stage_id
            )))
        }
    }

    /// Marks the input stage ID as complete.
    pub fn complete_input(&mut self, stage_id: usize) {
        if let Some(input) = self.inputs.get_mut(&stage_id) {
            input.complete = true;
        }
    }

    /// Returns true if all inputs are complete and we can resolve all
    /// UnresolvedShuffleExec operators to ShuffleReadExec
    pub fn resolvable(&self) -> bool {
        self.inputs.iter().all(|(_, input)| input.is_complete())
    }

    /// Change to the resolved state
    pub fn to_resolved(&self, options: &ConfigOptions) -> Result<ResolvedStage> {
        let input_locations = self
            .inputs
            .iter()
            .map(|(stage, input)| (*stage, input.partition_locations.clone()))
            .collect();
        let plan = crate::planner::remove_unresolved_shuffles(
            self.plan.clone(),
            &input_locations,
        )?;

        // ballista specific JoinSelection, as datafusion rule can't be used here.
        // Datafusion JoinSelection may produce plans which need change of partitions
        // in order to be valid.
        //
        // we should consider changing ballista core to support adding new stages
        // if plan changes.

        let optimize_join =
            crate::physical_optimizer::join_selection::JoinSelection::new();
        let plan = optimize_join.optimize(plan, options)?;

        let optimize_aggregate = AggregateStatistics::new();
        let plan = optimize_aggregate.optimize(plan, options)?;

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
    /// Creates a new resolved stage ready for task scheduling.
    pub fn new(
        stage_id: usize,
        stage_attempt_num: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_links: Vec<usize>,
        inputs: HashMap<usize, StageOutput>,
        last_attempt_failure_reasons: HashSet<String>,
        session_config: Arc<SessionConfig>,
    ) -> Self {
        let partitions = stage_input_partitions(&plan);

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
    pub fn to_running(&self) -> RunningStage {
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
    pub fn to_unresolved(&self) -> Result<UnresolvedStage> {
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
    /// Creates a new running stage with task tracking initialized.
    pub fn new(
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
            stage_running_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            partitions,
            output_links,
            inputs,
            plan,
            pending: PendingPartitions::new(partitions),
            task_infos: Vec::new(),
            task_failure_numbers: vec![0; partitions],
            stage_metrics: None,
            session_config,
        }
    }

    /// Converts this running stage to a successful stage after all tasks complete.
    pub fn to_successful(&self) -> SuccessfulStage {
        let task_infos = self.task_infos.clone();
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

    /// Converts this running stage to a failed stage. Still-running tasks are recorded
    /// as cancelled (`Failed(TaskKilled)`) since failing the stage cancels them.
    pub fn to_failed(&self, error_message: String) -> FailedStage {
        let task_infos = self
            .task_infos
            .iter()
            .map(|info| {
                if matches!(info.task_status, task_status::Status::Running(_)) {
                    TaskInfo {
                        task_status: task_status::Status::Failed(FailedTask {
                            error: "killed".to_string(),
                            retryable: false,
                            count_to_failures: false,
                            failed_reason: Some(FailedReason::TaskKilled(TaskKilled {})),
                        }),
                        ..info.clone()
                    }
                } else {
                    info.clone()
                }
            })
            .collect();

        FailedStage {
            stage_id: self.stage_id,
            stage_attempt_num: self.stage_attempt_num,
            partitions: self.partitions,
            output_links: self.output_links.clone(),
            plan: self.plan.clone(),
            task_infos,
            stage_metrics: self.stage_metrics.clone(),
            error_message,
        }
    }

    /// Change to the unresolved state and bump the stage attempt number
    pub fn to_unresolved(
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

    /// Returns `true` if every plan input partition has been processed
    /// successfully at least once and no partitions remain in `pending`.
    /// Retried tasks may leave Failed entries in `task_infos`; a partition
    /// counts as covered if any Successful task's slice includes it.
    pub fn is_successful(&self) -> bool {
        if !self.pending.is_empty() {
            return false;
        }
        let mut covered = vec![false; self.partitions];
        for info in &self.task_infos {
            if matches!(info.task_status, task_status::Status::Successful(_)) {
                for p in &info.global_input_partition_ids {
                    covered[*p] = true;
                }
            }
        }
        covered.iter().all(|c| *c)
    }

    /// Returns the number of task attempts that reached Successful status
    /// (multiple attempts may exist for the same partition after retries).
    pub fn successful_tasks(&self) -> usize {
        self.task_infos
            .iter()
            .filter(|info| matches!(info.task_status, task_status::Status::Successful(_)))
            .count()
    }

    /// Returns the number of tasks that have been scheduled (started) so far
    /// — every entry in `task_infos` counts, including retries and losses.
    pub fn scheduled_tasks(&self) -> usize {
        self.task_infos.len()
    }

    /// Returns a vector of currently running tasks in this stage: tuples of
    /// `(task_id, stage_id, executor_id)`. `task_id` is the task's append
    /// slot in `task_infos`.
    pub fn running_tasks(&self) -> Vec<(usize, usize, String)> {
        self.task_infos
            .iter()
            .enumerate()
            .filter_map(|(task_id, info)| match &info.task_status {
                task_status::Status::Running(RunningTask { executor_id }) => {
                    Some((task_id, self.stage_id, executor_id.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// Returns the number of plan input partitions still waiting to be
    /// handed to a task. This is what scheduling decisions draw from: as
    /// executors free vcores, `pending.next_slice(exec.vcores)` drains
    /// this pool into fresh tasks.
    pub fn available_tasks(&self) -> usize {
        self.pending.remaining()
    }

    /// Apply a status update for the task at `task_id` (its append slot in
    /// `task_infos`).
    ///
    /// Rejects (returns false) if the task's status is already terminal
    /// Failed with a lost/killed reason (i.e., `reset_tasks` moved its
    /// partitions back to pending because the executor died) — a late
    /// status from that attempt should be ignored.
    ///
    /// On success, updates the task's status and adjusts per-partition
    /// failure counters using the task's `global_input_partition_ids`.
    pub fn update_task_info(&mut self, task_id: usize, status: TaskStatus) -> bool {
        debug!("Updating TaskInfo for task_id {task_id}");
        let task_info = &self.task_infos[task_id];
        if let task_status::Status::Failed(FailedTask {
            failed_reason:
                Some(FailedReason::TaskKilled(_)) | Some(FailedReason::ResultLost(_)),
            ..
        }) = &task_info.task_status
        {
            warn!(
                "Ignore TaskStatus update for task_id {task_id} because it was already reset (executor lost)"
            );
            return false;
        }
        let scheduled_time = task_info.scheduled_time;
        let global_input_partition_ids = task_info.global_input_partition_ids.clone();
        let vcores_consumed = task_info.vcores_consumed;
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
            global_input_partition_ids: global_input_partition_ids.clone(),
            vcores_consumed,
        };
        self.task_infos[task_id] = updated_task_info;

        match task_status {
            task_status::Status::Failed(failed_task) if failed_task.retryable => {
                for p in &global_input_partition_ids {
                    self.task_failure_numbers[*p] += 1;
                }
            }
            task_status::Status::Successful(_) => {
                for p in &global_input_partition_ids {
                    self.task_failure_numbers[*p] = 0;
                }
            }
            _ => {}
        }
        true
    }

    /// update and upsert the task metrics to the stage metrics
    pub fn update_task_metrics(
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
                return Err(BallistaError::Internal(format!(
                    "Error updating task metrics to stage {}, task metrics array size {} does not equal \
                with the stage metrics array size {} for task {}",
                    self.stage_id,
                    metrics.len(),
                    combined_metrics.len(),
                    partition
                )));
            }
            let metrics_values_array = metrics
                .into_iter()
                .map(|ms| Self::metrics_set_from_task_metrics(ms, partition))
                .collect::<Result<Vec<_>>>()?;

            combined_metrics
                .iter_mut()
                .zip(metrics_values_array)
                .map(|(existing_metrics, new_partition_metrics)| {
                    Self::upsert_metrics_set_for_partition(
                        existing_metrics,
                        new_partition_metrics,
                        partition,
                    )
                })
                .collect()
        } else {
            metrics
                .into_iter()
                .map(|ms| Self::metrics_set_from_task_metrics(ms, partition))
                .collect::<Result<Vec<_>>>()?
        };
        self.stage_metrics = Some(new_metrics_set);

        Ok(())
    }

    /// Converts task metrics into a metrics set for a specific partition
    fn metrics_set_from_task_metrics(
        metrics: OperatorMetricsSet,
        partition: usize,
    ) -> Result<MetricsSet> {
        let mut metrics_set = MetricsSet::new();
        for metric in metrics.metrics {
            let metric_value: MetricValue = metric.try_into()?;
            metrics_set.push(Arc::new(Metric::new(metric_value, Some(partition))));
        }
        Ok(metrics_set)
    }

    /// Upserts raw metrics from a completed task into the stage metrics
    pub fn upsert_metrics_set_for_partition(
        existing_metrics: &mut MetricsSet,
        new_partition_metrics: MetricsSet,
        partition: usize,
    ) -> MetricsSet {
        let mut updated_metrics = MetricsSet::new();
        // Task metrics are snapshots, so replace any prior metrics for this partition.
        for metric in existing_metrics.iter() {
            if metric.partition() != Some(partition) {
                updated_metrics.push(metric.clone());
            }
        }
        for metric in new_partition_metrics.iter() {
            updated_metrics.push(metric.clone());
        }
        updated_metrics
    }

    /// Returns the highest per-partition failure count across the
    /// partitions in the given task's slice. Callers use this to decide
    /// whether any partition in the task has exhausted its retry budget.
    pub fn task_failure_number(&self, task_id: usize) -> usize {
        self.task_infos[task_id]
            .global_input_partition_ids
            .iter()
            .map(|p| self.task_failure_numbers[*p])
            .max()
            .unwrap_or(0)
    }

    /// Mark the task as lost/killed and push its partitions back to the
    /// front of `pending` so they are retried on the next bind. Does not
    /// touch failure counts — those are updated in `update_task_info`.
    pub fn reset_task_info(&mut self, task_id: usize) {
        let task = &mut self.task_infos[task_id];
        let partitions = task.global_input_partition_ids.clone();
        task.task_status = task_status::Status::Failed(FailedTask {
            error: "task reset for retry".to_string(),
            retryable: true,
            count_to_failures: false,
            failed_reason: Some(FailedReason::TaskKilled(TaskKilled {})),
        });
        self.pending.reschedule(partitions);
    }

    /// Reset the running and completed tasks on a given executor by
    /// marking their `TaskInfo` as `Failed(ResultLost)` and pushing their
    /// partition slices back to `pending`. Returns the number of tasks
    /// reset.
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        let mut to_reschedule: Vec<usize> = vec![];
        for task in self.task_infos.iter_mut() {
            let matches_exec = match &task.task_status {
                task_status::Status::Running(RunningTask { executor_id })
                | task_status::Status::Successful(SuccessfulTask {
                    executor_id, ..
                }) => executor == executor_id,
                _ => false,
            };
            if matches_exec {
                task.task_status = task_status::Status::Failed(FailedTask {
                    error: format!("Task failure due to Executor {executor} lost"),
                    retryable: true,
                    count_to_failures: false,
                    failed_reason: Some(FailedReason::ResultLost(ResultLost {})),
                });
                to_reschedule.extend(task.global_input_partition_ids.iter().copied());
                reset += 1;
            }
        }
        self.pending.reschedule(to_reschedule);
        reset
    }

    /// Remove input partitions from an input stage on a given executor.
    /// Return the HashSet of removed map partition ids
    pub fn remove_input_partitions(
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
            Err(BallistaError::Internal(format!(
                "Error remove input partition for Stage {}, {} is not a valid child stage ID",
                self.stage_id, input_stage_id
            )))
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
    /// Change to the running state and bump the stage attempt number.
    ///
    /// task_infos is append-only across retries — the same partition may
    /// appear in multiple entries (an old Failed one from a lost executor
    /// plus a later Successful one from the retry). Walk newest-to-oldest
    /// and, for each partition, only consider the *latest* attempt: if it's
    /// Successful the partition is done; otherwise it goes back to pending.
    /// Naive iteration would push the same partition multiple times.
    pub fn to_running(&self) -> RunningStage {
        let mut pending = PendingPartitions::empty(self.partitions);
        let mut seen = vec![false; self.partitions];
        let mut to_reschedule: Vec<usize> = vec![];
        for task in self.task_infos.iter().rev() {
            let needs_reschedule =
                !matches!(task.task_status, task_status::Status::Successful(_));
            for &p in &task.global_input_partition_ids {
                if p < seen.len() && !seen[p] {
                    seen[p] = true;
                    if needs_reschedule {
                        to_reschedule.push(p);
                    }
                }
            }
        }
        to_reschedule.sort_unstable();
        pending.reschedule(to_reschedule);
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
            pending,
            task_infos: self.task_infos.clone(),
            // It is Ok to forget the previous task failure attempts
            task_failure_numbers: vec![0; self.partitions],
            stage_metrics,
            session_config: self.session_config.clone(),
            stage_running_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }
    }

    /// Mark successful tasks on a lost executor as `Failed(ResultLost)` so
    /// `to_running` will reschedule their partitions on the next attempt.
    /// Returns the number of tasks reset.
    pub fn reset_tasks(&mut self, executor: &str) -> usize {
        let mut reset = 0;
        let failure_reason = format!("Task failure due to Executor {executor} lost");
        for task in self.task_infos.iter_mut() {
            let hit = matches!(
                &task.task_status,
                task_status::Status::Successful(SuccessfulTask { executor_id, .. })
                    if executor == executor_id
            );
            if hit {
                task.launch_time = 0;
                task.start_exec_time = 0;
                task.end_exec_time = 0;
                task.finish_time = 0;
                task.task_status = task_status::Status::Failed(FailedTask {
                    error: failure_reason.clone(),
                    retryable: true,
                    count_to_failures: false,
                    failed_reason: Some(FailedReason::ResultLost(ResultLost {})),
                });
                reset += 1;
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
    /// Returns the number of task attempts that reached Successful status
    /// before the stage failed.
    pub fn successful_tasks(&self) -> usize {
        self.task_infos
            .iter()
            .filter(|info| matches!(info.task_status, task_status::Status::Successful(_)))
            .count()
    }
    /// Returns the number of tasks scheduled (every task_infos entry).
    pub fn scheduled_tasks(&self) -> usize {
        self.task_infos.len()
    }

    /// A failed stage has no schedulable work remaining.
    pub fn available_tasks(&self) -> usize {
        0
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

/// Total plan input partitions a stage must process. Frozen at resolve.
/// Number of TASKS is emergent — bind time draws slices from a cursor
/// (`PendingPartitions`) sized to whichever executor's free vcores show
/// up, so one 16-vcore exec covers 16 partitions in a single task while
/// four 4-vcore execs cover the same 16 in four tasks.
///
/// The count comes from the shuffle writer's immediate child's
/// `output_partitioning().partition_count()`. That is the number of
/// independent output-partition polling contexts the writer needs to
/// drive, which matches the "one tokio worker per vcore" invariant: one
/// task = one `execute(i)` on the top plan = one primary tokio pipeline.
/// Anything an internal `RepartitionExec` spawns is per-plan-instance
/// machinery shared across those polls and does not become a separate
/// scheduling unit.
fn stage_input_partitions(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if plan.downcast_ref::<ShuffleWriterExec>().is_some()
        || plan.downcast_ref::<SortShuffleWriterExec>().is_some()
    {
        plan.children()[0]
            .properties()
            .output_partitioning()
            .partition_count()
    } else {
        plan.properties().output_partitioning().partition_count()
    }
    .max(1)
}

/// This data structure collects the partition locations for an `ExecutionStage`.
/// Each `ExecutionStage` will hold a `StageOutput`s for each of its child stages.
/// When all tasks for the child stage are complete, it will mark the `StageOutput`
/// as complete.
#[derive(Clone, Debug, Default)]
pub struct StageOutput {
    /// Map from partition -> partition locations
    pub partition_locations: HashMap<usize, Vec<PartitionLocation>>,
    /// Flag indicating whether all tasks are complete
    pub complete: bool,
}

impl StageOutput {
    /// Creates a new empty stage output.
    pub fn new() -> Self {
        Self {
            partition_locations: HashMap::new(),
            complete: false,
        }
    }

    /// Adds a `PartitionLocation` to this stage output.
    pub fn add_partition(&mut self, partition_location: PartitionLocation) {
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

    /// Returns true if all partitions for this stage output are complete.
    pub fn is_complete(&self) -> bool {
        self.complete
    }
    /// returns vector of partition locations
    /// which is compatible with ShuffleReader vector format
    ///
    /// `output_partition_count` is the number of expected
    ///  output partition number
    pub fn partition_locations(
        mut self,
        output_partition_count: usize,
    ) -> Vec<Vec<PartitionLocation>> {
        let mut partition_locations = Vec::new();
        for i in 0..output_partition_count {
            let p = self.partition_locations.remove(&i).unwrap_or_default();
            partition_locations.push(p);
        }

        partition_locations
    }

    /// returns vector of partition locations
    /// which is compatible with ShuffleReader vector format
    /// supporting broadcast shuffle read.
    /// All partitions are merged into one
    pub fn partition_locations_broadcast(self) -> Vec<Vec<PartitionLocation>> {
        self.partition_locations.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::serde::protobuf::{
        OperatorMetric, SuccessfulTask, TaskStatus, operator_metric, task_status,
    };
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionConfig;
    use std::collections::HashMap;

    fn make_running_stage(partitions: usize) -> RunningStage {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        RunningStage::new(
            1,
            0,
            plan,
            partitions,
            vec![],
            HashMap::new(),
            Arc::new(SessionConfig::default()),
        )
    }

    fn make_task_status(task_id: u32) -> TaskStatus {
        TaskStatus {
            task_id,
            job_id: "test-job".to_string(),
            stage_id: 1,
            stage_attempt_num: 0,
            launch_time: 100,
            start_exec_time: 200,
            end_exec_time: 300,
            status: Some(task_status::Status::Successful(SuccessfulTask {
                executor_id: "executor-1".to_string(),
                partitions: vec![],
            })),
            metrics: vec![],
        }
    }

    fn make_operator_metrics_set(
        output_rows: u64,
        elapsed_compute_nanos: u64,
    ) -> OperatorMetricsSet {
        OperatorMetricsSet {
            metrics: vec![
                OperatorMetric {
                    metric: Some(operator_metric::Metric::OutputRows(output_rows)),
                },
                OperatorMetric {
                    metric: Some(operator_metric::Metric::ElapseTime(
                        elapsed_compute_nanos,
                    )),
                },
            ],
        }
    }

    /// Push one Running task_info covering the given partitions onto the
    /// stage (test helper mirroring what a real bind would do).
    fn append_running_task(
        stage: &mut RunningStage,
        task_id: usize,
        executor: &str,
        partitions: Vec<usize>,
    ) {
        let vcores_consumed = partitions.len() as u32;
        stage.task_infos.push(TaskInfo {
            task_id,
            scheduled_time: 50,
            launch_time: 100,
            start_exec_time: 200,
            end_exec_time: 0,
            finish_time: 0,
            task_status: task_status::Status::Running(RunningTask {
                executor_id: executor.to_string(),
            }),
            global_input_partition_ids: partitions,
            vcores_consumed,
        });
    }

    /// Verify that a normal status update transitions the task to Successful.
    #[test]
    fn test_update_task_info_normal_update_succeeds() {
        let mut stage = make_running_stage(2);
        append_running_task(&mut stage, 0, "executor-1", vec![0]);

        let status = make_task_status(0);
        let result = stage.update_task_info(0, status);

        assert!(result);
        assert!(matches!(
            stage.task_infos[0].task_status,
            task_status::Status::Successful(_)
        ));
    }

    /// After `reset_tasks` marks a task as ResultLost, a late status update
    /// from the (now lost) executor must be rejected without panicking.
    #[test]
    fn test_update_task_info_after_executor_lost() {
        let mut stage = make_running_stage(2);
        append_running_task(&mut stage, 0, "executor-1", vec![0]);
        append_running_task(&mut stage, 1, "executor-1", vec![1]);
        // Both partitions were drained by binds; nothing left pending.
        stage.pending.next_slice(2);

        // Executor heartbeat times out - tasks are marked lost and
        // their partitions get pushed back to pending.
        let reset_count = stage.reset_tasks("executor-1");
        assert_eq!(reset_count, 2);
        assert_eq!(stage.pending.remaining(), 2);
        assert!(matches!(
            &stage.task_infos[0].task_status,
            task_status::Status::Failed(FailedTask {
                failed_reason: Some(FailedReason::ResultLost(_)),
                ..
            })
        ));

        // Late status update from the (now lost) executor.
        let status = make_task_status(0);
        let result = stage.update_task_info(0, status);

        // Should gracefully reject the update, not panic.
        assert!(!result);
    }

    #[test]
    fn test_update_task_metrics_keeps_raw_partition_snapshots() {
        let mut stage = make_running_stage(3);

        stage
            .update_task_metrics(0, vec![make_operator_metrics_set(100, 10)])
            .unwrap();

        let metrics = stage.stage_metrics.as_ref().unwrap();
        assert_eq!(metrics.len(), 1);

        let operator_metrics = &metrics[0];
        assert_eq!(operator_metrics.iter().count(), 2);
        assert!(
            operator_metrics
                .iter()
                .all(|metric| metric.partition() == Some(0))
        );

        let aggregated = operator_metrics.aggregate_by_name();
        assert_eq!(aggregated.output_rows(), Some(100));
        assert_eq!(aggregated.elapsed_compute().unwrap(), 10);

        stage
            .update_task_metrics(1, vec![make_operator_metrics_set(200, 20)])
            .unwrap();
        stage
            .update_task_metrics(2, vec![make_operator_metrics_set(300, 30)])
            .unwrap();

        let metrics = stage.stage_metrics.as_ref().unwrap();
        let operator_metrics = &metrics[0];
        assert_eq!(operator_metrics.iter().count(), 6);

        let partitions = operator_metrics
            .iter()
            .filter(|metric| matches!(metric.value(), MetricValue::OutputRows(_)))
            .map(|metric| metric.partition())
            .collect::<Vec<_>>();
        assert_eq!(partitions, vec![Some(0), Some(1), Some(2)]);

        let aggregated = operator_metrics.aggregate_by_name();
        assert_eq!(aggregated.output_rows(), Some(600));
        assert_eq!(aggregated.elapsed_compute().unwrap(), 60);

        stage
            .update_task_metrics(1, vec![make_operator_metrics_set(250, 25)])
            .unwrap();

        let metrics = stage.stage_metrics.as_ref().unwrap();
        let operator_metrics = &metrics[0];
        assert_eq!(operator_metrics.iter().count(), 6);

        let mut output_rows = operator_metrics
            .iter()
            .filter_map(|metric| match metric.value() {
                MetricValue::OutputRows(value) => {
                    Some((metric.partition(), value.value()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        output_rows.sort_by_key(|(partition, _)| *partition);
        assert_eq!(
            output_rows,
            vec![(Some(0), 100), (Some(1), 250), (Some(2), 300)]
        );

        let aggregated = operator_metrics.aggregate_by_name();
        assert_eq!(aggregated.output_rows(), Some(650));
        assert_eq!(aggregated.elapsed_compute().unwrap(), 65);
    }
}
