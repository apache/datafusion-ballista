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

use crate::display::DisplayableBallistaExecutionPlan;
use crate::planner::DistributedPlanner;
use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{ShuffleWriterExec, UnresolvedShuffleExec};

use ballista_core::serde::protobuf::{
    self, CompletedJob, JobStatus, OperatorMetricsSet, QueuedJob, TaskStatus,
};
use ballista_core::serde::protobuf::{job_status, FailedJob, ShuffleWritePartition};
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, PartitionId, PartitionLocation, PartitionStats,
};
use datafusion::physical_plan::{
    accept, ExecutionPlan, ExecutionPlanVisitor, Metric, Partitioning,
};
use log::{debug, info};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};

use ballista_core::utils::collect_plan_metrics;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use std::sync::Arc;

/// This data structure collects the partition locations for an `ExecutionStage`.
/// Each `ExecutionStage` will hold a `StageOutput`s for each of its child stages.
/// When all tasks for the child stage are complete, it will mark the `StageOutput`
#[derive(Clone, Debug, Default)]
pub struct StageOutput {
    /// Map from partition -> partition locations
    pub(crate) partition_locations: HashMap<usize, Vec<PartitionLocation>>,
    /// Flag indicating whether all tasks are complete
    pub(crate) complete: bool,
}

impl StageOutput {
    pub fn new() -> Self {
        Self {
            partition_locations: HashMap::new(),
            complete: false,
        }
    }

    /// Add a `PartitionLocation` to the `StageOutput`
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

    pub fn is_complete(&self) -> bool {
        self.complete
    }
}

/// A stage in the ExecutionGraph.
///
/// This represents a set of tasks (one per each `partition`) which can
/// be executed concurrently.
#[derive(Clone)]
pub struct ExecutionStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Total number of output partitions for this stage.
    /// This stage will produce on task for partition.
    pub(crate) partitions: usize,
    /// Output partitioning for this stage.
    pub(crate) output_partitioning: Option<Partitioning>,
    /// Represents the outputs from this stage's child stages.
    /// This stage can only be resolved an executed once all child stages are completed.
    pub(crate) inputs: HashMap<usize, StageOutput>,
    // `ExecutionPlan` for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Status of each already scheduled task. If status is None, the partition has not yet been scheduled
    pub(crate) task_statuses: Vec<Option<task_status::Status>>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    /// If `output_links` is empty then this the final stage in the `ExecutionGraph`
    pub(crate) output_links: Vec<usize>,
    /// Flag indicating whether all input partitions have been resolved and the plan
    /// has UnresovledShuffleExec operators resolved to ShuffleReadExec operators.
    pub(crate) resolved: bool,
    /// Combined metrics of the already finished tasks in the stage, If it is None, no task is finished yet.
    pub(crate) stage_metrics: Option<Vec<MetricsSet>>,
}

impl Debug for ExecutionStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent();
        let scheduled_tasks = self.task_statuses.iter().filter(|t| t.is_some()).count();

        write!(
            f,
            "Stage[id={}, partitions={:?}, children={}, completed_tasks={}, resolved={}, scheduled_tasks={}, available_tasks={}]\nInputs{:?}\n\n{}",
            self.stage_id,
            self.partitions,
            self.inputs.len(),
            self.completed_tasks(),
            self.resolved,
            scheduled_tasks,
            self.available_tasks(),
            self.inputs,
            plan
        )
    }
}

impl ExecutionStage {
    pub fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_partitioning: Option<Partitioning>,
        output_links: Vec<usize>,
        child_stages: Vec<usize>,
    ) -> Self {
        let num_tasks = plan.output_partitioning().partition_count();

        let resolved = child_stages.is_empty();

        let mut inputs: HashMap<usize, StageOutput> = HashMap::new();

        for input_stage_id in &child_stages {
            inputs.insert(*input_stage_id, StageOutput::new());
        }

        Self {
            stage_id,
            partitions: num_tasks,
            output_partitioning,
            inputs,
            plan,
            task_statuses: vec![None; num_tasks],
            output_links,
            resolved,
            stage_metrics: None,
        }
    }

    /// Returns true if all inputs are complete and we can resolve all
    /// UnresolvedShuffleExec operators to ShuffleReadExec
    pub fn resolvable(&self) -> bool {
        self.inputs.iter().all(|(_, outputs)| outputs.is_complete())
    }

    /// Returns `true` if all tasks for this stage are complete
    pub fn complete(&self) -> bool {
        self.task_statuses
            .iter()
            .all(|status| matches!(status, Some(task_status::Status::Completed(_))))
    }

    /// Returns the number of tasks
    pub fn completed_tasks(&self) -> usize {
        self.task_statuses
            .iter()
            .filter(|status| matches!(status, Some(task_status::Status::Completed(_))))
            .count()
    }

    /// Marks the input stage ID as complete.
    pub fn complete_input(&mut self, stage_id: usize) {
        if let Some(input) = self.inputs.get_mut(&stage_id) {
            input.complete = true;
        }
    }

    /// Returns true if the stage plan has all UnresolvedShuffleExec operators resolved to
    /// ShuffleReadExec
    pub fn resolved(&self) -> bool {
        self.resolved
    }

    /// Returns the number of tasks in this stage which are available for scheduling.
    /// If the stage is not yet resolved, then this will return `0`, otherwise it will
    /// return the number of tasks where the task status is not yet set.
    pub fn available_tasks(&self) -> usize {
        if self.resolved {
            self.task_statuses.iter().filter(|s| s.is_none()).count()
        } else {
            0
        }
    }

    /// Resolve any UnresolvedShuffleExec operators within this stage's plan
    pub fn resolve_shuffles(&mut self) -> Result<()> {
        println!("Resolving shuffles\n{:?}", self);
        if self.resolved {
            // If this stage has no input shuffles, then it is already resolved
            Ok(())
        } else {
            let input_locations = self
                .inputs
                .iter()
                .map(|(stage, outputs)| (*stage, outputs.partition_locations.clone()))
                .collect();
            // Otherwise, rewrite the plan to replace UnresolvedShuffleExec with ShuffleReadExec
            let new_plan = crate::planner::remove_unresolved_shuffles(
                self.plan.clone(),
                &input_locations,
            )?;
            self.plan = new_plan;
            self.resolved = true;
            Ok(())
        }
    }

    /// Update the status for task partition
    pub fn update_task_status(&mut self, partition: usize, status: task_status::Status) {
        debug!("Updating task status for partition {}", partition);
        self.task_statuses[partition] = Some(status);
    }

    /// update and combine the task metrics to the stage metrics
    pub fn update_task_metrics(
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

    pub fn combine_metrics_set(
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

    /// Add input partitions published from an input stage.
    pub fn add_input_partitions(
        &mut self,
        stage_id: usize,
        _partition_id: usize,
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

            execution_stages.insert(
                stage_id,
                ExecutionStage::new(
                    stage_id,
                    stage,
                    partitioning,
                    output_links,
                    child_stages,
                ),
            );
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
/// Stage[id=2, partitions=4, children=1, completed_tasks=0, resolved=false, scheduled_tasks=0, available_tasks=0]
/// Inputs{1: StageOutput { partition_locations: {}, complete: false }}
///
/// ShuffleWriterExec: None
///   AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[SUM(?table?.gmv)]
///     CoalesceBatchesExec: target_batch_size=4096
///       UnresolvedShuffleExec
///
/// Stage[id=1, partitions=1, children=0, completed_tasks=0, resolved=true, scheduled_tasks=0, available_tasks=1]
/// Inputs{}
///
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
    /// ID for this job
    pub(crate) job_id: String,
    /// Session ID for this job
    pub(crate) session_id: String,
    /// Status of this job
    pub(crate) status: JobStatus,
    /// Map from Stage ID -> ExecutionStage
    pub(crate) stages: HashMap<usize, ExecutionStage>,
    /// Total number fo output partitions
    pub(crate) output_partitions: usize,
    /// Locations of this `ExecutionGraph` final output locations
    pub(crate) output_locations: Vec<PartitionLocation>,
}

impl ExecutionGraph {
    pub fn new(
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

    /// An ExecutionGraph is complete if all its stages are complete
    pub fn complete(&self) -> bool {
        self.stages.values().all(|s| s.complete())
    }

    /// Update task statuses and task metrics in the graph.
    /// This will also push shuffle partitions to their respective shuffle read stages.
    pub fn update_task_status(
        &mut self,
        executor: &ExecutorMetadata,
        statuses: Vec<TaskStatus>,
    ) -> Result<()> {
        for status in statuses.into_iter() {
            if let TaskStatus {
                task_id:
                    Some(protobuf::PartitionId {
                        job_id,
                        stage_id,
                        partition_id,
                    }),
                metrics: operator_metrics,
                status: Some(task_status),
            } = status
            {
                if job_id != self.job_id() {
                    return Err(BallistaError::Internal(format!(
                        "Error updating job {}: Invalid task status job ID {}",
                        self.job_id(),
                        job_id
                    )));
                }

                let stage_id = stage_id as usize;
                let partition = partition_id as usize;
                if let Some(stage) = self.stages.get_mut(&stage_id) {
                    stage.update_task_status(partition, task_status.clone());
                    let stage_plan = stage.plan.clone();
                    let stage_complete = stage.complete();

                    // TODO Should be able to reschedule this task.
                    if let task_status::Status::Failed(failed_task) = task_status {
                        self.status = JobStatus {
                            status: Some(job_status::Status::Failed(FailedJob {
                                error: format!(
                                    "Task {}/{}/{} failed: {}",
                                    job_id, stage_id, partition_id, failed_task.error
                                ),
                            })),
                        };
                        return Ok(());
                    } else if let task_status::Status::Completed(completed_task) =
                        task_status
                    {
                        // update task metrics for completed task
                        stage.update_task_metrics(partition, operator_metrics)?;

                        // if this stage is completed, we want to combine the stage metrics to plan's metric set and print out the plan
                        if stage_complete && stage.stage_metrics.as_ref().is_some() {
                            // The plan_metrics collected here is a snapshot clone from the plan metrics.
                            // They are all empty now and need to combine with the stage metrics in the ExecutionStages
                            let mut plan_metrics =
                                collect_plan_metrics(stage_plan.as_ref());
                            let stage_metrics = stage
                                .stage_metrics
                                .as_ref()
                                .expect("stage metrics should not be None.");
                            if plan_metrics.len() != stage_metrics.len() {
                                return Err(BallistaError::Internal(format!("Error combine stage metrics to plan for stage {},  plan metrics array size {} does not equal \
                to the stage metrics array size {}", stage_id, plan_metrics.len(), stage_metrics.len())));
                            }
                            plan_metrics.iter_mut().zip(stage_metrics).for_each(
                                |(plan_metric, stage_metric)| {
                                    stage_metric
                                        .iter()
                                        .for_each(|s| plan_metric.push(s.clone()));
                                },
                            );

                            info!(
                                "=== [{}/{}/{}] Stage finished, physical plan with metrics ===\n{}\n",
                                job_id,
                                stage_id,
                                partition,
                                DisplayableBallistaExecutionPlan::new(stage_plan.as_ref(), plan_metrics.as_ref()).indent()
                            );
                        }

                        let locations = partition_to_location(
                            self.job_id.as_str(),
                            stage_id,
                            executor,
                            completed_task.partitions,
                        );

                        let output_links = stage.output_links.clone();
                        if output_links.is_empty() {
                            // If `output_links` is empty, then this is a final stage
                            self.output_locations.extend(locations);
                        } else {
                            for link in output_links.into_iter() {
                                // If this is an intermediate stage, we need to push its `PartitionLocation`s to the parent stage
                                if let Some(linked_stage) = self.stages.get_mut(&link) {
                                    linked_stage.add_input_partitions(
                                        stage_id,
                                        partition,
                                        locations.clone(),
                                    )?;

                                    // If all tasks for this stage are complete, mark the input complete in the parent stage
                                    if stage_complete {
                                        linked_stage.complete_input(stage_id);
                                    }

                                    // If all input partitions are ready, we can resolve any UnresolvedShuffleExec in the parent stage plan
                                    if linked_stage.resolvable() {
                                        linked_stage.resolve_shuffles()?;
                                    }
                                } else {
                                    return Err(BallistaError::Internal(format!("Error updating job {}: Invalid output link {} for stage {}", job_id, stage_id, link)));
                                }
                            }
                        }
                    }
                } else {
                    return Err(BallistaError::Internal(format!(
                        "Invalid stage ID {} for job {}",
                        stage_id,
                        self.job_id()
                    )));
                }
            }
        }

        Ok(())
    }

    /// Total number of tasks in this plan that are ready for scheduling
    pub fn available_tasks(&self) -> usize {
        self.stages
            .iter()
            .map(|(_, stage)| stage.available_tasks())
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
        self.stages.iter_mut().find(|(_stage_id, stage)| {
            stage.resolved() && stage.available_tasks() > 0
        }).map(|(stage_id, stage)| {
            let (partition_id,_) = stage
                .task_statuses
                .iter()
                .enumerate()
                .find(|(_partition,status)| status.is_none())
                .ok_or_else(|| {
                BallistaError::Internal(format!("Error getting next task for job {}: Stage {} is ready but has no pending tasks", job_id, stage_id))
            })?;

             let partition = PartitionId {
                job_id,
                stage_id: *stage_id,
                partition_id
            };

            // Set the status to Running
            stage.task_statuses[partition_id] = Some(task_status::Status::Running(RunningTask {
                executor_id: executor_id.to_owned()
            }));

            Ok(Task {
                session_id,
                partition,
                plan: stage.plan.clone(),
                output_partitioning: stage.output_partitioning.clone()
            })
        }).transpose()
    }

    pub fn finalize(&mut self) -> Result<()> {
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

    pub fn update_status(&mut self, status: JobStatus) {
        self.status = status;
    }

    /// Reset the status for the given task. This should be called is a task failed to
    /// launch and it needs to be returned to the set of available tasks and be
    /// re-scheduled.
    pub fn reset_task_status(&mut self, task: Task) {
        let stage_id = task.partition.stage_id;
        let partition = task.partition.partition_id;

        if let Some(stage) = self.stages.get_mut(&stage_id) {
            stage.task_statuses[partition] = None;
        }
    }

    pub fn output_locations(&self) -> Vec<PartitionLocation> {
        self.output_locations.clone()
    }
}

impl Debug for ExecutionGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let stages = self
            .stages
            .iter()
            .map(|(_, stage)| format!("{:?}", stage))
            .collect::<Vec<String>>()
            .join("\n");
        write!(f, "ExecutionGraph[job_id={}, session_id={}, available_tasks={}, complete={}]\n{}", self.job_id, self.session_id, self.available_tasks(), self.complete(), stages)
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
    use crate::state::execution_graph::ExecutionGraph;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{self, job_status, task_status};
    use ballista_core::serde::scheduler::{ExecutorMetadata, ExecutorSpecification};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, sum, Expr};

    use datafusion::logical_plan::JoinType;
    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::test_util::scan_empty;

    use std::sync::Arc;

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
        agg_graph.finalize()?;

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

    fn drain_tasks(graph: &mut ExecutionGraph) -> Result<()> {
        let executor = test_executor();
        let job_id = graph.job_id().to_owned();
        while let Some(task) = graph.pop_next_task("executor-id")? {
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
            let task_status = protobuf::TaskStatus {
                status: Some(task_status::Status::Completed(protobuf::CompletedTask {
                    executor_id: "executor-1".to_owned(),
                    partitions,
                })),
                metrics: vec![],
                task_id: Some(protobuf::PartitionId {
                    job_id: job_id.clone(),
                    stage_id: task.partition.stage_id as u32,
                    partition_id: task.partition.partition_id as u32,
                }),
            };

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

        ExecutionGraph::new("job", "session", plan).unwrap()
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
            .limit(None, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let optimized_plan = ctx.optimize(&logical_plan).unwrap();

        let plan = ctx.create_physical_plan(&optimized_plan).await.unwrap();

        ExecutionGraph::new("job", "session", plan).unwrap()
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

        let graph = ExecutionGraph::new("job", "session", plan).unwrap();

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

        let graph = ExecutionGraph::new("job", "session", plan).unwrap();

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

        let graph = ExecutionGraph::new("job", "session", plan).unwrap();

        println!("{:?}", graph);

        graph
    }

    fn test_executor() -> ExecutorMetadata {
        ExecutorMetadata {
            id: "executor-2".to_string(),
            host: "localhost2".to_string(),
            port: 8080,
            grpc_port: 9090,
            specification: ExecutorSpecification { task_slots: 1 },
        }
    }
}
