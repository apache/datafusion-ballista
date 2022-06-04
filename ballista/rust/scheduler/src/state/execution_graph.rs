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

use crate::planner::DistributedPlanner;
use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{
    ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec,
};

use ballista_core::serde::protobuf::{
    self, CompletedJob, JobStatus, QueuedJob, TaskStatus,
};
use ballista_core::serde::protobuf::{job_status, FailedJob, ShuffleWritePartition};
use ballista_core::serde::protobuf::{task_status, RunningTask};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, PartitionId, PartitionLocation, PartitionStats,
};
use datafusion::physical_plan::{
    accept, with_new_children_if_necessary, ExecutionPlan, ExecutionPlanVisitor,
    Partitioning,
};
use log::{debug, info};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};

use datafusion::physical_plan::display::DisplayableExecutionPlan;
use std::sync::Arc;

/// A single stage in the ExecutionGraph
#[derive(Clone)]
pub struct ExecutionStage {
    /// Stage ID
    pub(crate) stage_id: usize,
    /// Total number of output partitions from this stage per task
    pub(crate) output_partitions: Vec<usize>,
    /// Output partitioning for this stage.
    pub(crate) output_partitioning: Option<Partitioning>,
    /// The number of output partitions from the previous stage for each input partition. In the case of a repartition
    /// operation, the child stage may produce multiple partitions that need to be aggregated
    /// in the parent stage.
    /// For example, if the child stage has 10 partitions and does a hash repartition (i.e. for groupBy aggregation)
    /// then `input_partition_count` would be vec![10; 10]. However if the child stage has only 1 partition
    /// and does a hash repartition then  `input_partition_count` would be vec![10; 1]. This can happen
    /// when the child stage partition count is determined by the number of underlying files it needs to scan,
    /// in which case the overall queries desired partitioning can differ from the individual stage partitioning.
    pub(crate) input_partition_count: Vec<usize>,
    /// Execution Plan for this stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Locations of input partitions for this stage.
    pub(crate) input_locations: HashMap<usize, Vec<PartitionLocation>>,
    /// Statistics for output partitions from this stage,
    pub(crate) partitions_stats: HashMap<usize, Vec<PartitionStats>>,
    /// Status of each already scheduled task. If status is None, the partition has not yet been scheduled
    pub(crate) task_statuses: Vec<Option<task_status::Status>>,
    /// Stage ID of the stage that will take this stages outputs as inputs.
    pub(crate) output_link: Option<usize>,
    /// Flag indicating whether all input partitions have been resolved and the plan
    /// has UnresovledShuffleExec operators resolved to ShuffleReadExec operators.
    pub(crate) resolved: bool,
}

impl Debug for ExecutionStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let plan = DisplayableExecutionPlan::new(self.plan.as_ref()).indent();
        let running_tasks = self
            .task_statuses
            .iter()
            .filter(|t| t.is_some())
            .collect::<Vec<_>>()
            .len();
        write!(f, "Stage[id={}, output_partitions={:?}, input_partition_count={:?}, resolved={}, scheduled_tasks={}, available_tasks={}]\n{}", self.stage_id, self.output_partitions, self.input_partition_count, self.resolved, running_tasks, self.available_tasks(), plan)
    }
}

impl ExecutionStage {
    pub fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        output_partitioning: Option<Partitioning>,
        input_partition_count: Vec<usize>,
        output_link: Option<usize>,
    ) -> Self {
        let mut output_partitions = vec![];
        let num_tasks = plan.output_partitioning().partition_count();

        for _ in 0..num_tasks {
            output_partitions.push(
                output_partitioning
                    .as_ref()
                    .map(|p| p.partition_count())
                    .unwrap_or(1),
            )
        }

        let resolved = if input_partition_count.len() == 0 {
            true
        } else {
            false
        };
        Self {
            stage_id,
            output_partitions,
            output_partitioning,
            input_partition_count,
            plan,
            input_locations: HashMap::with_capacity(num_tasks),
            partitions_stats: HashMap::with_capacity(num_tasks),
            task_statuses: vec![None; num_tasks],
            output_link,
            resolved,
        }
    }

    /// Returns true if all input partitions are present and we can resolve the stage plans
    /// UnresolvedShuffleExec operators to ShuffleReadExec
    pub fn resolvable(&self) -> bool {
        self.input_locations.len() == self.input_partition_count.len()
    }

    /// Returns true if the stage plan has all UnresolvedShuffleExec operators resolved to
    /// ShuffleReadExec
    pub fn resolved(&self) -> bool {
        self.resolved
    }

    pub fn available_tasks(&self) -> usize {
        if self.resolved {
            self.task_statuses
                .iter()
                .filter(|s| s.is_none())
                .collect::<Vec<&Option<_>>>()
                .len()
        } else {
            0
        }
    }

    /// Resolve any UnresolvedShuffleExec operators within this stage's plan
    pub fn resolve_shuffles(&mut self) -> Result<()> {
        println!("Resolving shuffles\n{:?}", self);
        if self.input_partition_count.is_empty() || self.resolved {
            // If this stage has no input shuffles, then it is already resolved
            Ok(())
        } else {
            // Otherwise, rewrite the plan to replace UnresolvedShuffleExec with ShuffleReadExec
            let new_plan =
                remove_unresolved_shuffles(self.plan.clone(), &self.input_locations)?;
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

    pub fn add_input_partitions(&mut self, partitions: Vec<PartitionLocation>) {
        for partition in partitions {
            let partition_id = partition.partition_id.partition_id;
            if let Some(parts) = self.input_locations.get_mut(&partition_id) {
                parts.push(partition)
            } else {
                self.input_locations.insert(partition_id, vec![partition]);
            }
        }
    }
}

struct ExecutionStageBuilder {
    /// Stage ID which is currently being visited
    current_stage_id: usize,
    input_partition_counts: HashMap<usize, Vec<usize>>,
    /// Map from Stage ID -> output link
    output_links: HashMap<usize, usize>,
}

impl ExecutionStageBuilder {
    pub fn new() -> Self {
        Self {
            current_stage_id: 0,
            input_partition_counts: HashMap::new(),
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
            let output_link = self.output_links.remove(&stage_id);
            let input_partition_count = self
                .input_partition_counts
                .remove(&stage_id)
                .unwrap_or(vec![0; 0]);

            execution_stages.insert(
                stage_id,
                ExecutionStage::new(
                    stage_id,
                    stage,
                    partitioning,
                    input_partition_count,
                    output_link,
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
            let num_input_tasks = unresolved_shuffle.input_partition_count;
            let partitions_per_task = unresolved_shuffle.output_partition_count;
            self.output_links
                .insert(unresolved_shuffle.stage_id, self.current_stage_id);
            self.input_partition_counts.insert(
                self.current_stage_id,
                vec![partitions_per_task; num_input_tasks],
            );
        }
        Ok(true)
    }
}

#[derive(Clone)]
pub struct Task {
    pub session_id: String,
    pub partition: PartitionId,
    pub plan: Arc<dyn ExecutionPlan>,
    pub output_partitioning: Option<Partitioning>,
}

impl Debug for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Task[session_id: {}, job: {}, stage: {}, partition: {}]",
            self.session_id,
            self.partition.job_id,
            self.partition.stage_id,
            self.partition.partition_id
        )
    }
}

/// Represents the graph for a distributed query plan.
#[derive(Clone)]
pub struct ExecutionGraph {
    pub(crate) job_id: String,
    pub(crate) session_id: String,
    pub(crate) status: JobStatus,
    pub(crate) stages: HashMap<usize, ExecutionStage>,
    pub(crate) output_partitions: usize,
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

    pub fn complete(&self) -> bool {
        self.output_partitions == self.output_locations.len()
    }

    /// Update task statuses in the graph. This will push shuffle partitions to their
    /// respective shuffle read stages.
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
                status: Some(task_status),
            } = status
            {
                if &job_id != self.job_id() {
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
                        let locations = partition_to_location(
                            self.job_id.as_str(),
                            stage_id,
                            partition,
                            executor,
                            completed_task.partitions,
                        );

                        if let Some(link) = stage.output_link {
                            if let Some(linked_stage) = self.stages.get_mut(&link) {
                                linked_stage.add_input_partitions(locations);

                                // If all input partitions are ready, we can resolve any UnresolvedShuffleExec in the stage plan
                                if linked_stage.resolvable() {
                                    linked_stage.resolve_shuffles()?;
                                }
                            } else {
                                return Err(BallistaError::Internal(format!("Error updating job {}: Invalid output link {} for stage {}", job_id, stage_id, link)));
                            }
                        } else {
                            self.output_locations.extend(locations);
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

    pub fn available_tasks(&self) -> usize {
        let mut available = 0;

        for (_, stage) in &self.stages {
            available += stage.available_tasks();
        }

        available
    }

    /// Get next task that can be assigned to the given executor.
    /// This method should only be called when the resulting task is immediately
    /// being launched as the status will be set to Running and it will not be
    /// available to the scheduler.
    /// If the task is not launch the status must be reset to allow the task to
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
    pub fn reset_task_status(&mut self, task: Task, _status: task_status::Status) {
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
    partition_id: usize,
    executor: &ExecutorMetadata,
    shuffles: Vec<ShuffleWritePartition>,
) -> Vec<PartitionLocation> {
    shuffles
        .into_iter()
        .map(|shuffle| PartitionLocation {
            partition_id: PartitionId {
                job_id: job_id.to_owned(),
                stage_id,
                partition_id,
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

fn remove_unresolved_shuffles(
    plan: Arc<dyn ExecutionPlan>,
    input_locations: &HashMap<usize, Vec<PartitionLocation>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for child in plan.children() {
        if let Some(unresolved_shuffle) =
            child.as_any().downcast_ref::<UnresolvedShuffleExec>()
        {
            let mut relevant_locations = vec![];

            for i in 0..unresolved_shuffle.output_partition_count {
                if let Some(x) = input_locations.get(&i) {
                    relevant_locations.push(x.to_owned());
                } else {
                    relevant_locations.push(vec![]);
                }
            }
            info!(
                "Creating shuffle reader: {}",
                relevant_locations
                    .iter()
                    .map(|c| c
                        .iter()
                        .map(|l| l.path.clone())
                        .collect::<Vec<_>>()
                        .join(", "))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
            new_children.push(Arc::new(ShuffleReaderExec::try_new(
                relevant_locations,
                unresolved_shuffle.schema().clone(),
            )?))
        } else {
            new_children.push(remove_unresolved_shuffles(child, &input_locations)?);
        }
    }
    Ok(with_new_children_if_necessary(plan, new_children)?)
}

#[cfg(test)]
mod test {
    use crate::state::execution_graph::ExecutionGraph;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{self, job_status, task_status};
    use ballista_core::serde::scheduler::{ExecutorMetadata, ExecutorSpecification};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, Expr, sum};

    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::test_util::scan_empty;
    use std::sync::Arc;
    use datafusion::logical_plan::JoinType;
    use rand::{Rng, thread_rng};

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

        assert!(
            join_graph.complete(),
            "Failed to complete join plan"
        );

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

    #[tokio::test]
    async fn test_partition_counts() -> Result<()> {
        let mut agg_graph = test_aggregation_plan(4).await;

        assert_eq!(agg_graph.output_partitions, 4);

        let stage_1_out = agg_graph
            .stages
            .get(&1)
            .cloned()
            .map(|first_stage| first_stage.output_partitions)
            .unwrap();
        assert_eq!(stage_1_out, vec![4]);

        let stage_1_in = agg_graph
            .stages
            .get(&2)
            .cloned()
            .map(|stage| stage.input_partition_count)
            .unwrap();
        assert_eq!(stage_1_in, vec![4]);

        let stage_2_out = agg_graph
            .stages
            .get(&2)
            .cloned()
            .map(|first_stage| first_stage.output_partitions)
            .unwrap();
        assert_eq!(stage_2_out, vec![1, 1, 1, 1]);

        Ok(())
    }

    fn drain_tasks(graph: &mut ExecutionGraph) -> Result<()> {
        let mut rng = thread_rng();
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
                    path: "path".to_string(),
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
            .limit(1)
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


        let left_plan = scan_empty(Some("left"), &schema, None)
            .unwrap();

        let right_plan = scan_empty(Some("right"), &schema, None)
            .unwrap()
            .build()
            .unwrap();

        let sort_expr = Expr::Sort {
            expr: Box::new(col("id")),
            asc: false,
            nulls_first: false
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
