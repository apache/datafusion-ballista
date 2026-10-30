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

//! The execution graph of a job run with `ballista.execution.mode =
//! pipelined`, [`PipelinedExecutionGraph`].
//!
//! A blocking job runs stage by stage, each stage becoming runnable once the
//! stages it reads have written their shuffle files. A pipelined job instead
//! places every task of every stage at once, and stages stream to each other
//! while they run (see `ballista_core::execution_plans::pipelined`). So:
//!
//! - Stages are planned with the static [`DistributedPlanner`], and each
//!   stage's root becomes a `PipelinedShuffleWriterExec`.
//! - Placement is all or nothing: a partially placed job would deadlock, its
//!   consumers waiting on producers that never run. The scheduler asks for a
//!   [`Placement`] with [`plan_placement`], reserves the vcores, and commits
//!   it with [`PipelinedExecutionGraph::place`], which resolves every reader
//!   to the producer tasks it streams from.
//! - The job reserves, on each executor, the most vcores any one of its
//!   stages uses there rather than the sum, since its stages share CPU and a
//!   task waiting on another is a parked future, not a thread. For the same
//!   reason a stage wider than the cluster, such as a `UNION ALL` of several
//!   scans, can run several partitions per vcore: it waits for every vcore
//!   to be free instead of failing. The reservation is released when the job
//!   finishes, not task by task.
//! - Nothing is retried. A failed task or a lost executor fails the job.
//!   When the final stage completes, tasks still running — producers whose
//!   consumer stopped early, as under a `LIMIT` — are cancelled.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::{
    PipelinedShuffleReaderExec, PipelinedShuffleWriterExec, ShuffleWriter,
    UnresolvedShuffleExec, compute_global_output_partition_ids,
};
use ballista_core::serde::protobuf::failed_task::FailedReason;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{
    AvailableVcores, FailedJob, JobStatus, RunningJob, SuccessfulJob, TaskStatus,
    task_status,
};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, PartitionId, PartitionLocation, PipelinedPartitionLocation, TaskKey,
};
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use datafusion::prelude::SessionConfig;
use log::{info, warn};

use crate::cache::CACHE_DATA_LOST;
use crate::cluster::{BoundTask, ExecutorSlot, stage_has_input_collapse};
use crate::planner::{DistributedPlanner, find_unresolved_shuffles};
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::timestamp_millis;
use crate::state::execution_graph::{
    ExecutionGraph, ExecutionGraphBox, ExecutionStage, ExecutionStageBuilder,
    RunningTaskInfo, TaskDescription, create_task_info,
};
use crate::state::execution_stage::{
    PendingPartitions, RunningStage, is_external_fetch_failure, stage_input_partitions,
};
use crate::state::task_builder::stages_read_whole;

/// What placing one stage of a pipelined job takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRequirement {
    /// The stage.
    pub stage_id: usize,
    /// Input partitions the stage's tasks process between them.
    pub partitions: usize,
    /// The stage collapses its input to one output partition, so all of its
    /// partitions must go to a single task.
    pub collapse: bool,
}

/// Where every task of a pipelined job runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// For each stage, its tasks in task-id order: the executor each runs on
    /// and the partitions it processes.
    pub tasks: BTreeMap<usize, Vec<(String, Vec<usize>)>>,
    /// Vcores to reserve on each executor: the most any one stage uses there.
    pub reservation: Vec<ExecutorSlot>,
}

/// The outcome of [`plan_placement`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementDecision {
    /// The job fits in the free vcores now.
    Fits(Placement),
    /// The job does not fit now, but will once vcores free up.
    Wait,
}

/// Decides where every task of a pipelined job runs, given each executor's
/// free vcores.
///
/// Each stage is planned against the full free capacity, because a job's
/// stages share the vcores it reserves (see the [module documentation](self)).
/// A collapse stage gets one task and one vcore. Any other stage gets one
/// vcore per partition: its partitions are spread round-robin over the
/// executors with room, in contiguous ranges, and each executor's range is
/// cut into tasks of at most `max_partitions_per_task` partitions (`0` means
/// unbounded).
///
/// `total_vcores` is the cluster's capacity when idle. A stage wider than
/// that needs every vcore free, and then shares them: its partitions are
/// dealt round-robin again once each free vcore has one.
pub fn plan_placement(
    requirements: &[StageRequirement],
    available: &[AvailableVcores],
    total_vcores: usize,
    max_partitions_per_task: usize,
) -> PlacementDecision {
    // Most free first, then by id, so placement is deterministic.
    let mut budgets: Vec<(&str, usize)> = available
        .iter()
        .filter(|a| a.vcores > 0)
        .map(|a| (a.executor_id.as_str(), a.vcores as usize))
        .collect();
    budgets.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    if budgets.is_empty() {
        return PlacementDecision::Wait;
    }
    let cap = if max_partitions_per_task == 0 {
        usize::MAX
    } else {
        max_partitions_per_task
    };

    let mut reserved: HashMap<&str, usize> = HashMap::new();
    let mut tasks = BTreeMap::new();
    for requirement in requirements {
        let mut stage_tasks = vec![];
        if requirement.collapse {
            let executor = budgets[0].0;
            stage_tasks
                .push((executor.to_owned(), (0..requirement.partitions).collect()));
            let entry = reserved.entry(executor).or_default();
            *entry = (*entry).max(1);
        } else {
            let Some(counts) = spread(requirement.partitions, &budgets, total_vcores)
            else {
                return PlacementDecision::Wait;
            };
            let mut next = 0;
            for ((executor, free), count) in budgets.iter().zip(counts) {
                if count == 0 {
                    continue;
                }
                let partitions: Vec<usize> = (next..next + count).collect();
                next += count;
                for chunk in partitions.chunks(cap) {
                    stage_tasks.push(((*executor).to_owned(), chunk.to_vec()));
                }
                let entry = reserved.entry(executor).or_default();
                *entry = (*entry).max(count.min(*free));
            }
        }
        tasks.insert(requirement.stage_id, stage_tasks);
    }

    let mut reservation: Vec<ExecutorSlot> = reserved
        .into_iter()
        .map(|(executor, vcores)| (executor.to_owned(), vcores as u32))
        .collect();
    reservation.sort();
    PlacementDecision::Fits(Placement { tasks, reservation })
}

/// Deals `partitions` one at a time, round-robin, to the executors that still
/// have a free vcore, and deals what is left the same way once every free
/// vcore has a partition. Returns how many each executor got, or `None` if
/// fewer vcores are free than the stage needs: one per partition, but no more
/// than the cluster's `total_vcores`.
fn spread(
    partitions: usize,
    budgets: &[(&str, usize)],
    total_vcores: usize,
) -> Option<Vec<usize>> {
    let free: usize = budgets.iter().map(|(_, free)| free).sum();
    if free < partitions.min(total_vcores) || free == 0 {
        return None;
    }
    let mut counts = vec![0; budgets.len()];
    let mut left = partitions;
    while left > 0 {
        let round = left.min(free);
        let mut dealt = vec![0; budgets.len()];
        let mut total = 0;
        while total < round {
            for (i, (_, free)) in budgets.iter().enumerate() {
                if total == round {
                    break;
                }
                if dealt[i] < *free {
                    dealt[i] += 1;
                    total += 1;
                }
            }
        }
        for (count, dealt) in counts.iter_mut().zip(dealt) {
            *count += dealt;
        }
        left -= round;
    }
    Some(counts)
}

/// The [`ExecutionGraph`] of a pipelined job. See the
/// [module documentation](self).
#[derive(Clone)]
pub struct PipelinedExecutionGraph {
    scheduler_id: Option<String>,
    job_id: JobId,
    job_name: String,
    session_id: String,
    status: JobStatus,
    queued_at: u64,
    start_time: u64,
    end_time: u64,
    stages: HashMap<usize, ExecutionStage>,
    /// The stage whose output the client reads.
    final_stage_id: usize,
    /// Whether every task has been placed. A job is placed once.
    placed: bool,
    /// Vcores reserved on each executor, held until the job finishes.
    reservation: Vec<ExecutorSlot>,
    /// Executors running a task of this job. Losing one fails the job.
    executors: HashSet<String>,
    session_config: Arc<SessionConfig>,
    logical_plan: Option<String>,
    physical_plan: Arc<dyn ExecutionPlan>,
}

impl PipelinedExecutionGraph {
    /// Plans `plan` into stages and turns each stage's root into a
    /// `PipelinedShuffleWriterExec`. A stage read through a broadcast reader
    /// retains its outputs for replay to every consumer task.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scheduler_id: &str,
        job_id: &JobId,
        job_name: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
        session_config: Arc<SessionConfig>,
        planner: &mut dyn DistributedPlanner,
        logical_plan: Option<String>,
    ) -> Result<Self> {
        let shuffle_stages =
            planner.plan_query_stages(job_id, plan.clone(), session_config.options())?;

        // Stages whose every output partition more than one task reads in
        // full: a broadcast join's build side, and the input of a collapse
        // inside a stage of several tasks, such as a cross join's left side.
        // Their outputs are kept for each reader rather than queued once.
        let mut replayed_stages = HashSet::new();
        for stage in &shuffle_stages {
            let stage_plan: Arc<dyn ExecutionPlan> = stage.clone();
            for unresolved in find_unresolved_shuffles(&stage_plan)? {
                if unresolved.broadcast {
                    replayed_stages.insert(unresolved.stage_id);
                }
            }
            // A collapse stage runs as one task, so it reads its inputs once.
            if !stage_has_input_collapse(&stage_plan) {
                replayed_stages.extend(stages_read_whole(&stage_plan));
            }
        }

        let pipelined_stages = shuffle_stages
            .into_iter()
            .map(|stage| {
                let input = stage.children()[0].clone();
                let writer = PipelinedShuffleWriterExec::try_new(
                    job_id.clone(),
                    stage.stage_id(),
                    input,
                    stage.shuffle_output_partitioning().cloned(),
                    replayed_stages.contains(&stage.stage_id()),
                )?;
                Ok(Arc::new(writer) as Arc<dyn ShuffleWriter>)
            })
            .collect::<Result<Vec<_>>>()?;

        let stages =
            ExecutionStageBuilder::new(session_config.clone()).build(pipelined_stages)?;
        let final_stage_id = stages
            .iter()
            .filter(|(_, stage)| stage.output_links().is_empty())
            .map(|(stage_id, _)| *stage_id)
            .max()
            .ok_or_else(|| {
                BallistaError::Internal(format!(
                    "pipelined job {job_id} has no final stage"
                ))
            })?;

        let started_at = timestamp_millis();
        Ok(Self {
            scheduler_id: Some(scheduler_id.to_owned()),
            job_id: job_id.clone(),
            job_name: job_name.to_owned(),
            session_id: session_id.to_owned(),
            status: JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.to_owned(),
                status: Some(Status::Running(RunningJob {
                    queued_at,
                    started_at,
                    scheduler: scheduler_id.to_owned(),
                    pipelined_output_locations: vec![],
                })),
            },
            queued_at,
            start_time: started_at,
            end_time: 0,
            stages,
            final_stage_id,
            placed: false,
            reservation: vec![],
            executors: HashSet::new(),
            session_config,
            logical_plan,
            physical_plan: plan,
        })
    }

    fn is_running(&self) -> bool {
        matches!(self.status.status, Some(Status::Running(_)))
    }

    /// Whether the job still waits to be placed: running, and not placed yet.
    /// A job can stop waiting between planning its placement and committing
    /// it, if it is cancelled or fails meanwhile.
    pub fn awaits_placement(&self) -> bool {
        !self.placed && self.is_running()
    }

    /// What placing each stage takes, or `None` once the job is placed or no
    /// longer running.
    pub fn placement_requirements(&self) -> Option<Vec<StageRequirement>> {
        if !self.awaits_placement() {
            return None;
        }
        let mut requirements: Vec<StageRequirement> = self
            .stages
            .iter()
            .map(|(stage_id, stage)| {
                let plan = stage.plan_arc();
                StageRequirement {
                    stage_id: *stage_id,
                    partitions: stage_input_partitions(&plan),
                    collapse: stage_has_input_collapse(&plan),
                }
            })
            .collect();
        requirements.sort_by_key(|r| r.stage_id);
        Some(requirements)
    }

    /// The scheduler's `max_partitions_per_task` for this job.
    pub fn max_partitions_per_task(&self) -> usize {
        self.session_config
            .options()
            .extensions
            .get::<ballista_core::config::BallistaConfig>()
            .map(|config| config.max_partitions_per_task())
            .unwrap_or(0)
    }

    /// Commits a placement whose vcores the scheduler has reserved: resolves
    /// every reader to the producer tasks it streams from, marks every stage
    /// running, publishes the final stage's locations in the job status, and
    /// returns the tasks to launch.
    ///
    /// `executors` must hold the metadata of every executor the placement
    /// uses.
    pub fn place(
        &mut self,
        placement: &Placement,
        executors: &HashMap<String, ExecutorMetadata>,
    ) -> Result<Vec<BoundTask>> {
        if !self.awaits_placement() {
            return Err(BallistaError::Internal(format!(
                "pipelined job {} is already placed or no longer running",
                self.job_id
            )));
        }
        let missing: Vec<usize> = self
            .stages
            .keys()
            .filter(|stage_id| !placement.tasks.contains_key(stage_id))
            .copied()
            .collect();
        if !missing.is_empty() {
            return Err(BallistaError::Internal(format!(
                "placement of pipelined job {} leaves stages {missing:?} unplaced",
                self.job_id
            )));
        }

        // Where every producer task serves each of its output partitions.
        let mut outputs: HashMap<
            usize,
            BTreeMap<usize, Vec<PipelinedPartitionLocation>>,
        > = HashMap::new();
        for (stage_id, tasks) in &placement.tasks {
            let plan = self.stage(*stage_id)?.plan_arc();
            let stage_outputs = outputs.entry(*stage_id).or_default();
            for (task_id, (executor_id, partitions)) in tasks.iter().enumerate() {
                let executor_meta = executors.get(executor_id).ok_or_else(|| {
                    BallistaError::Internal(format!(
                        "no metadata for executor {executor_id} placed in pipelined job {}",
                        self.job_id
                    ))
                })?;
                for partition in compute_global_output_partition_ids(&plan, partitions) {
                    stage_outputs.entry(partition).or_default().push(
                        PipelinedPartitionLocation {
                            partition_id: PartitionId::new(
                                &self.job_id,
                                *stage_id,
                                partition,
                            ),
                            task_id,
                            executor_meta: executor_meta.clone(),
                        },
                    );
                }
            }
        }

        let reserved: HashMap<&str, u32> = placement
            .reservation
            .iter()
            .map(|(executor_id, vcores)| (executor_id.as_str(), *vcores))
            .collect();
        let mut bound = vec![];
        for (stage_id, tasks) in &placement.tasks {
            let stage = self.stage(*stage_id)?;
            let (output_links, inputs) = match stage {
                ExecutionStage::UnResolved(stage) => {
                    (stage.output_links.clone(), stage.inputs.clone())
                }
                ExecutionStage::Resolved(stage) => {
                    (stage.output_links.clone(), stage.inputs.clone())
                }
                other => {
                    return Err(BallistaError::Internal(format!(
                        "stage {stage_id} of pipelined job {} is {} before placement",
                        self.job_id,
                        other.variant_name()
                    )));
                }
            };
            let plan = resolve_pipelined_readers(stage.plan_arc(), &outputs)?;
            let partitions = stage_input_partitions(&plan);
            let mut running = RunningStage::new(
                *stage_id,
                0,
                plan.clone(),
                partitions,
                output_links,
                inputs,
                self.session_config.clone(),
            );
            // Every partition is handed out here, so nothing is pending.
            running.pending = PendingPartitions::empty(partitions);
            for (task_id, (executor_id, slice)) in tasks.iter().enumerate() {
                let mut task_info = create_task_info(executor_id.clone(), task_id);
                task_info.global_input_partition_ids = slice.clone();
                // The job's reservation covers every task and is released
                // when the job ends, so no task carries vcores of its own.
                task_info.vcores_consumed = 0;
                running.task_infos.push(task_info);
                bound.push((
                    executor_id.clone(),
                    TaskDescription {
                        session_id: self.session_id.clone(),
                        key: TaskKey {
                            job_id: self.job_id.clone(),
                            stage_id: *stage_id,
                            task_id,
                        },
                        stage_attempt_num: 0,
                        task_attempt: 0,
                        global_input_partition_ids: slice.clone(),
                        // The job's reservation on the executor, which sizes
                        // the memory pool all its tasks there share.
                        vcores_consumed: reserved
                            .get(executor_id.as_str())
                            .copied()
                            .unwrap_or(1),
                        plan: plan.clone(),
                        session_config: self.session_config.clone(),
                    },
                ));
            }
            self.stages
                .insert(*stage_id, ExecutionStage::Running(running));
        }

        let final_locations: Vec<PipelinedPartitionLocation> = outputs
            .remove(&self.final_stage_id)
            .unwrap_or_default()
            .into_values()
            .flatten()
            .collect();
        if let Some(Status::Running(running)) = &mut self.status.status {
            running.pipelined_output_locations =
                final_locations.into_iter().map(Into::into).collect();
        }
        self.reservation = placement.reservation.clone();
        self.executors = placement
            .tasks
            .values()
            .flatten()
            .map(|(executor_id, _)| executor_id.clone())
            .collect();
        self.placed = true;
        info!(
            "Placed pipelined job {}: {} tasks over {} executors, reserving {:?}",
            self.job_id,
            bound.len(),
            self.executors.len(),
            self.reservation
        );
        Ok(bound)
    }

    /// Takes the vcores the job reserved, to be released. Empty once taken,
    /// so they are released once.
    pub fn take_reservation(&mut self) -> Vec<ExecutorSlot> {
        std::mem::take(&mut self.reservation)
    }

    /// If losing `executor_id` breaks this job, the reason to fail it with.
    /// The lost executor's share of the reservation is dropped, since its
    /// capacity is already gone.
    pub fn lost_executor(&mut self, executor_id: &str) -> Option<String> {
        self.reservation.retain(|(id, _)| id != executor_id);
        (self.is_running() && self.executors.contains(executor_id)).then(|| {
            format!(
                "executor {executor_id}, which was running part of pipelined job {}, was lost",
                self.job_id
            )
        })
    }

    fn stage(&self, stage_id: usize) -> Result<&ExecutionStage> {
        self.stages.get(&stage_id).ok_or_else(|| {
            BallistaError::Internal(format!(
                "pipelined job {} has no stage {stage_id}",
                self.job_id
            ))
        })
    }

    /// Marks every stage whose tasks have all succeeded as successful.
    fn succeed_finished_stages(&mut self) {
        let finished: Vec<usize> = self
            .stages
            .iter()
            .filter_map(|(stage_id, stage)| match stage {
                ExecutionStage::Running(running) if running.is_successful() => {
                    Some(*stage_id)
                }
                _ => None,
            })
            .collect();
        for stage_id in finished {
            self.succeed_stage(stage_id);
        }
    }
}

/// Replaces every `UnresolvedShuffleExec` in `plan` with a
/// `PipelinedShuffleReaderExec` over the producer tasks in `outputs`.
fn resolve_pipelined_readers(
    plan: Arc<dyn ExecutionPlan>,
    outputs: &HashMap<usize, BTreeMap<usize, Vec<PipelinedPartitionLocation>>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(unresolved) = plan.downcast_ref::<UnresolvedShuffleExec>() {
        if unresolved.coalesce.is_some() {
            return Err(BallistaError::Internal(format!(
                "pipelined jobs do not support coalesced readers (stage {})",
                unresolved.stage_id
            )));
        }
        let empty = BTreeMap::new();
        let produced = outputs.get(&unresolved.stage_id).unwrap_or(&empty);
        let schema = unresolved.schema.clone();
        let reader = if unresolved.broadcast {
            PipelinedShuffleReaderExec::new_broadcast(
                unresolved.stage_id,
                produced.values().flatten().cloned().collect(),
                schema,
            )
        } else {
            let partitions = (0..unresolved.output_partition_count)
                .map(|p| produced.get(&p).cloned().unwrap_or_default())
                .collect();
            PipelinedShuffleReaderExec::try_new(
                unresolved.stage_id,
                partitions,
                schema,
                unresolved.properties().output_partitioning().clone(),
            )?
        };
        return Ok(Arc::new(reader));
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| resolve_pipelined_readers(child.clone(), outputs))
        .collect::<Result<Vec<_>>>()?;
    Ok(replace_children_if_necessary(plan, children)?)
}

impl ExecutionGraph for PipelinedExecutionGraph {
    fn job_id(&self) -> &JobId {
        &self.job_id
    }

    fn job_name(&self) -> &str {
        &self.job_name
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn scheduler_id(&self) -> Option<&str> {
        self.scheduler_id.as_deref()
    }

    fn session_config(&self) -> Arc<SessionConfig> {
        self.session_config.clone()
    }

    fn status(&self) -> &JobStatus {
        &self.status
    }

    fn logical_plan(&self) -> Option<&str> {
        self.logical_plan.as_deref()
    }

    fn physical_plan(&self) -> Arc<dyn ExecutionPlan> {
        self.physical_plan.clone()
    }

    fn start_time(&self) -> u64 {
        self.start_time
    }

    fn end_time(&self) -> u64 {
        self.end_time
    }

    fn completed_stages(&self) -> usize {
        self.stages
            .values()
            .filter(|stage| matches!(stage, ExecutionStage::Successful(_)))
            .count()
    }

    fn is_successful(&self) -> bool {
        self.stages
            .values()
            .all(|stage| matches!(stage, ExecutionStage::Successful(_)))
    }

    /// Stages never become runnable one at a time: [`Self::place`] starts
    /// them all.
    fn revive(&mut self) -> bool {
        false
    }

    /// Records task outcomes. The first failure fails the job; the final
    /// stage completing finishes it and cancels whatever is still running.
    fn update_task_status(
        &mut self,
        _executor: &ExecutorMetadata,
        task_statuses: Vec<TaskStatus>,
        _max_task_failures: usize,
        _max_stage_failures: usize,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        if !self.is_running() {
            // Late reports from tasks cancelled when the job finished or
            // failed.
            return Ok(vec![]);
        }
        let mut failure = None;
        for status in task_statuses {
            let stage_id = status.stage_id as usize;
            let task_id = status.task_id as usize;
            let Some(ExecutionStage::Running(stage)) = self.stages.get_mut(&stage_id)
            else {
                warn!(
                    "Ignoring status of task {task_id} in stage {stage_id} of pipelined job {}: the stage is not running",
                    self.job_id
                );
                continue;
            };
            if task_id >= stage.task_infos.len() {
                warn!(
                    "Ignoring status of unknown task {task_id} in stage {stage_id} of pipelined job {}",
                    self.job_id
                );
                continue;
            }
            let outcome = status.status.clone();
            let metrics = status.metrics.clone();
            if !stage.update_task_info(task_id, status) {
                continue;
            }
            match outcome {
                Some(task_status::Status::Successful(_)) => {
                    stage.update_task_metrics(task_id, metrics)?;
                }
                Some(task_status::Status::Failed(failed)) => {
                    failure.get_or_insert_with(|| {
                        // Cache data is another job's output: say so, so the
                        // lost build is dropped and a retry rebuilds it.
                        if let Some(FailedReason::FetchPartitionError(fetch)) =
                            &failed.failed_reason
                            && is_external_fetch_failure(
                                &stage.inputs,
                                &fetch.executor_id,
                                fetch.map_stage_id as usize,
                                fetch.map_partition_id as usize,
                            )
                        {
                            format!(
                                "stage {stage_id} {CACHE_DATA_LOST} on executor {}",
                                fetch.executor_id
                            )
                        } else {
                            format!(
                                "task {task_id} of stage {stage_id} failed: {}",
                                failed.error
                            )
                        }
                    });
                }
                _ => {}
            }
        }

        if let Some(reason) = failure {
            let fail_message = format!(
                "Pipelined job {} failed because {reason}. Pipelined jobs do not retry tasks",
                self.job_id
            );
            self.fail_job(fail_message.clone());
            return Ok(vec![QueryStageSchedulerEvent::JobRunningFailed {
                job_id: self.job_id.clone(),
                fail_message,
                queued_at: self.queued_at,
                failed_at: timestamp_millis(),
            }]);
        }

        let final_finished = matches!(
            self.stages.get(&self.final_stage_id),
            Some(ExecutionStage::Running(stage)) if stage.is_successful()
        );
        if !final_finished {
            self.succeed_finished_stages();
            return Ok(vec![]);
        }

        // The client has read the whole result. Anything still running is
        // a producer whose output nobody needs any more.
        let leftovers = self.running_tasks();
        for stage in self.stages.values_mut() {
            if let ExecutionStage::Running(running) = stage {
                *stage = ExecutionStage::Successful(running.to_successful());
            }
        }
        self.succeed_job()?;
        let mut events = vec![];
        if !leftovers.is_empty() {
            info!(
                "Pipelined job {} finished; cancelling {} tasks it no longer needs",
                self.job_id,
                leftovers.len()
            );
            events.push(QueryStageSchedulerEvent::CancelTasks(leftovers));
        }
        events.push(QueryStageSchedulerEvent::JobFinished {
            job_id: self.job_id.clone(),
            queued_at: self.queued_at,
            completed_at: timestamp_millis(),
        });
        Ok(events)
    }

    fn running_stages(&self) -> Vec<usize> {
        self.stages
            .iter()
            .filter(|(_, stage)| matches!(stage, ExecutionStage::Running(_)))
            .map(|(stage_id, _)| *stage_id)
            .collect()
    }

    fn running_tasks(&self) -> Vec<RunningTaskInfo> {
        self.stages
            .values()
            .flat_map(|stage| match stage {
                ExecutionStage::Running(stage) => stage
                    .running_tasks()
                    .into_iter()
                    .map(|(task_id, stage_id, executor_id)| RunningTaskInfo {
                        task_id,
                        job_id: self.job_id.clone(),
                        stage_id,
                        executor_id,
                    })
                    .collect(),
                _ => vec![],
            })
            .collect()
    }

    /// Nothing is ever pending: [`Self::place`] hands out every partition.
    fn available_tasks(&self) -> usize {
        0
    }

    /// Stage-by-stage binding never applies to a pipelined job.
    fn fetch_running_stage(
        &mut self,
        _black_list: &[usize],
    ) -> Option<&mut RunningStage> {
        None
    }

    fn update_status(&mut self, status: JobStatus) {
        self.status = status;
    }

    /// A pipelined job has no shuffle files to read; its final stage streams
    /// from the locations in the running status.
    fn output_locations(&self) -> Vec<PartitionLocation> {
        vec![]
    }

    /// Nothing is rolled back: losing an executor fails the job instead (see
    /// [`PipelinedExecutionGraph::lost_executor`]).
    fn reset_stages_on_lost_executor(
        &mut self,
        _executor_id: &str,
    ) -> Result<(HashSet<usize>, Vec<RunningTaskInfo>)> {
        Ok((HashSet::new(), vec![]))
    }

    fn resolve_stage(&mut self, _stage_id: usize) -> Result<bool> {
        Ok(false)
    }

    fn succeed_stage(&mut self, stage_id: usize) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Successful(stage.to_successful()));
            true
        } else {
            false
        }
    }

    fn fail_stage(&mut self, stage_id: usize, err_msg: String) -> bool {
        if let Some(ExecutionStage::Running(stage)) = self.stages.remove(&stage_id) {
            self.stages
                .insert(stage_id, ExecutionStage::Failed(stage.to_failed(err_msg)));
            true
        } else {
            false
        }
    }

    fn rollback_running_stage(
        &mut self,
        _stage_id: usize,
        _failure_reasons: HashSet<String>,
    ) -> Result<Vec<RunningTaskInfo>> {
        Ok(vec![])
    }

    fn rollback_resolved_stage(&mut self, _stage_id: usize) -> Result<bool> {
        Ok(false)
    }

    fn rerun_successful_stage(&mut self, _stage_id: usize) -> bool {
        false
    }

    fn fail_job(&mut self, error: String) {
        self.end_time = timestamp_millis();
        self.status = JobStatus {
            job_id: self.job_id.to_string(),
            job_name: self.job_name.clone(),
            status: Some(Status::Failed(FailedJob {
                error,
                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
            })),
        };
    }

    fn succeed_job(&mut self) -> Result<()> {
        if !self.is_successful() {
            return Err(BallistaError::Internal(format!(
                "Attempt to finalize an incomplete pipelined job {}",
                self.job_id
            )));
        }
        let pipelined_output_locations = match &self.status.status {
            Some(Status::Running(running)) => running.pipelined_output_locations.clone(),
            _ => vec![],
        };
        self.end_time = timestamp_millis();
        self.status = JobStatus {
            job_id: self.job_id.to_string(),
            job_name: self.job_name.clone(),
            status: Some(Status::Successful(SuccessfulJob {
                partition_location: vec![],
                queued_at: self.queued_at,
                started_at: self.start_time,
                ended_at: self.end_time,
                pipelined_output_locations,
            })),
        };
        Ok(())
    }

    fn stages(&self) -> &HashMap<usize, ExecutionStage> {
        &self.stages
    }

    #[cfg(test)]
    fn pop_next_task(&mut self, _executor_id: &str) -> Result<Option<TaskDescription>> {
        Ok(None)
    }

    fn stage_count(&self) -> usize {
        self.stages.len()
    }

    fn cloned(&self) -> ExecutionGraphBox {
        Box::new(self.clone())
    }

    fn as_pipelined(&self) -> Option<&PipelinedExecutionGraph> {
        Some(self)
    }

    fn as_pipelined_mut(&mut self) -> Option<&mut PipelinedExecutionGraph> {
        Some(self)
    }
}

impl Debug for PipelinedExecutionGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let stages = self
            .stages
            .values()
            .map(|stage| format!("{stage:?}"))
            .collect::<Vec<String>>()
            .join("");
        write!(
            f,
            "PipelinedExecutionGraph[job_id={}, session_id={}, placed={}]\n{}",
            self.job_id, self.session_id, self.placed, stages
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchedulerConfig;
    use crate::metrics::default_metrics_collector;
    use crate::planner::DefaultDistributedPlanner;
    use crate::test_utils::{
        SchedulerTest, TaskRunnerFn, await_condition, mock_completed_task, mock_executor,
        mock_failed_task, test_aggregation_plan, test_join_plan,
    };
    use ballista_core::config::{BALLISTA_EXECUTION_MODE, TaskSchedulingPolicy};
    use ballista_core::extension::SessionConfigExt;
    use ballista_core::serde::protobuf::{
        ExecutionError, FailedTask, MultiTaskDefinition, failed_task,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::logical_expr::{LogicalPlan, col};
    use datafusion::test_util::scan_empty_with_partitions;
    use std::sync::Mutex;
    use std::time::Duration;

    fn available(executors: &[(&str, u32)]) -> Vec<AvailableVcores> {
        executors
            .iter()
            .map(|(id, vcores)| AvailableVcores {
                executor_id: (*id).to_owned(),
                vcores: *vcores,
            })
            .collect()
    }

    fn requirement(
        stage_id: usize,
        partitions: usize,
        collapse: bool,
    ) -> StageRequirement {
        StageRequirement {
            stage_id,
            partitions,
            collapse,
        }
    }

    fn fits(decision: PlacementDecision) -> Placement {
        match decision {
            PlacementDecision::Fits(placement) => placement,
            other => panic!("expected the job to fit, got {other:?}"),
        }
    }

    #[test]
    fn placement_spreads_each_stage_and_reserves_the_widest_per_executor() {
        let placement = fits(plan_placement(
            &[requirement(1, 4, false), requirement(2, 3, false)],
            &available(&[("a", 4), ("b", 4)]),
            8,
            0,
        ));
        assert_eq!(
            placement.tasks[&1],
            vec![("a".to_owned(), vec![0, 1]), ("b".to_owned(), vec![2, 3])]
        );
        assert_eq!(
            placement.tasks[&2],
            vec![("a".to_owned(), vec![0, 1]), ("b".to_owned(), vec![2])]
        );
        // The stages share the reservation: the most either uses, not the sum.
        assert_eq!(
            placement.reservation,
            vec![("a".to_owned(), 2), ("b".to_owned(), 2)]
        );
    }

    #[test]
    fn collapse_stage_gets_one_task_and_one_vcore() {
        let placement = fits(plan_placement(
            &[requirement(1, 3, true)],
            &available(&[("a", 2), ("b", 4)]),
            6,
            0,
        ));
        assert_eq!(placement.tasks[&1], vec![("b".to_owned(), vec![0, 1, 2])]);
        assert_eq!(placement.reservation, vec![("b".to_owned(), 1)]);
    }

    #[test]
    fn placement_caps_task_size() {
        let placement = fits(plan_placement(
            &[requirement(1, 4, false)],
            &available(&[("a", 4)]),
            4,
            3,
        ));
        assert_eq!(
            placement.tasks[&1],
            vec![("a".to_owned(), vec![0, 1, 2]), ("a".to_owned(), vec![3])]
        );
    }

    #[test]
    fn placement_waits_for_free_vcores() {
        assert_eq!(
            plan_placement(
                &[requirement(1, 4, false)],
                &available(&[("a", 1), ("b", 2)]),
                8,
                0
            ),
            PlacementDecision::Wait
        );
        assert_eq!(
            plan_placement(&[requirement(1, 4, false)], &[], 8, 0),
            PlacementDecision::Wait
        );
        // A stage wider than the cluster waits until every vcore is free.
        assert_eq!(
            plan_placement(
                &[requirement(1, 9, false)],
                &available(&[("a", 4), ("b", 3)]),
                8,
                0
            ),
            PlacementDecision::Wait
        );
    }

    #[test]
    fn stage_wider_than_the_cluster_shares_its_vcores() {
        let placement = fits(plan_placement(
            &[requirement(1, 9, false), requirement(2, 2, false)],
            &available(&[("a", 4), ("b", 4)]),
            8,
            0,
        ));
        assert_eq!(
            placement.tasks[&1],
            vec![
                ("a".to_owned(), vec![0, 1, 2, 3, 4]),
                ("b".to_owned(), vec![5, 6, 7, 8])
            ]
        );
        assert_eq!(
            placement.tasks[&2],
            vec![("a".to_owned(), vec![0]), ("b".to_owned(), vec![1])]
        );
        // No executor reserves more than it has.
        assert_eq!(
            placement.reservation,
            vec![("a".to_owned(), 4), ("b".to_owned(), 4)]
        );
    }

    #[test]
    fn wide_stage_spreads_in_proportion_to_free_vcores() {
        assert_eq!(spread(9, &[("a", 3), ("b", 1)], 4), Some(vec![7, 2]));
        assert_eq!(spread(4, &[("a", 3), ("b", 1)], 4), Some(vec![3, 1]));
        assert_eq!(spread(2, &[("a", 3), ("b", 1)], 4), Some(vec![1, 1]));
        assert_eq!(spread(5, &[("a", 3), ("b", 1)], 8), None);
        assert_eq!(spread(1, &[], 0), None);
    }

    async fn pipelined_join_graph() -> PipelinedExecutionGraph {
        let plan = test_join_plan(4).await.physical_plan();
        PipelinedExecutionGraph::new(
            "localhost:50050",
            &"job".into(),
            "",
            "session",
            plan,
            0,
            Arc::new(SessionConfig::new_with_ballista()),
            &mut DefaultDistributedPlanner::new(),
            None,
        )
        .unwrap()
    }

    fn executors(ids: &[&str]) -> HashMap<String, ExecutorMetadata> {
        ids.iter()
            .map(|id| ((*id).to_owned(), mock_executor((*id).to_owned())))
            .collect()
    }

    fn place(graph: &mut PipelinedExecutionGraph) -> Vec<BoundTask> {
        let requirements = graph.placement_requirements().unwrap();
        let placement = fits(plan_placement(
            &requirements,
            &available(&[("a", 4), ("b", 4)]),
            8,
            0,
        ));
        graph.place(&placement, &executors(&["a", "b"])).unwrap()
    }

    fn contains_unresolved(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.is::<UnresolvedShuffleExec>()
            || plan.children().iter().any(|c| contains_unresolved(c))
    }

    fn readers(plan: &Arc<dyn ExecutionPlan>) -> Vec<PipelinedShuffleReaderExec> {
        let mut found = vec![];
        if let Some(reader) = plan.downcast_ref::<PipelinedShuffleReaderExec>() {
            found.push(reader.clone());
        }
        for child in plan.children() {
            found.extend(readers(child));
        }
        found
    }

    #[tokio::test]
    async fn every_stage_root_is_a_pipelined_writer() {
        let graph = pipelined_join_graph().await;
        assert!(graph.stages.len() > 2, "a join plan has several stages");
        for stage in graph.stages.values() {
            assert!(stage.plan().is::<PipelinedShuffleWriterExec>());
        }
        assert!(
            graph.stages[&graph.final_stage_id]
                .output_links()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn placement_resolves_every_reader_to_its_producer_tasks() {
        let mut graph = pipelined_join_graph().await;
        let tasks = place(&mut graph);

        assert_eq!(graph.placement_requirements(), None, "a job is placed once");
        assert_eq!(graph.available_tasks(), 0);
        assert_eq!(graph.running_stages().len(), graph.stages.len());
        for (_, task) in &tasks {
            assert!(!contains_unresolved(&task.plan));
            for reader in readers(&task.plan) {
                let producer = &graph.stages[&reader.stage_id];
                let producers = producer.task_infos().map(|infos| infos.len()).unwrap();
                let hashed = producer
                    .plan()
                    .downcast_ref::<PipelinedShuffleWriterExec>()
                    .and_then(|w| w.shuffle_output_partitioning())
                    .is_some();
                for locations in &reader.partition {
                    if hashed {
                        // Every producer task hashes into every partition.
                        assert_eq!(locations.len(), producers);
                    } else {
                        // A passthrough partition comes from the one task
                        // that processed it.
                        assert_eq!(locations.len(), 1);
                    }
                    assert!(locations.iter().all(|l| l.task_id < producers));
                }
            }
        }

        let Some(Status::Running(running)) = &graph.status().status else {
            panic!("a placed job is running");
        };
        assert!(!running.pipelined_output_locations.is_empty());
        assert!(
            running.pipelined_output_locations.iter().all(|l| l
                .partition_id
                .as_ref()
                .unwrap()
                .stage_id
                == graph.final_stage_id as u32)
        );
        assert!(!graph.take_reservation().is_empty());
        assert!(graph.take_reservation().is_empty(), "released once");
    }

    /// Plans `plan` as a pipelined job with `partitions` target partitions.
    async fn pipelined_graph(
        plan: LogicalPlan,
        partitions: usize,
    ) -> PipelinedExecutionGraph {
        let config =
            SessionConfig::new_with_ballista().with_target_partitions(partitions);
        let state =
            datafusion::prelude::SessionContext::new_with_config(config.clone()).state();
        let plan = state
            .create_physical_plan(&state.optimize(&plan).unwrap())
            .await
            .unwrap();
        PipelinedExecutionGraph::new(
            "localhost:50050",
            &"job".into(),
            "",
            "session",
            plan,
            0,
            Arc::new(config),
            &mut DefaultDistributedPlanner::new(),
            None,
        )
        .unwrap()
    }

    /// A cross join reads its left side in full in every task of its stage,
    /// so that side's producer keeps its output for each of them; a queue
    /// could be read by only one.
    #[tokio::test]
    async fn an_input_every_task_reads_in_full_is_replayed() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);
        let left = scan_empty_with_partitions(Some("left"), &schema, None, 2)
            .unwrap()
            .build()
            .unwrap();
        let plan = scan_empty_with_partitions(Some("right"), &schema, None, 2)
            .unwrap()
            .cross_join(left)
            .unwrap()
            .build()
            .unwrap();
        let graph = pipelined_graph(plan, 4).await;

        let mut read_whole = vec![];
        for stage in graph.stages.values() {
            let plan = stage.plan_arc();
            if !stage_has_input_collapse(&plan) {
                read_whole.extend(stages_read_whole(&plan));
            }
        }
        assert!(
            !read_whole.is_empty(),
            "the join stage reads its left side whole"
        );
        for stage_id in read_whole {
            let writer = graph.stages[&stage_id]
                .plan()
                .downcast_ref::<PipelinedShuffleWriterExec>()
                .unwrap();
            assert!(writer.broadcast(), "stage {stage_id} is not replayed");
        }
        // A stage read slice by slice still streams through queues.
        let queued = graph.stages.values().filter(|stage| {
            !stage
                .plan()
                .downcast_ref::<PipelinedShuffleWriterExec>()
                .unwrap()
                .broadcast()
        });
        assert!(queued.count() > 0);
    }

    #[tokio::test]
    async fn a_job_cancelled_while_being_placed_is_not_placed() {
        let mut graph = pipelined_join_graph().await;
        let requirements = graph.placement_requirements().unwrap();
        let placement = fits(plan_placement(
            &requirements,
            &available(&[("a", 4), ("b", 4)]),
            8,
            0,
        ));
        // The client cancels after the scheduler planned the placement and
        // reserved its vcores, but before it committed it.
        graph.abort_running("cancelled".to_owned());
        assert!(!graph.awaits_placement());
        assert!(graph.place(&placement, &executors(&["a", "b"])).is_err());
        assert!(
            matches!(graph.status().status, Some(Status::Failed(_))),
            "{:?}",
            graph.status()
        );
        assert!(graph.take_reservation().is_empty());
    }

    #[tokio::test]
    async fn every_task_on_an_executor_carries_the_jobs_reservation_there() {
        let mut graph = pipelined_join_graph().await;
        let requirements = graph.placement_requirements().unwrap();
        let placement = fits(plan_placement(
            &requirements,
            &available(&[("a", 4), ("b", 2)]),
            6,
            0,
        ));
        let reserved: HashMap<String, u32> =
            placement.reservation.iter().cloned().collect();
        let bound = graph.place(&placement, &executors(&["a", "b"])).unwrap();
        assert!(bound.len() > 2, "{bound:?}");
        // The executor sizes the memory pool all the job's tasks share from it.
        for (executor_id, task) in &bound {
            assert_eq!(
                task.vcores_consumed, reserved[executor_id],
                "task {:?} on {executor_id}",
                task.key
            );
        }
    }

    #[tokio::test]
    async fn job_finishes_when_its_final_stage_does_and_cancels_the_rest() {
        let mut graph = pipelined_join_graph().await;
        let tasks = place(&mut graph);

        // Only the final stage reports: its consumer read everything, but the
        // producers are still running, as when a LIMIT is satisfied early.
        let mut events = vec![];
        let final_stage_id = graph.final_stage_id;
        for (executor_id, task) in tasks
            .iter()
            .filter(|(_, task)| task.key.stage_id == final_stage_id)
        {
            events.extend(
                graph
                    .update_task_status(
                        &mock_executor(executor_id.clone()),
                        vec![mock_completed_task(task.clone(), executor_id)],
                        1,
                        1,
                    )
                    .unwrap(),
            );
        }
        assert!(graph.is_successful());
        assert!(matches!(
            events.as_slice(),
            [
                QueryStageSchedulerEvent::CancelTasks(leftovers),
                QueryStageSchedulerEvent::JobFinished { .. },
            ] if !leftovers.is_empty()
        ));
    }

    #[tokio::test]
    async fn a_failed_task_fails_the_job_without_retry() {
        let mut graph = pipelined_join_graph().await;
        let tasks = place(&mut graph);
        let (executor_id, task) = &tasks[0];
        let failed = mock_failed_task(
            task.clone(),
            FailedTask {
                error: "boom".to_owned(),
                retryable: true,
                count_to_failures: true,
                failed_reason: Some(failed_task::FailedReason::ExecutionError(
                    ExecutionError {},
                )),
            },
        );
        let events = graph
            .update_task_status(&mock_executor(executor_id.clone()), vec![failed], 4, 4)
            .unwrap();
        assert!(matches!(
            events.as_slice(),
            [QueryStageSchedulerEvent::JobRunningFailed { fail_message, .. }]
                if fail_message.contains("boom")
        ));
        assert!(matches!(graph.status().status, Some(Status::Failed(_))));
        // Reports that arrive after the failure change nothing.
        let (executor_id, task) = &tasks[1];
        assert!(
            graph
                .update_task_status(
                    &mock_executor(executor_id.clone()),
                    vec![mock_completed_task(task.clone(), executor_id)],
                    1,
                    1
                )
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn losing_an_executor_it_uses_breaks_the_job() {
        let mut graph = pipelined_join_graph().await;
        assert_eq!(graph.lost_executor("a"), None, "nothing is placed yet");
        place(&mut graph);
        assert!(graph.lost_executor("a").is_some());
        assert_eq!(graph.lost_executor("elsewhere"), None);
    }

    #[tokio::test]
    async fn aggregation_plan_places_a_collapse_free_two_stage_job() {
        let plan = test_aggregation_plan(4).await.physical_plan();
        let graph = PipelinedExecutionGraph::new(
            "localhost:50050",
            &"job".into(),
            "",
            "session",
            plan,
            0,
            Arc::new(SessionConfig::new_with_ballista()),
            &mut DefaultDistributedPlanner::new(),
            None,
        )
        .unwrap();
        let requirements = graph.placement_requirements().unwrap();
        assert_eq!(requirements.len(), 2);
        assert!(requirements.iter().all(|r| r.partitions > 0));
    }

    // Scheduler-level tests: a real event loop over virtual executors.

    fn aggregation(partitions: usize) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);
        scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), partitions)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }

    async fn pipelined_scheduler(
        policy: TaskSchedulingPolicy,
        runner: Option<Arc<dyn crate::test_utils::TaskRunner>>,
    ) -> SchedulerTest {
        let mut test = SchedulerTest::new(
            SchedulerConfig::default().with_scheduler_policy(policy),
            default_metrics_collector().unwrap(),
            2,
            4,
            runner,
        )
        .await
        .unwrap();
        test.set_session_option(BALLISTA_EXECUTION_MODE, "pipelined")
            .unwrap();
        test
    }

    #[tokio::test]
    async fn pipelined_job_runs_to_success_and_releases_its_vcores() -> Result<()> {
        let mut test = pipelined_scheduler(TaskSchedulingPolicy::PushStaged, None).await;
        assert_eq!(test.free_vcores().await?, 8);
        let (status, _) = test.run("pipelined", &aggregation(4)).await?;
        assert!(
            matches!(status.status, Some(Status::Successful(_))),
            "{status:?}"
        );
        let released = await_condition(Duration::from_millis(20), 100, || async {
            Ok(test.free_vcores().await? == 8)
        })
        .await?;
        assert!(released, "the job's vcores were not released");
        Ok(())
    }

    #[tokio::test]
    async fn pipelined_job_launches_every_stage_before_any_task_finishes() -> Result<()> {
        let launched: Arc<Mutex<HashSet<u32>>> = Arc::default();
        let recorder = launched.clone();
        // Tasks never report, so a stage-by-stage scheduler would launch only
        // the first stage.
        let runner =
            TaskRunnerFn::new(move |_executor: String, task: MultiTaskDefinition| {
                recorder.lock().unwrap().insert(task.stage_id);
                vec![]
            });
        let mut test =
            pipelined_scheduler(TaskSchedulingPolicy::PushStaged, Some(Arc::new(runner)))
                .await;
        let job_id = test.submit("pipelined", &aggregation(4)).await?;

        let all_launched = await_condition(Duration::from_millis(20), 100, || async {
            Ok(launched.lock().unwrap().len() == 2)
        })
        .await?;
        assert!(
            all_launched,
            "launched stages: {:?}",
            launched.lock().unwrap()
        );
        assert!(test.free_vcores().await? < 8, "the job reserved vcores");

        test.cancel(&job_id).await?;
        let released = await_condition(Duration::from_millis(20), 100, || async {
            Ok(test.free_vcores().await? == 8)
        })
        .await?;
        assert!(released, "cancelling the job did not release its vcores");
        Ok(())
    }

    #[tokio::test]
    async fn pipelined_job_fails_when_a_task_fails() -> Result<()> {
        let runner =
            TaskRunnerFn::new(|executor_id: String, task: MultiTaskDefinition| {
                task.task_ids
                    .iter()
                    .map(|id| TaskStatus {
                        task_id: id.task_id,
                        job_id: task.job_id.clone(),
                        stage_id: task.stage_id,
                        stage_attempt_num: task.stage_attempt_num,
                        launch_time: 0,
                        start_exec_time: 0,
                        end_exec_time: 0,
                        metrics: vec![],
                        status: Some(task_status::Status::Failed(FailedTask {
                            error: format!("injected failure on {executor_id}"),
                            retryable: true,
                            count_to_failures: true,
                            failed_reason: Some(
                                failed_task::FailedReason::ExecutionError(
                                    ExecutionError {},
                                ),
                            ),
                        })),
                    })
                    .collect()
            });
        let mut test =
            pipelined_scheduler(TaskSchedulingPolicy::PushStaged, Some(Arc::new(runner)))
                .await;
        let (status, _) = test.run("pipelined", &aggregation(4)).await?;
        let Some(Status::Failed(failed)) = status.status else {
            panic!("expected the job to fail, got {status:?}");
        };
        assert!(failed.error.contains("do not retry"), "{}", failed.error);
        let released = await_condition(Duration::from_millis(20), 100, || async {
            Ok(test.free_vcores().await? == 8)
        })
        .await?;
        assert!(released, "a failed job's vcores were not released");
        Ok(())
    }

    #[tokio::test]
    async fn pipelined_job_wider_than_the_cluster_shares_its_vcores() -> Result<()> {
        let mut test = pipelined_scheduler(TaskSchedulingPolicy::PushStaged, None).await;
        test.set_session_option("datafusion.execution.target_partitions", "64")?;
        let (status, _) = test.run("pipelined", &aggregation(64)).await?;
        assert!(
            matches!(status.status, Some(Status::Successful(_))),
            "{status:?}"
        );
        let released = await_condition(Duration::from_millis(20), 100, || async {
            Ok(test.free_vcores().await? == 8)
        })
        .await?;
        assert!(released, "the job's vcores were not released");
        Ok(())
    }

    /// The vcores a failed or cancelled job gives back go to a pipelined job
    /// waiting for them, even though no task report or new job follows to
    /// prompt another scheduling pass.
    #[tokio::test]
    async fn a_waiting_pipelined_job_is_placed_once_the_job_holding_its_vcores_ends()
    -> Result<()> {
        use crate::test_utils::{TaskRunner, default_task_runner};
        for end in ["fail", "cancel"] {
            // The first job's tasks never report, so it holds its vcores until
            // it is ended below. Every other job's tasks succeed.
            let first: Arc<Mutex<Option<String>>> = Arc::default();
            let succeed = default_task_runner();
            let runner =
                TaskRunnerFn::new(move |executor: String, task: MultiTaskDefinition| {
                    let mut first = first.lock().unwrap();
                    if *first.get_or_insert_with(|| task.job_id.clone()) == task.job_id {
                        vec![]
                    } else {
                        succeed.run(executor, task)
                    }
                });
            let mut test = pipelined_scheduler(
                TaskSchedulingPolicy::PushStaged,
                Some(Arc::new(runner)),
            )
            .await;
            // As wide as the cluster, so each job needs every vcore.
            test.set_session_option("datafusion.execution.target_partitions", "8")?;
            let placed = |status: Option<JobStatus>| {
                matches!(
                    status.and_then(|s| s.status),
                    Some(Status::Running(RunningJob { pipelined_output_locations, .. }))
                        if !pipelined_output_locations.is_empty()
                )
            };

            let holder = test.submit("holder", &aggregation(8)).await?;
            let held = await_condition(Duration::from_millis(20), 100, || async {
                Ok(test.free_vcores().await? == 0)
            })
            .await?;
            assert!(held, "the first job did not take every vcore");
            let waiter = test.submit("waiter", &aggregation(8)).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                !placed(test.job_status(&waiter).await?),
                "{end}: placed too early"
            );

            if end == "fail" {
                test.post_scheduler_event(QueryStageSchedulerEvent::JobRunningFailed {
                    job_id: holder,
                    fail_message: "injected failure".to_owned(),
                    queued_at: 0,
                    failed_at: 0,
                })
                .await?;
            } else {
                test.cancel(&holder).await?;
            }
            let ran = await_condition(Duration::from_millis(20), 200, || async {
                Ok(placed(test.job_status(&waiter).await?))
            })
            .await?;
            assert!(ran, "{end}: the waiting job was never placed");
        }
        Ok(())
    }

    #[tokio::test]
    async fn pipelined_job_fails_when_an_executor_it_uses_is_lost() -> Result<()> {
        let runner =
            TaskRunnerFn::new(|_executor: String, _task: MultiTaskDefinition| vec![]);
        let mut test =
            pipelined_scheduler(TaskSchedulingPolicy::PushStaged, Some(Arc::new(runner)))
                .await;
        let job_id = test.submit("pipelined", &aggregation(4)).await?;
        let reserved = await_condition(Duration::from_millis(20), 100, || async {
            Ok(test.free_vcores().await? < 8)
        })
        .await?;
        assert!(reserved, "the job was never placed");

        test.lose_executor("virtual-executor-0").await?;
        let failed = await_condition(Duration::from_millis(20), 100, || async {
            Ok(matches!(
                test.job_status(&job_id).await?,
                Some(JobStatus {
                    status: Some(Status::Failed(_)),
                    ..
                })
            ))
        })
        .await?;
        assert!(failed, "losing an executor did not fail the job");
        Ok(())
    }

    /// An executor removed while alive registers again under the same id: the
    /// failed job's reservation must not hand it the vcores it had before on
    /// top of its fresh capacity.
    #[tokio::test]
    async fn a_lost_executor_registering_again_gets_only_its_own_vcores() -> Result<()> {
        let runner =
            TaskRunnerFn::new(|_executor: String, _task: MultiTaskDefinition| vec![]);
        let mut test =
            pipelined_scheduler(TaskSchedulingPolicy::PushStaged, Some(Arc::new(runner)))
                .await;
        test.set_session_option("datafusion.execution.target_partitions", "8")?;
        let job_id = test.submit("pipelined", &aggregation(8)).await?;
        let reserved = await_condition(Duration::from_millis(20), 100, || async {
            Ok(test.free_vcores().await? == 0)
        })
        .await?;
        assert!(reserved, "the job was never placed on every vcore");

        test.lose_executor("virtual-executor-0").await?;
        test.register_executor("virtual-executor-0", 4).await?;
        let failed = await_condition(Duration::from_millis(20), 100, || async {
            Ok(matches!(
                test.job_status(&job_id).await?,
                Some(JobStatus {
                    status: Some(Status::Failed(_)),
                    ..
                })
            ))
        })
        .await?;
        assert!(failed, "losing an executor did not fail the job");
        // The reservation is released before the job is evicted.
        let evicted = await_condition(Duration::from_millis(20), 100, || async {
            Ok(!test.is_active(&job_id))
        })
        .await?;
        assert!(evicted, "the failed job was not evicted");
        assert_eq!(test.free_vcores().await?, 8);
        Ok(())
    }

    #[tokio::test]
    async fn pipelined_job_is_rejected_under_pull_staged_scheduling() -> Result<()> {
        let mut test = pipelined_scheduler(TaskSchedulingPolicy::PullStaged, None).await;
        let (status, _) = test.run("pipelined", &aggregation(4)).await?;
        let Some(Status::Failed(failed)) = status.status else {
            panic!("expected the job to fail, got {status:?}");
        };
        assert!(failed.error.contains("push-staged"), "{}", failed.error);
        Ok(())
    }
}
