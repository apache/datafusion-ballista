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
use crate::scheduler_server::SessionBuilder;
use crate::state::backend::{Keyspace, Lock, StateBackendClient};
use crate::state::execution_graph::{ExecutionGraph, ExecutionStage, Task};
use crate::state::executor_manager::ExecutorReservation;
use crate::state::{decode_protobuf, encode_protobuf, with_lock};
use ballista_core::config::BallistaConfig;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;

use crate::state::session_manager::create_datafusion_context;
use ballista_core::serde::protobuf::{
    self, task_status, JobStatus, PartitionId, StageInputPartition, TaskDefinition,
    TaskStatus,
};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::{
    ExecutorMetadata, PartitionLocation, PartitionStats,
};
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use log::{debug, info, warn};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::default::Default;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

type ExecutorClients = Arc<RwLock<HashMap<String, ExecutorGrpcClient<Channel>>>>;

#[derive(Clone)]
pub struct TaskManager<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<dyn StateBackendClient>,
    clients: ExecutorClients,
    session_builder: SessionBuilder,
    codec: BallistaCodec<T, U>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskManager<T, U> {
    pub fn new(
        state: Arc<dyn StateBackendClient>,
        session_builder: SessionBuilder,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            state,
            clients: Default::default(),
            session_builder,
            codec,
        }
    }

    /// Generate an ExecutionGraph for the job and save it to the persistent state.
    pub async fn submit_job(
        &self,
        job_id: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        let graph = ExecutionGraph::new(job_id, session_id, plan)?;
        self.state
            .put(
                Keyspace::ActiveJobs,
                job_id.to_owned(),
                self.encode_execution_graph(graph)?,
            )
            .await
    }

    pub async fn get_job_status(&self, job_id: &str) -> Result<JobStatus> {
        let graph = self.get_execution_graph(job_id).await?;

        Ok(graph.status())
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

    /// Atomically update given task statuses in the respective job and return a tuple containing:
    /// 1. A list of QueryStageSchedulerEvent to publish.
    /// 2. A list of reservations that can now be offered.
    ///
    /// When a task is updated, there may or may not be more tasks pending for its job. If there are more
    /// tasks pending then we want to reschedule one of those tasks on the same task slot. In the former case
    /// we will "move" that task slot to the ExecutorReservation where it can be dealt with by the scheduler appropriately, either
    /// scheduling a task from another job on it or simply returning it to the pool of available executors.
    pub(crate) async fn update_task_statuses(
        &self,
        executor: &ExecutorMetadata,
        task_status: Vec<TaskStatus>,
    ) -> Result<(Vec<QueryStageSchedulerEvent>, Vec<ExecutorReservation>)> {
        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;

        with_lock(lock, async {
            let mut events: Vec<QueryStageSchedulerEvent> = vec![];
            let mut reservation: Vec<ExecutorReservation> = vec![];

            let mut job_updates: HashMap<String, Vec<TaskStatus>> = HashMap::new();

            for status in task_status {
                if let Some(job_id) = status.task_id.as_ref().map(|id| &id.job_id) {
                    if let Some(statuses) = job_updates.get_mut(job_id) {
                        statuses.push(status)
                    } else {
                        job_updates.insert(job_id.clone(), vec![status]);
                    }
                } else {
                    warn!("Received task with no job ID");
                }
            }

            // We need to acquire a lock on each job before updating it. We
            // collect locks here and they will be dropped when we are done.
            // let mut locks: Vec<Box<dyn Lock>> = vec![];

            let mut txn_ops: Vec<(Keyspace, String, Vec<u8>)> = vec![];

            for (job_id, statuses) in job_updates {
                let num_tasks = statuses.len();
                debug!("Updating {} tasks in job {}", num_tasks, job_id);

                // let job_lock = self.state.lock(Keyspace::ActiveJobs, &job_id).await?;
                // locks.push(job_lock);

                let mut graph = self.get_execution_graph(&job_id).await?;

                graph.update_task_status(&executor, statuses)?;

                if graph.complete() {
                    // If this ExecutionGraph is complete, finalize it
                    info!(
                        "Job {} is complete, finalizing output partitions",
                        graph.job_id()
                    );
                    graph.finalize()?;
                    events.push(QueryStageSchedulerEvent::JobFinished(job_id.clone()));
                } else {
                    // Otherwise keep the task slots reserved for this job
                    for _ in 0..num_tasks {
                        reservation.push(ExecutorReservation::new_assigned(
                            executor.id.to_owned(),
                            job_id.clone(),
                        ));
                    }
                }

                txn_ops.push((
                    Keyspace::ActiveJobs,
                    job_id.clone(),
                    self.encode_execution_graph(graph)?,
                ));
            }

            self.state.put_txn(txn_ops).await?;

            Ok((events, reservation))
        })
        .await
    }

    /// Take a list of executor reservations and fill them with tasks that are ready
    /// to be scheduled. When the reservation is filled, the underlying stage task in the
    /// `ExecutionGraph` will be set to a status of Running, so if the task is not subsequently launched
    /// we must ensure that the task status is reset.
    pub async fn fill_reservations(
        &self,
        reservations: &[ExecutorReservation],
    ) -> Result<(Vec<(String, Task)>, Vec<ExecutorReservation>)> {
        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;

        with_lock(lock, async {
            let mut assignments: Vec<(String, Task)> = vec![];
            let mut free_reservations: Vec<ExecutorReservation> = vec![];
            // let _txn_ops: Vec<(Keyspace, String, Vec<u8>)> = vec![];

            // Need to collect graphs we update so we can update them in storage when we are done
            let mut graphs: HashMap<String, ExecutionGraph> = HashMap::new();

            // First try and fill reservations for particular jobs. If the job has no more tasks
            // free the reservation.
            for reservation in reservations {
                debug!(
                "Filling reservation for executor {} from job {:?}",
                reservation.executor_id, reservation.job_id
            );
                let executor_id = &reservation.executor_id;
                if let Some(job_id) = &reservation.job_id {
                    if let Some(graph) = graphs.get_mut(job_id) {
                        if let Ok(Some(next_task)) = graph.pop_next_task(executor_id) {
                            debug!(
                            "Filled reservation for executor {} with task {:?}",
                            executor_id, next_task
                        );
                            assignments.push((executor_id.clone(), next_task));
                        } else {
                            debug!("Cannot fill reservation for executor {} from job {}, freeing reservation", executor_id, job_id);
                            free_reservations
                                .push(ExecutorReservation::new_free(executor_id.clone()));
                        }
                    } else {
                        // let lock = self.state.lock(Keyspace::ActiveJobs, job_id).await?;
                        let mut graph = self.get_execution_graph(job_id).await?;

                        if let Ok(Some(next_task)) = graph.pop_next_task(executor_id) {
                            debug!(
                            "Filled reservation for executor {} with task {:?}",
                            executor_id, next_task
                        );
                            assignments.push((executor_id.clone(), next_task));
                            graphs.insert(job_id.clone(), graph);
                            // locks.push(lock);
                        } else {
                            debug!("Cannot fill reservation for executor {} from job {}, freeing reservation", executor_id, job_id);
                            free_reservations
                                .push(ExecutorReservation::new_free(executor_id.clone()));
                        }
                    }
                } else {
                    free_reservations.push(reservation.clone());
                }
            }

            let mut other_jobs: Vec<String> =
                self.get_active_jobs().await?.into_iter().collect();

            let mut unassigned: Vec<ExecutorReservation> = vec![];
            // Now try and find tasks for free reservations from current set of graphs
            for reservation in free_reservations {
                debug!(
                "Filling free reservation for executor {}",
                reservation.executor_id
            );
                let mut assigned = false;
                let executor_id = reservation.executor_id.clone();

                // Try and find a task in the graphs we already have locks on
                if let Ok(Some(assignment)) = find_next_task(&executor_id, &mut graphs) {
                    debug!(
                    "Filled free reservation for executor {} with task {:?}",
                    reservation.executor_id, assignment.1
                );
                    // First check if we can find another task
                    assignments.push(assignment);
                    assigned = true;
                } else {
                    // Otherwise start searching through other active jobs.
                    debug!(
                    "Filling free reservation for executor {} from active jobs {:?}",
                    reservation.executor_id, other_jobs
                );
                    while let Some(job_id) = other_jobs.pop() {
                        if graphs.get(&job_id).is_none() {
                            // let lock = self.state.lock(Keyspace::ActiveJobs, &job_id).await?;
                            let mut graph = self.get_execution_graph(&job_id).await?;

                            if let Ok(Some(task)) = graph.pop_next_task(&executor_id) {
                                debug!(
                                "Filled free reservation for executor {} with task {:?}",
                                reservation.executor_id, task
                            );
                                assignments.push((executor_id.clone(), task));
                                // locks.push(lock);
                                graphs.insert(job_id, graph);
                                assigned = true;
                                break;
                            } else {
                                debug!("No available tasks for job {}", job_id);
                            }
                        }
                    }
                }

                if !assigned {
                    debug!(
                    "Unable to fill reservation for executor {}, no tasks available",
                    executor_id
                );
                    unassigned.push(reservation);
                }
            }

            // Transactional update graphs now that we have assigned tasks
            let txn_ops: Vec<(Keyspace, String, Vec<u8>)> = graphs
                .into_iter()
                .map(|(job_id, graph)| {
                    let value = self.encode_execution_graph(graph)?;
                    Ok((Keyspace::ActiveJobs, job_id, value))
                })
                .collect::<Result<Vec<_>>>()?;

            self.state.put_txn(txn_ops).await?;

            Ok((assignments, unassigned))
        }).await
    }

    /// Move the given job to the CompletedJobs keyspace in persistent storage.
    pub async fn complete_job(&self, job_id: &str) -> Result<()> {
        debug!("Moving job {} from Active to Completed", job_id);
        let lock = self.state.lock(Keyspace::ActiveJobs, job_id).await?;
        with_lock(
            lock,
            self.state
                .mv(Keyspace::ActiveJobs, Keyspace::CompletedJobs, job_id),
        )
        .await
    }

    #[cfg(not(test))]
    /// Launch the given task on the specified executor
    pub async fn launch_task(
        &self,
        executor: &ExecutorMetadata,
        task: Task,
    ) -> Result<()> {
        info!("Launching task {:?} on executor {:?}", task, executor.id);
        let task_definition = self.prepare_task_definition(task)?;
        let mut clients = self.clients.write().await;
        if let Some(client) = clients.get_mut(&executor.id) {
            client
                .launch_task(protobuf::LaunchTaskParams {
                    task: vec![task_definition],
                })
                .await
                .map_err(|e| {
                    BallistaError::Internal(format!(
                        "Failed to connect to executor {}: {:?}",
                        executor.id, e
                    ))
                })?;
        } else {
            let executor_id = executor.id.clone();
            let executor_url = format!("http://{}:{}", executor.host, executor.grpc_port);
            let mut client = ExecutorGrpcClient::connect(executor_url).await?;
            clients.insert(executor_id, client.clone());
            client
                .launch_task(protobuf::LaunchTaskParams {
                    task: vec![task_definition],
                })
                .await
                .map_err(|e| {
                    BallistaError::Internal(format!(
                        "Failed to connect to executor {}: {:?}",
                        executor.id, e
                    ))
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn launch_task(
        &self,
        _executor: &ExecutorMetadata,
        _task: Task,
    ) -> Result<()> {
        Ok(())
    }

    /// Retrieve the number of available tasks for the given job. The value returned
    /// is strictly a point-in-time snapshot
    pub async fn get_available_task_count(&self, job_id: &str) -> Result<usize> {
        let graph = self.get_execution_graph(job_id).await?;

        Ok(graph.available_tasks())
    }

    #[allow(dead_code)]
    pub fn prepare_task_definition(&self, task: Task) -> Result<TaskDefinition> {
        debug!("Preparing task definition for {:?}", task);
        let mut plan_buf: Vec<u8> = vec![];
        let plan_proto =
            U::try_from_physical_plan(task.plan, self.codec.physical_extension_codec())?;
        plan_proto.try_encode(&mut plan_buf)?;

        let output_partitioning =
            hash_partitioning_to_proto(task.output_partitioning.as_ref())?;

        let task_definition = TaskDefinition {
            task_id: Some(PartitionId {
                job_id: task.partition.job_id.clone(),
                stage_id: task.partition.stage_id as u32,
                partition_id: task.partition.partition_id as u32,
            }),
            plan: plan_buf,
            output_partitioning,
            session_id: task.session_id,
            props: vec![],
        };
        Ok(task_definition)
    }

    // Return a set of active job IDs.
    async fn get_active_jobs(&self) -> Result<HashSet<String>> {
        debug!("Scanning for active job IDs");
        self.state.scan_keys(Keyspace::ActiveJobs).await
    }

    async fn save_execution_graph(&self, graph: ExecutionGraph) -> Result<()> {
        let job_id = graph.job_id().to_owned();
        debug!("Saving ExecutionGraph for job {}", job_id);

        self.state
            .put(
                Keyspace::ActiveJobs,
                job_id,
                self.encode_execution_graph(graph)?,
            )
            .await
    }

    pub(crate) async fn get_execution_graph(
        &self,
        job_id: &str,
    ) -> Result<ExecutionGraph> {
        let value = self.state.get(Keyspace::ActiveJobs, job_id).await?;

        if value.is_empty() {
            let value = self.state.get(Keyspace::CompletedJobs, job_id).await?;
            self.decode_execution_graph(value).await
        } else {
            self.decode_execution_graph(value).await
        }
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        let value = self.state.get(Keyspace::Sessions, session_id).await?;

        let settings: protobuf::SessionSettings = decode_protobuf(&value)?;

        let mut config_builder = BallistaConfig::builder();
        for kv_pair in &settings.configs {
            config_builder = config_builder.set(&kv_pair.key, &kv_pair.value);
        }
        let config = config_builder.build()?;

        Ok(create_datafusion_context(&config, self.session_builder))
    }

    async fn decode_execution_graph(&self, value: Vec<u8>) -> Result<ExecutionGraph> {
        let proto: protobuf::ExecutionGraph = decode_protobuf(&value)?;

        let session_id = &proto.session_id;

        let session_ctx = self.get_session(session_id).await?;
        let mut stages: HashMap<usize, ExecutionStage> = HashMap::new();
        for stage in proto.stages {
            let plan_proto = U::try_decode(stage.plan.as_slice())?;
            let plan = plan_proto.try_into_physical_plan(
                session_ctx.as_ref(),
                session_ctx.runtime_env().as_ref(),
                self.codec.physical_extension_codec(),
            )?;

            let stage_id = stage.stage_id as usize;
            let output_partitions: Vec<usize> = stage
                .output_partitions
                .into_iter()
                .map(|n| n as usize)
                .collect();

            let mut partitions_stats: HashMap<usize, Vec<PartitionStats>> =
                HashMap::new();
            let mut input_locations: HashMap<
                usize,
                HashMap<usize, Vec<PartitionLocation>>,
            > = HashMap::new();

            for part in stage.partition_stats {
                let partition_id = part.partition as usize;
                let stats = part.partition_stats.into_iter().map(|p| p.into()).collect();
                partitions_stats.insert(partition_id, stats);
            }

            for loc in stage.input_locations {
                let stage_id = loc.stage_id as usize;
                let mut stage_locations: HashMap<usize, Vec<PartitionLocation>> =
                    HashMap::new();

                for task_inputs in loc.task_inputs {
                    let partition_id = task_inputs.partition as usize;
                    let locations = task_inputs
                        .partition_location
                        .into_iter()
                        .map(|l| l.try_into())
                        .collect::<Result<Vec<_>>>()?;

                    stage_locations.insert(partition_id, locations);
                }

                input_locations.insert(stage_id, stage_locations);
            }

            let mut task_statuses: Vec<Option<task_status::Status>> =
                vec![None; output_partitions.len()];

            for status in stage.task_statuses {
                if let Some(task_id) = status.task_id.as_ref() {
                    task_statuses[task_id.partition_id as usize] = status.status
                }
            }

            // This is a little hacky but since we can't make an optional
            // primitive field in protobuf, we just use 0 to encode None.
            // Should work since stage IDs are 1-indexed.
            let output_link = if stage.output_link == 0 {
                None
            } else {
                Some(stage.output_link as usize)
            };

            let output_partitioning: Option<Partitioning> =
                parse_protobuf_hash_partitioning(
                    stage.output_partitioning.as_ref(),
                    session_ctx.as_ref(),
                )?;

            let execution_stage = ExecutionStage {
                stage_id: stage.stage_id as usize,
                output_partitions,
                output_partitioning,
                input_partition_count: stage
                    .input_partition_count
                    .into_iter()
                    .map(|n| n as usize)
                    .collect(),
                plan,
                input_locations,
                partitions_stats,
                task_statuses,
                output_link,
                resolved: stage.resolved,
            };
            stages.insert(stage_id, execution_stage);
        }

        let output_locations: Vec<PartitionLocation> = proto
            .output_locations
            .into_iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(ExecutionGraph {
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

    fn encode_execution_graph(&self, graph: ExecutionGraph) -> Result<Vec<u8>> {
        let job_id = graph.job_id().to_owned();

        let stages = graph
            .stages
            .into_iter()
            .map(|(stage_id, stage)| {
                // This is a little hacky but since we can't make an optional
                // primitive field in protobuf, we just use 0 to encode None.
                // Should work since stage IDs are 1-indexed.
                let output_link = if let Some(link) = stage.output_link {
                    link as u32
                } else {
                    0
                };

                let mut plan: Vec<u8> = vec![];

                U::try_from_physical_plan(
                    stage.plan,
                    self.codec.physical_extension_codec(),
                )
                .and_then(|proto| proto.try_encode(&mut plan))?;

                let mut input_locations: Vec<StageInputPartition> = vec![];

                for (stage, location) in stage.input_locations {
                    let task_inputs = location
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
                        .collect::<Result<Vec<_>>>()?;
                    input_locations.push(protobuf::StageInputPartition {
                        stage_id: stage as u32,
                        task_inputs,
                    });
                }

                // let input_locations = stage
                //     .input_locations
                //     .into_iter()
                //     .map(|(stage, location)| {
                //         let partition_location: Vec<protobuf::PartitionLocation> =
                //             location
                //                 .into_iter()
                //                 .map(|(partition, loc)| {
                //                     protobuf::StageInputPartition {
                //                         stage_id: stage as u32,
                //                         partition: partition as u32,
                //                         partition_location,
                //                     }
                //                 })
                //                 .collect::<Result<Vec<_>>>()?;
                //
                //         Ok(partition_location)
                //
                //         // Ok(protobuf::StageInputPartition {
                //         //     partition: partition as u32,
                //         //     partition_location,
                //         // })
                //     })
                //     .flatten()
                //     .collect::<Result<Vec<_>>>()?;

                let partition_stats: Vec<protobuf::OutputPartitionStats> = stage
                    .partitions_stats
                    .into_iter()
                    .map(|(partition, stats)| {
                        let partition_stats: Vec<protobuf::PartitionStats> =
                            stats.into_iter().map(|stats| stats.into()).collect();

                        Ok(protobuf::OutputPartitionStats {
                            partition: partition as u32,
                            partition_stats,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

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
                            status: Some(status),
                        })
                    })
                    .collect();

                let output_partitioning =
                    hash_partitioning_to_proto(stage.output_partitioning.as_ref())?;

                Ok(protobuf::ExecutionGraphStage {
                    stage_id: stage_id as u64,
                    output_partitions: stage
                        .output_partitions
                        .into_iter()
                        .map(|n| n as u64)
                        .collect(),
                    output_partitioning,
                    input_partition_count: stage
                        .input_partition_count
                        .into_iter()
                        .map(|n| n as u64)
                        .collect(),
                    plan,
                    input_locations,
                    partition_stats,
                    task_statuses,
                    output_link,
                    resolved: stage.resolved,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let output_locations: Vec<protobuf::PartitionLocation> = graph
            .output_locations
            .into_iter()
            .map(|loc| loc.try_into())
            .collect::<Result<Vec<_>>>()?;

        encode_protobuf(&protobuf::ExecutionGraph {
            job_id: graph.job_id,
            session_id: graph.session_id,
            status: Some(graph.status),
            stages,
            output_partitions: graph.output_partitions as u64,
            output_locations,
        })
    }
}

fn find_next_task(
    executor_id: &str,
    graphs: &mut HashMap<String, ExecutionGraph>,
) -> Result<Option<(String, Task)>> {
    for (_, graph) in graphs {
        if let Ok(Some(task)) = graph.pop_next_task(executor_id) {
            return Ok(Some((executor_id.to_owned(), task)));
        }
    }
    Ok(None)
}