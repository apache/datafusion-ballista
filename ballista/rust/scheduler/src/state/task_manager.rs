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
use crate::state::execution_graph::{ExecutionGraph, Task};
use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};
use crate::state::{decode_protobuf, encode_protobuf, with_lock};
use ballista_core::config::BallistaConfig;
#[cfg(not(test))]
use ballista_core::error::BallistaError;
use ballista_core::error::Result;
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;

use crate::state::session_manager::create_datafusion_context;
use ballista_core::serde::protobuf::{
    self, job_status, CancelTasksParams, FailedJob, JobStatus, TaskDefinition, TaskStatus,
};
use ballista_core::serde::scheduler::to_proto::hash_partitioning_to_proto;
use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::{AsExecutionPlan, BallistaCodec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use log::{debug, error, info, warn};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

type ExecutorClients = Arc<RwLock<HashMap<String, ExecutorGrpcClient<Channel>>>>;

#[derive(Clone)]
pub struct TaskManager<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<dyn StateBackendClient>,
    #[allow(dead_code)]
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
        info!("Submitting execution graph: {:?}", graph);
        self.state
            .put(
                Keyspace::ActiveJobs,
                job_id.to_owned(),
                self.encode_execution_graph(graph)?,
            )
            .await?;

        Ok(())
    }

    /// Get the status of of a job. First look in Active/Completed jobs, and then in Failed jobs
    pub async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Ok(graph) = self.get_execution_graph(job_id).await {
            Ok(Some(graph.status()))
        } else {
            let value = self.state.get(Keyspace::FailedJobs, job_id).await?;

            if !value.is_empty() {
                let status = decode_protobuf(&value)?;
                Ok(Some(status))
            } else {
                Ok(None)
            }
        }
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
    /// tasks pending then we want to reschedule one of those tasks on the same task slot. In that case
    /// we will set the `job_id` on the `ExecutorReservation` so the scheduler attempts to assign tasks from
    /// the same job. Note that when the scheduler attempts to fill the reservation, there is no guarantee
    /// that the available task is still available.
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
                debug!("Task Update\n{:?}", status);
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

            let mut txn_ops: Vec<(Keyspace, String, Vec<u8>)> = vec![];

            for (job_id, statuses) in job_updates {
                let num_tasks = statuses.len();
                debug!("Updating {} tasks in job {}", num_tasks, job_id);

                let mut graph = self.get_execution_graph(&job_id).await?;

                graph.update_task_status(executor, statuses)?;

                if graph.complete() {
                    // If this ExecutionGraph is complete, finalize it
                    info!(
                        "Job {} is complete, finalizing output partitions",
                        graph.job_id()
                    );
                    graph.finalize()?;
                    events.push(QueryStageSchedulerEvent::JobFinished(job_id.clone()));

                    for _ in 0..num_tasks {
                        reservation
                            .push(ExecutorReservation::new_free(executor.id.to_owned()));
                    }
                } else if let Some(job_status::Status::Failed(failure)) =
                    graph.status().status
                {
                    events.push(QueryStageSchedulerEvent::JobFailed(
                        job_id.clone(),
                        failure.error,
                    ));

                    for _ in 0..num_tasks {
                        reservation
                            .push(ExecutorReservation::new_free(executor.id.to_owned()));
                    }
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
    ///
    /// Here we use the following  algorithm:
    ///
    /// 1. For each reservation with a `job_id` assigned try and assign another task from the same job.
    /// 2. If a reservation either does not have a `job_id` or there are no available tasks for its `job_id`,
    ///    add it to a list of "free" reservations.
    /// 3. For each free reservation, try to assign a task from one of the jobs we have already considered.
    /// 4. If we cannot find a task, then looks for a task among all active jobs
    /// 5. If we cannot find a task in all active jobs, then add the reservation to the list of unassigned reservations
    ///
    /// Finally, we return:
    /// 1. A list of assignments which is a (Executor ID, Task) tuple
    /// 2. A list of unassigned reservations which we could not find tasks for
    /// 3. The number of pending tasks across active jobs
    pub async fn fill_reservations(
        &self,
        reservations: &[ExecutorReservation],
    ) -> Result<(Vec<(String, Task)>, Vec<ExecutorReservation>, usize)> {
        // Reinitialize the free reservations.
        let free_reservations: Vec<ExecutorReservation> = reservations
            .iter()
            .map(|reservation| {
                ExecutorReservation::new_free(reservation.executor_id.clone())
            })
            .collect();

        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;
        with_lock(lock, async {
            let mut jobs: Vec<String> =
                self.get_active_jobs().await?.into_iter().collect();

            let mut assignments: Vec<(String, Task)> = vec![];
            let mut unassigned: Vec<ExecutorReservation> = vec![];
            // Need to collect graphs we update so we can update them in storage when we are done
            let mut graphs: HashMap<String, ExecutionGraph> = HashMap::new();
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
                        reservation.executor_id, jobs
                    );
                    while let Some(job_id) = jobs.pop() {
                        if graphs.get(&job_id).is_none() {
                            let mut graph = self.get_execution_graph(&job_id).await?;

                            if let Ok(Some(task)) = graph.pop_next_task(&executor_id) {
                                debug!(
                                    "Filled free reservation for executor {} with task {:?}",
                                    reservation.executor_id, task
                                );
                                assignments.push((executor_id.clone(), task));
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

            let mut pending_tasks = 0;

            // Transactional update graphs now that we have assigned tasks
            let txn_ops: Vec<(Keyspace, String, Vec<u8>)> = graphs
                .into_iter()
                .map(|(job_id, graph)| {
                    pending_tasks += graph.available_tasks();
                    let value = self.encode_execution_graph(graph)?;
                    Ok((Keyspace::ActiveJobs, job_id, value))
                })
                .collect::<Result<Vec<_>>>()?;

            self.state.put_txn(txn_ops).await?;

            Ok((assignments, unassigned, pending_tasks))
        }).await
    }

    /// Move the given job to the CompletedJobs keyspace in persistent storage.
    pub async fn complete_job(&self, job_id: &str) -> Result<()> {
        debug!("Moving job {} from Active to Completed", job_id);
        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;
        with_lock(
            lock,
            self.state
                .mv(Keyspace::ActiveJobs, Keyspace::CompletedJobs, job_id),
        )
        .await
    }

    pub(crate) async fn cancel_job(
        &self,
        job_id: &str,
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;

        let running_tasks = self
            .get_execution_graph(job_id)
            .await
            .map(|graph| graph.running_tasks())
            .unwrap_or_else(|_| vec![]);

        info!(
            "Cancelling {} running tasks for job {}",
            running_tasks.len(),
            job_id
        );

        self.fail_job_inner(lock, job_id, "Cancelled".to_owned())
            .await?;

        let mut tasks: HashMap<&str, Vec<protobuf::PartitionId>> = Default::default();

        for (partition, executor_id) in &running_tasks {
            if let Some(parts) = tasks.get_mut(executor_id.as_str()) {
                parts.push(protobuf::PartitionId {
                    job_id: job_id.to_owned(),
                    stage_id: partition.stage_id as u32,
                    partition_id: partition.partition_id as u32,
                })
            } else {
                tasks.insert(
                    executor_id.as_str(),
                    vec![protobuf::PartitionId {
                        job_id: job_id.to_owned(),
                        stage_id: partition.stage_id as u32,
                        partition_id: partition.partition_id as u32,
                    }],
                );
            }
        }

        for (executor_id, partitions) in tasks {
            if let Ok(mut client) = executor_manager.get_client(executor_id).await {
                client
                    .cancel_tasks(CancelTasksParams {
                        partition_id: partitions,
                    })
                    .await?;
            } else {
                error!("Failed to get client for executor ID {}", executor_id)
            }
        }

        Ok(())
    }

    /// Mark a job as failed. This will create a key under the FailedJobs keyspace
    /// and remove the job from ActiveJobs or QueuedJobs
    /// TODO this should be atomic
    pub async fn fail_job(&self, job_id: &str, error_message: String) -> Result<()> {
        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;
        self.fail_job_inner(lock, job_id, error_message).await
    }

    async fn fail_job_inner(
        &self,
        lock: Box<dyn Lock>,
        job_id: &str,
        error_message: String,
    ) -> Result<()> {
        with_lock(lock, self.state.delete(Keyspace::ActiveJobs, job_id)).await?;

        let status = JobStatus {
            status: Some(job_status::Status::Failed(FailedJob {
                error: error_message,
            })),
        };
        let value = encode_protobuf(&status)?;

        self.state
            .put(Keyspace::FailedJobs, job_id.to_owned(), value)
            .await
    }

    #[cfg(not(test))]
    /// Launch the given task on the specified executor
    pub(crate) async fn launch_task(
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
            let connection =
                ballista_core::utils::create_grpc_client_connection(executor_url).await?;
            let mut client = ExecutorGrpcClient::new(connection);
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

    /// In unit tests, we do not have actual executors running, so it simplifies things to just noop.
    #[cfg(test)]
    pub(crate) async fn launch_task(
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
            task_id: Some(protobuf::PartitionId {
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

    ///  Return a set of active job IDs. This will return all keys
    /// in the `ActiveJobs` keyspace stripped of any prefixes used for
    /// the storage layer (i.e. just the Job IDs).
    async fn get_active_jobs(&self) -> Result<HashSet<String>> {
        debug!("Scanning for active job IDs");
        self.state.scan_keys(Keyspace::ActiveJobs).await
    }

    /// Get the `ExecutionGraph` for the given job ID. This will search fist in the `ActiveJobs`
    /// keyspace and then, if it doesn't find anything, search the `CompletedJobs` keyspace.
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

        ExecutionGraph::decode_execution_graph(proto, &self.codec, &session_ctx).await
    }

    fn encode_execution_graph(&self, graph: ExecutionGraph) -> Result<Vec<u8>> {
        let proto = ExecutionGraph::encode_execution_graph(graph, &self.codec)?;

        encode_protobuf(&proto)
    }
}

/// Find the next available task in a set of `ExecutionGraph`s
fn find_next_task(
    executor_id: &str,
    graphs: &mut HashMap<String, ExecutionGraph>,
) -> Result<Option<(String, Task)>> {
    for graph in graphs.values_mut() {
        if let Ok(Some(task)) = graph.pop_next_task(executor_id) {
            return Ok(Some((executor_id.to_owned(), task)));
        }
    }
    Ok(None)
}
