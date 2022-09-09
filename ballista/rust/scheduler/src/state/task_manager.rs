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
use std::collections::HashMap;
use std::default::Default;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

type ExecutorClients = Arc<RwLock<HashMap<String, ExecutorGrpcClient<Channel>>>>;
type ExecutionGraphCache = Arc<RwLock<HashMap<String, Arc<RwLock<ExecutionGraph>>>>>;

#[derive(Clone)]
pub struct TaskManager<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    state: Arc<dyn StateBackendClient>,
    #[allow(dead_code)]
    clients: ExecutorClients,
    session_builder: SessionBuilder,
    codec: BallistaCodec<T, U>,
    scheduler_id: String,
    // Cache for active execution graphs curated by this scheduler
    active_job_cache: ExecutionGraphCache,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskManager<T, U> {
    pub fn new(
        state: Arc<dyn StateBackendClient>,
        session_builder: SessionBuilder,
        codec: BallistaCodec<T, U>,
        scheduler_id: String,
    ) -> Self {
        Self {
            state,
            clients: Default::default(),
            session_builder,
            codec,
            scheduler_id,
            active_job_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Generate an ExecutionGraph for the job and save it to the persistent state.
    /// By default, this job will be curated by the scheduler which receives it.
    /// Then we will also save it to the active execution graph
    pub async fn submit_job(
        &self,
        job_id: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<()> {
        let mut graph =
            ExecutionGraph::new(&self.scheduler_id, job_id, session_id, plan)?;
        info!("Submitting execution graph: {:?}", graph);
        self.state
            .put(
                Keyspace::ActiveJobs,
                job_id.to_owned(),
                self.encode_execution_graph(graph.clone())?,
            )
            .await?;

        graph.revive();

        let mut active_graph_cache = self.active_job_cache.write().await;
        active_graph_cache.insert(job_id.to_owned(), Arc::new(RwLock::new(graph)));

        Ok(())
    }

    /// Get the status of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs, and then in Failed jobs
    pub async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let status = graph.read().await.status();
            Ok(Some(status))
        } else if let Ok(graph) = self.get_execution_graph(job_id).await {
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

    /// Update given task statuses in the respective job and return a tuple containing:
    /// 1. A list of QueryStageSchedulerEvent to publish.
    /// 2. A list of reservations that can now be offered.
    pub(crate) async fn update_task_statuses(
        &self,
        executor: &ExecutorMetadata,
        task_status: Vec<TaskStatus>,
    ) -> Result<Vec<QueryStageSchedulerEvent>> {
        let mut job_updates: HashMap<String, Vec<TaskStatus>> = HashMap::new();
        for status in task_status {
            debug!("Task Update\n{:?}", status);
            if let Some(job_id) = status.task_id.as_ref().map(|id| &id.job_id) {
                let job_task_statuses =
                    job_updates.entry(job_id.clone()).or_insert_with(Vec::new);
                job_task_statuses.push(status);
            } else {
                warn!("Received task with no job ID");
            }
        }

        let mut events: Vec<QueryStageSchedulerEvent> = vec![];
        for (job_id, statuses) in job_updates {
            let num_tasks = statuses.len();
            debug!("Updating {} tasks in job {}", num_tasks, job_id);

            let graph = self.get_active_execution_graph(&job_id).await;
            let job_event = if let Some(graph) = graph {
                let mut graph = graph.write().await;
                graph.update_task_status(executor, statuses)?
            } else {
                // TODO Deal with curator changed case
                error!("Fail to find job {} in the active cache and it may not be curated by this scheduler", job_id);
                None
            };

            if let Some(event) = job_event {
                events.push(event);
            }
        }

        Ok(events)
    }

    /// Take a list of executor reservations and fill them with tasks that are ready
    /// to be scheduled.
    ///
    /// Here we use the following  algorithm:
    ///
    /// 1. For each free reservation, try to assign a task from one of the active jobs
    /// 2. If we cannot find a task in all active jobs, then add the reservation to the list of unassigned reservations
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

        let mut assignments: Vec<(String, Task)> = vec![];
        let mut pending_tasks = 0usize;
        let mut assign_tasks = 0usize;
        let job_cache = self.active_job_cache.read().await;
        for (_job_id, graph) in job_cache.iter() {
            let mut graph = graph.write().await;
            for reservation in free_reservations.iter().skip(assign_tasks) {
                if let Some(task) = graph.pop_next_task(&reservation.executor_id)? {
                    assignments.push((reservation.executor_id.clone(), task));
                    assign_tasks += 1;
                } else {
                    break;
                }
            }
            if assign_tasks >= free_reservations.len() {
                pending_tasks = graph.available_tasks();
                break;
            }
        }

        let mut unassigned = vec![];
        for reservation in free_reservations.iter().skip(assign_tasks) {
            unassigned.push(reservation.clone());
        }
        Ok((assignments, unassigned, pending_tasks))
    }

    /// Mark a job as completed. This will create a key under the CompletedJobs keyspace
    /// and remove the job from ActiveJobs
    pub async fn complete_job(&self, job_id: &str) -> Result<()> {
        debug!("Moving job {} from Active to Completed", job_id);
        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;
        with_lock(lock, self.state.delete(Keyspace::ActiveJobs, job_id)).await?;

        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let graph = graph.read().await.clone();
            if graph.complete() {
                let value = self.encode_execution_graph(graph)?;
                self.state
                    .put(Keyspace::CompletedJobs, job_id.to_owned(), value)
                    .await?;
            } else {
                error!("Job {} has not finished and cannot be completed", job_id);
            }
        } else {
            warn!("Fail to find job {} in the cache", job_id);
        }

        Ok(())
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
        debug!("Moving job {} from Active or Queue to Failed", job_id);
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

        let value = if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let mut graph = graph.write().await;
            graph.fail_job(error_message);
            let graph = graph.clone();

            self.encode_execution_graph(graph)?
        } else {
            warn!("Fail to find job {} in the cache", job_id);

            let status = JobStatus {
                status: Some(job_status::Status::Failed(FailedJob {
                    error: error_message.clone(),
                })),
            };
            encode_protobuf(&status)?
        };

        self.state
            .put(Keyspace::FailedJobs, job_id.to_owned(), value)
            .await?;

        Ok(())
    }

    /// Mark a job as failed. This will create a key under the FailedJobs keyspace
    /// and remove the job from ActiveJobs or QueuedJobs
    /// TODO this should be atomic
    pub async fn fail_running_job(&self, job_id: &str) -> Result<()> {
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let graph = graph.read().await.clone();
            let value = self.encode_execution_graph(graph)?;

            debug!("Moving job {} from Active to Failed", job_id);
            let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;
            with_lock(lock, self.state.delete(Keyspace::ActiveJobs, job_id)).await?;
            self.state
                .put(Keyspace::FailedJobs, job_id.to_owned(), value)
                .await?;
        } else {
            warn!("Fail to find job {} in the cache", job_id);
        }

        Ok(())
    }

    pub async fn update_job(&self, job_id: &str) -> Result<()> {
        debug!("Update job {} in Active", job_id);
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let mut graph = graph.write().await;
            graph.revive();
            let graph = graph.clone();
            let value = self.encode_execution_graph(graph)?;
            self.state
                .put(Keyspace::ActiveJobs, job_id.to_owned(), value)
                .await?;
        } else {
            warn!("Fail to find job {} in the cache", job_id);
        }

        Ok(())
    }

    pub async fn executor_lost(&self, executor_id: &str) -> Result<()> {
        // Collect graphs we update so we can update them in storage
        let mut updated_graphs: HashMap<String, ExecutionGraph> = HashMap::new();
        {
            let job_cache = self.active_job_cache.read().await;
            for (job_id, graph) in job_cache.iter() {
                let mut graph = graph.write().await;
                let reset = graph.reset_stages(executor_id)?;
                if !reset.is_empty() {
                    updated_graphs.insert(job_id.to_owned(), graph.clone());
                }
            }
        }

        let lock = self.state.lock(Keyspace::ActiveJobs, "").await?;
        with_lock(lock, async {
            // Transactional update graphs
            let txn_ops: Vec<(Keyspace, String, Vec<u8>)> = updated_graphs
                .into_iter()
                .map(|(job_id, graph)| {
                    let value = self.encode_execution_graph(graph)?;
                    Ok((Keyspace::ActiveJobs, job_id, value))
                })
                .collect::<Result<Vec<_>>>()?;
            self.state.put_txn(txn_ops).await?;
            Ok(())
        })
        .await
    }

    #[cfg(not(test))]
    /// Launch the given task on the specified executor
    pub(crate) async fn launch_task(
        &self,
        executor: &ExecutorMetadata,
        task: Task,
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        info!("Launching task {:?} on executor {:?}", task, executor.id);
        let task_definition = self.prepare_task_definition(task)?;
        let mut client = executor_manager.get_client(&executor.id).await?;
        client
            .launch_task(protobuf::LaunchTaskParams {
                tasks: vec![task_definition],
                scheduler_id: self.scheduler_id.clone(),
            })
            .await
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Failed to connect to executor {}: {:?}",
                    executor.id, e
                ))
            })?;
        Ok(())
    }

    /// In unit tests, we do not have actual executors running, so it simplifies things to just noop.
    #[cfg(test)]
    pub(crate) async fn launch_task(
        &self,
        _executor: &ExecutorMetadata,
        _task: Task,
        _executor_manager: &ExecutorManager,
    ) -> Result<()> {
        Ok(())
    }

    /// Retrieve the number of available tasks for the given job. The value returned
    /// is strictly a point-in-time snapshot
    pub async fn get_available_task_count(&self, job_id: &str) -> Result<usize> {
        if let Some(graph) = self.get_active_execution_graph(job_id).await {
            let available_tasks = graph.read().await.available_tasks();
            Ok(available_tasks)
        } else {
            warn!("Fail to find job {} in the cache", job_id);
            Ok(0)
        }
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

    /// Get the `ExecutionGraph` for the given job ID from cache
    pub(crate) async fn get_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraph>>> {
        let active_graph_cache = self.active_job_cache.read().await;
        active_graph_cache.get(job_id).cloned()
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

    /// Generate a new random Job ID
    pub fn generate_job_id(&self) -> String {
        let mut rng = thread_rng();
        std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(7)
            .collect()
    }
}
