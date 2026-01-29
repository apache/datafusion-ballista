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

use std::time::Duration;

use ballista_core::error::BallistaError;
use ballista_core::error::Result;
use ballista_core::serde::protobuf;
use log::trace;

use crate::cluster::{BoundTask, ClusterState, ExecutorSlot};
use crate::config::SchedulerConfig;

use crate::state::execution_graph::RunningTaskInfo;
use crate::state::task_manager::JobInfoCache;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;
use ballista_core::serde::protobuf::{
    CancelTasksParams, ExecutorHeartbeat, MultiTaskDefinition, RemoveJobDataParams,
    StopExecutorParams, executor_status,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};

use ballista_core::utils::{
    GrpcClientConfig, create_grpc_client_endpoint, get_time_before,
};

use dashmap::DashMap;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tonic::transport::Channel;

type ExecutorClients = Arc<DashMap<String, ExecutorGrpcClient<Channel>>>;

/// Manages executor lifecycle and communication for the Ballista scheduler.
///
/// The `ExecutorManager` is responsible for:
/// - Registering and removing executors
/// - Tracking executor health via heartbeats
/// - Binding tasks to available executor slots
/// - Launching tasks on executors
/// - Cleaning up job data on executors
#[derive(Clone)]
pub struct ExecutorManager {
    /// Cluster state for tracking executor registration and task slots.
    cluster_state: Arc<dyn ClusterState>,
    /// Scheduler configuration.
    config: Arc<SchedulerConfig>,
    /// Cached gRPC clients for communicating with executors.
    clients: ExecutorClients,
    /// Jobs pending cleanup on each executor.
    pending_cleanup_jobs: Arc<DashMap<String, HashSet<String>>>,
    /// Configuration for gRPC client connections.
    grpc_client_config: GrpcClientConfig,
}

impl ExecutorManager {
    /// Creates a new `ExecutorManager` with the given cluster state and configuration.
    pub(crate) fn new(
        cluster_state: Arc<dyn ClusterState>,
        config: Arc<SchedulerConfig>,
    ) -> Self {
        let grpc_client_config =
            if let Some(config_producer) = &config.override_config_producer {
                let session_config = config_producer();
                let ballista_config = session_config.ballista_config();
                GrpcClientConfig::from(&ballista_config)
            } else {
                GrpcClientConfig::default()
            };
        Self {
            cluster_state,
            config,
            clients: Default::default(),
            pending_cleanup_jobs: Default::default(),
            grpc_client_config,
        }
    }

    /// Initializes the executor manager and underlying cluster state.
    pub async fn init(&self) -> Result<()> {
        self.cluster_state.init().await?;

        Ok(())
    }

    /// Binds ready-to-run tasks from active jobs to available executor slots.
    ///
    /// Returns a list of bound tasks that can be launched on executors.
    pub async fn bind_schedulable_tasks(
        &self,
        running_jobs: Arc<HashMap<String, JobInfoCache>>,
    ) -> Result<Vec<BoundTask>> {
        if running_jobs.is_empty() {
            debug!("There's no active jobs for binding tasks");
            return Ok(vec![]);
        }
        let alive_executors = self.get_alive_executors();
        if alive_executors.is_empty() {
            debug!("There's no alive executors for binding tasks");
            return Ok(vec![]);
        }
        self.cluster_state
            .bind_schedulable_tasks(
                self.config.task_distribution.clone(),
                running_jobs,
                Some(alive_executors),
            )
            .await
    }

    /// Returns reserved task slots to the pool of available slots.
    ///
    /// This operation is atomic: either all slots are returned or none are.
    pub async fn unbind_tasks(&self, executor_slots: Vec<ExecutorSlot>) -> Result<()> {
        self.cluster_state.unbind_tasks(executor_slots).await
    }

    /// Sends RPC requests to executors to cancel the specified running tasks.
    pub async fn cancel_running_tasks(&self, tasks: Vec<RunningTaskInfo>) -> Result<()> {
        let mut tasks_to_cancel: HashMap<String, Vec<protobuf::RunningTaskInfo>> =
            Default::default();

        for task_info in tasks {
            let infos = tasks_to_cancel.entry(task_info.executor_id).or_default();
            infos.push(protobuf::RunningTaskInfo {
                task_id: task_info.task_id as u32,
                job_id: task_info.job_id,
                stage_id: task_info.stage_id as u32,
                partition_id: task_info.partition_id as u32,
            });
        }

        let executor_manager = self.clone();
        tokio::spawn(async move {
            for (executor_id, infos) in tasks_to_cancel {
                if let Ok(mut client) = executor_manager
                    .get_client(&executor_id, &executor_manager.grpc_client_config)
                    .await
                {
                    if let Err(e) = client
                        .cancel_tasks(CancelTasksParams { task_infos: infos })
                        .await
                    {
                        error!(
                            "Fail to cancel tasks for executor ID {executor_id} due to {e:?}"
                        );
                    }
                } else {
                    error!(
                        "Failed to get client for executor ID {executor_id} to cancel tasks"
                    )
                }
            }
        });

        Ok(())
    }

    /// Send rpc to Executors to clean up the job data by delayed clean_up_interval seconds
    pub(crate) fn clean_up_job_data_delayed(
        &self,
        job_id: String,
        clean_up_interval: u64,
    ) {
        if clean_up_interval == 0 {
            info!(
                "The interval is 0 and the clean up for job data {job_id} will not triggered"
            );
            return;
        }

        let executor_manager = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval)).await;
            executor_manager.clean_up_job_data_inner(job_id).await;
        });
    }

    /// Sends RPC requests to executors to clean up job data in a spawned task.
    pub fn clean_up_job_data(&self, job_id: String) {
        let executor_manager = self.clone();
        tokio::spawn(async move {
            executor_manager.clean_up_job_data_inner(job_id).await;
        });
    }

    /// 1. Push strategy: Send rpc to Executors to clean up the job data
    /// 2. Poll strategy: Save cleanup job ids and send them to executors
    async fn clean_up_job_data_inner(&self, job_id: String) {
        let alive_executors = self.get_alive_executors();

        for executor in alive_executors {
            let job_id_clone = job_id.to_owned();

            if self.config.is_push_staged_scheduling() {
                if let Ok(mut client) =
                    self.get_client(&executor, &self.grpc_client_config).await
                {
                    tokio::spawn(async move {
                        if let Err(err) = client
                            .remove_job_data(RemoveJobDataParams {
                                job_id: job_id_clone,
                            })
                            .await
                        {
                            warn!(
                                "Failed to call remove_job_data on Executor {executor} due to {err:?}"
                            )
                        }
                    });
                } else {
                    warn!("Failed to get client for Executor {executor}")
                }
            } else {
                self.pending_cleanup_jobs
                    .entry(executor)
                    .or_default()
                    .insert(job_id.clone());
            }
        }
    }

    /// Returns a list of all executors along with the timestamp of their last recorded heartbeat.
    pub async fn get_executor_state(
        &self,
    ) -> Result<Vec<(ExecutorMetadata, Option<Duration>)>> {
        let mut state: Vec<(ExecutorMetadata, Option<Duration>)> = vec![];
        for metadata in self.cluster_state.registered_executor_metadata().await {
            let duration = self
                .cluster_state
                .get_executor_heartbeat(&metadata.id)
                .map(|hb| hb.timestamp)
                .map(Duration::from_secs);
            state.push((metadata, duration));
        }

        Ok(state)
    }

    /// Returns executor metadata for the provided executor ID.
    ///
    /// Returns an error if the executor does not exist.
    pub async fn get_executor_metadata(
        &self,
        executor_id: &str,
    ) -> Result<ExecutorMetadata> {
        self.cluster_state.get_executor_metadata(executor_id).await
    }

    /// Saves executor metadata for pull-based task scheduling.
    ///
    /// For push-based scheduling, use [`Self::register_executor`] instead.
    pub async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        trace!(
            "save executor metadata {} with {} task slots (pull-based registration)",
            metadata.id, metadata.specification.task_slots
        );
        self.cluster_state.save_executor_metadata(metadata).await
    }

    /// Registers the executor with the scheduler for push-based task scheduling.
    ///
    /// This saves both the executor metadata and available task slots to persistent state.
    /// For pull-based scheduling, use [`Self::save_executor_metadata`] instead.
    pub async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        specification: ExecutorData,
    ) -> Result<()> {
        debug!(
            "registering executor {} with {} task slots (push-based registration)",
            metadata.id, specification.total_task_slots
        );

        ExecutorManager::test_connectivity(&metadata).await?;

        self.cluster_state
            .register_executor(metadata, specification)
            .await?;

        Ok(())
    }

    /// Removes the executor from the cluster state.
    pub async fn remove_executor(
        &self,
        executor_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        info!("Removing executor {executor_id}: {reason:?}");
        self.cluster_state.remove_executor(executor_id).await
    }

    /// Sends a stop request to the specified executor.
    pub async fn stop_executor(&self, executor_id: &str, stop_reason: String) {
        let executor_id = executor_id.to_string();
        match self
            .get_client(&executor_id, &self.grpc_client_config)
            .await
        {
            Ok(mut client) => {
                tokio::task::spawn(async move {
                    match client
                        .stop_executor(StopExecutorParams {
                            executor_id: executor_id.to_string(),
                            reason: stop_reason,
                            force: true,
                        })
                        .await
                    {
                        Err(error) => {
                            warn!("Failed to send stop_executor rpc due to, {error}");
                        }
                        Ok(_value) => {}
                    }
                });
            }
            Err(_) => {
                warn!(
                    "Executor is already dead, failed to connect to Executor {executor_id}"
                );
            }
        }
    }

    /// Launches multiple tasks on the specified executor.
    pub async fn launch_multi_task(
        &self,
        executor_id: &str,
        multi_tasks: Vec<MultiTaskDefinition>,
        scheduler_id: String,
    ) -> Result<()> {
        let mut client = self
            .get_client(executor_id, &self.grpc_client_config)
            .await?;
        client
            .launch_multi_task(protobuf::LaunchMultiTaskParams {
                multi_tasks,
                scheduler_id,
            })
            .await
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Failed to connect to executor {executor_id}: {e:?}"
                ))
            })?;

        Ok(())
    }

    pub(crate) fn drain_pending_cleanup_jobs(
        &self,
        executor_id: &str,
    ) -> HashSet<String> {
        self.pending_cleanup_jobs
            .remove(executor_id)
            .map(|(_, jobs)| jobs)
            .unwrap_or_default()
    }

    pub(crate) async fn save_executor_heartbeat(
        &self,
        heartbeat: ExecutorHeartbeat,
    ) -> Result<()> {
        self.cluster_state
            .save_executor_heartbeat(heartbeat.clone())
            .await?;

        Ok(())
    }

    pub(crate) fn is_dead_executor(&self, executor_id: &str) -> bool {
        self.cluster_state
            .get_executor_heartbeat(executor_id)
            .is_none_or(|heartbeat| {
                matches!(
                    heartbeat.status,
                    Some(ballista_core::serde::generated::ballista::ExecutorStatus {
                        status: Some(executor_status::Status::Dead(_))
                    })
                )
            })
    }

    /// Retrieve the set of all executor IDs where the executor has been observed in the last
    /// `last_seen_ts_threshold` seconds.
    pub(crate) fn get_alive_executors(&self) -> HashSet<String> {
        let last_seen_ts_threshold =
            get_time_before(self.config.executor_timeout_seconds);
        self.cluster_state
            .executor_heartbeats()
            .iter()
            .filter_map(|(exec, heartbeat)| {
                let active = match heartbeat
                    .status
                    .as_ref()
                    .and_then(|status| status.status.as_ref())
                {
                    Some(executor_status::Status::Active(_)) => true,
                    Some(executor_status::Status::Terminating(_))
                    | Some(executor_status::Status::Dead(_)) => false,
                    None => {
                        // If config is poll-based scheduling, treat executors with no status as active
                        !self.config.is_push_staged_scheduling()
                    }
                    _ => false,
                };
                let live = heartbeat.timestamp > last_seen_ts_threshold;

                (active && live).then(|| exec.clone())
            })
            .collect()
    }

    /// Return a list of expired executors
    pub(crate) fn get_expired_executors(&self) -> Vec<ExecutorHeartbeat> {
        // Threshold for last heartbeat from Active executor before marking dead
        let last_seen_threshold = get_time_before(self.config.executor_timeout_seconds);

        // Threshold for last heartbeat for Fenced executor before marking dead
        let termination_wait_threshold =
            get_time_before(self.config.executor_termination_grace_period);

        self.cluster_state
            .executor_heartbeats()
            .iter()
            .filter_map(|(_exec, heartbeat)| {
                let terminating = matches!(
                    heartbeat
                        .status
                        .as_ref()
                        .and_then(|status| status.status.as_ref()),
                    Some(executor_status::Status::Terminating(_))
                );

                let grace_period_expired =
                    heartbeat.timestamp <= termination_wait_threshold;

                let expired = heartbeat.timestamp <= last_seen_threshold;

                ((terminating && grace_period_expired) || expired)
                    .then(|| heartbeat.clone())
            })
            .collect::<Vec<_>>()
    }

    async fn get_client(
        &self,
        executor_id: &str,
        grpc_client_config: &GrpcClientConfig,
    ) -> Result<ExecutorGrpcClient<Channel>> {
        let client = self.clients.get(executor_id).map(|value| value.clone());

        if let Some(client) = client {
            Ok(client)
        } else {
            let executor_metadata = self.get_executor_metadata(executor_id).await?;
            let executor_url = format!(
                "http://{}:{}",
                executor_metadata.host, executor_metadata.grpc_port
            );
            let mut endpoint =
                create_grpc_client_endpoint(executor_url, Some(grpc_client_config))?;

            if let Some(ref override_fn) =
                self.config.override_create_grpc_client_endpoint
            {
                endpoint = override_fn(endpoint).map_err(|e| {
                    BallistaError::GrpcConnectionError(format!(
                        "Failed to customize endpoint for executor {executor_id}: {e}"
                    ))
                })?;
            }

            let connection = endpoint.connect().await?;
            let client = ExecutorGrpcClient::new(connection);

            {
                self.clients.insert(executor_id.to_owned(), client.clone());
            }
            Ok(client)
        }
    }

    #[cfg(not(test))]
    async fn test_connectivity(metadata: &ExecutorMetadata) -> Result<()> {
        let executor_url = format!("http://{}:{}", metadata.host, metadata.grpc_port);
        debug!("Connecting to executor {executor_url:?}");
        let _ = protobuf::executor_grpc_client::ExecutorGrpcClient::connect(executor_url)
            .await
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Failed to register executor at {}:{}, could not connect: {:?}",
                    metadata.host, metadata.grpc_port, e
                ))
            })?;
        Ok(())
    }

    #[cfg(test)]
    async fn test_connectivity(_metadata: &ExecutorMetadata) -> Result<()> {
        Ok(())
    }
}
