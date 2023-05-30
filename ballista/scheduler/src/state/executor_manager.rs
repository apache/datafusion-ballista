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

#[cfg(not(test))]
use ballista_core::error::BallistaError;
use ballista_core::error::Result;
use ballista_core::serde::protobuf;

use crate::cluster::ClusterState;
use crate::config::SchedulerConfig;

use crate::state::execution_graph::RunningTaskInfo;
use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;
use ballista_core::serde::protobuf::{
    executor_status, CancelTasksParams, ExecutorHeartbeat, RemoveJobDataParams,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use ballista_core::utils::{create_grpc_client_connection, get_time_before};
use dashmap::DashMap;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tonic::transport::Channel;

type ExecutorClients = Arc<DashMap<String, ExecutorGrpcClient<Channel>>>;

/// Represents a task slot that is reserved (i.e. available for scheduling but not visible to the
/// rest of the system).
/// When tasks finish we want to preferentially assign new tasks from the same job, so the reservation
/// can already be assigned to a particular job ID. In that case, the scheduler will try to schedule
/// available tasks for that job to the reserved task slot.
#[derive(Clone, Debug)]
pub struct ExecutorReservation {
    pub executor_id: String,
    pub job_id: Option<String>,
}

impl ExecutorReservation {
    pub fn new_free(executor_id: String) -> Self {
        Self {
            executor_id,
            job_id: None,
        }
    }

    pub fn new_assigned(executor_id: String, job_id: String) -> Self {
        Self {
            executor_id,
            job_id: Some(job_id),
        }
    }

    pub fn assign(mut self, job_id: String) -> Self {
        self.job_id = Some(job_id);
        self
    }

    pub fn assigned(&self) -> bool {
        self.job_id.is_some()
    }
}

#[derive(Clone)]
pub struct ExecutorManager {
    cluster_state: Arc<dyn ClusterState>,
    config: Arc<SchedulerConfig>,
    clients: ExecutorClients,
}

impl ExecutorManager {
    pub(crate) fn new(
        cluster_state: Arc<dyn ClusterState>,
        config: Arc<SchedulerConfig>,
    ) -> Self {
        Self {
            cluster_state,
            config,
            clients: Default::default(),
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.cluster_state.init().await?;

        Ok(())
    }

    /// Reserve up to n executor task slots. Once reserved these slots will not be available
    /// for scheduling.
    /// This operation is atomic, so if this method return an Err, no slots have been reserved.
    pub async fn reserve_slots(&self, n: u32) -> Result<Vec<ExecutorReservation>> {
        let alive_executors = self.get_alive_executors();

        debug!("Alive executors: {alive_executors:?}");

        self.cluster_state
            .reserve_slots(n, self.config.task_distribution, Some(alive_executors))
            .await
    }

    /// Returned reserved task slots to the pool of available slots. This operation is atomic
    /// so either the entire pool of reserved task slots it returned or none are.
    pub async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        self.cluster_state.cancel_reservations(reservations).await
    }

    /// Send rpc to Executors to cancel the running tasks
    pub async fn cancel_running_tasks(&self, tasks: Vec<RunningTaskInfo>) -> Result<()> {
        let mut tasks_to_cancel: HashMap<&str, Vec<protobuf::RunningTaskInfo>> =
            Default::default();

        for task_info in &tasks {
            if let Some(infos) = tasks_to_cancel.get_mut(task_info.executor_id.as_str()) {
                infos.push(protobuf::RunningTaskInfo {
                    task_id: task_info.task_id as u32,
                    job_id: task_info.job_id.clone(),
                    stage_id: task_info.stage_id as u32,
                    partition_id: task_info.partition_id as u32,
                })
            } else {
                tasks_to_cancel.insert(
                    task_info.executor_id.as_str(),
                    vec![protobuf::RunningTaskInfo {
                        task_id: task_info.task_id as u32,
                        job_id: task_info.job_id.clone(),
                        stage_id: task_info.stage_id as u32,
                        partition_id: task_info.partition_id as u32,
                    }],
                );
            }
        }

        for (executor_id, infos) in tasks_to_cancel {
            if let Ok(mut client) = self.get_client(executor_id).await {
                client
                    .cancel_tasks(CancelTasksParams { task_infos: infos })
                    .await?;
            } else {
                error!(
                    "Failed to get client for executor ID {} to cancel tasks",
                    executor_id
                )
            }
        }
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
                "The interval is 0 and the clean up for job data {} will not triggered",
                job_id
            );
            return;
        }

        let executor_manager = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval)).await;
            executor_manager.clean_up_job_data_inner(job_id).await;
        });
    }

    /// Send rpc to Executors to clean up the job data in a spawn thread
    pub fn clean_up_job_data(&self, job_id: String) {
        let executor_manager = self.clone();
        tokio::spawn(async move {
            executor_manager.clean_up_job_data_inner(job_id).await;
        });
    }

    /// Send rpc to Executors to clean up the job data
    async fn clean_up_job_data_inner(&self, job_id: String) {
        let alive_executors = self.get_alive_executors();
        for executor in alive_executors {
            let job_id_clone = job_id.to_owned();
            if let Ok(mut client) = self.get_client(&executor).await {
                tokio::spawn(async move {
                    if let Err(err) = client
                        .remove_job_data(RemoveJobDataParams {
                            job_id: job_id_clone,
                        })
                        .await
                    {
                        warn!(
                            "Failed to call remove_job_data on Executor {} due to {:?}",
                            executor, err
                        )
                    }
                });
            } else {
                warn!("Failed to get client for Executor {}", executor)
            }
        }
    }

    pub async fn get_client(
        &self,
        executor_id: &str,
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
            let connection = create_grpc_client_connection(executor_url).await?;
            let client = ExecutorGrpcClient::new(connection);

            {
                self.clients.insert(executor_id.to_owned(), client.clone());
            }
            Ok(client)
        }
    }

    /// Get a list of all executors along with the timestamp of their last recorded heartbeat
    pub async fn get_executor_state(&self) -> Result<Vec<(ExecutorMetadata, Duration)>> {
        let heartbeat_timestamps: Vec<(String, u64)> = self
            .cluster_state
            .executor_heartbeats()
            .into_iter()
            .map(|(executor_id, heartbeat)| (executor_id, heartbeat.timestamp))
            .collect();

        let mut state: Vec<(ExecutorMetadata, Duration)> = vec![];
        for (executor_id, ts) in heartbeat_timestamps {
            let duration = Duration::from_secs(ts);

            let metadata = self.get_executor_metadata(&executor_id).await?;

            state.push((metadata, duration));
        }

        Ok(state)
    }

    pub async fn get_executor_metadata(
        &self,
        executor_id: &str,
    ) -> Result<ExecutorMetadata> {
        self.cluster_state.get_executor_metadata(executor_id).await
    }

    pub async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        self.cluster_state.save_executor_metadata(metadata).await
    }

    /// Register the executor with the scheduler. This will save the executor metadata and the
    /// executor data to persistent state.
    ///
    /// If `reserve` is true, then any available task slots will be reserved and dispatched for scheduling.
    /// If `reserve` is false, then the executor data will be saved as is.
    ///
    /// In general, reserve should be true is the scheduler is using push-based scheduling and false
    /// if the scheduler is using pull-based scheduling.
    pub async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        specification: ExecutorData,
        reserve: bool,
    ) -> Result<Vec<ExecutorReservation>> {
        debug!(
            "registering executor {} with {} task slots",
            metadata.id, specification.total_task_slots
        );

        self.test_scheduler_connectivity(&metadata).await?;

        if !reserve {
            self.cluster_state
                .register_executor(metadata, specification.clone(), reserve)
                .await?;

            Ok(vec![])
        } else {
            let mut specification = specification;
            let num_slots = specification.available_task_slots as usize;
            let mut reservations: Vec<ExecutorReservation> = vec![];
            for _ in 0..num_slots {
                reservations.push(ExecutorReservation::new_free(metadata.id.clone()));
            }

            specification.available_task_slots = 0;

            self.cluster_state
                .register_executor(metadata, specification, reserve)
                .await?;

            Ok(reservations)
        }
    }

    /// Remove the executor within the scheduler.
    pub async fn remove_executor(
        &self,
        executor_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        info!("Removing executor {}: {:?}", executor_id, reason);
        self.cluster_state.remove_executor(executor_id).await
    }

    #[cfg(not(test))]
    async fn test_scheduler_connectivity(
        &self,
        metadata: &ExecutorMetadata,
    ) -> Result<()> {
        let executor_url = format!("http://{}:{}", metadata.host, metadata.grpc_port);
        debug!("Connecting to executor {:?}", executor_url);
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
    async fn test_scheduler_connectivity(
        &self,
        _metadata: &ExecutorMetadata,
    ) -> Result<()> {
        Ok(())
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
            .map_or(true, |heartbeat| {
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
                let active = matches!(
                    heartbeat
                        .status
                        .as_ref()
                        .and_then(|status| status.status.as_ref()),
                    Some(executor_status::Status::Active(_))
                );
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
}

#[cfg(test)]
mod test {
    use crate::config::{SchedulerConfig, TaskDistribution};
    use std::sync::Arc;

    use crate::scheduler_server::timestamp_secs;
    use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};
    use crate::test_utils::test_cluster_context;
    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::executor_status::Status;
    use ballista_core::serde::protobuf::{ExecutorHeartbeat, ExecutorStatus};
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorMetadata, ExecutorSpecification,
    };

    #[tokio::test]
    async fn test_reserve_and_cancel() -> Result<()> {
        test_reserve_and_cancel_inner(TaskDistribution::Bias).await?;
        test_reserve_and_cancel_inner(TaskDistribution::RoundRobin).await?;

        Ok(())
    }

    async fn test_reserve_and_cancel_inner(
        task_distribution: TaskDistribution,
    ) -> Result<()> {
        let cluster = test_cluster_context();

        let config = SchedulerConfig::default().with_task_distribution(task_distribution);
        let executor_manager =
            ExecutorManager::new(cluster.cluster_state(), Arc::new(config));

        let executors = test_executors(10, 4);

        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        // Reserve all the slots
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(
            reservations.len(),
            40,
            "Expected 40 reservations for policy {task_distribution:?}"
        );

        // Now cancel them
        executor_manager.cancel_reservations(reservations).await?;

        // Now reserve again
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(
            reservations.len(),
            40,
            "Expected 40 reservations for policy {task_distribution:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_partial() -> Result<()> {
        test_reserve_partial_inner(TaskDistribution::Bias).await?;
        test_reserve_partial_inner(TaskDistribution::RoundRobin).await?;

        Ok(())
    }

    async fn test_reserve_partial_inner(
        task_distribution: TaskDistribution,
    ) -> Result<()> {
        let cluster = test_cluster_context();

        let config = SchedulerConfig::default().with_task_distribution(task_distribution);
        let executor_manager =
            ExecutorManager::new(cluster.cluster_state(), Arc::new(config));

        let executors = test_executors(10, 4);

        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        // Reserve all the slots
        let reservations = executor_manager.reserve_slots(30).await?;

        assert_eq!(reservations.len(), 30);

        // Try to reserve 30 more. Only ten are available though so we should only get 10
        let more_reservations = executor_manager.reserve_slots(30).await?;

        assert_eq!(more_reservations.len(), 10);

        // Now cancel them
        executor_manager.cancel_reservations(reservations).await?;
        executor_manager
            .cancel_reservations(more_reservations)
            .await?;

        // Now reserve again
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(reservations.len(), 40);

        let more_reservations = executor_manager.reserve_slots(30).await?;

        assert_eq!(more_reservations.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_concurrent() -> Result<()> {
        test_reserve_concurrent_inner(TaskDistribution::Bias).await?;
        test_reserve_concurrent_inner(TaskDistribution::RoundRobin).await?;

        Ok(())
    }

    async fn test_reserve_concurrent_inner(
        task_distribution: TaskDistribution,
    ) -> Result<()> {
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<Result<Vec<ExecutorReservation>>>(1000);

        let executors = test_executors(10, 4);

        let config = SchedulerConfig::default().with_task_distribution(task_distribution);
        let cluster = test_cluster_context();
        let executor_manager =
            ExecutorManager::new(cluster.cluster_state(), Arc::new(config));

        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        {
            let sender = sender;
            // Spawn 20 async tasks to each try and reserve all 40 slots
            for _ in 0..20 {
                let executor_manager = executor_manager.clone();
                let sender = sender.clone();
                tokio::task::spawn(async move {
                    let reservations = executor_manager.reserve_slots(40).await;
                    sender.send(reservations).await.unwrap();
                });
            }
        }

        let mut total_reservations: Vec<ExecutorReservation> = vec![];

        while let Some(Ok(reservations)) = receiver.recv().await {
            total_reservations.extend(reservations);
        }

        // The total number of reservations should never exceed the number of slots
        assert_eq!(total_reservations.len(), 40);

        Ok(())
    }

    #[tokio::test]
    async fn test_register_reserve() -> Result<()> {
        test_register_reserve_inner(TaskDistribution::Bias).await?;
        test_register_reserve_inner(TaskDistribution::RoundRobin).await?;

        Ok(())
    }

    async fn test_register_reserve_inner(
        task_distribution: TaskDistribution,
    ) -> Result<()> {
        let cluster = test_cluster_context();

        let config = SchedulerConfig::default().with_task_distribution(task_distribution);
        let executor_manager =
            ExecutorManager::new(cluster.cluster_state(), Arc::new(config));

        let executors = test_executors(10, 4);

        for (executor_metadata, executor_data) in executors {
            let reservations = executor_manager
                .register_executor(executor_metadata, executor_data, true)
                .await?;

            assert_eq!(reservations.len(), 4);
        }

        // All slots should be reserved
        let reservations = executor_manager.reserve_slots(1).await?;

        assert_eq!(reservations.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_ignore_fenced_executors() -> Result<()> {
        test_ignore_fenced_executors_inner(TaskDistribution::Bias).await?;
        test_ignore_fenced_executors_inner(TaskDistribution::RoundRobin).await?;

        Ok(())
    }

    async fn test_ignore_fenced_executors_inner(
        task_distribution: TaskDistribution,
    ) -> Result<()> {
        let cluster = test_cluster_context();

        let config = SchedulerConfig::default().with_task_distribution(task_distribution);
        let executor_manager =
            ExecutorManager::new(cluster.cluster_state(), Arc::new(config));

        // Setup two executors initially
        let executors = test_executors(2, 4);

        for (executor_metadata, executor_data) in executors {
            let _ = executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        // Fence one of the executors
        executor_manager
            .save_executor_heartbeat(ExecutorHeartbeat {
                executor_id: "executor-0".to_string(),
                timestamp: timestamp_secs(),
                metrics: vec![],
                status: Some(ExecutorStatus {
                    status: Some(Status::Terminating(String::default())),
                }),
            })
            .await?;

        let reservations = executor_manager.reserve_slots(8).await?;

        assert_eq!(reservations.len(), 4, "Expected only four reservations");

        assert!(
            reservations
                .iter()
                .all(|res| res.executor_id == "executor-1"),
            "Expected all reservations from non-fenced executor",
        );

        Ok(())
    }

    fn test_executors(
        total_executors: usize,
        slots_per_executor: u32,
    ) -> Vec<(ExecutorMetadata, ExecutorData)> {
        let mut result: Vec<(ExecutorMetadata, ExecutorData)> = vec![];

        for i in 0..total_executors {
            result.push((
                ExecutorMetadata {
                    id: format!("executor-{i}"),
                    host: format!("host-{i}"),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification {
                        task_slots: slots_per_executor,
                    },
                },
                ExecutorData {
                    executor_id: format!("executor-{i}"),
                    total_task_slots: slots_per_executor,
                    available_task_slots: slots_per_executor,
                },
            ));
        }

        result
    }
}
