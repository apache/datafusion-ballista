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

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::state::backend::{Keyspace, StateBackendClient};

use crate::state::{decode_into, encode_protobuf};
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf;

use ballista_core::serde::protobuf::executor_grpc_client::ExecutorGrpcClient;
use ballista_core::serde::protobuf::ExecutorHeartbeat;
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorDataChange, ExecutorMetadata,
};
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
pub(crate) struct ExecutorManager {
    state: Arc<dyn StateBackendClient>,
    executor_metadata: Arc<RwLock<HashMap<String, ExecutorMetadata>>>,
    executors_heartbeat: Arc<RwLock<HashMap<String, ExecutorHeartbeat>>>,
    executors_data: Arc<RwLock<HashMap<String, ExecutorData>>>,
}

impl ExecutorManager {
    pub(crate) fn new(state: Arc<dyn StateBackendClient>) -> Self {
        Self {
            state,
            executor_metadata: Arc::new(RwLock::new(HashMap::new())),
            executors_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            executors_data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn init(&self) -> Result<()> {
        todo!()
    }

    /// Reserve up to n executor task slots. Once reserved these slots will not be available
    /// for scheduling.
    /// This operation is atomic, so if this method return an Err, no slots have been reserved.
    pub async fn reserve_slots(&self, n: u32) -> Result<Vec<ExecutorReservation>> {
        debug!("Attempting to reserve {} executor slots", n);
        let start = Instant::now();
        let _lock = self.state.lock(Keyspace::Slots, "global").await?;
        let mut reservations: Vec<ExecutorReservation> = vec![];
        let mut desired: u32 = n;

        let executor_slots = self.state.scan(Keyspace::Slots, None).await?;

        let mut txn_ops: Vec<(Keyspace, String, Vec<u8>)> = vec![];

        for (key, value) in executor_slots {
            let mut data = decode_into::<protobuf::ExecutorData, ExecutorData>(&value)?;
            let executor_id = data.executor_id.clone();
            let take = std::cmp::min(data.available_task_slots, desired);

            for _ in 0..take {
                reservations.push(ExecutorReservation::new_free(executor_id.clone()));
                data.available_task_slots -= 1;
                desired -= 1;
            }

            let proto: protobuf::ExecutorData = data.into();
            let new_data = encode_protobuf(&proto)?;
            txn_ops.push((Keyspace::Slots, executor_id, new_data));

            if desired <= 0 {
                break;
            }
        }

        self.state.put_txn(txn_ops).await?;

        let elapsed = start.elapsed();
        info!(
            "Reserved {} executor slots in {:?}",
            reservations.len(),
            elapsed
        );

        Ok(reservations)
    }

    /// Returned reserved task slots to the pool of available slots. This operation is atomic
    /// so either the entire pool of reserved task slots it returned or none are.
    pub async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        let num_reservations = reservations.len();
        debug!("Cancelling {} reservations", num_reservations);
        let start = Instant::now();

        let _lock = self.state.lock(Keyspace::Slots, "global").await?;

        let mut executor_slots: HashMap<String, ExecutorData> = HashMap::new();

        for reservation in reservations {
            let executor_id = &reservation.executor_id;
            if let Some(data) = executor_slots.get_mut(executor_id) {
                data.available_task_slots += 1;
            } else {
                let value = self.state.get(Keyspace::Slots, executor_id).await?;
                let mut data =
                    decode_into::<protobuf::ExecutorData, ExecutorData>(&value)?;
                data.available_task_slots += 1;
                executor_slots.insert(executor_id.clone(), data);
            }
        }

        let txn_ops: Vec<(Keyspace, String, Vec<u8>)> = executor_slots
            .into_iter()
            .map(|(executor_id, data)| {
                let proto: protobuf::ExecutorData = data.into();
                let new_data = encode_protobuf(&proto)?;
                Ok((Keyspace::Slots, executor_id, new_data))
            })
            .collect::<Result<Vec<_>>>()?;

        self.state.put_txn(txn_ops).await?;

        let elapsed = start.elapsed();
        info!(
            "Cancelled {} reservations in {:?}",
            num_reservations, elapsed
        );

        Ok(())
    }

    pub async fn get_executor_state(&self) -> Result<Vec<(ExecutorMetadata, Duration)>> {
        todo!()
    }

    pub async fn get_executor_metadata(
        &self,
        executor_id: &str,
    ) -> Result<ExecutorMetadata> {
        {
            let metadata_cache = self.executor_metadata.read();
            if let Some(cached) = metadata_cache.get(executor_id) {
                return Ok(cached.clone());
            }
        }

        let value = self.state.get(Keyspace::Executors, executor_id).await?;

        let decoded =
            decode_into::<protobuf::ExecutorMetadata, ExecutorMetadata>(&value)?;
        Ok(decoded)
    }

    pub async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        let executor_id = metadata.id.clone();
        let proto: protobuf::ExecutorMetadata = metadata.into();
        let value = encode_protobuf(&proto)?;

        self.state
            .put(Keyspace::Executors, executor_id, value)
            .await
    }

    /// Register the executor with the scheduler. This will save the executor metadata and the
    /// executor data to persistent state.
    ///
    /// If `reserve` is true, then any available task slots will be reserved and dispatched for scheduling.
    /// If `reserve` is false, then the executor data will be saved as is.
    ///
    /// In general, reserve should be true is the scheduler is using push-based scheduling and false
    /// if the scheduler is using poll-based scheduling.
    pub async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        specification: ExecutorData,
        reserve: bool,
    ) -> Result<Vec<ExecutorReservation>> {
        self.test_scheduler_connectivity(&metadata).await?;

        let executor_id = metadata.id.clone();

        //TODO this should be in a transaction
        // Now that we know we can connect, save the metadata and slots
        self.save_executor_metadata(metadata).await?;

        if !reserve {
            let proto: protobuf::ExecutorData = specification.into();
            let value = encode_protobuf(&proto)?;
            self.state.put(Keyspace::Slots, executor_id, value).await?;
            Ok(vec![])
        } else {
            let mut specification = specification;
            let num_slots = specification.available_task_slots as usize;
            let mut reservations: Vec<ExecutorReservation> = vec![];
            for _ in 0..num_slots {
                reservations.push(ExecutorReservation::new_free(executor_id.clone()));
            }

            specification.available_task_slots = 0;
            let proto: protobuf::ExecutorData = specification.into();
            let value = encode_protobuf(&proto)?;
            self.state.put(Keyspace::Slots, executor_id, value).await?;
            Ok(vec![])
        }
    }

    #[cfg(not(test))]
    async fn test_scheduler_connectivity(
        &self,
        metadata: &ExecutorMetadata,
    ) -> Result<()> {
        let executor_url = format!("http://{}:{}", metadata.host, metadata.grpc_port);
        debug!("Connecting to executor {:?}", executor_url);
        let _ = ExecutorGrpcClient::connect(executor_url)
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
        let executor_id = heartbeat.executor_id.clone();
        let value = encode_protobuf(&heartbeat)?;
        self.state
            .put(Keyspace::Heartbeats, executor_id, value)
            .await?;

        let mut executors_heartbeat = self.executors_heartbeat.write();
        executors_heartbeat.insert(heartbeat.executor_id.clone(), heartbeat);

        Ok(())
    }

    pub(crate) fn get_executors_heartbeat(&self) -> Vec<ExecutorHeartbeat> {
        let executors_heartbeat = self.executors_heartbeat.read();
        executors_heartbeat
            .iter()
            .map(|(_exec, heartbeat)| heartbeat.clone())
            .collect()
    }

    /// last_seen_ts_threshold is in seconds
    pub(crate) fn get_alive_executors(
        &self,
        last_seen_ts_threshold: u64,
    ) -> HashSet<String> {
        let executors_heartbeat = self.executors_heartbeat.read();
        executors_heartbeat
            .iter()
            .filter_map(|(exec, heartbeat)| {
                (heartbeat.timestamp > last_seen_ts_threshold).then(|| exec.clone())
            })
            .collect()
    }

    #[allow(dead_code)]
    fn get_alive_executors_within_one_minute(&self) -> HashSet<String> {
        let now_epoch_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let last_seen_threshold = now_epoch_ts
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(|| Duration::from_secs(0));
        self.get_alive_executors(last_seen_threshold.as_secs())
    }

    pub(crate) fn save_executor_data(&self, executor_data: ExecutorData) {
        let mut executors_data = self.executors_data.write();
        executors_data.insert(executor_data.executor_id.clone(), executor_data);
    }

    pub(crate) fn update_executor_data(&self, executor_data_change: &ExecutorDataChange) {
        let mut executors_data = self.executors_data.write();
        if let Some(executor_data) =
            executors_data.get_mut(&executor_data_change.executor_id)
        {
            let available_task_slots = executor_data.available_task_slots as i32
                + executor_data_change.task_slots;
            if available_task_slots < 0 {
                error!(
                    "Available task slots {} for executor {} is less than 0",
                    available_task_slots, executor_data.executor_id
                );
            } else {
                info!(
                    "available_task_slots for executor {} becomes {}",
                    executor_data.executor_id, available_task_slots
                );
                executor_data.available_task_slots = available_task_slots as u32;
            }
        } else {
            warn!(
                "Could not find executor data for {}",
                executor_data_change.executor_id
            );
        }
    }

    pub(crate) fn get_executor_data(&self, executor_id: &str) -> Option<ExecutorData> {
        let executors_data = self.executors_data.read();
        executors_data.get(executor_id).cloned()
    }

    /// There are two checks:
    /// 1. firstly alive
    /// 2. secondly available task slots > 0
    #[cfg(not(test))]
    #[allow(dead_code)]
    pub(crate) fn get_available_executors_data(&self) -> Vec<ExecutorData> {
        let mut res = {
            let alive_executors = self.get_alive_executors_within_one_minute();
            let executors_data = self.executors_data.read();
            executors_data
                .iter()
                .filter_map(|(exec, data)| {
                    (data.available_task_slots > 0 && alive_executors.contains(exec))
                        .then(|| data.clone())
                })
                .collect::<Vec<ExecutorData>>()
        };
        res.sort_by(|a, b| Ord::cmp(&b.available_task_slots, &a.available_task_slots));
        res
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn get_available_executors_data(&self) -> Vec<ExecutorData> {
        let mut res: Vec<ExecutorData> =
            self.executors_data.read().values().cloned().collect();
        res.sort_by(|a, b| Ord::cmp(&b.available_task_slots, &a.available_task_slots));
        res
    }
}

#[cfg(test)]
mod test {
    use crate::state::backend::standalone::StandaloneClient;
    use crate::state::executor_manager::{ExecutorManager, ExecutorReservation};
    use ballista_core::error::Result;
    use ballista_core::serde::scheduler::{
        ExecutorData, ExecutorMetadata, ExecutorSpecification,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_reserve_and_cancel() -> Result<()> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);

        let executor_manager = ExecutorManager::new(state_storage);

        let executors = test_executors(10, 4);

        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        // Reserve all the slots
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(reservations.len(), 40);

        // Now cancel them
        executor_manager.cancel_reservations(reservations).await?;

        // Now reserve again
        let reservations = executor_manager.reserve_slots(40).await?;

        assert_eq!(reservations.len(), 40);

        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_partial() -> Result<()> {
        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);

        let executor_manager = ExecutorManager::new(state_storage);

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
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Result<Vec<ExecutorReservation>>>(1000);

        let executors = test_executors(10, 4);

        let state_storage = Arc::new(StandaloneClient::try_new_temporary()?);

        let executor_manager = ExecutorManager::new(state_storage);


        for (executor_metadata, executor_data) in executors {
            executor_manager
                .register_executor(executor_metadata, executor_data, false)
                .await?;
        }

        {
            let sender = sender;
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

        assert_eq!(total_reservations.len(), 40);

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
                    id: format!("executor-{}", i),
                    host: format!("host-{}", i),
                    port: 8080,
                    grpc_port: 9090,
                    specification: ExecutorSpecification {
                        task_slots: slots_per_executor,
                    },
                },
                ExecutorData {
                    executor_id: format!("executor-{}", i),
                    total_task_slots: slots_per_executor,
                    available_task_slots: slots_per_executor,
                },
            ));
        }

        result
    }
}
