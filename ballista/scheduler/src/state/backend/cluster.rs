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

use crate::state::backend::{
    Keyspace, Operation, StateBackendClient, TaskDistribution, WatchEvent,
};
use crate::state::executor_manager::ExecutorReservation;
use crate::state::{decode_into, decode_protobuf, encode_protobuf, with_lock};
use ballista_core::error;
use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::{
    executor_status, ExecutorHeartbeat, ExecutorStatus,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use futures::{Stream, StreamExt};
use log::{debug, info};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub type ExecutorHeartbeatStream = Pin<Box<dyn Stream<Item = ExecutorHeartbeat> + Send>>;

/// A trait that contains the necessary method to maintain a globally consistent view of cluster resources
#[tonic::async_trait]
pub trait ClusterState: Send + Sync {
    /// Reserve up to `num_slots` executor task slots. If not enough task slots are available, reserve
    /// as many as possible.
    ///
    /// If `executors` is provided, only reserve slots of the specified executor IDs
    async fn reserve_slots(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> error::Result<Vec<ExecutorReservation>>;

    /// Reserve exactly `num_slots` executor task slots. If not enough task slots are available,
    /// returns an empty vec
    ///
    /// If `executors` is provided, only reserve slots of the specified executor IDs
    async fn reserve_slots_exact(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> error::Result<Vec<ExecutorReservation>>;

    /// Cancel the specified reservations. This will make reserved executor slots available to other
    /// tasks.
    /// This operations should be atomic. Either all reservations are cancelled or none are
    async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> error::Result<()>;

    /// Register a new executor in the cluster. If `reserve` is true, then the executors task slots
    /// will be reserved and returned in the response and none of the new executors task slots will be
    /// available to other tasks.
    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
        reserve: bool,
    ) -> error::Result<Vec<ExecutorReservation>>;

    /// Save the executor metadata. This will overwrite existing metadata for the executor ID
    async fn save_executor_metadata(
        &self,
        metadata: ExecutorMetadata,
    ) -> error::Result<()>;

    /// Get executor metadata for the provided executor ID. Returns an error if the executor does not exist
    async fn get_executor_metadata(
        &self,
        executor_id: &str,
    ) -> error::Result<ExecutorMetadata>;

    /// Save the executor heartbeat
    async fn save_executor_heartbeat(
        &self,
        heartbeat: ExecutorHeartbeat,
    ) -> error::Result<()>;

    /// Remove the executor from the cluster
    async fn remove_executor(&self, executor_id: &str) -> error::Result<()>;

    /// Return the stream of executor heartbeats observed by all schedulers in the cluster.
    /// This can be aggregated to provide an eventually consistent view of all executors within the cluster
    async fn executor_heartbeat_stream(&self) -> error::Result<ExecutorHeartbeatStream>;

    /// Return a map of the last seen heartbeat for all active executors
    async fn executor_heartbeats(
        &self,
    ) -> error::Result<HashMap<String, ExecutorHeartbeat>>;
}

/// Default implementation of `ClusterState` that can use the key-value interface defined in
/// `StateBackendClient
pub struct DefaultClusterState {
    kv_store: Arc<dyn StateBackendClient>,
}

impl DefaultClusterState {
    pub fn new(kv_store: Arc<dyn StateBackendClient>) -> Self {
        Self { kv_store }
    }
}

#[tonic::async_trait]
impl ClusterState for DefaultClusterState {
    async fn reserve_slots(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> error::Result<Vec<ExecutorReservation>> {
        let lock = self.kv_store.lock(Keyspace::Slots, "global").await?;

        with_lock(lock, async {
            debug!("Attempting to reserve {} executor slots", num_slots);
            let start = Instant::now();

            let executors = match executors {
                Some(executors) => executors,
                None => {
                    let heartbeats = self.executor_heartbeats().await?;

                    get_alive_executors(60, heartbeats)?
                }
            };

            let (reservations, txn_ops) = match distribution {
                TaskDistribution::Bias => {
                    reserve_slots_bias(self.kv_store.as_ref(), num_slots, executors)
                        .await?
                }
                TaskDistribution::RoundRobin => {
                    reserve_slots_round_robin(
                        self.kv_store.as_ref(),
                        num_slots,
                        executors,
                    )
                    .await?
                }
            };

            self.kv_store.apply_txn(txn_ops).await?;

            let elapsed = start.elapsed();
            info!(
                "Reserved {} executor slots in {:?}",
                reservations.len(),
                elapsed
            );

            Ok(reservations)
        })
        .await
    }

    async fn reserve_slots_exact(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> error::Result<Vec<ExecutorReservation>> {
        let lock = self.kv_store.lock(Keyspace::Slots, "global").await?;

        with_lock(lock, async {
            debug!("Attempting to reserve {} executor slots", num_slots);
            let start = Instant::now();

            let executors = match executors {
                Some(executors) => executors,
                None => {
                    let heartbeats = self.executor_heartbeats().await?;

                    get_alive_executors(60, heartbeats)?
                }
            };

            let (reservations, txn_ops) = match distribution {
                TaskDistribution::Bias => {
                    reserve_slots_bias(self.kv_store.as_ref(), num_slots, executors)
                        .await?
                }
                TaskDistribution::RoundRobin => {
                    reserve_slots_round_robin(
                        self.kv_store.as_ref(),
                        num_slots,
                        executors,
                    )
                    .await?
                }
            };

            let elapsed = start.elapsed();
            if reservations.len() as u32 == num_slots {
                self.kv_store.apply_txn(txn_ops).await?;

                info!(
                    "Reserved {} executor slots in {:?}",
                    reservations.len(),
                    elapsed
                );

                Ok(reservations)
            } else {
                info!(
                    "Failed to reserve exactly {} executor slots in {:?}",
                    reservations.len(),
                    elapsed
                );

                Ok(vec![])
            }
        })
        .await
    }

    async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> error::Result<()> {
        let lock = self.kv_store.lock(Keyspace::Slots, "global").await?;

        with_lock(lock, async {
            let num_reservations = reservations.len();
            debug!("Cancelling {} reservations", num_reservations);
            let start = Instant::now();

            let mut executor_slots: HashMap<String, ExecutorData> = HashMap::new();

            for reservation in reservations {
                let executor_id = &reservation.executor_id;
                if let Some(data) = executor_slots.get_mut(executor_id) {
                    data.available_task_slots += 1;
                } else {
                    let value = self.kv_store.get(Keyspace::Slots, executor_id).await?;
                    let mut data =
                        decode_into::<protobuf::ExecutorData, ExecutorData>(&value)?;
                    data.available_task_slots += 1;
                    executor_slots.insert(executor_id.clone(), data);
                }
            }

            let txn_ops: Vec<(Operation, Keyspace, String)> = executor_slots
                .into_iter()
                .map(|(executor_id, data)| {
                    let proto: protobuf::ExecutorData = data.into();
                    let new_data = encode_protobuf(&proto)?;
                    Ok((Operation::Put(new_data), Keyspace::Slots, executor_id))
                })
                .collect::<error::Result<Vec<_>>>()?;

            self.kv_store.apply_txn(txn_ops).await?;

            let elapsed = start.elapsed();
            info!(
                "Cancelled {} reservations in {:?}",
                num_reservations, elapsed
            );

            Ok(())
        })
        .await
    }

    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
        reserve: bool,
    ) -> error::Result<Vec<ExecutorReservation>> {
        let executor_id = metadata.id.clone();

        let current_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                BallistaError::Internal(format!("Error getting current timestamp: {e:?}"))
            })?
            .as_secs();

        //TODO this should be in a transaction
        // Now that we know we can connect, save the metadata and slots
        self.save_executor_metadata(metadata).await?;
        self.save_executor_heartbeat(ExecutorHeartbeat {
            executor_id: executor_id.clone(),
            timestamp: current_ts,
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Active("".to_string())),
            }),
        })
        .await?;

        if !reserve {
            let proto: protobuf::ExecutorData = spec.into();
            let value = encode_protobuf(&proto)?;
            self.kv_store
                .put(Keyspace::Slots, executor_id, value)
                .await?;
            Ok(vec![])
        } else {
            let mut specification = spec;
            let num_slots = specification.available_task_slots as usize;
            let mut reservations: Vec<ExecutorReservation> = vec![];
            for _ in 0..num_slots {
                reservations.push(ExecutorReservation::new_free(executor_id.clone()));
            }

            specification.available_task_slots = 0;

            let proto: protobuf::ExecutorData = specification.into();
            let value = encode_protobuf(&proto)?;
            self.kv_store
                .put(Keyspace::Slots, executor_id, value)
                .await?;
            Ok(reservations)
        }
    }

    async fn save_executor_metadata(
        &self,
        metadata: ExecutorMetadata,
    ) -> error::Result<()> {
        let executor_id = metadata.id.clone();
        let proto: protobuf::ExecutorMetadata = metadata.into();
        let value = encode_protobuf(&proto)?;

        self.kv_store
            .put(Keyspace::Executors, executor_id, value)
            .await
    }

    async fn get_executor_metadata(
        &self,
        executor_id: &str,
    ) -> error::Result<ExecutorMetadata> {
        let value = self.kv_store.get(Keyspace::Executors, executor_id).await?;

        let decoded =
            decode_into::<protobuf::ExecutorMetadata, ExecutorMetadata>(&value)?;
        Ok(decoded)
    }

    async fn save_executor_heartbeat(
        &self,
        heartbeat: ExecutorHeartbeat,
    ) -> error::Result<()> {
        let executor_id = heartbeat.executor_id.clone();
        let value = encode_protobuf(&heartbeat)?;
        self.kv_store
            .put(Keyspace::Heartbeats, executor_id, value)
            .await
    }

    async fn remove_executor(&self, executor_id: &str) -> error::Result<()> {
        let current_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                BallistaError::Internal(format!("Error getting current timestamp: {e:?}"))
            })?
            .as_secs();

        let value = encode_protobuf(&ExecutorHeartbeat {
            executor_id: executor_id.to_owned(),
            timestamp: current_ts,
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Dead("".to_string())),
            }),
        })?;
        self.kv_store
            .put(Keyspace::Heartbeats, executor_id.to_owned(), value)
            .await?;

        // TODO Check the Executor reservation logic for push-based scheduling

        Ok(())
    }

    async fn executor_heartbeat_stream(&self) -> error::Result<ExecutorHeartbeatStream> {
        let events = self
            .kv_store
            .watch(Keyspace::Heartbeats, String::default())
            .await?;

        Ok(events
            .filter_map(|event| {
                futures::future::ready(match event {
                    WatchEvent::Put(_, value) => {
                        if let Ok(heartbeat) =
                            decode_protobuf::<ExecutorHeartbeat>(&value)
                        {
                            Some(heartbeat)
                        } else {
                            None
                        }
                    }
                    WatchEvent::Delete(_) => None,
                })
            })
            .boxed())
    }

    async fn executor_heartbeats(
        &self,
    ) -> error::Result<HashMap<String, ExecutorHeartbeat>> {
        let heartbeats = self.kv_store.scan(Keyspace::Heartbeats, None).await?;

        let mut heartbeat_map = HashMap::with_capacity(heartbeats.len());

        for (_, value) in heartbeats {
            let data: ExecutorHeartbeat = decode_protobuf(&value)?;
            if let Some(ExecutorStatus {
                status: Some(executor_status::Status::Active(_)),
            }) = &data.status
            {
                heartbeat_map.insert(data.executor_id.clone(), data);
            }
        }

        Ok(heartbeat_map)
    }
}

/// Return the set of executor IDs which have heartbeated within `last_seen_threshold` seconds
fn get_alive_executors(
    last_seen_threshold: u64,
    heartbeats: HashMap<String, ExecutorHeartbeat>,
) -> error::Result<HashSet<String>> {
    let now_epoch_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    let last_seen_threshold = now_epoch_ts
        .checked_sub(Duration::from_secs(last_seen_threshold))
        .ok_or_else(|| {
            BallistaError::Internal(format!(
                "Error getting alive executors, invalid last_seen_threshold of {last_seen_threshold}"
            ))
        })?
        .as_secs();

    Ok(heartbeats
        .iter()
        .filter_map(|(exec, heartbeat)| {
            (heartbeat.timestamp > last_seen_threshold).then(|| exec.clone())
        })
        .collect())
}

/// It will get ExecutorReservation from one executor as many as possible.
/// By this way, it can reduce the chance of decoding and encoding ExecutorData.
/// However, it may make the whole cluster unbalanced,
/// which means some executors may be very busy while other executors may be idle.
async fn reserve_slots_bias(
    state: &dyn StateBackendClient,
    mut n: u32,
    executors: HashSet<String>,
) -> error::Result<(Vec<ExecutorReservation>, Vec<(Operation, Keyspace, String)>)> {
    let mut reservations: Vec<ExecutorReservation> = vec![];
    let mut txn_ops: Vec<(Operation, Keyspace, String)> = vec![];

    for executor_id in executors {
        if n == 0 {
            break;
        }

        let value = state.get(Keyspace::Slots, &executor_id).await?;
        let mut data = decode_into::<protobuf::ExecutorData, ExecutorData>(&value)?;
        let take = std::cmp::min(data.available_task_slots, n);

        for _ in 0..take {
            reservations.push(ExecutorReservation::new_free(executor_id.clone()));
            data.available_task_slots -= 1;
            n -= 1;
        }

        let proto: protobuf::ExecutorData = data.into();
        let new_data = encode_protobuf(&proto)?;
        txn_ops.push((Operation::Put(new_data), Keyspace::Slots, executor_id));
    }

    Ok((reservations, txn_ops))
}

/// Create ExecutorReservation in a round robin way to evenly assign tasks to executors
async fn reserve_slots_round_robin(
    state: &dyn StateBackendClient,
    mut n: u32,
    executors: HashSet<String>,
) -> error::Result<(Vec<ExecutorReservation>, Vec<(Operation, Keyspace, String)>)> {
    let mut reservations: Vec<ExecutorReservation> = vec![];
    let mut txn_ops: Vec<(Operation, Keyspace, String)> = vec![];

    let all_executor_data = state
        .scan(Keyspace::Slots, None)
        .await?
        .into_iter()
        .map(|(_, data)| decode_into::<protobuf::ExecutorData, ExecutorData>(&data))
        .collect::<error::Result<Vec<ExecutorData>>>()?;

    let mut available_executor_data: Vec<ExecutorData> = all_executor_data
        .into_iter()
        .filter_map(|data| {
            (data.available_task_slots > 0 && executors.contains(&data.executor_id))
                .then_some(data)
        })
        .collect();
    available_executor_data
        .sort_by(|a, b| Ord::cmp(&b.available_task_slots, &a.available_task_slots));

    // Exclusive
    let mut last_updated_idx = 0usize;
    loop {
        let n_before = n;
        for (idx, data) in available_executor_data.iter_mut().enumerate() {
            if n == 0 {
                break;
            }

            // Since the vector is sorted in descending order,
            // if finding one executor has not enough slots, the following will have not enough, either
            if data.available_task_slots == 0 {
                break;
            }

            reservations.push(ExecutorReservation::new_free(data.executor_id.clone()));
            data.available_task_slots -= 1;
            n -= 1;

            if idx >= last_updated_idx {
                last_updated_idx = idx + 1;
            }
        }

        if n_before == n {
            break;
        }
    }

    for (idx, data) in available_executor_data.into_iter().enumerate() {
        if idx >= last_updated_idx {
            break;
        }
        let executor_id = data.executor_id.clone();
        let proto: protobuf::ExecutorData = data.into();
        let new_data = encode_protobuf(&proto)?;
        txn_ops.push((Operation::Put(new_data), Keyspace::Slots, executor_id));
    }

    Ok((reservations, txn_ops))
}

#[cfg(test)]
mod tests {
    use crate::state::backend::cluster::{ClusterState, DefaultClusterState};
    use crate::state::backend::sled::SledClient;

    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{
        executor_status, ExecutorHeartbeat, ExecutorStatus,
    };

    use futures::StreamExt;

    use std::sync::Arc;

    #[tokio::test]
    async fn test_heartbeat_stream() -> Result<()> {
        let sled = Arc::new(SledClient::try_new_temporary()?);

        let cluster_state: Arc<dyn ClusterState> =
            Arc::new(DefaultClusterState::new(sled));

        for i in 0..10 {
            let mut heartbeat_stream = cluster_state.executor_heartbeat_stream().await?;

            cluster_state
                .save_executor_heartbeat(ExecutorHeartbeat {
                    executor_id: i.to_string(),
                    timestamp: 0,
                    metrics: vec![],
                    status: Some(ExecutorStatus {
                        status: Some(executor_status::Status::Active(String::default())),
                    }),
                })
                .await?;

            let received = if let Some(event) = heartbeat_stream.next().await {
                event.executor_id == i.to_string()
            } else {
                false
            };

            assert!(received, "{}", "Did not receive heartbeat for executor {i}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_heartbeats() -> Result<()> {
        let sled = Arc::new(SledClient::try_new_temporary()?);

        let cluster_state: Arc<dyn ClusterState> =
            Arc::new(DefaultClusterState::new(sled));

        // Add 10 executor heartbeats
        for i in 0..10 {
            cluster_state
                .save_executor_heartbeat(ExecutorHeartbeat {
                    executor_id: i.to_string(),
                    timestamp: i as u64,
                    metrics: vec![],
                    status: Some(ExecutorStatus {
                        status: Some(executor_status::Status::Active(String::default())),
                    }),
                })
                .await?;
        }

        let heartbeats = cluster_state.executor_heartbeats().await?;

        // Check that all 10 are present in the global view
        for i in 0..10 {
            let id = i.to_string();
            if let Some(hb) = heartbeats.get(&id) {
                assert_eq!(
                    hb.executor_id,
                    i.to_string(),
                    "Expected heartbeat in map for {i}"
                );
                assert_eq!(hb.timestamp, i, "Expected timestamp to be correct for {i}");
            } else {
                panic!("Expected heartbeat for executor {}", i);
            }
        }

        // Send new heartbeat with updated timestamp
        cluster_state
            .save_executor_heartbeat(ExecutorHeartbeat {
                executor_id: "0".to_string(),
                timestamp: 100,
                metrics: vec![],
                status: Some(ExecutorStatus {
                    status: Some(executor_status::Status::Active(String::default())),
                }),
            })
            .await?;

        let heartbeats = cluster_state.executor_heartbeats().await?;

        if let Some(hb) = heartbeats.get("0") {
            assert_eq!(hb.executor_id, "0", "Expected heartbeat in map for 0");
            assert_eq!(hb.timestamp, 100, "Expected timestamp to be updated for 0");
        }

        for i in 1..10 {
            let id = i.to_string();
            if let Some(hb) = heartbeats.get(&id) {
                assert_eq!(
                    hb.executor_id,
                    i.to_string(),
                    "Expected heartbeat in map for {i}"
                );
                assert_eq!(hb.timestamp, i, "Expected timestamp to be correct for {i}");
            } else {
                panic!("Expected heartbeat for executor {}", i);
            }
        }

        Ok(())
    }
}
