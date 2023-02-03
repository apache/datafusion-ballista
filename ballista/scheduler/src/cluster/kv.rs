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

use crate::cluster::storage::{KeyValueStore, Keyspace, Lock, Operation, WatchEvent};
use crate::cluster::{
    reserve_slots_bias, reserve_slots_round_robin, ClusterState, ExecutorHeartbeatStream,
    JobState, JobStateEventStream, JobStatus, TaskDistribution,
};
use crate::state::execution_graph::ExecutionGraph;
use crate::state::executor_manager::ExecutorReservation;
use crate::state::{decode_into, decode_protobuf};
use async_trait::async_trait;
use ballista_core::config::BallistaConfig;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::{
    self, AvailableTaskSlots, ExecutorHeartbeat, ExecutorTaskSlots,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use ballista_core::serde::AsExecutionPlan;
use ballista_core::serde::BallistaCodec;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use futures::StreamExt;
use itertools::Itertools;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use dashmap::DashMap;

/// State implementation based on underlying `KeyValueStore`
pub struct KeyValueState<
    S: KeyValueStore,
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    /// Underlying `KeyValueStore`
    store: S,
    /// Codec used to serialize/deserialize execution plan
    codec: BallistaCodec<T, U>,
    /// Name of current scheduler. Should be `{host}:{port}`
    scheduler: String,
    /// In-memory store of queued jobs. Map from Job ID -> (Job Name, queued_at timestamp)
    queued_jobs: DashMap<String, (String, u64)>,
}

impl<S: KeyValueStore, T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    KeyValueState<S, T, U>
{
    pub fn new(
        scheduler: impl Into<String>,
        store: S,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            store,
            scheduler: scheduler.into(),
            codec,
            queued_jobs: DashMap::new(),
        }
    }
}

#[async_trait]
impl<S: KeyValueStore, T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    ClusterState for KeyValueState<S, T, U>
{
    async fn reserve_slots(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<ExecutorReservation>> {
        let lock = self.store.lock(Keyspace::Slots, "global").await?;

        with_lock(lock, async {
            let resources = self.store.get(Keyspace::Slots, "all").await?;

            let mut slots =
                ExecutorTaskSlots::decode(resources.as_slice()).map_err(|err| {
                    BallistaError::Internal(format!(
                        "Unexpected value in executor slots state: {:?}",
                        err
                    ))
                })?;

            let slots_iter = slots.task_slots.iter_mut().filter(|slots| {
                executors
                    .as_ref()
                    .map(|executors| executors.contains(&slots.executor_id))
                    .unwrap_or(true)
            });

            let reservations = match distribution {
                TaskDistribution::Bias => reserve_slots_bias(slots_iter, num_slots),
                TaskDistribution::RoundRobin => {
                    reserve_slots_round_robin(slots_iter, num_slots)
                }
            };

            if !reservations.is_empty() {
                self.store
                    .put(Keyspace::Slots, "all".to_owned(), slots.encode_to_vec())
                    .await?
            }

            Ok(reservations)
        })
        .await
    }

    async fn reserve_slots_exact(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<ExecutorReservation>> {
        let lock = self.store.lock(Keyspace::Slots, "global").await?;

        with_lock(lock, async {
            let resources = self.store.get(Keyspace::Slots, "all").await?;

            let mut slots =
                ExecutorTaskSlots::decode(resources.as_slice()).map_err(|err| {
                    BallistaError::Internal(format!(
                        "Unexpected value in executor slots state: {:?}",
                        err
                    ))
                })?;

            let slots_iter = slots.task_slots.iter_mut().filter(|slots| {
                executors
                    .as_ref()
                    .map(|executors| executors.contains(&slots.executor_id))
                    .unwrap_or(true)
            });

            let reservations = match distribution {
                TaskDistribution::Bias => reserve_slots_bias(slots_iter, num_slots),
                TaskDistribution::RoundRobin => {
                    reserve_slots_round_robin(slots_iter, num_slots)
                }
            };

            if reservations.len() == num_slots as usize {
                self.store
                    .put(Keyspace::Slots, "all".to_owned(), slots.encode_to_vec())
                    .await?;
                Ok(reservations)
            } else {
                Ok(vec![])
            }
        })
        .await
    }

    async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        let lock = self.store.lock(Keyspace::Slots, "global").await?;

        with_lock(lock, async {
            let resources = self.store.get(Keyspace::Slots, "all").await?;

            let mut slots =
                ExecutorTaskSlots::decode(resources.as_slice()).map_err(|err| {
                    BallistaError::Internal(format!(
                        "Unexpected value in executor slots state: {:?}",
                        err
                    ))
                })?;

            let mut increments = HashMap::new();
            for ExecutorReservation { executor_id, .. } in reservations {
                if let Some(inc) = increments.get_mut(&executor_id) {
                    *inc += 1;
                } else {
                    increments.insert(executor_id, 1usize);
                }
            }

            for executor_slots in slots.task_slots.iter_mut() {
                if let Some(slots) = increments.get(&executor_slots.executor_id) {
                    executor_slots.slots += *slots as u32;
                }
            }

            Ok(())
        })
        .await
    }

    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
        reserve: bool,
    ) -> Result<Vec<ExecutorReservation>> {
        let executor_id = metadata.id.clone();

        let current_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Error getting current timestamp: {:?}",
                    e
                ))
            })?
            .as_secs();

        //TODO this should be in a transaction
        // Now that we know we can connect, save the metadata and slots
        self.save_executor_metadata(metadata).await?;
        self.save_executor_heartbeat(ExecutorHeartbeat {
            executor_id: executor_id.clone(),
            timestamp: current_ts,
            metrics: vec![],
            status: Some(protobuf::ExecutorStatus {
                status: Some(protobuf::executor_status::Status::Active("".to_string())),
            }),
        })
        .await?;

        if !reserve {
            let proto: protobuf::ExecutorData = spec.into();
            self.store
                .put(Keyspace::Slots, executor_id, proto.encode_to_vec())
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
            self.store
                .put(Keyspace::Slots, executor_id, proto.encode_to_vec())
                .await?;
            Ok(reservations)
        }
    }

    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        let executor_id = metadata.id.clone();
        let proto: protobuf::ExecutorMetadata = metadata.into();

        self.store
            .put(Keyspace::Executors, executor_id, proto.encode_to_vec())
            .await
    }

    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata> {
        let value = self.store.get(Keyspace::Executors, executor_id).await?;

        let decoded =
            decode_into::<protobuf::ExecutorMetadata, ExecutorMetadata>(&value)?;
        Ok(decoded)
    }

    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()> {
        let executor_id = heartbeat.executor_id.clone();
        self.store
            .put(Keyspace::Heartbeats, executor_id, heartbeat.encode_to_vec())
            .await
    }

    async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        let current_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                BallistaError::Internal(format!(
                    "Error getting current timestamp: {:?}",
                    e
                ))
            })?
            .as_secs();

        let value = ExecutorHeartbeat {
            executor_id: executor_id.to_owned(),
            timestamp: current_ts,
            metrics: vec![],
            status: Some(protobuf::ExecutorStatus {
                status: Some(protobuf::executor_status::Status::Dead("".to_string())),
            }),
        }
        .encode_to_vec();

        self.store
            .put(Keyspace::Heartbeats, executor_id.to_owned(), value)
            .await?;

        // TODO Check the Executor reservation logic for push-based scheduling

        Ok(())
    }

    async fn executor_heartbeat_stream(&self) -> Result<ExecutorHeartbeatStream> {
        let events = self
            .store
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

    async fn executor_heartbeats(&self) -> Result<HashMap<String, ExecutorHeartbeat>> {
        let heartbeats = self.store.scan(Keyspace::Heartbeats, None).await?;

        let mut heartbeat_map = HashMap::with_capacity(heartbeats.len());

        for (_, value) in heartbeats {
            let data: ExecutorHeartbeat = decode_protobuf(&value)?;
            if let Some(protobuf::ExecutorStatus {
                status: Some(protobuf::executor_status::Status::Active(_)),
            }) = &data.status
            {
                heartbeat_map.insert(data.executor_id.clone(), data);
            }
        }

        Ok(heartbeat_map)
    }
}

#[async_trait]
impl<S: KeyValueStore, T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> JobState
    for KeyValueState<S, T, U>
{
    async fn accept_job(&self, job_id: &str, job_name: &str, queued_at: u64) -> Result<()> {
        todo!()
    }

    async fn submit_job(&self, job_id: String, graph: &ExecutionGraph) -> Result<()> {
        let status = graph.status();
        let encoded_graph =
            ExecutionGraph::encode_execution_graph(graph.clone(), &self.codec)?;

        self.store
            .apply_txn(vec![
                (
                    Operation::Put(status.encode_to_vec()),
                    Keyspace::JobStatus,
                    job_id.clone(),
                ),
                (
                    Operation::Put(encoded_graph.encode_to_vec()),
                    Keyspace::ExecutionGraph,
                    job_id.clone(),
                ),
            ])
            .await?;

        Ok(())
    }

    async fn get_jobs(&self) -> Result<HashSet<String>> {
        self.store.scan_keys(Keyspace::JobStatus).await
    }

    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        let value = self.store.get(Keyspace::JobStatus, job_id).await?;

        (!value.is_empty())
            .then(|| decode_protobuf(value.as_slice()))
            .transpose()
    }

    async fn get_execution_graph(&self, job_id: &str) -> Result<Option<ExecutionGraph>> {
        let value = self.store.get(Keyspace::ExecutionGraph, job_id).await?;

        if value.is_empty() {
            return Ok(None);
        }

        let proto: protobuf::ExecutionGraph = decode_protobuf(value.as_slice())?;

        let session = self.get_session(&proto.session_id).await?;

        Ok(Some(
            ExecutionGraph::decode_execution_graph(proto, &self.codec, session.as_ref())
                .await?,
        ))
    }

    async fn save_job(&self, job_id: &str, graph: &ExecutionGraph) -> Result<()> {
        let status = graph.status();
        let encoded_graph =
            ExecutionGraph::encode_execution_graph(graph.clone(), &self.codec)?;

        self.store
            .apply_txn(vec![
                (
                    Operation::Put(status.encode_to_vec()),
                    Keyspace::JobStatus,
                    job_id.to_string(),
                ),
                (
                    Operation::Put(encoded_graph.encode_to_vec()),
                    Keyspace::ExecutionGraph,
                    job_id.to_string(),
                ),
            ])
            .await
    }

    async fn remove_job(&self, job_id: &str) -> Result<()> {
        todo!()
    }

    async fn try_acquire_job(&self, _job_id: &str) -> Result<Option<ExecutionGraph>> {
        Err(BallistaError::NotImplemented(
            "Work stealing is not currently implemented".to_string(),
        ))
    }

    async fn job_state_events(&self) -> JobStateEventStream {
        todo!()
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        todo!()
    }

    async fn create_session(
        &self,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        todo!()
    }

    async fn update_session(
        &self,
        session_id: &str,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        todo!()
    }
}

async fn with_lock<Out, F: Future<Output = Out>>(mut lock: Box<dyn Lock>, op: F) -> Out {
    let result = op.await;
    lock.unlock().await;
    result
}
