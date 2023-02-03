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

use crate::cluster::{
    reserve_slots_bias, reserve_slots_round_robin, ClusterState, ExecutorHeartbeatStream,
    JobState, JobStateEvent, JobStateEventStream, JobStatus, TaskDistribution,
};
use crate::state::execution_graph::ExecutionGraph;
use crate::state::executor_manager::ExecutorReservation;
use async_trait::async_trait;
use ballista_core::config::BallistaConfig;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::{ExecutorHeartbeat, ExecutorTaskSlots};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use dashmap::DashMap;
use datafusion::prelude::SessionContext;

use crate::cluster::event::ClusterEventSender;
use crate::scheduler_server::SessionBuilder;
use crate::state::session_manager::{
    create_datafusion_context, update_datafusion_context,
};
use ballista_core::serde::protobuf::job_status::Status;
use itertools::Itertools;
use log::warn;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct InMemoryClusterState {
    /// Current available task slots for each executor
    task_slots: Mutex<ExecutorTaskSlots>,
    /// Current executors
    executors: DashMap<String, (ExecutorMetadata, ExecutorData)>,
    /// Last heartbeat received for each executor
    heartbeats: DashMap<String, ExecutorHeartbeat>,
    /// Broadcast channel sender for heartbeats, If `None` there are not
    /// subscribers
    heartbeat_sender: ClusterEventSender<ExecutorHeartbeat>,
}

#[async_trait]
impl ClusterState for InMemoryClusterState {
    async fn reserve_slots(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<ExecutorReservation>> {
        let mut guard = self.task_slots.lock();

        let slots_iter = guard.task_slots.iter_mut().filter(|slots| {
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

        Ok(reservations)
    }

    async fn reserve_slots_exact(
        &self,
        num_slots: u32,
        distribution: TaskDistribution,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<ExecutorReservation>> {
        let mut guard = self.task_slots.lock();

        let rollback = guard.clone();

        let slots_iter = guard.task_slots.iter_mut().filter(|slots| {
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

        if reservations.len() as u32 != num_slots {
            *guard = rollback;
            Ok(vec![])
        } else {
            Ok(reservations)
        }
    }

    async fn cancel_reservations(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<()> {
        let mut increments = HashMap::new();
        for ExecutorReservation { executor_id, .. } in reservations {
            if let Some(inc) = increments.get_mut(&executor_id) {
                *inc += 1;
            } else {
                increments.insert(executor_id, 1usize);
            }
        }

        let mut guard = self.task_slots.lock();

        for executor_slots in guard.task_slots.iter_mut() {
            if let Some(slots) = increments.get(&executor_slots.executor_id) {
                executor_slots.slots += *slots as u32;
            }
        }

        Ok(())
    }

    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        mut spec: ExecutorData,
        reserve: bool,
    ) -> Result<Vec<ExecutorReservation>> {
        if reserve {
            let slots = std::mem::take(&mut spec.available_task_slots) as usize;

            let reservations = (0..slots)
                .into_iter()
                .map(|_| ExecutorReservation::new_free(metadata.id.clone()))
                .collect();

            self.executors.insert(metadata.id.clone(), (metadata, spec));

            Ok(reservations)
        } else {
            self.executors.insert(metadata.id.clone(), (metadata, spec));

            Ok(vec![])
        }
    }

    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        todo!()
    }

    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata> {
        todo!()
    }

    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()> {
        if let Some(mut last) = self.heartbeats.get_mut(&heartbeat.executor_id) {
            let _ = std::mem::replace(last.deref_mut(), heartbeat.clone());
        } else {
            self.heartbeats
                .insert(heartbeat.executor_id.clone(), heartbeat.clone());
        }

        self.heartbeat_sender.send(&heartbeat);

        Ok(())
    }

    async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        {
            let mut guard = self.task_slots.lock();

            if let Some((idx, _)) = guard
                .task_slots
                .iter()
                .find_position(|slots| slots.executor_id == executor_id)
            {
                guard.task_slots.swap_remove(idx);
            }
        }

        self.executors.remove(executor_id);
        self.heartbeats.remove(executor_id);

        Ok(())
    }

    async fn executor_heartbeat_stream(&self) -> Result<ExecutorHeartbeatStream> {
        Ok(Box::pin(self.heartbeat_sender.subscribe()))
    }

    async fn executor_heartbeats(&self) -> Result<HashMap<String, ExecutorHeartbeat>> {
        Ok(self
            .heartbeats
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect())
    }
}

/// Implementation of `JobState` which keeps all state in memory. If using `InMemoryJobState`
/// no job state will be shared between schedulers
pub struct InMemoryJobState {
    scheduler: String,
    /// Jobs which have either completed successfully or failed
    completed_jobs: DashMap<String, (JobStatus, Option<ExecutionGraph>)>,
    /// In-memory store of queued jobs. Map from Job ID -> (Job Name, queued_at timestamp)
    queued_jobs: DashMap<String, (String, u64)>,
    /// Active ballista sessions
    sessions: DashMap<String, Arc<SessionContext>>,
    /// `SessionBuilder` for building DataFusion `SessionContext` from `BallistaConfig`
    session_builder: SessionBuilder,
    /// Sender of job events
    job_event_sender: ClusterEventSender<JobStateEvent>,
}

impl InMemoryJobState {
    pub fn new(scheduler: impl Into<String>, session_builder: SessionBuilder) -> Self {
        Self {
            scheduler: scheduler.into(),
            completed_jobs: Default::default(),
            queued_jobs: Default::default(),
            sessions: Default::default(),
            session_builder,
            job_event_sender: ClusterEventSender::new(100),
        }
    }
}

#[async_trait]
impl JobState for InMemoryJobState {

    async fn submit_job(&self, job_id: String, _graph: &ExecutionGraph) -> Result<()> {
        self.job_event_sender.send(&JobStateEvent::JobAcquired {
            job_id,
            owner: self.scheduler.clone(),
        });

        Ok(())
    }

    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        Ok(self
            .completed_jobs
            .get(job_id)
            .map(|pair| pair.value().0.clone()))
    }

    async fn get_execution_graph(&self, job_id: &str) -> Result<Option<ExecutionGraph>> {
        Ok(self
            .completed_jobs
            .get(job_id)
            .and_then(|pair| pair.value().1.clone()))
    }

    async fn try_acquire_job(&self, _job_id: &str) -> Result<Option<ExecutionGraph>> {
        // Always return None. The only state stored here are for completed jobs
        // which cannot be acquired
        Ok(None)
    }

    async fn save_job(&self, job_id: &str, graph: &ExecutionGraph) -> Result<()> {
        let status = graph.status();

        // If job is either successful or failed, save to completed jobs
        if matches!(
            status.status,
            Some(Status::Successful(_)) | Some(Status::Failed(_))
        ) {
            self.completed_jobs
                .insert(job_id.to_string(), (status, Some(graph.clone())));
        }

        Ok(())
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        self.sessions
            .get(session_id)
            .map(|sess| sess.clone())
            .ok_or_else(|| {
                BallistaError::General(format!("No session for {} found", session_id))
            })
    }

    async fn create_session(
        &self,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        let session = create_datafusion_context(config, self.session_builder);
        self.sessions.insert(session.session_id(), session.clone());

        Ok(session)
    }

    async fn update_session(
        &self,
        session_id: &str,
        config: &BallistaConfig,
    ) -> Result<Arc<SessionContext>> {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            *session = update_datafusion_context(session.clone(), config);
            Ok(session.clone())
        } else {
            let session = create_datafusion_context(config, self.session_builder);
            self.sessions
                .insert(session_id.to_string(), session.clone());

            Ok(session)
        }
    }

    async fn job_state_events(&self) -> JobStateEventStream {
        Box::pin(self.job_event_sender.subscribe())
    }

    async fn remove_job(&self, job_id: &str) -> Result<()> {
        if self.completed_jobs.remove(job_id).is_none() {
            warn!("Tried to delete non-existent job {job_id} from state");
        }
        Ok(())
    }

    async fn get_jobs(&self) -> Result<HashSet<String>> {
        Ok(self
            .completed_jobs
            .iter()
            .map(|pair| pair.key().clone())
            .collect())
    }

    async fn accept_job(&self, job_id: &str, job_name: &str, queued_at: u64) -> Result<()> {
        todo!()
    }
}
