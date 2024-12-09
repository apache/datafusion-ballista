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
    bind_task_bias, bind_task_consistent_hash, bind_task_round_robin, get_scan_files,
    is_skip_consistent_hash, BoundTask, ClusterState, ExecutorSlot, JobState,
    JobStateEvent, JobStateEventStream, JobStatus, TaskDistributionPolicy, TopologyNode,
};
use crate::state::execution_graph::ExecutionGraph;
use async_trait::async_trait;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::{
    executor_status, AvailableTaskSlots, ExecutorHeartbeat, ExecutorStatus, FailedJob,
    QueuedJob,
};
use ballista_core::serde::scheduler::{ExecutorData, ExecutorMetadata};
use ballista_core::ConfigProducer;
use dashmap::DashMap;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::cluster::event::ClusterEventSender;
use crate::scheduler_server::{timestamp_millis, timestamp_secs, SessionBuilder};
use crate::state::session_manager::create_datafusion_context;
use crate::state::task_manager::JobInfoCache;
use ballista_core::serde::protobuf::job_status::Status;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;

use ballista_core::consistent_hash::node::Node;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub struct InMemoryClusterState {
    /// Current available task slots for each executor
    task_slots: Mutex<HashMap<String, AvailableTaskSlots>>,
    /// Current executors
    executors: DashMap<String, ExecutorMetadata>,
    /// Last heartbeat received for each executor
    heartbeats: DashMap<String, ExecutorHeartbeat>,
}

impl InMemoryClusterState {
    /// Get the topology nodes of the cluster for consistent hashing
    fn get_topology_nodes(
        &self,
        guard: &MutexGuard<HashMap<String, AvailableTaskSlots>>,
        executors: Option<HashSet<String>>,
    ) -> HashMap<String, TopologyNode> {
        let mut nodes: HashMap<String, TopologyNode> = HashMap::new();
        for (executor_id, slots) in guard.iter() {
            if let Some(executors) = executors.as_ref() {
                if !executors.contains(executor_id) {
                    continue;
                }
            }
            if let Some(executor) = self.executors.get(&slots.executor_id) {
                let node = TopologyNode::new(
                    &executor.host,
                    executor.port,
                    &slots.executor_id,
                    self.heartbeats
                        .get(&executor.id)
                        .map(|heartbeat| heartbeat.timestamp)
                        .unwrap_or(0),
                    slots.slots,
                );
                if let Some(existing_node) = nodes.get(node.name()) {
                    if existing_node.last_seen_ts < node.last_seen_ts {
                        nodes.insert(node.name().to_string(), node);
                    }
                } else {
                    nodes.insert(node.name().to_string(), node);
                }
            }
        }
        nodes
    }
}

#[async_trait]
impl ClusterState for InMemoryClusterState {
    async fn bind_schedulable_tasks(
        &self,
        distribution: TaskDistributionPolicy,
        active_jobs: Arc<HashMap<String, JobInfoCache>>,
        executors: Option<HashSet<String>>,
    ) -> Result<Vec<BoundTask>> {
        let mut guard = self.task_slots.lock().await;

        let available_slots: Vec<&mut AvailableTaskSlots> = guard
            .values_mut()
            .filter_map(|data| {
                (data.slots > 0
                    && executors
                        .as_ref()
                        .map(|executors| executors.contains(&data.executor_id))
                        .unwrap_or(true))
                .then_some(data)
            })
            .collect();

        let bound_tasks = match distribution {
            TaskDistributionPolicy::Bias => {
                bind_task_bias(available_slots, active_jobs, |_| false).await
            }
            TaskDistributionPolicy::RoundRobin => {
                bind_task_round_robin(available_slots, active_jobs, |_| false).await
            }
            TaskDistributionPolicy::ConsistentHash {
                num_replicas,
                tolerance,
            } => {
                let mut bound_tasks = bind_task_round_robin(
                    available_slots,
                    active_jobs.clone(),
                    |stage_plan: Arc<dyn ExecutionPlan>| {
                        if let Ok(scan_files) = get_scan_files(stage_plan) {
                            // Should be opposite to consistent hash ones.
                            !is_skip_consistent_hash(&scan_files)
                        } else {
                            false
                        }
                    },
                )
                .await;
                info!("{} tasks bound by round robin policy", bound_tasks.len());
                let (bound_tasks_consistent_hash, ch_topology) =
                    bind_task_consistent_hash(
                        self.get_topology_nodes(&guard, executors),
                        num_replicas,
                        tolerance,
                        active_jobs,
                        |_, plan| get_scan_files(plan),
                    )
                    .await?;
                info!(
                    "{} tasks bound by consistent hashing policy",
                    bound_tasks_consistent_hash.len()
                );
                if !bound_tasks_consistent_hash.is_empty() {
                    bound_tasks.extend(bound_tasks_consistent_hash);
                    // Update the available slots
                    let ch_topology = ch_topology.unwrap();
                    for node in ch_topology.nodes() {
                        if let Some(data) = guard.get_mut(&node.id) {
                            data.slots = node.available_slots;
                        } else {
                            error!("Fail to find executor data for {}", &node.id);
                        }
                    }
                }
                bound_tasks
            }
        };

        Ok(bound_tasks)
    }

    async fn unbind_tasks(&self, executor_slots: Vec<ExecutorSlot>) -> Result<()> {
        let mut increments = HashMap::new();
        for (executor_id, num_slots) in executor_slots {
            let v = increments.entry(executor_id).or_insert_with(|| 0);
            *v += num_slots;
        }

        let mut guard = self.task_slots.lock().await;

        for (executor_id, num_slots) in increments {
            if let Some(data) = guard.get_mut(&executor_id) {
                data.slots += num_slots;
            }
        }

        Ok(())
    }

    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
    ) -> Result<()> {
        let executor_id = metadata.id.clone();

        self.save_executor_metadata(metadata).await?;
        self.save_executor_heartbeat(ExecutorHeartbeat {
            executor_id: executor_id.clone(),
            timestamp: timestamp_secs(),
            metrics: vec![],
            status: Some(ExecutorStatus {
                status: Some(executor_status::Status::Active(String::default())),
            }),
        })
        .await?;

        let mut guard = self.task_slots.lock().await;

        guard.insert(
            executor_id.clone(),
            AvailableTaskSlots {
                executor_id,
                slots: spec.available_task_slots,
            },
        );

        Ok(())
    }

    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> Result<()> {
        self.executors.insert(metadata.id.clone(), metadata);
        Ok(())
    }

    async fn get_executor_metadata(&self, executor_id: &str) -> Result<ExecutorMetadata> {
        self.executors
            .get(executor_id)
            .map(|pair| pair.value().clone())
            .ok_or_else(|| {
                BallistaError::Internal(format!(
                    "Not executor with ID {executor_id} found"
                ))
            })
    }

    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> Result<()> {
        let executor_id = heartbeat.executor_id.clone();
        if let Some(mut last) = self.heartbeats.get_mut(&executor_id) {
            let _ = std::mem::replace(last.deref_mut(), heartbeat);
        } else {
            self.heartbeats.insert(executor_id, heartbeat);
        }

        Ok(())
    }

    async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        {
            let mut guard = self.task_slots.lock().await;

            guard.remove(executor_id);
        }

        self.heartbeats.remove(executor_id);

        Ok(())
    }

    fn executor_heartbeats(&self) -> HashMap<String, ExecutorHeartbeat> {
        self.heartbeats
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    fn get_executor_heartbeat(&self, executor_id: &str) -> Option<ExecutorHeartbeat> {
        self.heartbeats.get(executor_id).map(|r| r.value().clone())
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
    /// In-memory store of running job statuses. Map from Job ID -> JobStatus
    running_jobs: DashMap<String, JobStatus>,
    /// Active ballista sessions
    sessions: DashMap<String, Arc<SessionContext>>,
    /// `SessionBuilder` for building DataFusion `SessionContext` from `BallistaConfig`
    session_builder: SessionBuilder,
    /// Sender of job events
    job_event_sender: ClusterEventSender<JobStateEvent>,
    /// Config producer
    config_producer: ConfigProducer,
}

impl InMemoryJobState {
    pub fn new(
        scheduler: impl Into<String>,
        session_builder: SessionBuilder,
        config_producer: ConfigProducer,
    ) -> Self {
        Self {
            scheduler: scheduler.into(),
            completed_jobs: Default::default(),
            queued_jobs: Default::default(),
            running_jobs: Default::default(),
            sessions: Default::default(),
            session_builder,
            job_event_sender: ClusterEventSender::new(100),
            config_producer,
        }
    }
}

#[async_trait]
impl JobState for InMemoryJobState {
    async fn submit_job(&self, job_id: String, graph: &ExecutionGraph) -> Result<()> {
        if self.queued_jobs.get(&job_id).is_some() {
            self.running_jobs
                .insert(job_id.clone(), graph.status().clone());
            self.queued_jobs.remove(&job_id);

            self.job_event_sender.send(&JobStateEvent::JobAcquired {
                job_id,
                owner: self.scheduler.clone(),
            });

            Ok(())
        } else {
            Err(BallistaError::Internal(format!(
                "Failed to submit job {job_id}, not found in queued jobs"
            )))
        }
    }

    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some((job_name, queued_at)) = self.queued_jobs.get(job_id).as_deref() {
            return Ok(Some(JobStatus {
                job_id: job_id.to_string(),
                job_name: job_name.clone(),
                status: Some(Status::Queued(QueuedJob {
                    queued_at: *queued_at,
                })),
            }));
        }

        if let Some(status) = self.running_jobs.get(job_id).as_deref().cloned() {
            return Ok(Some(status));
        }

        if let Some((status, _)) = self.completed_jobs.get(job_id).as_deref() {
            return Ok(Some(status.clone()));
        }

        Ok(None)
    }

    async fn get_execution_graph(&self, job_id: &str) -> Result<Option<ExecutionGraph>> {
        Ok(self
            .completed_jobs
            .get(job_id)
            .as_deref()
            .and_then(|(_, graph)| graph.clone()))
    }

    async fn try_acquire_job(&self, _job_id: &str) -> Result<Option<ExecutionGraph>> {
        // Always return None. The only state stored here are for completed jobs
        // which cannot be acquired
        Ok(None)
    }

    async fn save_job(&self, job_id: &str, graph: &ExecutionGraph) -> Result<()> {
        let status = graph.status().clone();

        debug!("saving state for job {job_id} with status {:?}", status);

        // If job is either successful or failed, save to completed jobs
        if matches!(
            status.status,
            Some(Status::Successful(_)) | Some(Status::Failed(_))
        ) {
            self.completed_jobs
                .insert(job_id.to_string(), (status, Some(graph.clone())));
            self.running_jobs.remove(job_id);
        } else if let Some(old_status) =
            self.running_jobs.insert(job_id.to_string(), status)
        {
            self.job_event_sender.send(&JobStateEvent::JobUpdated {
                job_id: job_id.to_string(),
                status: old_status,
            })
        }

        Ok(())
    }

    async fn get_session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        self.sessions
            .get(session_id)
            .map(|sess| sess.clone())
            .ok_or_else(|| {
                BallistaError::General(format!("No session for {session_id} found"))
            })
    }

    async fn create_session(
        &self,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>> {
        let session = create_datafusion_context(config, self.session_builder.clone())?;
        self.sessions.insert(session.session_id(), session.clone());

        Ok(session)
    }

    async fn update_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> Result<Arc<SessionContext>> {
        let session = create_datafusion_context(config, self.session_builder.clone())?;
        self.sessions
            .insert(session_id.to_string(), session.clone());

        Ok(session)
    }

    async fn remove_session(
        &self,
        session_id: &str,
    ) -> Result<Option<Arc<SessionContext>>> {
        Ok(self.sessions.remove(session_id).map(|(_key, value)| value))
    }

    async fn job_state_events(&self) -> Result<JobStateEventStream> {
        Ok(Box::pin(self.job_event_sender.subscribe()))
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

    fn accept_job(&self, job_id: &str, job_name: &str, queued_at: u64) -> Result<()> {
        self.queued_jobs
            .insert(job_id.to_string(), (job_name.to_string(), queued_at));

        Ok(())
    }

    fn pending_job_number(&self) -> usize {
        self.queued_jobs.len()
    }

    async fn fail_unscheduled_job(&self, job_id: &str, reason: String) -> Result<()> {
        if let Some((job_id, (job_name, queued_at))) = self.queued_jobs.remove(job_id) {
            self.completed_jobs.insert(
                job_id.clone(),
                (
                    JobStatus {
                        job_id,
                        job_name,
                        status: Some(Status::Failed(FailedJob {
                            error: reason,
                            queued_at,
                            started_at: 0,
                            ended_at: timestamp_millis(),
                        })),
                    },
                    None,
                ),
            );

            Ok(())
        } else {
            Err(BallistaError::Internal(format!(
                "Could not fail unscheduler job {job_id}, job not found in queued jobs"
            )))
        }
    }

    fn produce_config(&self) -> SessionConfig {
        (self.config_producer)()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::cluster::memory::InMemoryJobState;
    use crate::cluster::test_util::{test_job_lifecycle, test_job_planning_failure};
    use crate::test_utils::{
        test_aggregation_plan, test_join_plan, test_two_aggregations_plan,
    };
    use ballista_core::error::Result;
    use ballista_core::utils::{default_config_producer, default_session_builder};

    #[tokio::test]
    async fn test_in_memory_job_lifecycle() -> Result<()> {
        test_job_lifecycle(
            InMemoryJobState::new(
                "",
                Arc::new(default_session_builder),
                Arc::new(default_config_producer),
            ),
            test_aggregation_plan(4).await,
        )
        .await?;
        test_job_lifecycle(
            InMemoryJobState::new(
                "",
                Arc::new(default_session_builder),
                Arc::new(default_config_producer),
            ),
            test_two_aggregations_plan(4).await,
        )
        .await?;
        test_job_lifecycle(
            InMemoryJobState::new(
                "",
                Arc::new(default_session_builder),
                Arc::new(default_config_producer),
            ),
            test_join_plan(4).await,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_in_memory_job_planning_failure() -> Result<()> {
        test_job_planning_failure(
            InMemoryJobState::new(
                "",
                Arc::new(default_session_builder),
                Arc::new(default_config_producer),
            ),
            test_aggregation_plan(4).await,
        )
        .await?;
        test_job_planning_failure(
            InMemoryJobState::new(
                "",
                Arc::new(default_session_builder),
                Arc::new(default_config_producer),
            ),
            test_two_aggregations_plan(4).await,
        )
        .await?;
        test_job_planning_failure(
            InMemoryJobState::new(
                "",
                Arc::new(default_session_builder),
                Arc::new(default_config_producer),
            ),
            test_join_plan(4).await,
        )
        .await?;

        Ok(())
    }
}
