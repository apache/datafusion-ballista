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

use crate::cluster::{ClusterState, JobState, JobStateEvent, TaskDistribution};
use crate::scheduler_server::timestamp_millis;
use crate::state::execution_graph::ExecutionGraph;
use crate::state::executor_manager::ExecutorReservation;
use crate::test_utils::{await_condition, mock_completed_task, mock_executor};
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{executor_status, JobStatus};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorMetadata, ExecutorSpecification,
};
use futures::StreamExt;
use itertools::Itertools;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub struct ClusterStateTest<S: ClusterState> {
    state: Arc<S>,
    reservations: Vec<ExecutorReservation>,
    total_task_slots: u32,
}

impl<S: ClusterState> ClusterStateTest<S> {
    pub async fn new(state: S) -> Result<Self> {
        Ok(Self {
            state: Arc::new(state),
            reservations: vec![],
            total_task_slots: 0,
        })
    }

    pub async fn register_executor(
        mut self,
        executor_id: &str,
        task_slots: u32,
    ) -> Result<Self> {
        self.state
            .register_executor(
                ExecutorMetadata {
                    id: executor_id.to_string(),
                    host: executor_id.to_string(),
                    port: 0,
                    grpc_port: 0,
                    specification: ExecutorSpecification { task_slots },
                },
                ExecutorData {
                    executor_id: executor_id.to_string(),
                    total_task_slots: task_slots,
                    available_task_slots: task_slots,
                },
                false,
            )
            .await?;

        self.total_task_slots += task_slots;

        Ok(self)
    }

    pub async fn remove_executor(self, executor_id: &str) -> Result<Self> {
        self.state.remove_executor(executor_id).await?;

        Ok(self)
    }

    pub async fn assert_live_executor(
        self,
        executor_id: &str,
        task_slots: u32,
    ) -> Result<Self> {
        let executor = self.state.get_executor_metadata(executor_id).await;
        assert!(
            executor.is_ok(),
            "Metadata for executor {} not found in state",
            executor_id
        );
        assert_eq!(
            executor.unwrap().specification.task_slots,
            task_slots,
            "Unexpected number of task slots for executor"
        );

        // Heratbeat stream is async so wait up to 500ms for it to show up
        await_condition(Duration::from_millis(50), 10, || {
            let found_heartbeat = self.state.get_executor_heartbeat(executor_id).map_or(
                false,
                |heartbeat| {
                    matches!(
                        heartbeat.status,
                        Some(ballista_core::serde::generated::ballista::ExecutorStatus {
                            status: Some(executor_status::Status::Active(_))
                        })
                    )
                },
            );

            futures::future::ready(Ok(found_heartbeat))
        })
        .await?;

        Ok(self)
    }

    pub async fn assert_dead_executor(self, executor_id: &str) -> Result<Self> {
        // Heratbeat stream is async so wait up to 500ms for it to show up
        await_condition(Duration::from_millis(50), 10, || {
            let found_heartbeat = self.state.get_executor_heartbeat(executor_id).map_or(
                true,
                |heartbeat| {
                    matches!(
                        heartbeat.status,
                        Some(ballista_core::serde::generated::ballista::ExecutorStatus {
                            status: Some(executor_status::Status::Dead(_))
                        })
                    )
                },
            );

            futures::future::ready(Ok(found_heartbeat))
        })
        .await?;

        Ok(self)
    }

    pub async fn try_reserve_slots(
        mut self,
        num_slots: u32,
        distribution: TaskDistribution,
        filter: Option<Vec<String>>,
        exact: bool,
    ) -> Result<Self> {
        let filter = filter.map(|f| f.into_iter().collect::<HashSet<String>>());
        let reservations = if exact {
            self.state
                .reserve_slots_exact(num_slots, distribution, filter)
                .await?
        } else {
            self.state
                .reserve_slots(num_slots, distribution, filter)
                .await?
        };

        self.reservations.extend(reservations);

        Ok(self)
    }

    pub async fn cancel_reservations(mut self, num_slots: usize) -> Result<Self> {
        if self.reservations.len() < num_slots {
            return Err(BallistaError::General(format!(
                "Not enough reservations to cancel, expected {} but found {}",
                num_slots,
                self.reservations.len()
            )));
        }

        let to_keep = self.reservations.split_off(num_slots);

        self.state
            .cancel_reservations(std::mem::take(&mut self.reservations))
            .await?;

        self.reservations = to_keep;

        Ok(self)
    }

    pub fn assert_open_reservations(self, n: usize) -> Self {
        assert_eq!(
            self.reservations.len(),
            n,
            "Expectedt {} open reservations but found {}",
            n,
            self.reservations.len()
        );
        self
    }

    pub fn assert_open_reservations_with<F: Fn(&ExecutorReservation) -> bool>(
        self,
        n: usize,
        predicate: F,
    ) -> Self {
        assert_eq!(
            self.reservations.len(),
            n,
            "Expected {} open reservations but found {}",
            n,
            self.reservations.len()
        );

        for res in &self.reservations {
            assert!(predicate(res), "Predicate failed on reservation {:?}", res);
        }
        self
    }

    pub async fn fuzz_reservation(
        mut self,
        concurrency: usize,
        distribution: TaskDistribution,
    ) -> Result<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1_000);

        let total_slots = self.total_task_slots;
        for _ in 0..concurrency {
            let state = self.state.clone();
            let sender_clone = sender.clone();
            tokio::spawn(async move {
                let mut open_reservations = vec![];
                for i in 0..10 {
                    if i % 2 == 0 {
                        let to_reserve = rand::random::<u32>() % total_slots;

                        let reservations = state
                            .reserve_slots(to_reserve, distribution, None)
                            .await
                            .unwrap();

                        sender_clone
                            .send(FuzzEvent::Reserved(reservations.clone()))
                            .await
                            .unwrap();

                        open_reservations = reservations;
                    } else {
                        state
                            .cancel_reservations(open_reservations.clone())
                            .await
                            .unwrap();
                        sender_clone
                            .send(FuzzEvent::Cancelled(std::mem::take(
                                &mut open_reservations,
                            )))
                            .await
                            .unwrap();
                    }
                }
            });
        }

        drop(sender);

        while let Some(event) = receiver.recv().await {
            match event {
                FuzzEvent::Reserved(reservations) => {
                    self.reservations.extend(reservations);
                    assert!(
                        self.reservations.len() <= total_slots as usize,
                        "More than total number of slots was reserved"
                    );
                }
                FuzzEvent::Cancelled(reservations) => {
                    for res in reservations {
                        let idx = self
                            .reservations
                            .iter()
                            .find_position(|r| r.executor_id == res.executor_id);
                        assert!(idx.is_some(), "Received invalid cancellation, not existing reservation for executor ID {}", res.executor_id);

                        self.reservations.swap_remove(idx.unwrap().0);
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
enum FuzzEvent {
    Reserved(Vec<ExecutorReservation>),
    Cancelled(Vec<ExecutorReservation>),
}

pub async fn test_fuzz_reservations<S: ClusterState>(
    state: S,
    concurrency: usize,
    distribution: TaskDistribution,
    num_executors: usize,
    task_slots_per_executor: usize,
) -> Result<()> {
    let mut test = ClusterStateTest::new(state).await?;

    for idx in 0..num_executors {
        test = test
            .register_executor(idx.to_string().as_str(), task_slots_per_executor as u32)
            .await?;
    }

    test.fuzz_reservation(concurrency, distribution).await
}

pub async fn test_executor_registration<S: ClusterState>(state: S) -> Result<()> {
    let test = ClusterStateTest::new(state).await?;

    test.register_executor("1", 10)
        .await?
        .register_executor("2", 10)
        .await?
        .register_executor("3", 10)
        .await?
        .assert_live_executor("1", 10)
        .await?
        .assert_live_executor("2", 10)
        .await?
        .assert_live_executor("3", 10)
        .await?
        .remove_executor("1")
        .await?
        .assert_dead_executor("1")
        .await?
        .remove_executor("2")
        .await?
        .assert_dead_executor("2")
        .await?
        .remove_executor("3")
        .await?
        .assert_dead_executor("3")
        .await?;

    Ok(())
}

pub async fn test_reservation<S: ClusterState>(
    state: S,
    distribution: TaskDistribution,
) -> Result<()> {
    let test = ClusterStateTest::new(state).await?;

    test.register_executor("1", 10)
        .await?
        .register_executor("2", 10)
        .await?
        .register_executor("3", 10)
        .await?
        .try_reserve_slots(10, distribution, None, false)
        .await?
        .assert_open_reservations(10)
        .cancel_reservations(10)
        .await?
        .try_reserve_slots(30, distribution, None, false)
        .await?
        .assert_open_reservations(30)
        .cancel_reservations(15)
        .await?
        .assert_open_reservations(15)
        .try_reserve_slots(30, distribution, None, false)
        .await?
        .assert_open_reservations(30)
        .cancel_reservations(30)
        .await?
        .assert_open_reservations(0)
        .try_reserve_slots(50, distribution, None, false)
        .await?
        .assert_open_reservations(30)
        .cancel_reservations(30)
        .await?
        .try_reserve_slots(20, distribution, Some(vec!["1".to_string()]), false)
        .await?
        .assert_open_reservations_with(10, |res| res.executor_id == "1")
        .cancel_reservations(10)
        .await?
        .try_reserve_slots(
            20,
            distribution,
            Some(vec!["2".to_string(), "3".to_string()]),
            false,
        )
        .await?
        .assert_open_reservations_with(20, |res| {
            res.executor_id == "2" || res.executor_id == "3"
        });

    Ok(())
}

pub struct JobStateTest<S: JobState> {
    state: Arc<S>,
    events: Arc<RwLock<Vec<JobStateEvent>>>,
}

impl<S: JobState> JobStateTest<S> {
    pub async fn new(state: S) -> Result<Self> {
        let events = Arc::new(RwLock::new(vec![]));

        let mut event_stream = state.job_state_events().await?;
        let events_clone = events.clone();
        tokio::spawn(async move {
            while let Some(event) = event_stream.next().await {
                let mut guard = events_clone.write().await;

                guard.push(event);
            }
        });

        Ok(Self {
            state: Arc::new(state),
            events,
        })
    }

    pub async fn queue_job(self, job_id: &str) -> Result<Self> {
        self.state
            .accept_job(job_id, "", timestamp_millis())
            .await?;
        Ok(self)
    }

    pub async fn fail_planning(self, job_id: &str) -> Result<Self> {
        self.state
            .fail_unscheduled_job(job_id, "failed planning".to_string())
            .await?;
        Ok(self)
    }

    pub async fn assert_queued(self, job_id: &str) -> Result<Self> {
        let status = self.state.get_job_status(job_id).await?;

        assert!(status.is_some(), "Queued job {} not found", job_id);

        let status = status.unwrap();
        assert!(
            matches!(&status, JobStatus {
            job_id: status_job_id, status: Some(Status::Queued(_)), ..
        } if status_job_id.as_str() == job_id),
            "Expected queued status but found {:?}",
            status
        );

        Ok(self)
    }

    pub async fn submit_job(self, graph: &ExecutionGraph) -> Result<Self> {
        self.state
            .submit_job(graph.job_id().to_string(), graph)
            .await?;
        Ok(self)
    }

    pub async fn assert_job_running(self, job_id: &str) -> Result<Self> {
        let status = self.state.get_job_status(job_id).await?;

        assert!(status.is_some(), "Job status not found for {}", job_id);

        let status = status.unwrap();
        assert!(
            matches!(&status, JobStatus {
            job_id: status_job_id, status: Some(Status::Running(_)), ..
        } if status_job_id.as_str() == job_id),
            "Expected running status but found {:?}",
            status
        );

        Ok(self)
    }

    pub async fn update_job(self, graph: &ExecutionGraph) -> Result<Self> {
        self.state.save_job(graph.job_id(), graph).await?;
        Ok(self)
    }

    pub async fn assert_job_failed(self, job_id: &str) -> Result<Self> {
        let status = self.state.get_job_status(job_id).await?;

        assert!(status.is_some(), "Job status not found for {}", job_id);

        let status = status.unwrap();
        assert!(
            matches!(&status, JobStatus {
            job_id: status_job_id, status: Some(Status::Failed(_)), ..
        } if status_job_id.as_str() == job_id),
            "Expected failed status but found {:?}",
            status
        );

        Ok(self)
    }

    pub async fn assert_job_successful(self, job_id: &str) -> Result<Self> {
        let status = self.state.get_job_status(job_id).await?;

        assert!(status.is_some(), "Job status not found for {}", job_id);
        let status = status.unwrap();
        assert!(
            matches!(&status, JobStatus {
            job_id: status_job_id, status: Some(Status::Successful(_)), ..
        } if status_job_id.as_str() == job_id),
            "Expected success status but found {:?}",
            status
        );

        Ok(self)
    }

    pub async fn assert_event(self, event: JobStateEvent) -> Result<Self> {
        let events = self.events.clone();
        let found = await_condition(Duration::from_millis(50), 10, || async {
            let guard = events.read().await;

            Ok(guard.iter().any(|ev| ev == &event))
        })
        .await?;

        assert!(found, "Expected event {:?}", event);

        Ok(self)
    }
}

pub async fn test_job_lifecycle<S: JobState>(
    state: S,
    mut graph: ExecutionGraph,
) -> Result<()> {
    let test = JobStateTest::new(state).await?;

    let job_id = graph.job_id().to_string();

    let test = test
        .queue_job(&job_id)
        .await?
        .assert_queued(&job_id)
        .await?
        .submit_job(&graph)
        .await?
        .assert_job_running(&job_id)
        .await?;

    drain_tasks(&mut graph)?;
    graph.succeed_job()?;

    test.update_job(&graph)
        .await?
        .assert_job_successful(&job_id)
        .await?;

    Ok(())
}

pub async fn test_job_planning_failure<S: JobState>(
    state: S,
    graph: ExecutionGraph,
) -> Result<()> {
    let test = JobStateTest::new(state).await?;

    let job_id = graph.job_id().to_string();

    test.queue_job(&job_id)
        .await?
        .fail_planning(&job_id)
        .await?
        .assert_job_failed(&job_id)
        .await?;

    Ok(())
}

fn drain_tasks(graph: &mut ExecutionGraph) -> Result<()> {
    let executor = mock_executor("executor-id1".to_string());
    while let Some(task) = graph.pop_next_task(&executor.id)? {
        let task_status = mock_completed_task(task, &executor.id);
        graph.update_task_status(&executor, vec![task_status], 1, 1)?;
    }

    Ok(())
}
