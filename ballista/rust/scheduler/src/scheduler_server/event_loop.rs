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

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use log::{error, info};

use crate::scheduler_server::event::SchedulerServerEvent;
use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::EventAction;

use ballista_core::serde::scheduler::ExecutorMetadata;
use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan};

use crate::state::execution_graph::Task;
use crate::state::executor_manager::ExecutorReservation;
// use crate::state::task_scheduler::TaskScheduler;
use crate::state::SchedulerState;

pub(crate) struct SchedulerServerEventAction<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
> {
    state: Arc<SchedulerState<T, U>>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    SchedulerServerEventAction<T, U>
{
    pub fn new(state: Arc<SchedulerState<T, U>>) -> Self {
        Self { state }
    }

    async fn offer_reservation(
        &self,
        reservations: Vec<ExecutorReservation>,
    ) -> Result<Option<SchedulerServerEvent>> {
        let free_list = match self
            .state
            .task_manager
            .fill_reservations(&reservations)
            .await
        {
            Ok((assignments, mut unassigned_reservations)) => {
                for (executor_id, task) in assignments.into_iter() {
                    match self
                        .state
                        .executor_manager
                        .get_executor_metadata(&executor_id)
                        .await
                    {
                        Ok(executor) => {
                            if let Err(e) =
                                self.state.task_manager.launch_task(&executor, task).await
                            {
                                error!("Failed to launch new task: {:?}", e);
                                unassigned_reservations.push(
                                    ExecutorReservation::new_free(executor_id.clone()),
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to launch new task, could not get executor metadata: {:?}", e);
                            unassigned_reservations
                                .push(ExecutorReservation::new_free(executor_id.clone()));
                        }
                    }
                }
                unassigned_reservations
            }
            Err(e) => {
                error!("Error filling reservations: {:?}", e);
                reservations
            }
        };

        // If any reserved slots remain, return them to the pool
        if free_list.len() > 0 {
            self.state
                .executor_manager
                .cancel_reservations(free_list)
                .await?;
        }
        Ok(None)
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
    EventAction<SchedulerServerEvent> for SchedulerServerEventAction<T, U>
{
    // TODO
    fn on_start(&self) {
        info!("Starting SchedulerServerEvent handler")
    }

    // TODO
    fn on_stop(&self) {
        info!("Stopping SchedulerServerEvent handler")
    }

    async fn on_receive(
        &self,
        event: SchedulerServerEvent,
    ) -> Result<Option<SchedulerServerEvent>> {
        match event {
            SchedulerServerEvent::Offer(reservations) => {
                self.offer_reservation(reservations).await
            }
        }
    }

    // TODO
    fn on_error(&self, error: BallistaError) {
        error!("Error in SchedulerServerEvent handler: {:?}", error);
    }
}

pub(crate) async fn offer_reservations<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
    Fut,
    F: FnOnce(&ExecutorMetadata, Task) -> Fut,
>(
    state: &SchedulerState<T, U>,
    reservations: Vec<ExecutorReservation>,
    _launch_fn: F,
) -> Result<Option<SchedulerServerEvent>>
where
    Fut: Future,
{
    let free_list = match state.task_manager.fill_reservations(&reservations).await {
        Ok((assignments, mut unassigned_reservations)) => {
            for (executor_id, task) in assignments.into_iter() {
                match state
                    .executor_manager
                    .get_executor_metadata(&executor_id)
                    .await
                {
                    Ok(executor) => {
                        if let Err(e) =
                            state.task_manager.launch_task(&executor, task).await
                        {
                            error!("Failed to launch new task: {:?}", e);
                            unassigned_reservations
                                .push(ExecutorReservation::new_free(executor_id.clone()));
                        }
                    }
                    Err(e) => {
                        error!("Failed to launch new task, could not get executor metadata: {:?}", e);
                        unassigned_reservations
                            .push(ExecutorReservation::new_free(executor_id.clone()));
                    }
                }
            }
            unassigned_reservations
        }
        Err(e) => {
            error!("Error filling reservations: {:?}", e);
            reservations
        }
    };

    // If any reserved slots remain, return them to the pool
    if free_list.len() > 0 {
        state
            .executor_manager
            .cancel_reservations(free_list)
            .await?;
    }
    Ok(None)
}

#[cfg(test)]
mod test {}
