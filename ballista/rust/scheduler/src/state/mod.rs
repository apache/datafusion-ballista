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

use std::any::type_name;
use std::future::Future;

use std::sync::Arc;

use prost::Message;

use ballista_core::error::{BallistaError, Result};

use crate::scheduler_server::SessionBuilder;

use ballista_core::serde::{AsExecutionPlan, BallistaCodec};
use datafusion_proto::logical_plan::AsLogicalPlan;

use crate::state::backend::{Lock, StateBackendClient};

use crate::state::executor_manager::ExecutorManager;
use crate::state::session_manager::SessionManager;
use crate::state::task_manager::TaskManager;

pub mod backend;
pub mod execution_graph;
pub mod executor_manager;
pub mod session_manager;
pub mod session_registry;
mod task_manager;

pub fn decode_protobuf<T: Message + Default>(bytes: &[u8]) -> Result<T> {
    T::decode(bytes).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not deserialize {}: {}",
            type_name::<T>(),
            e
        ))
    })
}

pub fn decode_into<T: Message + Default, U: From<T>>(bytes: &[u8]) -> Result<U> {
    T::decode(bytes)
        .map_err(|e| {
            BallistaError::Internal(format!(
                "Could not deserialize {}: {}",
                type_name::<T>(),
                e
            ))
        })
        .map(|t| t.into())
}

pub fn encode_protobuf<T: Message + Default>(msg: &T) -> Result<Vec<u8>> {
    let mut value: Vec<u8> = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut value).map_err(|e| {
        BallistaError::Internal(format!(
            "Could not serialize {}: {}",
            type_name::<T>(),
            e
        ))
    })?;
    Ok(value)
}

#[derive(Clone)]
pub(super) struct SchedulerState<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>
{
    pub executor_manager: ExecutorManager,
    pub task_manager: TaskManager<T, U>,
    pub session_manager: SessionManager,
    _codec: BallistaCodec<T, U>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    pub fn new(
        config_client: Arc<dyn StateBackendClient>,
        _namespace: String,
        session_builder: SessionBuilder,
        codec: BallistaCodec<T, U>,
    ) -> Self {
        Self {
            executor_manager: ExecutorManager::new(config_client.clone()),
            task_manager: TaskManager::new(
                config_client.clone(),
                session_builder,
                codec.clone(),
            ),
            session_manager: SessionManager::new(config_client, session_builder),
            _codec: codec,
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.executor_manager.init().await
    }
}

pub async fn with_lock<Out, F: Future<Output = Out>>(lock: Box<dyn Lock>, op: F) -> Out {
    let mut lock = lock;
    let result = op.await;
    lock.unlock().await;

    result
}

#[cfg(all(test, feature = "sled"))]
mod test {}
