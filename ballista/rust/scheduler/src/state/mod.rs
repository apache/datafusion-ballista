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

use std::sync::Arc;

use prost::Message;

use ballista_core::error::{BallistaError, Result};

use crate::scheduler_server::SessionBuilder;

use ballista_core::serde::{AsExecutionPlan, AsLogicalPlan, BallistaCodec};

use crate::state::backend::StateBackendClient;

use crate::state::executor_manager::ExecutorManager;
use crate::state::session_manager::SessionManager;
use crate::state::task_manager::TaskManager;

pub mod backend;
pub mod execution_graph;
pub mod executor_manager;
pub mod session_manager;
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
    // persistent_state: PersistentSchedulerState<T, U>,
    pub executor_manager: ExecutorManager,
    // pub stage_manager: StageManager,
    pub task_manager: TaskManager<T, U>,
    pub session_manager: SessionManager,
    codec: BallistaCodec<T, U>,
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
            session_manager: SessionManager::new(config_client.clone(), session_builder),
            codec,
        }
    }

    pub async fn init(&self) -> Result<()> {
        // TODO, can probably pre-load some cacheable data here.
        Ok(())
    }

    pub fn get_codec(&self) -> &BallistaCodec<T, U> {
        &self.codec
    }
}

#[cfg(all(test, feature = "sled"))]
mod test {

    // #[tokio::test]
    // async fn executor_metadata() -> Result<(), BallistaError> {
    //     let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
    //         SchedulerState::new(
    //             Arc::new(StandaloneClient::try_new_temporary()?),
    //             "test".to_string(),
    //             default_session_builder,
    //             BallistaCodec::default(),
    //         );
    //     let meta = ExecutorMetadata {
    //         id: "123".to_owned(),
    //         host: "localhost".to_owned(),
    //         port: 123,
    //         grpc_port: 124,
    //         specification: ExecutorSpecification { task_slots: 2 },
    //     };
    //     state.save_executor_metadata(meta.clone()).await?;
    //     let result: Vec<_> = state
    //         .get_executors_metadata()
    //         .await?
    //         .into_iter()
    //         .map(|(meta, _)| meta)
    //         .collect();
    //     assert_eq!(vec![meta], result);
    //     Ok(())
    // }
    //
    // #[tokio::test]
    // async fn job_metadata() -> Result<(), BallistaError> {
    //     let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
    //         SchedulerState::new(
    //             Arc::new(StandaloneClient::try_new_temporary()?),
    //             "test".to_string(),
    //             default_session_builder,
    //             BallistaCodec::default(),
    //         );
    //     let meta = JobStatus {
    //         status: Some(job_status::Status::Queued(QueuedJob {})),
    //     };
    //     state.save_job_metadata("job", &meta).await?;
    //     let result = state.get_job_metadata("job").unwrap();
    //     assert!(result.status.is_some());
    //     match result.status.unwrap() {
    //         job_status::Status::Queued(_) => (),
    //         _ => panic!("Unexpected status"),
    //     }
    //     Ok(())
    // }
    //
    // #[tokio::test]
    // async fn job_metadata_non_existant() -> Result<(), BallistaError> {
    //     let state: SchedulerState<LogicalPlanNode, PhysicalPlanNode> =
    //         SchedulerState::new(
    //             Arc::new(StandaloneClient::try_new_temporary()?),
    //             "test".to_string(),
    //             default_session_builder,
    //             BallistaCodec::default(),
    //         );
    //     let meta = JobStatus {
    //         status: Some(job_status::Status::Queued(QueuedJob {})),
    //     };
    //     state.save_job_metadata("job", &meta).await?;
    //     let result = state.get_job_metadata("job2");
    //     assert!(result.is_none());
    //     Ok(())
    // }
}
