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

use ballista_core::{
    config::BallistaConfig,
    serde::protobuf::{
        scheduler_grpc_client::SchedulerGrpcClient, CreateSessionParams, KeyValuePair,
    },
    utils::{create_df_ctx_with_ballista_query_planner, create_grpc_client_connection},
};
use datafusion::{error::DataFusionError, prelude::SessionContext};
use datafusion_proto::protobuf::LogicalPlanNode;

/// [SessionContext] extension which provides Ballista distribution
/// support to DataFusion
#[async_trait::async_trait]
pub trait SessionContextExt {
    /// Create a context for executing queries against a standalone Ballista scheduler instance
    /// It wills start local ballista cluster with scheduler and executor.
    #[cfg(feature = "standalone")]
    async fn standalone_with_config(
        config: &BallistaConfig,
    ) -> datafusion::error::Result<SessionContext>;

    /// Create a context for executing queries against a standalone Ballista scheduler instance
    /// It wills start local ballista cluster with scheduler and executor.
    #[cfg(feature = "standalone")]
    async fn standalone() -> datafusion::error::Result<SessionContext>;

    // To be added at the later stage
    // #[cfg(feature = "standalone")]
    // async fn standalone_with_state(
    //     config: &BallistaConfig,
    //     session_state: SessionState,
    // ) -> datafusion::error::Result<SessionContext>;

    /// Create a context for executing queries against a remote Ballista scheduler instance
    async fn remote_with_config(
        host: &str,
        port: u16,
        config: &BallistaConfig,
    ) -> datafusion::error::Result<SessionContext>;

    /// Create a context for executing queries against a remote Ballista scheduler instance
    async fn remote(host: &str, port: u16) -> datafusion::error::Result<SessionContext>;

    // To be added at the later stage
    // async fn remote_with_state(
    //     host: &str,
    //     port: u16,
    //     config: &BallistaConfig,
    //     session_state: SessionState,
    // ) -> datafusion::error::Result<SessionContext>;
}

#[async_trait::async_trait]
impl SessionContextExt for SessionContext {
    async fn remote_with_config(
        host: &str,
        port: u16,
        config: &BallistaConfig,
    ) -> datafusion::error::Result<SessionContext> {
        let scheduler_url = format!("http://{}:{}", &host, port);
        log::info!(
            "Connecting to Ballista scheduler at {}",
            scheduler_url.clone()
        );
        let connection = create_grpc_client_connection(scheduler_url.clone())
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

        let limit = config.default_grpc_client_max_message_size();
        let mut scheduler = SchedulerGrpcClient::new(connection)
            .max_encoding_message_size(limit)
            .max_decoding_message_size(limit);

        let remote_session_id = scheduler
            .create_session(CreateSessionParams {
                settings: config
                    .settings()
                    .iter()
                    .map(|(k, v)| KeyValuePair {
                        key: k.to_owned(),
                        value: v.to_owned(),
                    })
                    .collect::<Vec<_>>(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner()
            .session_id;

        log::info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );

        let ctx = {
            create_df_ctx_with_ballista_query_planner::<LogicalPlanNode>(
                scheduler_url,
                remote_session_id,
                &config,
            )
        };

        Ok(ctx)
    }

    async fn remote(host: &str, port: u16) -> datafusion::error::Result<SessionContext> {
        let config = BallistaConfig::builder()
            .build()
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
        Self::remote_with_config(host, port, &config).await
    }

    #[cfg(feature = "standalone")]
    async fn standalone_with_config(
        config: &BallistaConfig,
    ) -> datafusion::error::Result<Self> {
        use ballista_core::serde::BallistaCodec;
        use datafusion_proto::protobuf::PhysicalPlanNode;

        log::info!("Running in local mode. Scheduler will be run in-proc");

        let addr = ballista_scheduler::standalone::new_standalone_scheduler()
            .await
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;

        let scheduler_url = format!("http://localhost:{}", addr.port());
        let mut scheduler = loop {
            match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    log::info!("Attempting to connect to in-proc scheduler...");
                }
                Ok(scheduler) => break scheduler,
            }
        };

        let remote_session_id = scheduler
            .create_session(CreateSessionParams {
                settings: config
                    .settings()
                    .iter()
                    .map(|(k, v)| KeyValuePair {
                        key: k.to_owned(),
                        value: v.to_owned(),
                    })
                    .collect::<Vec<_>>(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner()
            .session_id;

        log::info!(
            "Server side SessionContext created with session id: {}",
            remote_session_id
        );

        let ctx = {
            create_df_ctx_with_ballista_query_planner::<LogicalPlanNode>(
                scheduler_url,
                remote_session_id,
                &config,
            )
        };

        let default_codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> =
            BallistaCodec::default();

        let concurrent_tasks = config.default_standalone_parallelism();
        ballista_executor::new_standalone_executor(
            scheduler,
            concurrent_tasks,
            default_codec,
        )
        .await
        .map_err(|e| DataFusionError::Configuration(e.to_string()))?;

        Ok(ctx)
    }

    #[cfg(feature = "standalone")]
    async fn standalone() -> datafusion::error::Result<Self> {
        let config = BallistaConfig::builder()
            .build()
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;

        Self::standalone_with_config(&config).await
    }
}
