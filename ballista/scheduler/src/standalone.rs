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

use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;
use crate::metrics::default_metrics_collector;
use crate::scheduler_server::SchedulerServer;
use ballista_core::ConfigProducer;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::{
    GrpcServerConfig, create_grpc_server, default_config_producer,
    default_session_builder,
};
use ballista_core::{
    BALLISTA_VERSION, error::Result,
    serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
};
use datafusion::execution::SessionState;
use datafusion::prelude::SessionConfig;
use datafusion_proto::protobuf::LogicalPlanNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Creates a standalone scheduler with default configuration.
///
/// Returns the socket address the scheduler is listening on.
/// Useful for testing and single-node deployments.
pub async fn new_standalone_scheduler() -> Result<SocketAddr> {
    let codec = BallistaCodec::default();
    new_standalone_scheduler_with_builder(
        Arc::new(default_session_builder),
        Arc::new(default_config_producer),
        codec,
    )
    .await
}

/// Creates a standalone scheduler from an existing session state.
///
/// Uses the session state's configuration and codecs to create the scheduler.
pub async fn new_standalone_scheduler_from_state(
    session_state: &SessionState,
) -> Result<SocketAddr> {
    let logical = session_state.config().ballista_logical_extension_codec();
    let physical = session_state.config().ballista_physical_extension_codec();
    let codec = BallistaCodec::new(logical, physical);
    let session_config = session_state.config().clone();
    let session_state = session_state.clone();
    let session_builder = Arc::new(move |_: SessionConfig| Ok(session_state.clone()));
    let config_producer = Arc::new(move || session_config.clone());

    new_standalone_scheduler_with_builder(session_builder, config_producer, codec).await
}

/// Creates a standalone scheduler with custom session builder, config producer, and codec.
///
/// Returns the socket address the scheduler is listening on.
pub async fn new_standalone_scheduler_with_builder(
    session_builder: crate::scheduler_server::SessionBuilder,
    config_producer: ConfigProducer,
    codec: BallistaCodec,
) -> Result<SocketAddr> {
    let config = config_producer();

    let cluster =
        BallistaCluster::new_memory("localhost:50050", session_builder, config_producer);

    let metrics_collector = default_metrics_collector()?;

    let mut scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            "localhost:50050".to_owned(),
            cluster,
            codec,
            Arc::new(SchedulerConfig::default()),
            metrics_collector,
        );

    scheduler_server.init().await?;
    let server = SchedulerGrpcServer::new(scheduler_server.clone())
        .max_decoding_message_size(config.ballista_grpc_client_max_message_size())
        .max_encoding_message_size(config.ballista_grpc_client_max_message_size());

    // Let the OS assign a random, free port
    let listener = TcpListener::bind("localhost:0").await?;
    let addr = listener.local_addr()?;
    info!("Ballista v{BALLISTA_VERSION} Rust Scheduler listening on {addr:?}");
    tokio::spawn(
        create_grpc_server(&GrpcServerConfig::default())
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                listener,
            )),
    );

    Ok(addr)
}
