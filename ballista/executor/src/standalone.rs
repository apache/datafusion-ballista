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

//! Standalone executor for embedded or testing scenarios.
//!
//! This module provides functions for creating executors that run in the same
//! process as the client, useful for testing and development purposes.

use crate::metrics::LoggingMetricsCollector;
use crate::{execution_loop, executor::Executor, flight_service::BallistaFlightService};
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::extension::SessionConfigExt;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::utils::{GrpcServerConfig, default_config_producer};
use ballista_core::{
    BALLISTA_VERSION,
    error::Result,
    serde::BallistaCodec,
    serde::protobuf::{ExecutorRegistration, scheduler_grpc_client::SchedulerGrpcClient},
    serde::scheduler::ExecutorSpecification,
    utils::create_grpc_server,
};
use ballista_core::{ConfigProducer, RuntimeProducer};
use datafusion::execution::{SessionState, SessionStateBuilder};
use log::info;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use uuid::Uuid;

/// Creates new standalone executor based on
/// session_state provided.
///
/// This provides flexible way of configuring underlying
/// components.
pub async fn new_standalone_executor_from_state(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
    session_state: &SessionState,
) -> Result<()> {
    let logical = session_state.config().ballista_logical_extension_codec();
    let physical = session_state.config().ballista_physical_extension_codec();

    let codec: BallistaCodec<
        datafusion_proto::protobuf::LogicalPlanNode,
        datafusion_proto::protobuf::PhysicalPlanNode,
    > = BallistaCodec::new(logical, physical);

    let config = session_state.config().clone().upgrade_for_ballista();
    let runtime = session_state.runtime_env().clone();

    let config_producer: ConfigProducer = Arc::new(move || config.clone());
    let runtime_producer: RuntimeProducer = Arc::new(move |_| Ok(runtime.clone()));

    new_standalone_executor_from_builder(
        scheduler,
        concurrent_tasks,
        config_producer,
        runtime_producer,
        codec,
        session_state.into(),
    )
    .await
}

/// Creates a standalone executor with custom configuration.
///
/// This function provides fine-grained control over executor configuration
/// by accepting custom producers for session config, runtime environment,
/// codec, and function registry.
///
/// The executor binds to a random available port on localhost.
pub async fn new_standalone_executor_from_builder(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
    config_producer: ConfigProducer,
    runtime_producer: RuntimeProducer,
    codec: BallistaCodec,
    function_registry: BallistaFunctionRegistry,
) -> Result<()> {
    // Let the OS assign a random, free port
    let listener = TcpListener::bind("localhost:0").await?;
    let address = listener.local_addr()?;
    info!("Ballista v{BALLISTA_VERSION} Rust Executor listening on {address:?}");

    let executor_meta = ExecutorRegistration {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        host: Some("localhost".to_string()),
        port: address.port() as u32,
        // TODO Make it configurable
        grpc_port: 50020,
        specification: Some(
            ExecutorSpecification {
                task_slots: concurrent_tasks as u32,
            }
            .into(),
        ),
    };

    let config = config_producer();
    let max_message_size = config.ballista_grpc_client_max_message_size();

    let work_dir = TempDir::new()?.path().to_str().unwrap().to_string();

    info!("work_dir: {work_dir}");

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime_producer,
        config_producer,
        Arc::new(function_registry),
        Arc::new(LoggingMetricsCollector::default()),
        concurrent_tasks,
        None,
    ));

    let service = BallistaFlightService::new();
    let server = FlightServiceServer::new(service)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);

    tokio::spawn(
        create_grpc_server(&GrpcServerConfig::default())
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                listener,
            )),
    );

    tokio::spawn(execution_loop::poll_loop(scheduler, executor, codec));
    Ok(())
}

/// Creates standalone executor with most values
/// set as default.
pub async fn new_standalone_executor(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
    codec: BallistaCodec,
) -> Result<()> {
    let session_state = SessionStateBuilder::new().with_default_features().build();
    let runtime = session_state.runtime_env().clone();
    let runtime_producer: RuntimeProducer = Arc::new(move |_| Ok(runtime.clone()));

    new_standalone_executor_from_builder(
        scheduler,
        concurrent_tasks,
        Arc::new(default_config_producer),
        runtime_producer,
        codec,
        (&session_state).into(),
    )
    .await
}
