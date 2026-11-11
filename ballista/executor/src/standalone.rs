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
    BALLISTA_PROTOCOL_VERSION, BALLISTA_VERSION,
    error::Result,
    ids::new_instance_id,
    serde::BallistaCodec,
    serde::protobuf::{ExecutorRegistration, scheduler_grpc_client::SchedulerGrpcClient},
    serde::scheduler::{ExecutorOperatingSystemSpecification, ExecutorSpecification},
    utils::create_grpc_server,
};
use ballista_core::{ConfigProducer, RuntimeProducer};
use datafusion::execution::{SessionState, SessionStateBuilder};
use log::info;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use tonic::transport::server::TcpIncoming;

/// Creates new standalone executor based on
/// session_state provided.
///
/// This provides flexible way of configuring underlying
/// components.
pub async fn new_standalone_executor_from_state(
    scheduler: SchedulerGrpcClient<Channel>,
    vcores: usize,
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
        vcores,
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
/// The executor binds to a random available port on localhost. It polls the
/// scheduler for work, unless `ballista.standalone.task_scheduling_policy` is
/// `push-staged` or the configuration asks for pipelined execution: then it
/// registers with the scheduler and takes the tasks the
/// scheduler pushes to it, and expects a standalone scheduler started from the
/// same configuration.
pub async fn new_standalone_executor_from_builder(
    scheduler: SchedulerGrpcClient<Channel>,
    vcores: usize,
    config_producer: ConfigProducer,
    runtime_producer: RuntimeProducer,
    codec: BallistaCodec,
    function_registry: BallistaFunctionRegistry,
) -> Result<()> {
    // Let the OS assign a random, free port
    let listener = TcpListener::bind("localhost:0").await?;
    let address = listener.local_addr()?;
    info!("Ballista v{BALLISTA_VERSION} Rust Executor listening on {address:?}");

    let config = config_producer();
    let max_message_size = config.ballista_grpc_client_max_message_size();
    let push_staged = matches!(
        config
            .ballista_config()
            .standalone_task_scheduling_policy()?,
        ballista_core::config::TaskSchedulingPolicy::PushStaged
    );

    // A push-staged executor serves the scheduler's task launches on a gRPC
    // port of its own. Bind it now and keep it bound until the server takes it
    // over, so no other port-0 bind in this process can be handed the same
    // number in between.
    let grpc_listener = if push_staged {
        Some(TcpListener::bind("127.0.0.1:0").await?)
    } else {
        None
    };
    let grpc_port = match &grpc_listener {
        Some(listener) => listener.local_addr()?.port(),
        // TODO Make it configurable
        None => 50020,
    };

    let executor_meta = ExecutorRegistration {
        id: new_instance_id(), // assign this executor a unique ID
        host: Some("localhost".to_string()),
        port: address.port() as u32,
        grpc_port: grpc_port as u32,
        specification: Some(
            ExecutorSpecification::default()
                .with_vcores(vcores as u32)
                .into(),
        ),
        os_info: Some(ExecutorOperatingSystemSpecification::default().into()),
        ballista_protocol_version: BALLISTA_PROTOCOL_VERSION,
    };

    let work_dir = TempDir::new()?.path().to_str().unwrap().to_string();

    info!("work_dir: {work_dir}");

    let executor = Arc::new(Executor::with_default_execution_engine(
        executor_meta,
        &work_dir,
        runtime_producer,
        config_producer,
        Arc::new(function_registry),
        Arc::new(LoggingMetricsCollector::default()),
        vcores,
    ));

    let service = BallistaFlightService::new(work_dir)
        .with_pipelined_registry(executor.pipelined_registry().clone());
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

    if let Some(grpc_listener) = grpc_listener {
        let process_config = crate::executor_process::ExecutorProcessConfig {
            bind_host: "127.0.0.1".to_owned(),
            port: address.port(),
            grpc_port,
            vcores,
            task_scheduling_policy:
                ballista_core::config::TaskSchedulingPolicy::PushStaged,
            grpc_max_decoding_message_size: max_message_size as u32,
            grpc_max_encoding_message_size: max_message_size as u32,
            ..Default::default()
        };
        let shutdown = crate::shutdown::ShutdownNotifier::new();
        let (stop_send, stop_receive) = tokio::sync::mpsc::channel::<bool>(10);
        let incoming = TcpIncoming::from(grpc_listener)
            .with_nodelay(Some(true))
            .with_keepalive(Some(Duration::from_secs(
                process_config.grpc_server_config.tcp_keepalive_seconds,
            )));
        info!(
            "Ballista v{BALLISTA_VERSION} Rust Executor Grpc Server listening on 127.0.0.1:{grpc_port}"
        );
        crate::executor_server::startup_with_incoming(
            scheduler,
            Arc::new(process_config),
            executor,
            codec,
            stop_send,
            &shutdown,
            crate::health::ExecutorHealth::new(),
            incoming,
        )
        .await?;
        // The executor's loops stop when the notifier is dropped, and its
        // `stop_executor` handler panics if the stop channel's receiver is
        // dropped, so keep both for the life of the runtime.
        tokio::spawn(async move {
            let _keep_alive = (shutdown, stop_receive);
            futures::future::pending::<()>().await
        });
    } else {
        tokio::spawn(execution_loop::poll_loop(
            scheduler,
            executor,
            codec,
            None,
            None,
            crate::health::ExecutorHealth::new(),
        ));
    }
    Ok(())
}

/// Creates standalone executor with most values
/// set as default.
pub async fn new_standalone_executor(
    scheduler: SchedulerGrpcClient<Channel>,
    vcores: usize,
    codec: BallistaCodec,
) -> Result<()> {
    use ballista_core::extension::{
        ballista_aggregate_functions, ballista_scalar_functions,
        ballista_window_functions,
    };

    let session_state = SessionStateBuilder::new()
        .with_default_features()
        .with_scalar_functions(ballista_scalar_functions())
        .with_aggregate_functions(ballista_aggregate_functions())
        .with_window_functions(ballista_window_functions())
        .build();

    let runtime = session_state.runtime_env().clone();
    let runtime_producer: RuntimeProducer = Arc::new(move |_| Ok(runtime.clone()));

    new_standalone_executor_from_builder(
        scheduler,
        vcores,
        Arc::new(default_config_producer),
        runtime_producer,
        codec,
        (&session_state).into(),
    )
    .await
}
