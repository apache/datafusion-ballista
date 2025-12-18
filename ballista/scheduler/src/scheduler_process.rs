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

#[cfg(feature = "rest-api")]
use crate::api::get_routes;
use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;
use crate::flight_proxy_service::BallistaFlightProxyService;
use crate::metrics::default_metrics_collector;
#[cfg(feature = "keda-scaler")]
use crate::scheduler_server::externalscaler::external_scaler_server::ExternalScalerServer;
use crate::scheduler_server::SchedulerServer;
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::{
    BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use ballista_core::utils::{create_grpc_server, GrpcServerConfig};
use ballista_core::BALLISTA_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use http::StatusCode;
use log::{error, info};
use std::{net::SocketAddr, sync::Arc};
use tokio::task::JoinHandle;
use tonic::service::RoutesBuilder;

/// Creates as initialized scheduler service
/// without exposing it as a grpc service
pub async fn create_scheduler<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    cluster: BallistaCluster,
    config: Arc<SchedulerConfig>,
) -> ballista_core::error::Result<SchedulerServer<T, U>> {
    // Should only call SchedulerServer::new() once in the process
    info!(
        "Starting Scheduler grpc server with task scheduling policy of {:?}",
        config.scheduling_policy
    );

    let codec_logical = config
        .override_logical_codec
        .clone()
        .unwrap_or_else(|| Arc::new(BallistaLogicalExtensionCodec::default()));

    let codec_physical = config
        .override_physical_codec
        .clone()
        .unwrap_or_else(|| Arc::new(BallistaPhysicalExtensionCodec::default()));

    let codec = BallistaCodec::new(codec_logical, codec_physical);
    let metrics_collector = default_metrics_collector()?;

    let mut scheduler_server = SchedulerServer::new(
        config.scheduler_name(),
        cluster,
        codec,
        config,
        metrics_collector,
    );

    scheduler_server.init().await?;

    Ok(scheduler_server)
}

/// Exposes scheduler grpc service
pub async fn start_grpc_service<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    address: SocketAddr,
    scheduler: SchedulerServer<T, U>,
) -> ballista_core::error::Result<()> {
    let config = &scheduler.state.config;
    let scheduler_grpc_server = SchedulerGrpcServer::new(scheduler.clone())
        .max_encoding_message_size(config.grpc_server_max_encoding_message_size as usize)
        .max_decoding_message_size(config.grpc_server_max_decoding_message_size as usize);

    let mut tonic_builder = RoutesBuilder::default();
    tonic_builder.add_service(scheduler_grpc_server);

    #[cfg(feature = "keda-scaler")]
    tonic_builder.add_service(ExternalScalerServer::new(scheduler.clone()));

    let tonic = tonic_builder.routes().into_axum_router();
    let tonic = tonic.fallback(|| async { (StatusCode::NOT_FOUND, "404 - Not Found") });

    #[cfg(feature = "rest-api")]
    let axum = get_routes(Arc::new(scheduler));
    #[cfg(feature = "rest-api")]
    let final_route = axum
        .merge(tonic)
        .into_make_service_with_connect_info::<SocketAddr>();

    #[cfg(not(feature = "rest-api"))]
    let final_route = tonic.into_make_service_with_connect_info::<SocketAddr>();

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(BallistaError::from)?;

    axum::serve(listener, final_route)
        .await
        .map_err(BallistaError::from)
}

fn start_flight_proxy_server(
    config: Arc<SchedulerConfig>,
) -> JoinHandle<Result<(), BallistaError>> {
    tokio::spawn(async move {
        let address = match config.advertise_flight_sql_endpoint.clone() {
            Some(flight_sql_endpoint) => flight_sql_endpoint
                .parse::<SocketAddr>()
                .map_err(|e: std::net::AddrParseError| {
                    error!(
                        "Error parsing advertise_flight_sql_endpoint: {}",
                        e.to_string()
                    );
                    BallistaError::Configuration(e.to_string())
                })?,
            _ => {
                return Err(BallistaError::Configuration(
                    "Expected advertise flight sql endpoint".into(),
                ));
            }
        };

        let max_encoding_message_size =
            config.grpc_server_max_encoding_message_size as usize;
        let max_decoding_message_size =
            config.grpc_server_max_decoding_message_size as usize;
        info!("Built-in arrow flight server proxy listening on: {address:?} max_encoding_size: {max_encoding_message_size} max_decoding_size: {max_decoding_message_size}");

        let grpc_server_config = GrpcServerConfig::default();
        let server_future = create_grpc_server(&grpc_server_config)
            .add_service(
                FlightServiceServer::new(BallistaFlightProxyService::new())
                    .max_decoding_message_size(max_decoding_message_size)
                    .max_encoding_message_size(max_encoding_message_size),
            )
            .serve(address);

        server_future.await.map_err(|e| {
            error!("Could not start built-in arrow flight server.");
            BallistaError::TonicError(e)
        })
    })
}

/// Creates scheduler and exposes it as grpc service
///
/// Method is a helper method which calls [create_scheduler] and [start_grpc_service]
pub async fn start_server(
    cluster: BallistaCluster,
    address: SocketAddr,
    config: Arc<SchedulerConfig>,
) -> ballista_core::error::Result<()> {
    info!("Ballista v{BALLISTA_VERSION} Scheduler listening on {address:?}");
    let scheduler =
        create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(cluster, config.clone())
            .await?;

    info!(
        "advertise_flight_sql_endpoint: {:?}",
        config.advertise_flight_sql_endpoint
    );
    match config.advertise_flight_sql_endpoint {
        Some(_) => {
            info!("Starting flight proxy");
            let _flight_proxy = start_flight_proxy_server(config);
            start_grpc_service(address, scheduler).await
        }
        None => start_grpc_service(address, scheduler).await,
    }
}
