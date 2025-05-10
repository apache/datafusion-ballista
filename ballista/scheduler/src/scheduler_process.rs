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

use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::{
    BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use ballista_core::utils::create_grpc_server;
use ballista_core::BALLISTA_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use http::StatusCode;
use log::info;
use std::{net::SocketAddr, sync::Arc};

#[cfg(feature = "rest-api")]
use crate::api::get_routes;
use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;

use crate::metrics::default_metrics_collector;
#[cfg(feature = "keda-scaler")]
use crate::scheduler_server::externalscaler::external_scaler_server::ExternalScalerServer;
use crate::scheduler_server::SchedulerServer;

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

    let tonic_builder = create_grpc_server().add_service(scheduler_grpc_server);

    #[cfg(feature = "keda-scaler")]
    let tonic_builder =
        tonic_builder.add_service(ExternalScalerServer::new(scheduler.clone()));

    let tonic = tonic_builder.into_service().into_axum_router();
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

/// Creates scheduler and exposes it as grpc service
///
/// Method is a helper method which calls [create_scheduler] and [start_grpc_service]
pub async fn start_server(
    cluster: BallistaCluster,
    address: SocketAddr,
    config: Arc<SchedulerConfig>,
) -> ballista_core::error::Result<()> {
    info!(
        "Ballista v{} Scheduler listening on {:?}",
        BALLISTA_VERSION, address
    );
    let scheduler =
        create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(cluster, config).await?;

    start_grpc_service(address, scheduler).await
}
