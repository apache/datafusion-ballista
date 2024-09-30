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

use anyhow::{Error, Result};
#[cfg(feature = "flight-sql")]
use arrow_flight::flight_service_server::FlightServiceServer;
use kapot_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use kapot_core::serde::KapotCodec;
use kapot_core::utils::create_grpc_server;
use kapot_core::KAPOT_VERSION;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use log::info;
use tokio::net::TcpListener;
use std::{net::SocketAddr, sync::Arc};

use crate::api::get_routes;
use crate::cluster::KapotCluster;
use crate::config::SchedulerConfig;
use crate::flight_sql::FlightSqlServiceImpl;
use crate::metrics::default_metrics_collector;
use crate::scheduler_server::externalscaler::external_scaler_server::ExternalScalerServer;
use crate::scheduler_server::SchedulerServer;

pub async fn start_server(
    cluster: KapotCluster,
    listener: TcpListener,
    config: Arc<SchedulerConfig>,
) -> Result<()> {
    info!(
        "kapot v{} Scheduler listening on {:?}",
        KAPOT_VERSION, listener.local_addr()
    );
    // Should only call SchedulerServer::new() once in the process
    info!(
        "Starting Scheduler grpc server with task scheduling policy of {:?}",
        config.scheduling_policy
    );

    let metrics_collector = default_metrics_collector()?;

    let mut scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            config.scheduler_name(),
            cluster,
            KapotCodec::default(),
            config,
            metrics_collector,
        );

    scheduler_server.init().await?;

    let config = &scheduler_server.state.config;
    let scheduler_grpc_server = SchedulerGrpcServer::new(scheduler_server.clone())
        .max_encoding_message_size(config.grpc_server_max_encoding_message_size as usize)
        .max_decoding_message_size(config.grpc_server_max_decoding_message_size as usize);

    let keda_scaler = ExternalScalerServer::new(scheduler_server.clone());

    let tonic_builder = create_grpc_server()
        .add_service(scheduler_grpc_server)
        .add_service(keda_scaler);

    #[cfg(feature = "flight-sql")]
    let tonic_builder = tonic_builder.add_service(FlightServiceServer::new(
        FlightSqlServiceImpl::new(scheduler_server.clone()),
    ));

    let tonic = tonic_builder.into_service().into_axum_router();

    let base_routes = get_routes(Arc::new(scheduler_server));
    let merged = base_routes
        .merge(tonic)
        .into_make_service_with_connect_info::<SocketAddr>();

    axum::serve(listener, merged)
        .await
        .map_err(Error::from)
}
