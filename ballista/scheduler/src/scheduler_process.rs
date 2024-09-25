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
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::create_grpc_server;
use ballista_core::BALLISTA_VERSION;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use log::info;
use std::{net::SocketAddr, sync::Arc};

use crate::api::get_routes;
use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;
use crate::flight_sql::FlightSqlServiceImpl;
use crate::metrics::default_metrics_collector;
use crate::scheduler_server::externalscaler::external_scaler_server::ExternalScalerServer;
use crate::scheduler_server::SchedulerServer;

pub async fn start_server(
    cluster: BallistaCluster,
    addr: SocketAddr,
    config: Arc<SchedulerConfig>,
) -> Result<()> {
    info!(
        "Ballista v{} Scheduler listening on {:?}",
        BALLISTA_VERSION, addr
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
            BallistaCodec::default(),
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

    let axum = get_routes(Arc::new(scheduler_server));
    let merged = axum
        .merge(tonic)
        .into_make_service_with_connect_info::<SocketAddr>();

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, merged).await.map_err(Error::from)
}
