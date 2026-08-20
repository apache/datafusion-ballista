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

use crate::api::SchedulerErrorResponse;
use crate::flight_proxy_service::BallistaFlightProxyService;

#[cfg(feature = "rest-api")]
use crate::api::get_routes;
use crate::api::health_routes;
use crate::api::route_disabled;
use crate::cluster::BallistaCluster;
use crate::config::SchedulerConfig;
use crate::metrics::default_metrics_collector;
use crate::scheduler_server::SchedulerServer;
#[cfg(feature = "keda-scaler")]
use crate::scheduler_server::externalscaler::external_scaler_server::ExternalScalerServer;
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::BALLISTA_VERSION;
use ballista_core::error::BallistaError;
use ballista_core::extension::BallistaConfigGrpcEndpoint;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::{
    BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use datafusion::DATAFUSION_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use http::{HeaderName, HeaderValue, StatusCode};
use log::info;
use std::{net::SocketAddr, sync::Arc};
use tonic::service::RoutesBuilder;
use tower_http::set_header::SetResponseHeaderLayer;
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

/// Wraps a router so every response carries `Server` and `X-App-Version`
/// headers, regardless of whether it was handled by the REST or gRPC routes
/// merged into it.
fn with_version_headers(router: axum::Router) -> axum::Router {
    let server_value = HeaderValue::from_str(&format!("ballista/{BALLISTA_VERSION}"))
        .expect("BALLISTA_VERSION should be a valid header value");

    let datafusion_value =
        HeaderValue::from_str(&format!("datafusion/{DATAFUSION_VERSION}"))
            .expect("DATAFUSION_VERSION should be a valid header value");

    router
        .layer(SetResponseHeaderLayer::overriding(
            http::header::SERVER,
            server_value,
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("x-powered-by"),
            datafusion_value,
        ))
}

/// Exposes scheduler grpc service
pub async fn start_grpc_service<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    address: SocketAddr,
    scheduler: SchedulerServer<T, U>,
) -> ballista_core::error::Result<()> {
    let config = scheduler.state.config.clone();
    let scheduler_grpc_server = SchedulerGrpcServer::new(scheduler.clone())
        .max_encoding_message_size(config.grpc_server_max_encoding_message_size as usize)
        .max_decoding_message_size(config.grpc_server_max_decoding_message_size as usize);

    let mut tonic_builder = RoutesBuilder::default();
    tonic_builder.add_service(scheduler_grpc_server);

    if config.enable_embedded_flight_proxy {
        info!("Adding embedded flight proxy service on scheduler");
        let customize_endpoint = config
            .override_create_grpc_client_endpoint
            .clone()
            .map(|f| Arc::new(BallistaConfigGrpcEndpoint::new(f)));

        // `BallistaFlightProxyService::new` takes decoding before encoding; these sizes
        // configure the proxy's own client to the executors.
        let flight_proxy = FlightServiceServer::new(BallistaFlightProxyService::new(
            config.grpc_server_max_decoding_message_size as usize,
            config.grpc_server_max_encoding_message_size as usize,
            config.use_tls,
            customize_endpoint,
        ))
        .max_decoding_message_size(config.grpc_server_max_decoding_message_size as usize)
        .max_encoding_message_size(config.grpc_server_max_encoding_message_size as usize);
        tonic_builder.add_service(flight_proxy);
    }

    #[cfg(feature = "keda-scaler")]
    tonic_builder.add_service(ExternalScalerServer::new(scheduler.clone()));

    let tonic = tonic_builder.routes().into_axum_router();

    // registering default handler for unmatched requests
    let tonic =
        tonic.fallback(|| async { SchedulerErrorResponse::new(StatusCode::NOT_FOUND) });

    let scheduler = Arc::new(scheduler);
    let health = health_routes(scheduler.clone());

    #[cfg(feature = "rest-api")]
    let merged = if config.disable_rest_api {
        tonic
            .merge(route_disabled(
                "REST API has been disabled at startup".to_string(),
            ))
            .merge(health)
    } else {
        let axum = get_routes(scheduler);
        axum.merge(tonic).merge(health)
    };

    #[cfg(not(feature = "rest-api"))]
    let merged = tonic
        .merge(route_disabled(
            "REST API has been disabled at compile time".to_string(),
        ))
        .merge(health);

    let final_route =
        with_version_headers(merged).into_make_service_with_connect_info::<SocketAddr>();

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
        "Ballista Scheduler v{BALLISTA_VERSION} (DataFusion v{DATAFUSION_VERSION}) listening on {address:?}"
    );
    config.validate()?;
    let scheduler =
        create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(cluster, config).await?;

    start_grpc_service(address, scheduler).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use tower::ServiceExt;

    #[tokio::test]
    async fn adds_server_and_app_version_headers() {
        let router = with_version_headers(
            axum::Router::new().route("/ping", get(|| async { "pong" })),
        );

        let response = router
            .oneshot(
                http::Request::builder()
                    .uri("/ping")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.headers().get(http::header::SERVER).unwrap(),
            &format!("ballista/{BALLISTA_VERSION}")[..],
        );
        assert_eq!(
            response.headers().get("x-powered-by").unwrap(),
            &format!("datafusion/{DATAFUSION_VERSION}")[..],
        );
    }
}
