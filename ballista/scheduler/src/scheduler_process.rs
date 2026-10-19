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
use ballista_core::flight_proxy_service::BallistaFlightProxyService;

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
use ballista_core::utils::{serve_until_shutdown, shutdown_signal, wait_until_idle};
use datafusion::DATAFUSION_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use http::{HeaderName, HeaderValue, StatusCode};
use log::info;
use std::time::Duration;
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
        config.scheduler_endpoint(),
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

/// Exposes scheduler grpc service on `address`.
pub async fn start_grpc_service<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    address: SocketAddr,
    scheduler: SchedulerServer<T, U>,
) -> ballista_core::error::Result<()> {
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(BallistaError::from)?;

    start_grpc_service_with_listener(listener, scheduler).await
}

/// Exposes scheduler grpc service on an already-bound listener.
///
/// Use this when the caller needs the bound address before the service starts,
/// such as binding port 0 in tests, or when the listener comes from a socket
/// activation mechanism.
pub async fn start_grpc_service_with_listener<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    listener: tokio::net::TcpListener,
    scheduler: SchedulerServer<T, U>,
) -> ballista_core::error::Result<()> {
    let mut shutdown = scheduler.shutdown_signal();
    let config = scheduler.state.config.clone();
    let scheduler_grpc_server = SchedulerGrpcServer::new(scheduler.clone())
        .max_encoding_message_size(config.grpc_server_max_encoding_message_size as usize)
        .max_decoding_message_size(config.grpc_server_max_decoding_message_size as usize);

    let mut tonic_builder = RoutesBuilder::default();
    tonic_builder.add_service(scheduler_grpc_server);

    let max_decoding = config.grpc_server_max_decoding_message_size as usize;
    let max_encoding = config.grpc_server_max_encoding_message_size as usize;

    // Wrap the endpoint override function in BallistaConfigGrpcEndpoint
    let customize_endpoint = config
        .override_create_grpc_client_endpoint
        .clone()
        .map(|f| Arc::new(BallistaConfigGrpcEndpoint::new(f)));

    // These sizes configure the proxy's own client to the executors.
    let flight_proxy = BallistaFlightProxyService::new(
        max_decoding,
        max_encoding,
        config.use_tls,
        customize_endpoint,
    );

    let scheduler = Arc::new(scheduler);

    // The Flight SQL frontend and the standalone proxy both serve
    // `arrow.flight.protocol.FlightService`, so at most one can be mounted.
    // The frontend subsumes the proxy: its `do_get_fallback` forwards Ballista's
    // own partition-fetch tickets, so Ballista clients keep working either way.
    let flight_sql_enabled = config.flight_sql_enabled();

    #[cfg(feature = "flight-sql")]
    if flight_sql_enabled {
        let mut flight_sql = ballista_flight_sql::BallistaFlightSqlService::new(
            scheduler.clone(),
            flight_proxy.clone(),
        );

        if let Some(authenticator) = config.override_flight_sql_authenticator.clone() {
            flight_sql = flight_sql.with_authenticator(authenticator);
        }

        if flight_sql.allows_anonymous() {
            log::warn!(
                "Arrow Flight SQL is enabled with no authenticator: any client that can \
                 reach the scheduler port can run queries, and unauthenticated clients \
                 share a single session. Set \
                 SchedulerConfig::with_flight_sql_authenticator, or restrict network \
                 access, before using this outside a trusted network."
            );
        }

        info!("Adding Arrow Flight SQL service on scheduler");
        tonic_builder.add_service(
            FlightServiceServer::new(flight_sql)
                .max_decoding_message_size(max_decoding)
                .max_encoding_message_size(max_encoding),
        );
    }

    #[allow(deprecated)]
    let enable_embedded_flight_proxy = config.enable_embedded_flight_proxy;

    // Warn even when the Flight SQL frontend is mounted in its place: the flag
    // still makes the scheduler advertise itself to clients as the proxy.
    if enable_embedded_flight_proxy {
        log::warn!(
            "enable_embedded_flight_proxy is deprecated and will be removed in \
             57.0.0: it puts result traffic on the scheduler. Run \
             ballista-result-service and point advertise_flight_endpoint at it \
             instead."
        );
    }

    if !flight_sql_enabled && enable_embedded_flight_proxy {
        info!("Adding embedded flight proxy service on scheduler");
        tonic_builder.add_service(
            FlightServiceServer::new(flight_proxy)
                .max_decoding_message_size(max_decoding)
                .max_encoding_message_size(max_encoding),
        );
    }

    #[cfg(feature = "keda-scaler")]
    tonic_builder.add_service(ExternalScalerServer::new(scheduler.as_ref().clone()));

    let tonic = tonic_builder.routes().into_axum_router();

    // registering default handler for unmatched requests
    let tonic =
        tonic.fallback(|| async { SchedulerErrorResponse::new(StatusCode::NOT_FOUND) });

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

    axum::serve(listener, final_route)
        .with_graceful_shutdown(async move { shutdown.recv().await })
        .await
        .map_err(BallistaError::from)
}

/// Creates scheduler and exposes it as grpc service
///
/// Method is a helper method which calls [create_scheduler] and [start_grpc_service]
///
/// On SIGTERM/Ctrl-C, the scheduler stops taking new jobs and fails `/readyz`
/// while it keeps serving executors and clients, so the jobs it has can finish.
/// Then it stops accepting connections and waits for in-flight requests and
/// streams (including the embedded Flight proxy, when enabled) to drain. The
/// whole shutdown is bounded by
/// [`SchedulerConfig::graceful_shutdown_timeout_seconds`], so a stalled job or
/// client can't hang it forever.
pub async fn start_server(
    cluster: BallistaCluster,
    address: SocketAddr,
    config: Arc<SchedulerConfig>,
) -> ballista_core::error::Result<()> {
    info!(
        "Ballista Scheduler v{BALLISTA_VERSION} (DataFusion v{DATAFUSION_VERSION}) listening on {address:?}"
    );
    config.validate()?;
    let graceful_shutdown_timeout_seconds = config.graceful_shutdown_timeout_seconds;
    let scheduler =
        create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(cluster, config).await?;

    serve_until_shutdown(
        start_grpc_service(address, scheduler.clone()),
        shutdown_signal(),
        || {
            // Executors and clients are still served, so the jobs it has can
            // finish; new ones are refused and `/readyz` reports not ready.
            scheduler.start_draining();
            info!(
                "Scheduler draining: waiting for {} active jobs",
                scheduler.active_job_number()
            );
            wait_until_idle(|| scheduler.active_job_number())
        },
        || scheduler.trigger_shutdown(),
        Duration::from_secs(graceful_shutdown_timeout_seconds),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use ballista_core::shutdown::Shutdown;
    use std::sync::Mutex;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::{broadcast, oneshot};
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

    /// A one-shot [`axum::serve::Listener`] backed by an in-memory duplex
    /// stream, so tests can drive `axum::serve` without a real OS socket.
    /// Yields its one connection, then never accepts another.
    struct OnceListener {
        conn: Option<tokio::io::DuplexStream>,
    }

    impl axum::serve::Listener for OnceListener {
        type Io = tokio::io::DuplexStream;
        type Addr = ();

        async fn accept(&mut self) -> (Self::Io, Self::Addr) {
            match self.conn.take() {
                Some(io) => (io, ()),
                None => std::future::pending().await,
            }
        }

        fn local_addr(&self) -> std::io::Result<Self::Addr> {
            Ok(())
        }
    }

    /// A request the handler never completes must not let
    /// `.with_graceful_shutdown(...)` finish on its own: this is exactly the
    /// unbounded wait that requires callers of this future (see
    /// [`start_server`]) to bound it with [`tokio::time::timeout`].
    #[tokio::test]
    async fn graceful_shutdown_waits_forever_on_a_stalled_request() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));

        // Stands in for a client stalled mid-fetch: the handler is invoked
        // (so there is a genuine in-flight request when shutdown fires) but
        // never returns on its own.
        let router = axum::Router::new().route(
            "/stall",
            get(move || {
                if let Some(tx) = entered_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                std::future::pending::<&'static str>()
            }),
        );

        let (mut client, server_io) = tokio::io::duplex(4096);
        let listener = OnceListener {
            conn: Some(server_io),
        };

        let (notify_shutdown, _) = broadcast::channel::<()>(1);
        let shutdown = Shutdown::new(notify_shutdown.subscribe());

        let serve_fut = tokio::spawn(async move {
            axum::serve(listener, router.into_make_service())
                .with_graceful_shutdown(async move {
                    let mut shutdown = shutdown;
                    shutdown.recv().await
                })
                .await
        });

        client
            .write_all(b"GET /stall HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        // Wait for the handler to actually start before signaling shutdown,
        // so there is a real in-flight request to drain, not a race.
        entered_rx.await.unwrap();
        let _ = notify_shutdown.send(());

        let result = tokio::time::timeout(Duration::from_millis(200), serve_fut).await;
        assert!(
            result.is_err(),
            "graceful shutdown should still be waiting on the stalled request"
        );

        drop(client);
    }
}
