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

//! Ballista Result Service: serves query results to clients by forwarding each
//! partition fetch to the executor that holds the partition. Point the
//! scheduler's `--advertise-flight-endpoint` at it.
//!
//! The `ballista-result-service` binary runs [`serve`] with a configuration
//! from its command line. Call [`serve`] directly to customize the connections
//! to executors, as `SchedulerConfig::override_create_grpc_client_endpoint`
//! does for the scheduler's embedded proxy.

mod http;
mod metered;
mod metrics;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::error::{BallistaError, Result};
use ballista_core::extension::{BallistaConfigGrpcEndpoint, EndpointOverrideFn};
use ballista_core::serving::{ForwardingBackend, ResultBackend, ServingFlightService};
use ballista_core::utils::{GrpcServerConfig, create_grpc_server};
use log::{error, info, warn};
use tonic::service::Routes;
use tonic::transport::server::Router;

use crate::metered::MeteredBackend;
use crate::metrics::{ResultMetricsCollector, default_metrics_collector};

/// Configuration for [`serve`].
#[derive(Clone)]
pub struct ResultServiceConfig {
    /// Address the Flight service, health probes (`/healthz` and `/readyz`)
    /// and metrics (`/api/metrics`) bind to.
    pub bind_address: SocketAddr,
    /// Maximum gRPC message size, in bytes, the service decodes.
    pub grpc_max_decoding_message_size: usize,
    /// Maximum gRPC message size, in bytes, the service encodes.
    pub grpc_max_encoding_message_size: usize,
    /// Time to let in-flight result streams finish after the shutdown signal
    /// before stopping anyway.
    pub graceful_shutdown_timeout: Duration,
    /// Connect to executors with `https://` URLs. The connections are only
    /// encrypted in a build with one of tonic's TLS features, typically with
    /// [`Self::override_create_grpc_client_endpoint`] setting the TLS
    /// configuration.
    pub use_tls: bool,
    /// Customizes the endpoint of each connection to an executor, as
    /// `SchedulerConfig::override_create_grpc_client_endpoint` does for the
    /// embedded proxy.
    pub override_create_grpc_client_endpoint: Option<EndpointOverrideFn>,
}

impl Default for ResultServiceConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([0, 0, 0, 0], 50055)),
            grpc_max_decoding_message_size: 16 * 1024 * 1024,
            grpc_max_encoding_message_size: 16 * 1024 * 1024,
            graceful_shutdown_timeout: Duration::from_secs(10),
            use_tls: false,
            override_create_grpc_client_endpoint: None,
        }
    }
}

/// Serves results until `shutdown` completes, then stops accepting connections
/// and gives in-flight result streams up to
/// [`ResultServiceConfig::graceful_shutdown_timeout`] to finish.
pub async fn serve(
    config: ResultServiceConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let backend = ForwardingBackend::new(
        config.grpc_max_decoding_message_size,
        config.grpc_max_encoding_message_size,
        config.use_tls,
        config
            .override_create_grpc_client_endpoint
            .clone()
            .map(|f| Arc::new(BallistaConfigGrpcEndpoint::new(f))),
    );
    let addr = config.bind_address;
    let server = server(backend, &config, default_metrics_collector()?);

    info!("Ballista Result Service listening on {addr}");

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = server.serve_with_shutdown(addr, async {
        let _ = stop_rx.await;
    });
    tokio::pin!(server);

    // Serve until the shutdown signal, unless the server fails first.
    tokio::select! {
        result = &mut server => {
            result.map_err(BallistaError::TonicError)?;
            info!("Ballista Result Service stopped");
            return Ok(());
        }
        () = shutdown => {}
    }

    // Stop accepting connections, then give in-flight streams a bounded time to
    // finish.
    let _ = stop_tx.send(());
    drain_or_time_out(server, config.graceful_shutdown_timeout)
        .await
        .map_err(BallistaError::TonicError)?;

    info!("Ballista Result Service stopped");
    Ok(())
}

/// Completes on Ctrl-C, or on SIGTERM where the platform has it.
///
/// If a signal cannot be listened for, that branch never completes, rather than
/// shutting the server down at startup.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("failed to listen for Ctrl-C: {e}");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(e) => {
                error!("failed to listen for SIGTERM: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!("Received Ctrl-C, shutting down"),
        () = terminate => info!("Received SIGTERM, shutting down"),
    }
}

/// The Flight service, health probes and metrics, served on one port.
fn server<B: ResultBackend>(
    backend: B,
    config: &ResultServiceConfig,
    metrics: Arc<dyn ResultMetricsCollector>,
) -> Router {
    let backend = MeteredBackend::new(backend, metrics.clone());
    let service = FlightServiceServer::new(ServingFlightService::new(backend))
        .max_decoding_message_size(config.grpc_max_decoding_message_size)
        .max_encoding_message_size(config.grpc_max_encoding_message_size);
    let routes = Routes::new(service)
        .into_axum_router()
        .merge(http::router(metrics));

    create_grpc_server(&GrpcServerConfig::default())
        // Health probes and metrics scrapers use HTTP/1.1.
        .accept_http1(true)
        .add_routes(Routes::from(routes))
}

/// Waits for the shutting-down `server` to finish its in-flight streams, but no
/// longer than `bound`, after which the streams still running are abandoned.
async fn drain_or_time_out<E>(
    server: impl Future<Output = std::result::Result<(), E>>,
    bound: Duration,
) -> std::result::Result<(), E> {
    match tokio::time::timeout(bound, server).await {
        Ok(result) => result,
        Err(_) => {
            warn!(
                "Graceful shutdown timed out after {}s; exiting with result \
                 streams still in flight",
                bound.as_secs()
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::NoopMetricsCollector;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tonic::transport::server::TcpIncoming;

    fn forwarding_backend() -> ForwardingBackend {
        ForwardingBackend::new(16 * 1024 * 1024, 16 * 1024 * 1024, false, None)
    }

    #[tokio::test]
    async fn health_probes_are_served_over_http1_on_the_flight_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = server(
            forwarding_backend(),
            &ResultServiceConfig::default(),
            Arc::new(NoopMetricsCollector),
        )
        .serve_with_incoming(TcpIncoming::from(listener));
        let server = tokio::spawn(server);

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        server.abort();

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("ready"), "{response}");
    }

    #[tokio::test]
    async fn a_stalled_drain_gives_up_once_the_bound_elapses() {
        let result: std::result::Result<(), String> =
            drain_or_time_out(std::future::pending(), Duration::from_millis(50)).await;
        assert!(result.is_ok(), "must give up after the bound, not hang");
    }

    #[tokio::test]
    async fn a_finished_drain_passes_its_result_through() {
        let ok: std::result::Result<(), String> =
            drain_or_time_out(async { Ok(()) }, Duration::from_secs(10)).await;
        assert!(ok.is_ok());

        let failed = drain_or_time_out(
            async { Err::<(), _>("server failed".to_string()) },
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(failed, Err("server failed".to_string()));
    }
}
