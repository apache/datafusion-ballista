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

mod http;
mod metered;
mod metrics;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::serving::{ForwardingBackend, ServingFlightService};
use ballista_core::utils::{GrpcServerConfig, create_grpc_server};
use clap::Parser;
use log::{error, info, warn};
use tonic::service::Routes;
use tonic::transport::server::Router;

use crate::metered::MeteredBackend;
use crate::metrics::{ResultMetricsCollector, default_metrics_collector};

/// Command-line configuration for the result service.
#[derive(Debug, Parser)]
#[command(name = "ballista-result-service", version, about)]
struct Config {
    /// Host or IP address the Flight service binds to.
    #[arg(long, default_value = "0.0.0.0")]
    bind_host: String,

    /// Port the Flight service, health probes (/healthz and /readyz) and
    /// metrics (/api/metrics) bind to.
    #[arg(long, default_value_t = 50055)]
    bind_port: u16,

    /// Use TLS when connecting to executors.
    #[arg(long, default_value_t = false)]
    use_tls: bool,

    /// Maximum gRPC message size, in bytes, this service will decode.
    #[arg(long, default_value_t = 16777216)]
    grpc_max_decoding_message_size: usize,

    /// Maximum gRPC message size, in bytes, this service will encode.
    #[arg(long, default_value_t = 16777216)]
    grpc_max_encoding_message_size: usize,

    /// Time in seconds to let in-flight result streams finish after a
    /// shutdown signal (SIGTERM or Ctrl-C) before exiting anyway. Keep it below
    /// your orchestrator's termination grace period (Kubernetes'
    /// terminationGracePeriodSeconds, 30 by default), so this bound is reached
    /// before a SIGKILL is.
    #[arg(long, default_value_t = 10)]
    graceful_shutdown_timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let config = Config::parse();

    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port)
        .parse()
        .map_err(|e| {
            format!(
                "invalid bind address {}:{}: {e}",
                config.bind_host, config.bind_port
            )
        })?;

    let server = server(&config, default_metrics_collector()?);

    info!("Ballista Result Service listening on {addr}");

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = server.serve_with_shutdown(addr, async {
        let _ = stop_rx.await;
    });
    tokio::pin!(server);

    // Serve until a shutdown signal arrives, unless the server fails first.
    tokio::select! {
        result = &mut server => {
            result?;
            info!("Ballista Result Service stopped");
            return Ok(());
        }
        () = shutdown_signal() => {}
    }

    // Stop accepting connections, then give in-flight streams a bounded time to
    // finish.
    let _ = stop_tx.send(());
    drain_or_time_out(
        server,
        Duration::from_secs(config.graceful_shutdown_timeout_seconds),
    )
    .await?;

    info!("Ballista Result Service stopped");
    Ok(())
}

/// The Flight service, health probes and metrics, served on one port.
fn server(config: &Config, metrics: Arc<dyn ResultMetricsCollector>) -> Router {
    let backend = MeteredBackend::new(
        ForwardingBackend::new(
            config.grpc_max_decoding_message_size,
            config.grpc_max_encoding_message_size,
            config.use_tls,
            None,
        ),
        metrics.clone(),
    );
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
    server: impl Future<Output = Result<(), E>>,
    bound: Duration,
) -> Result<(), E> {
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

/// Completes on Ctrl-C, or on SIGTERM where the platform has it.
///
/// If a signal cannot be listened for, that branch never completes, rather than
/// shutting the server down at startup.
async fn shutdown_signal() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::NoopMetricsCollector;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tonic::transport::server::TcpIncoming;

    #[tokio::test]
    async fn health_probes_are_served_over_http1_on_the_flight_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = Config::parse_from(["ballista-result-service"]);
        let server = server(&config, Arc::new(NoopMetricsCollector))
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
        let result: Result<(), String> =
            drain_or_time_out(std::future::pending(), Duration::from_millis(50)).await;
        assert!(result.is_ok(), "must give up after the bound, not hang");
    }

    #[tokio::test]
    async fn a_finished_drain_passes_its_result_through() {
        let ok: Result<(), String> =
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
