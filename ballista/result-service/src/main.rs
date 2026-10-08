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

//! Ballista standalone Result Service binary.
//!
//! A stateless data plane that serves query results to clients, decoupled from
//! the scheduler (control plane). In this first increment it forwards each fetch
//! to the producing executor named in the request — the same behavior as the
//! scheduler's deprecated embedded proxy, but as an independently scalable fleet
//! that is off the scheduler's data path. Point the scheduler's `--advertise-flight-endpoint`
//! at this service and clients fetch results here with no client changes.

use std::net::SocketAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::serving::{ForwardingBackend, ServingFlightService};
use ballista_core::utils::{GrpcServerConfig, create_grpc_server};
use clap::Parser;
use log::{error, info};

/// Command-line configuration for the Result Service.
#[derive(Debug, Parser)]
#[command(name = "ballista-result-service", version, about)]
struct Config {
    /// Host or IP address the Flight service binds to.
    #[arg(long, default_value = "0.0.0.0")]
    bind_host: String,

    /// Port the Flight service binds to.
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

    // Forwarding backend: relay every fetch to the executor named in the request.
    // This is the shared serving core, so behavior matches the executor's own
    // serving path and the scheduler's embedded proxy.
    let backend = ForwardingBackend::new(
        config.grpc_max_decoding_message_size,
        config.grpc_max_encoding_message_size,
        config.use_tls,
        None,
    );
    let service = FlightServiceServer::new(ServingFlightService::new(backend))
        .max_decoding_message_size(config.grpc_max_decoding_message_size)
        .max_encoding_message_size(config.grpc_max_encoding_message_size);

    info!("Ballista Result Service listening on {addr} (forwarding mode)");

    create_grpc_server(&GrpcServerConfig::default())
        .add_service(service)
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    info!("Ballista Result Service stopped");
    Ok(())
}

/// Completes on Ctrl-C, or on SIGTERM where the platform has it, so the server
/// stops accepting new fetches and lets in-flight streams finish. SIGTERM is
/// what Kubernetes sends before it kills a pod.
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
