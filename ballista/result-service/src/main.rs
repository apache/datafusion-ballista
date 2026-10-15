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

//! Command-line front end for [`ballista_result_service::serve`].

use std::net::SocketAddr;
use std::time::Duration;

use ballista_result_service::{ResultServiceConfig, serve, shutdown_signal};
use clap::Parser;

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
    /// before a SIGKILL is. 0 waits as long as it takes.
    #[arg(long, default_value_t = 10)]
    graceful_shutdown_timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let config = Config::parse();

    let bind_address: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port)
        .parse()
        .map_err(|e| {
            format!(
                "invalid bind address {}:{}: {e}",
                config.bind_host, config.bind_port
            )
        })?;

    serve(
        ResultServiceConfig {
            bind_address,
            grpc_max_decoding_message_size: config.grpc_max_decoding_message_size,
            grpc_max_encoding_message_size: config.grpc_max_encoding_message_size,
            graceful_shutdown_timeout: Duration::from_secs(
                config.graceful_shutdown_timeout_seconds,
            ),
            ..Default::default()
        },
        shutdown_signal(),
    )
    .await?;
    Ok(())
}
