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

//! A standalone Ballista Result Service for the chaos harness.
//!
//! Configured entirely from the environment, because `TestCluster` spawns it as
//! a child process (mirroring `chaos-scheduler`/`chaos-executor`). It forwards
//! every result fetch to the producing executor named in the request, using the
//! shared serving core in `ballista-core` — the same `ForwardingBackend` the
//! scheduler's embedded proxy uses and the standalone `ballista-result-service`
//! binary uses. The harness points the scheduler's `advertise_flight_endpoint`
//! at this process, so clients fetch results here instead of dialing executors.

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::serving::{ForwardingBackend, ServingFlightService};
use ballista_core::utils::{GrpcServerConfig, create_grpc_server};
use std::net::SocketAddr;

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    env_logger::init();

    let bind_host =
        std::env::var("CHAOS_BIND_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let bind_port: u16 = std::env::var("CHAOS_RESULT_SERVICE_PORT")
        .expect("CHAOS_RESULT_SERVICE_PORT must be set")
        .parse()
        .expect("CHAOS_RESULT_SERVICE_PORT must be a u16");

    let addr: SocketAddr = format!("{bind_host}:{bind_port}")
        .parse()
        .expect("result service address must parse");

    // Forwarding mode: relay each fetch to the executor named in the request.
    // `use_tls = false` — the harness runs everything on loopback.
    let backend = ForwardingBackend::new(MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE, false, None);
    let service = FlightServiceServer::new(ServingFlightService::new(backend))
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    log::info!("chaos result service listening on {addr} (forwarding mode)");

    create_grpc_server(&GrpcServerConfig::default())
        .add_service(service)
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(ballista_core::error::BallistaError::TonicError)
}
