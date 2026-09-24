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

//! A Ballista executor whose function registry includes the chaos UDFs.
//!
//! Configured entirely from the environment, because `TestCluster` spawns it as
//! a child process. Mirrors `examples/examples/custom-executor.rs`.

use ballista_executor::executor_process::{
    ExecutorProcessConfig, start_executor_process,
};
use ballista_executor::health::spawn_health_server;
use chaos_testing::registry::chaos_function_registry;
use std::net::SocketAddr;
use std::sync::Arc;

fn env_u16(key: &str) -> u16 {
    std::env::var(key)
        .unwrap_or_else(|_| panic!("{key} must be set"))
        .parse()
        .unwrap_or_else(|e| panic!("{key} must be a u16: {e}"))
}

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    env_logger::init();

    let config = ExecutorProcessConfig {
        // Loopback in the process harness; under Kubernetes the executor binds
        // all interfaces, finds the scheduler by Service name, and advertises its
        // own pod IP so the scheduler and peers can reach it for Arrow Flight.
        bind_host: std::env::var("CHAOS_BIND_HOST")
            .unwrap_or_else(|_| "127.0.0.1".into()),
        external_host: std::env::var("CHAOS_EXECUTOR_EXTERNAL_HOST").ok(),
        port: env_u16("CHAOS_EXECUTOR_PORT"),
        grpc_port: env_u16("CHAOS_EXECUTOR_GRPC_PORT"),
        scheduler_host: std::env::var("CHAOS_SCHEDULER_HOST")
            .unwrap_or_else(|_| "127.0.0.1".into()),
        scheduler_port: env_u16("CHAOS_SCHEDULER_PORT"),
        scheduler_connect_timeout_seconds: 10,
        vcores: std::env::var("CHAOS_CONCURRENT_TASKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4),
        work_dir: std::env::var("CHAOS_WORK_DIR").ok(),
        // The default is 60s. Executor-loss scenarios need the scheduler to see a
        // missing heartbeat within seconds, not minutes.
        executor_heartbeat_interval_seconds: std::env::var("CHAOS_HEARTBEAT_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1),
        override_function_registry: Some(chaos_function_registry()),
        ..Default::default()
    };

    // Kubernetes health probes. `start_executor_process` (the library entry
    // point) does not serve them — only the standalone binary does — so wire the
    // HTTP probe server here on the config's shared `ExecutorHealth` handle,
    // exactly as `ballista-executor`'s `bin/main.rs` does. `/healthz` is process
    // liveness; `/readyz` reflects heartbeat state. Only started when
    // `CHAOS_EXECUTOR_HEALTH_PORT` is set (the k8s manifest sets it), so the
    // process-based `TestCluster` harness spawns no extra server.
    let health_server = std::env::var("CHAOS_EXECUTOR_HEALTH_PORT")
        .ok()
        .map(|port| {
            let port: u16 = port.parse().unwrap_or_else(|e| {
                panic!("CHAOS_EXECUTOR_HEALTH_PORT must be a u16: {e}")
            });
            let addr: SocketAddr = format!("{}:{}", config.bind_host, port)
                .parse()
                .expect("health server address must parse");
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let handle = spawn_health_server(addr, config.health.clone(), shutdown_rx);
            (shutdown_tx, handle)
        });

    let result = start_executor_process(Arc::new(config)).await;

    // Ask the health server to stop so the process can exit cleanly.
    if let Some((shutdown_tx, handle)) = health_server {
        let _ = shutdown_tx.send(());
        if let Err(e) = handle.await {
            log::warn!("health server task join error: {e}");
        }
    }
    result
}
