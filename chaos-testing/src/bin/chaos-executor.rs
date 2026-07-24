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
use chaos_testing::registry::chaos_function_registry;
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
        bind_host: "127.0.0.1".to_string(),
        port: env_u16("CHAOS_EXECUTOR_PORT"),
        grpc_port: env_u16("CHAOS_EXECUTOR_GRPC_PORT"),
        scheduler_host: "127.0.0.1".to_string(),
        scheduler_port: env_u16("CHAOS_SCHEDULER_PORT"),
        concurrent_tasks: std::env::var("CHAOS_CONCURRENT_TASKS")
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

    start_executor_process(Arc::new(config)).await
}
