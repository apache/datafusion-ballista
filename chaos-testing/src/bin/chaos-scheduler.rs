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

//! A Ballista scheduler whose session builder includes the chaos UDFs, with
//! executor-loss detection tuned down from minutes to seconds.
//!
//! Mirrors `examples/examples/custom-scheduler.rs`.

use ballista_core::error::BallistaError;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use chaos_testing::registry::chaos_session_state;
use std::net::AddrParseError;
use std::sync::Arc;

fn env_parsed<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    env_logger::init();

    let config = SchedulerConfig {
        bind_host: "127.0.0.1".to_string(),
        bind_port: std::env::var("CHAOS_SCHEDULER_PORT")
            .expect("CHAOS_SCHEDULER_PORT must be set")
            .parse()
            .expect("CHAOS_SCHEDULER_PORT must be a u16"),
        // Defaults are 180s / 15s, which would make every executor-kill scenario
        // take three minutes. Tests override these per scenario.
        executor_timeout_seconds: env_parsed("CHAOS_EXECUTOR_TIMEOUT_SECONDS", 5),
        expire_dead_executor_interval_seconds: env_parsed(
            "CHAOS_EXPIRE_INTERVAL_SECONDS",
            1,
        ),
        task_max_failures: env_parsed("CHAOS_TASK_MAX_FAILURES", 4),
        stage_max_failures: env_parsed("CHAOS_STAGE_MAX_FAILURES", 4),
        override_session_builder: Some(Arc::new(chaos_session_state)),
        ..Default::default()
    };

    let addr = format!("{}:{}", config.bind_host, config.bind_port);
    let addr = addr
        .parse()
        .map_err(|e: AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await
}
