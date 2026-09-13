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

//! # Shuffle Affinity Scheduler
//!
//! Starts a scheduler that runs each task on the executor holding most of its shuffle
//! input, using [`ShuffleAffinityPolicy`]. Each scheduling round that places tasks logs
//! how many shuffle bytes were placed locally.
//!
//! `--bind-host` (an IP address, default `0.0.0.0`) and `--external-host` (the name
//! executors connect to, default `localhost`) match `ballista-scheduler`.
//!
//! ## Running
//!
//! ```bash
//! # Terminal 1: start this scheduler in place of `ballista-scheduler`
//! cargo run --release --example shuffle-affinity
//!
//! # Terminals 2 and 3: start two executors
//! RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50051
//! RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50052
//!
//! # Terminal 4: run a query
//! cargo run --release --example remote-sql
//! ```
//!
//! On a cluster, advertise a name the executors can reach and point them at it:
//!
//! ```bash
//! cargo run --release --example shuffle-affinity -- --external-host scheduler.example.com
//! ./target/release/ballista-executor --scheduler-host scheduler.example.com
//! ```

use std::net::AddrParseError;
use std::sync::Arc;

use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::error::{BallistaError, Result};
use ballista_examples::shuffle_affinity::{LocalityStats, ShuffleAffinityPolicy};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::scheduler_process::start_server;
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let (bind_host, external_host) = host_args()?;

    let policy = ShuffleAffinityPolicy::new();
    // Called after each round that places tasks; forward it to your own metrics here.
    policy.attach_observer(Arc::new(|round: &LocalityStats| {
        info!(
            "shuffle affinity placed {} of {} shuffle bytes locally ({:.1}%)",
            round.local_bytes,
            round.total_bytes,
            round.local_byte_ratio() * 100.0,
        );
    }));

    // The policy only applies when the scheduler pushes tasks to executors.
    let config = SchedulerConfig {
        bind_host,
        ..Default::default()
    }
    .with_hostname(external_host)
    .with_scheduler_policy(TaskSchedulingPolicy::PushStaged)
    .with_task_distribution(TaskDistributionPolicy::Custom(Arc::new(policy)));

    let addr = format!("{}:{}", config.bind_host, config.bind_port);
    let addr = addr
        .parse()
        .map_err(|e: AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await
}

/// Reads `--bind-host` and `--external-host`, defaulting to `ballista-scheduler`'s values.
fn host_args() -> Result<(String, String)> {
    let mut bind_host = "0.0.0.0".to_string();
    let mut external_host = "localhost".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let target = match flag.as_str() {
            "--bind-host" => &mut bind_host,
            "--external-host" => &mut external_host,
            _ => {
                return Err(BallistaError::Configuration(format!(
                    "unknown argument {flag}; expected --bind-host or --external-host"
                )));
            }
        };
        *target = args.next().ok_or_else(|| {
            BallistaError::Configuration(format!("{flag} needs a value"))
        })?;
    }
    Ok((bind_host, external_host))
}
