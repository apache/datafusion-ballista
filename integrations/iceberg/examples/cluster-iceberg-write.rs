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

//! # Multi-executor Iceberg cluster
//!
//! A deployed iceberg-ballista cluster is three kinds of process, and each must
//! install the Iceberg plan codecs — codecs are process configuration, not
//! something that travels over the wire:
//!
//! | process   | codecs are installed via                                   |
//! |-----------|------------------------------------------------------------|
//! | scheduler | `SchedulerConfig::override_{logical,physical}_codec`       |
//! | executor  | `ExecutorProcessConfig::override_{logical,physical}_codec` |
//! | client    | `register_iceberg_codecs` on its `SessionConfig`           |
//!
//! The stock `ballista-scheduler` / `ballista-executor` binaries have no flag
//! for this, so the scheduler and executor are small *custom* binaries.
//! [`run_scheduler`], [`run_executor`] and [`run_client`] below are the complete
//! recipe: each is what the corresponding `main.rs` contains. Notice that only
//! the client is configured with the catalog — the `IcebergCatalogConfig`
//! travels inside the serialized plans, and scheduler/executors rebuild their
//! catalog connections from it. (It includes storage credentials, so run the
//! cluster on a trusted/TLS network.)
//!
//! Everything below the recipe is demo plumbing: this example re-invokes itself
//! as `scheduler`/`executor` child processes and starts a dockerized Iceberg
//! REST catalog + MinIO, so one command runs a real one-scheduler, two-executor
//! cluster through a distributed INSERT and SELECT:
//!
//! ```bash
//! cargo run -p iceberg-ballista --example cluster-iceberg-write
//! ```
//!
//! All it needs is a running docker daemon. The child processes are killed and
//! the containers removed on the way out, including on `Ctrl-C`; only a hard
//! kill of the demo can leave a scheduler and two executors behind.

// Shared with the integration tests rather than duplicated.
#[path = "../tests/fixture/mod.rs"]
mod fixture;

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_executor::executor_process::{
    ExecutorProcessConfig, start_executor_process,
};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use iceberg::NamespaceIdent;
use iceberg_ballista::{
    IcebergCatalogConfig, IcebergLogicalCodec, IcebergPhysicalCodec,
    register_iceberg_codecs, register_iceberg_table,
};
use tokio::process::{Child, Command};

type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// ## 1. The scheduler binary
///
/// Decodes the client's logical plan (which carries the Iceberg table provider)
/// and encodes physical plan fragments for the executors, so it needs both
/// codecs.
async fn run_scheduler(port: u16) -> ExampleResult<()> {
    let config = Arc::new(SchedulerConfig {
        bind_port: port,
        // Pull-based only so the demo need not wait for executors (see
        // `run_demo`); a real deployment keeps the PushStaged default, and
        // whichever it picks the executors must match.
        scheduling_policy: TaskSchedulingPolicy::PullStaged,
        // The same pair of codecs the executors install below.
        override_logical_codec: Some(Arc::new(IcebergLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(IcebergPhysicalCodec::default())),
        ..Default::default()
    });
    let cluster = BallistaCluster::new_from_config(&config).await?;
    let address = format!("{}:{}", config.bind_host, config.bind_port).parse()?;
    start_server(cluster, address, config).await?;
    Ok(())
}

/// ## 2. The executor binary
///
/// Decodes the plan fragments the scheduler ships it — the Iceberg
/// scan/write/commit nodes. Without the codecs, every Iceberg query fails at
/// decode with an unknown-extension error.
async fn run_executor(
    scheduler_port: u16,
    flight_port: u16,
    grpc_port: u16,
) -> ExampleResult<()> {
    let config = ExecutorProcessConfig {
        // Ports are parameters only because the demo packs several executors
        // onto one host; a real deployment keeps the defaults.
        port: flight_port,
        grpc_port,
        scheduler_port,
        // Tolerate coming up before the scheduler does.
        scheduler_connect_timeout_seconds: 30,
        // Must agree with the scheduler's policy above.
        task_scheduling_policy: TaskSchedulingPolicy::PullStaged,
        override_logical_codec: Some(Arc::new(IcebergLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(IcebergPhysicalCodec::default())),
        ..Default::default()
    };
    start_executor_process(Arc::new(config)).await?;
    Ok(())
}

/// ## 3. The client
///
/// Any DataFusion application: install the codecs on the session config,
/// connect to the scheduler, register the table with its catalog config — then
/// plain SQL runs distributed.
async fn run_client(
    scheduler_url: &str,
    catalog_props: HashMap<String, String>,
    namespace: NamespaceIdent,
    table: String,
) -> ExampleResult<()> {
    let config = register_iceberg_codecs(
        SessionConfig::new_with_ballista().with_target_partitions(4),
    );
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::remote_with_state(scheduler_url, state).await?;

    let catalog_config = IcebergCatalogConfig::new("rest", "rest", catalog_props);
    register_iceberg_table(&ctx, "events", catalog_config, namespace, table).await?;

    // IcebergWriteExec runs on the executors; IcebergCommitExec appends one
    // atomic snapshot.
    println!("== INSERT ==");
    ctx.sql(
        "INSERT INTO events VALUES \
         (1, 'alice'), (2, 'bob'), (3, 'carol'), \
         (4, 'dave'), (5, 'erin'), (6, 'frank')",
    )
    .await?
    .show()
    .await?;

    println!("== SELECT ==");
    ctx.sql("SELECT id, name FROM events ORDER BY id")
        .await?
        .show()
        .await?;

    Ok(())
}

// ===========================================================================
// Demo plumbing — not part of the recipe.
//
// One binary, three roles: with no arguments it runs the demo, spawning itself
// as the scheduler and executor processes, so each role genuinely runs as its
// own OS process talking gRPC.
// ===========================================================================

const N_EXECUTORS: usize = 2;

const USAGE: &str = "usage: cluster-iceberg-write [scheduler <port> | executor <scheduler-port> <flight-port> <grpc-port>]";

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        [] => run_demo().await,
        ["scheduler", port] => run_scheduler(port.parse()?).await,
        ["executor", scheduler_port, flight_port, grpc_port] => {
            run_executor(
                scheduler_port.parse()?,
                flight_port.parse()?,
                grpc_port.parse()?,
            )
            .await
        }
        _ => Err(USAGE.into()),
    }
}

async fn run_demo() -> ExampleResult<()> {
    // Iceberg REST catalog + MinIO in docker, removed when this drops.
    println!("== starting Iceberg REST catalog and MinIO ==");
    let catalog_fixture = fixture::IcebergFixture::start().await;
    let props = catalog_fixture.props();
    let (namespace, table) = fixture::create_demo_table(&props).await;

    let scheduler_port = free_port()?;
    println!("== starting scheduler process on port {scheduler_port} ==");
    let mut scheduler = spawn_role(&["scheduler", &scheduler_port.to_string()])?;
    let scheduler_url = format!("http://localhost:{scheduler_port}");
    wait_for_scheduler(&scheduler_url, &mut scheduler).await?;

    // No need to wait for the executors: under pull-based scheduling, queued
    // tasks simply wait until an executor registers and polls for work.
    let mut executors = Vec::with_capacity(N_EXECUTORS);
    for i in 0..N_EXECUTORS {
        println!("== starting executor process {i} ==");
        executors.push(spawn_role(&[
            "executor",
            &scheduler_port.to_string(),
            &free_port()?.to_string(),
            &free_port()?.to_string(),
        ])?);
    }

    // Returning normally — on Ctrl-C too — drops the children and the fixture,
    // killing the processes and removing the containers.
    tokio::select! {
        result = run_client(&scheduler_url, props, namespace, table) => result,
        _ = tokio::signal::ctrl_c() => {
            println!("== interrupted, shutting the cluster down ==");
            Ok(())
        }
    }
}

/// Asks the OS for a free port. Racy in principle (another process could grab
/// it before the child binds), which is fine for a demo — and unavoidable, as
/// an executor registers the port it was configured with, not one it was
/// assigned by binding to `:0`.
fn free_port() -> ExampleResult<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

/// Re-invokes this example binary with `args`, e.g. `["scheduler", "50050"]`.
/// The child is killed when the returned handle drops.
fn spawn_role(args: &[&str]) -> ExampleResult<Child> {
    Ok(Command::new(std::env::current_exe()?)
        .args(args)
        .kill_on_drop(true)
        .spawn()?)
}

/// Waits until the scheduler's gRPC endpoint accepts a connection, bounded so a
/// scheduler that never comes up fails the demo instead of hanging it — and
/// reporting the child's exit status rather than a timeout if it died during
/// startup (a taken port, say).
async fn wait_for_scheduler(
    scheduler_url: &str,
    scheduler: &mut Child,
) -> ExampleResult<()> {
    for _ in 0..100 {
        if SchedulerGrpcClient::connect(scheduler_url.to_string())
            .await
            .is_ok()
        {
            return Ok(());
        }
        if let Some(status) = scheduler.try_wait()? {
            return Err(
                format!("scheduler process exited during startup: {status}").into()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("scheduler at {scheduler_url} unreachable after 10s").into())
}
