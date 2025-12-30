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

use std::env;
use std::error::Error;
use std::path::PathBuf;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::serde::{
    BallistaCodec, protobuf::scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_scheduler::SessionBuilder;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};

/// Returns the parquet test data directory, which is by default
/// stored in a git submodule rooted at
/// `examples/testdata`.
///
/// The default can be overridden by the optional environment variable
/// `EXAMPLES_TEST_DATA`
///
/// panics when the directory can not be found.
///
/// Example:
/// ```
/// use ballista_examples::test_util;
/// let testdata = test_util::examples_test_data();
/// let filename = format!("{testdata}/aggregate_test_100.csv");
/// assert!(std::path::PathBuf::from(filename).exists());
/// ```
#[allow(dead_code)]
pub fn example_test_data() -> String {
    match get_data_dir("EXAMPLES_TEST_DATA", "testdata") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get examples test data dir: {err}"),
    }
}

/// Returns a directory path for finding test data.
///
/// udf_env: name of an environment variable
///
/// submodule_dir: fallback path (relative to CARGO_MANIFEST_DIR)
///
///  Returns either:
/// The path referred to in `udf_env` if that variable is set and refers to a directory
/// The submodule_data directory relative to CARGO_MANIFEST_PATH
#[allow(dead_code)]
fn get_data_dir(udf_env: &str, submodule_data: &str) -> Result<PathBuf, Box<dyn Error>> {
    // Try user defined env.
    if let Ok(dir) = env::var(udf_env) {
        let trimmed = dir.trim().to_string();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_dir() {
                return Ok(pb);
            } else {
                return Err(format!(
                    "the data dir `{}` defined by env {udf_env} not found",
                    pb.display()
                )
                .into());
            }
        }
    }

    // The env is undefined or its value is trimmed to empty, let's try default dir.

    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
    // set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(format!(
            "env `{udf_env}` is undefined or has empty value, and the pre-defined data dir `{}` not found\n\
             HINT: try running `git submodule update --init`",
            pb.display(),
        ).into())
    }
}

/// starts a ballista cluster for integration tests
#[allow(dead_code)]
pub async fn setup_test_cluster() -> (String, u16) {
    let config = SessionConfig::new_with_ballista();
    let default_codec = BallistaCodec::default();

    let addr = ballista_scheduler::standalone::new_standalone_scheduler()
        .await
        .expect("scheduler to be created");

    let host = "localhost".to_string();

    let scheduler =
        connect_to_scheduler(format!("http://{}:{}", host, addr.port())).await;

    ballista_executor::new_standalone_executor(
        scheduler,
        config.ballista_standalone_parallelism(),
        default_codec,
    )
    .await
    .expect("executor to be created");

    log::info!("test scheduler created at: {}:{}", host, addr.port());

    (host, addr.port())
}

/// starts a ballista cluster for integration tests
#[allow(dead_code)]
pub async fn setup_test_cluster_with_state(session_state: SessionState) -> (String, u16) {
    let config = SessionConfig::new_with_ballista();

    let addr = ballista_scheduler::standalone::new_standalone_scheduler_from_state(
        &session_state,
    )
    .await
    .expect("scheduler to be created");

    let host = "localhost".to_string();

    let scheduler =
        connect_to_scheduler(format!("http://{}:{}", host, addr.port())).await;

    ballista_executor::new_standalone_executor_from_state(
        scheduler,
        config.ballista_standalone_parallelism(),
        &session_state,
    )
    .await
    .expect("executor to be created");

    log::info!("test scheduler created at: {}:{}", host, addr.port());

    (host, addr.port())
}

#[allow(dead_code)]
pub async fn setup_test_cluster_with_builders(
    config_producer: ConfigProducer,
    runtime_producer: RuntimeProducer,
    session_builder: SessionBuilder,
) -> (String, u16) {
    let config = config_producer();

    let logical = config.ballista_logical_extension_codec();
    let physical = config.ballista_physical_extension_codec();
    let codec: BallistaCodec<
        datafusion_proto::protobuf::LogicalPlanNode,
        datafusion_proto::protobuf::PhysicalPlanNode,
    > = BallistaCodec::new(logical, physical);

    let addr = ballista_scheduler::standalone::new_standalone_scheduler_with_builder(
        session_builder,
        config_producer.clone(),
        codec.clone(),
    )
    .await
    .expect("scheduler to be created");

    let host = "localhost".to_string();

    let scheduler =
        connect_to_scheduler(format!("http://{}:{}", host, addr.port())).await;

    ballista_executor::new_standalone_executor_from_builder(
        scheduler,
        config.ballista_standalone_parallelism(),
        config_producer,
        runtime_producer,
        codec,
        Default::default(),
    )
    .await
    .expect("executor to be created");

    log::info!("test scheduler created at: {}:{}", host, addr.port());

    (host, addr.port())
}

async fn connect_to_scheduler(
    scheduler_url: String,
) -> SchedulerGrpcClient<tonic::transport::Channel> {
    let mut retry = 50;
    loop {
        match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
            Err(_) if retry > 0 => {
                retry -= 1;
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                log::debug!("Re-attempting to connect to test scheduler...");
            }

            Err(_) => {
                log::error!("scheduler connection timed out");
                panic!("scheduler connection timed out")
            }
            Ok(scheduler) => break scheduler,
        }
    }
}

#[allow(dead_code)]
pub async fn standalone_context() -> SessionContext {
    SessionContext::standalone().await.unwrap()
}

#[allow(dead_code)]
pub async fn remote_context() -> SessionContext {
    let (host, port) = setup_test_cluster().await;
    SessionContext::remote(&format!("df://{host}:{port}"))
        .await
        .unwrap()
}

#[allow(dead_code)]
pub async fn standalone_context_with_state() -> SessionContext {
    let config = SessionConfig::new_with_ballista();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    SessionContext::standalone_with_state(state).await.unwrap()
}

#[allow(dead_code)]
pub async fn remote_context_with_state() -> SessionContext {
    let config = SessionConfig::new_with_ballista();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let (host, port) = setup_test_cluster_with_state(state.clone()).await;
    SessionContext::remote_with_state(&format!("df://{host}:{port}"), state)
        .await
        .unwrap()
}

#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        //.parse_filters("ballista=debug,ballista_scheduler-rs=debug,ballista_executor=debug,datafusion=debug")
        .is_test(true)
        .try_init();
}
