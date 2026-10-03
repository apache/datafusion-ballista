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

use ballista::prelude::SessionConfigExt;
use ballista_core::serde::{
    BallistaCodec, protobuf::scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_scheduler::SessionBuilder;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionConfig;
use object_store::aws::AmazonS3Builder;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{CmdWaitFor, ContainerPort, ExecCommand, WaitFor};
use testcontainers::{ContainerRequest, GenericImage, ImageExt};

pub const REGION: &str = "eu-west-1";
pub const BUCKET: &str = "ballista";
pub const ACCESS_KEY_ID: &str = "BALLISTA";
pub const SECRET_KEY: &str = "BALLISTASECRET";

/// S3-compatible server the integration tests run against.
///
/// These tests used MinIO until it withdrew its `minio/minio` image from Docker Hub
/// (2026-09-11) and then quay.io (2026-09-24). RustFS is an S3-compatible server
/// published on Docker Hub; see <https://github.com/rustfs/rustfs>.
const RUSTFS_IMAGE: &str = "rustfs/rustfs";
const RUSTFS_TAG: &str = "1.0.0";

/// Port the S3 API listens on inside the container.
pub const S3_PORT: u16 = 9000;

#[allow(dead_code)]
pub fn create_s3_store(
    host: &str,
    port: u16,
) -> std::result::Result<object_store::aws::AmazonS3, object_store::Error> {
    log::info!("create S3 client: host: {host}, port: {port}");
    AmazonS3Builder::new()
        .with_endpoint(format!("http://{host}:{port}"))
        .with_region(REGION)
        .with_bucket_name(BUCKET)
        .with_access_key_id(ACCESS_KEY_ID)
        .with_secret_access_key(SECRET_KEY)
        .with_allow_http(true)
        .build()
}

#[allow(dead_code)]
pub fn create_s3_container() -> ContainerRequest<GenericImage> {
    // RustFS logs only warnings by default, so wait on its health endpoint
    // rather than a startup message.
    let ready = HttpWaitStrategy::new("/health")
        .with_port(ContainerPort::Tcp(S3_PORT))
        .with_expected_status_code(200u16);

    GenericImage::new(RUSTFS_IMAGE, RUSTFS_TAG)
        .with_exposed_port(ContainerPort::Tcp(S3_PORT))
        .with_wait_for(WaitFor::http(ready))
        .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY_ID)
        .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
}

#[allow(dead_code)]
pub fn create_bucket_command() -> ExecCommand {
    // A top-level directory under the data volume is a bucket, which avoids
    // pulling an S3 client in just to create one.
    ExecCommand::new(vec![
        "mkdir".to_string(),
        format!("/data/{}", crate::common::BUCKET),
    ])
    .with_cmd_ready_condition(CmdWaitFor::exit_code(0))
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
    let codec = BallistaCodec::new(logical, physical);

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

#[ctor::ctor(unsafe)]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        .is_test(true)
        .try_init();
}
