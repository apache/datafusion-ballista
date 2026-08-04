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

//! Docker fixture for the Iceberg integration tests and the
//! `standalone-iceberg-write` example: an Iceberg REST catalog backed by MinIO.
//!
//! Each [`IcebergFixture`] is a private catalog on its own docker network, so
//! tests holding one share no namespace, table name, or catalog state. The
//! example includes this module by path, so it must not depend on anything
//! test-only.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use testcontainers_modules::minio::MinIO;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{
    CmdWaitFor, ExecCommand, IntoContainerPort, WaitFor,
};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const MINIO_TAG: &str = "RELEASE.2025-05-24T17-08-30Z";
const REST_IMAGE: &str = "apache/iceberg-rest-fixture";
const REST_TAG: &str = "1.10.1";

const MINIO_PORT: u16 = 9000;
const REST_PORT: u16 = 8181;

const BUCKET: &str = "icebergdata";
const ACCESS_KEY: &str = "admin";
const SECRET_KEY: &str = "password";
const REGION: &str = "us-east-1";

/// A running Iceberg REST catalog + MinIO pair, removed on drop.
pub struct IcebergFixture {
    props: HashMap<String, String>,
    _minio: ContainerAsync<MinIO>,
    _rest: ContainerAsync<GenericImage>,
}

impl IcebergFixture {
    /// Starts MinIO and an Iceberg REST catalog in front of it, panicking on any
    /// docker failure.
    pub async fn start() -> Self {
        // Container and network names are global to the docker daemon, and
        // several fixtures run at once.
        let suffix = unique_suffix();
        let network = format!("iceberg-ballista-{suffix}");
        // Also the hostname the REST catalog reaches MinIO on.
        let minio_host = format!("iceberg-ballista-minio-{suffix}");

        let minio = MinIO::default()
            .with_tag(MINIO_TAG)
            .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_network(&network)
            .with_container_name(&minio_host)
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .expect("start minio");

        create_bucket(&minio).await;

        let rest = GenericImage::new(REST_IMAGE, REST_TAG)
            .with_exposed_port(REST_PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/v1/config")
                    .with_port(REST_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_network(&network)
            .with_env_var("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .with_env_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .with_env_var("AWS_REGION", REGION)
            .with_env_var(
                "CATALOG_CATALOG__IMPL",
                "org.apache.iceberg.jdbc.JdbcCatalog",
            )
            .with_env_var(
                "CATALOG_URI",
                "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory",
            )
            .with_env_var("CATALOG_WAREHOUSE", format!("s3://{BUCKET}/demo"))
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var(
                "CATALOG_S3_ENDPOINT",
                format!("http://{minio_host}:{MINIO_PORT}"),
            )
            // Virtual-hosted style would need a `<bucket>.<host>` DNS alias,
            // which testcontainers cannot add.
            .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .expect("start iceberg rest catalog");

        let props = catalog_props(
            endpoint(&rest, REST_PORT).await,
            endpoint(&minio, MINIO_PORT).await,
        );

        Self {
            props,
            _minio: minio,
            _rest: rest,
        }
    }

    /// Catalog + storage properties addressing this fixture.
    pub fn props(&self) -> HashMap<String, String> {
        self.props.clone()
    }
}

/// Creates the warehouse bucket with the `mc` the MinIO image ships.
async fn create_bucket(minio: &ContainerAsync<MinIO>) {
    // The image preconfigures a `local` alias without credentials.
    exec(
        minio,
        [
            "mc",
            "alias",
            "set",
            "local",
            &format!("http://localhost:{MINIO_PORT}"),
            ACCESS_KEY,
            SECRET_KEY,
        ],
    )
    .await;
    exec(
        minio,
        ["mc", "mb", "--ignore-existing", &format!("local/{BUCKET}")],
    )
    .await;
}

/// Runs `cmd` in the container and waits for it to exit successfully.
async fn exec<'a>(
    container: &ContainerAsync<MinIO>,
    cmd: impl IntoIterator<Item = &'a str>,
) {
    let cmd: Vec<String> = cmd.into_iter().map(str::to_string).collect();
    let description = cmd.join(" ");
    container
        .exec(ExecCommand::new(cmd).with_cmd_ready_condition(CmdWaitFor::exit_code(0)))
        .await
        .unwrap_or_else(|e| panic!("`{description}` in minio container: {e}"));
}

/// `http://<host>:<published port>` for a container port, as seen from this
/// process.
async fn endpoint<I: testcontainers_modules::testcontainers::Image>(
    container: &ContainerAsync<I>,
    port: u16,
) -> String {
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(port.tcp())
        .await
        .expect("published port");
    format!("http://{host}:{port}")
}

fn catalog_props(rest_uri: String, s3_endpoint: String) -> HashMap<String, String> {
    HashMap::from([
        ("uri".to_string(), rest_uri),
        ("s3.endpoint".to_string(), s3_endpoint),
        ("s3.access-key-id".to_string(), ACCESS_KEY.to_string()),
        ("s3.secret-access-key".to_string(), SECRET_KEY.to_string()),
        ("s3.region".to_string(), REGION.to_string()),
        ("s3.path-style-access".to_string(), "true".to_string()),
    ])
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    format!("{}-{}", std::process::id(), nanos % 1_000_000_000)
}
