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

//! An Iceberg REST catalog storing its tables in RustFS.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{CmdWaitFor, ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, Image, ImageExt};

const S3_PORT: u16 = 9000;
const REST_PORT: u16 = 8181;
const BUCKET: &str = "warehouse";
const ACCESS_KEY: &str = "admin";
const SECRET_KEY: &str = "password";
const REGION: &str = "us-east-1";

/// RustFS and a REST catalog in front of it, removed on drop.
pub struct RestCatalog {
    pub catalog: Arc<dyn Catalog>,
    _s3: ContainerAsync<GenericImage>,
    _rest: ContainerAsync<GenericImage>,
}

impl RestCatalog {
    pub async fn start() -> Self {
        // Docker names are global, and tests run concurrently.
        let suffix = unique_suffix();
        let network = format!("ballista-iceberg-{suffix}");
        // Also the hostname the catalog reaches RustFS on.
        let s3_host = format!("ballista-iceberg-rustfs-{suffix}");

        let s3 = GenericImage::new("rustfs/rustfs", "1.0.0")
            .with_exposed_port(S3_PORT.tcp())
            // RustFS logs only warnings by default, so wait on its health check.
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/health")
                    .with_port(S3_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
            .with_network(&network)
            .with_container_name(&s3_host)
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .unwrap();
        // A top-level directory of RustFS's data volume is a bucket.
        s3.exec(
            ExecCommand::new(["mkdir", &format!("/data/{BUCKET}")])
                .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .unwrap();

        let rest = GenericImage::new("apache/iceberg-rest-fixture", "1.10.1")
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
            .with_env_var("CATALOG_WAREHOUSE", format!("s3://{BUCKET}/"))
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var("CATALOG_S3_ENDPOINT", format!("http://{s3_host}:{S3_PORT}"))
            // Virtual-hosted style would need a `<bucket>.<host>` DNS alias,
            // which testcontainers can't add.
            .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .unwrap();

        let props = HashMap::from([
            ("uri".to_string(), endpoint(&rest, REST_PORT).await),
            ("s3.endpoint".to_string(), endpoint(&s3, S3_PORT).await),
            ("s3.access-key-id".to_string(), ACCESS_KEY.to_string()),
            ("s3.secret-access-key".to_string(), SECRET_KEY.to_string()),
            ("s3.region".to_string(), REGION.to_string()),
            ("s3.path-style-access".to_string(), "true".to_string()),
        ]);
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .load("rest", props)
            .await
            .unwrap();
        Self {
            catalog: Arc::new(catalog),
            _s3: s3,
            _rest: rest,
        }
    }
}

/// `http://<host>:<published port>` of a container port, from this process.
async fn endpoint<I: Image>(container: &ContainerAsync<I>, port: u16) -> String {
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(port.tcp()).await.unwrap();
    format!("http://{host}:{port}")
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{}", std::process::id(), nanos % 1_000_000_000)
}
