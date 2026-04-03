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

//! Integration tests for the Riffle client.
//!
//! Tests that don't need a Riffle cluster run unconditionally.
//! Tests that need a running cluster use testcontainers (require Docker)
//! and are gated behind `#[ignore]` for CI without Docker.
//!
//! To build the test Docker image:
//! ```bash
//! cd ~/git/riffle && cargo build --release -p riffle-server
//! cp target/release/riffle-server ~/git/datafusion-ballista/ballista/riffle/docker/
//! cd ~/git/datafusion-ballista/ballista/riffle/docker
//! docker build -t riffle-test .
//! ```
//!
//! Then run: `cargo test -p ballista-riffle --test integration_test -- --ignored`

use ballista_riffle::serde::{
    ipc_bytes_to_record_batches, record_batches_to_ipc_bytes, shuffle_read_to_record_batches,
};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn test_batches(schema: &Arc<Schema>) -> Vec<RecordBatch> {
    vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["dave", "eve"])),
            ],
        )
        .unwrap(),
    ]
}

// ============================================================================
// Unit tests (no cluster needed)
// ============================================================================

#[tokio::test]
async fn test_serde_roundtrip() {
    let schema = test_schema();
    let batches = test_batches(&schema);

    let bytes = record_batches_to_ipc_bytes(&batches, &schema).unwrap();
    let result = ipc_bytes_to_record_batches(&bytes).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].num_rows(), 3);
    assert_eq!(result[1].num_rows(), 2);

    let ids = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

#[tokio::test]
async fn test_multi_block_serde() {
    let schema = test_schema();

    let block0 = record_batches_to_ipc_bytes(
        &[test_batches(&schema)[0].clone()],
        &schema,
    )
    .unwrap();
    let block1 = record_batches_to_ipc_bytes(
        &[test_batches(&schema)[1].clone()],
        &schema,
    )
    .unwrap();

    let mut concatenated = Vec::new();
    concatenated.extend_from_slice(&block0);
    concatenated.extend_from_slice(&block1);

    let segments = vec![
        ballista_riffle::client::BlockSegment {
            block_id: 0,
            offset: 0,
            length: block0.len() as i32,
            uncompress_length: block0.len() as i32,
            crc: 0,
            task_attempt_id: 0,
        },
        ballista_riffle::client::BlockSegment {
            block_id: 1,
            offset: block0.len() as i64,
            length: block1.len() as i32,
            uncompress_length: block1.len() as i32,
            crc: 0,
            task_attempt_id: 1,
        },
    ];

    let result = shuffle_read_to_record_batches(&concatenated, &segments).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].num_rows(), 3);
    assert_eq!(result[1].num_rows(), 2);
}

// ============================================================================
// Integration tests (require Docker + riffle-test image)
// Gated behind the "testcontainers" feature flag.
//
// Run with: cargo test -p ballista-riffle --features testcontainers
// ============================================================================

#[cfg(feature = "testcontainers")]
mod container_tests {
    use super::*;
    use ballista_riffle::client::RiffleClient;
    use ballista_riffle::config::RiffleConfig;
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    fn uuid() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("{ts}")
    }

    /// Full lifecycle test against a Riffle cluster started via testcontainers.
    ///
    /// Requires the `riffle-test` Docker image to be built first.
    /// See module-level docs for build instructions.
    #[tokio::test]
    async fn test_riffle_lifecycle_with_testcontainers() {
        let _ = env_logger::try_init();

    // Start coordinator container
    let coordinator = GenericImage::new("riffle-test", "latest")
        .with_wait_for(WaitFor::message_on_stdout("Coordinator is ready"))
        .with_exposed_port(19995.tcp())
        .with_exposed_port(21000.tcp())
        .with_cmd(vec!["coordinator"])
        .start()
        .await
        .expect("Failed to start coordinator container");

    let coord_host = coordinator.get_host().await.unwrap();
    let coord_rpc_port = coordinator.get_host_port_ipv4(21000).await.unwrap();
    let coord_http_port = coordinator.get_host_port_ipv4(19995).await.unwrap();

    println!("Coordinator: {coord_host}:{coord_rpc_port} (HTTP: {coord_http_port})");

    // Get coordinator's container IP for inter-container communication
    let coord_container_ip = coordinator
        .get_bridge_ip_address()
        .await
        .unwrap()
        .to_string();

    // Start riffle server container linked to coordinator
    let server = GenericImage::new("riffle-test", "latest")
        .with_wait_for(WaitFor::message_on_stderr("http server"))
        .with_exposed_port(19998.tcp())
        .with_exposed_port(21100.tcp())
        .with_startup_timeout(std::time::Duration::from_secs(30))
        .with_env_var("COORDINATOR_HOST", &coord_container_ip)
        .with_cmd(vec!["server"])
        .start()
        .await
        .expect("Failed to start riffle server container");

    let server_host = server.get_host().await.unwrap();
    let server_grpc_port = server.get_host_port_ipv4(21100).await.unwrap();

    println!("Riffle server: {server_host}:{server_grpc_port}");

    // Wait for server to register with coordinator
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Run the lifecycle test
    let schema = test_schema();
    let batches = test_batches(&schema);
    let app_id = format!("test-{}", uuid());
    let shuffle_id = 1;

    let config = RiffleConfig {
        coordinator_host: coord_host.to_string(),
        coordinator_port: coord_rpc_port,
        app_id: app_id.clone(),
        ..Default::default()
    };

    let client = RiffleClient::connect(config)
        .await
        .expect("Failed to connect to coordinator");

    println!("Connected to coordinator");

    // Register app
    client.register_application().await.expect("register_application failed");
    println!("Registered application: {app_id}");

    // Get assignments
    let assignments = client
        .get_shuffle_assignments(shuffle_id, 2)
        .await
        .expect("get_shuffle_assignments failed");

    assert!(!assignments.is_empty(), "No assignments returned");
    let srv = assignments[0].server.first().expect("No server");
    println!("Assignment: {}:{}", srv.ip, srv.port);

    // The server IP in the assignment is the container's internal IP.
    // For testcontainers, we need to use the mapped host port.
    let push_host = &server_host.to_string();
    let push_port = server_grpc_port as i32;

    // Register shuffle
    client
        .register_shuffle(shuffle_id, 2, push_host, push_port)
        .await
        .expect("register_shuffle failed");
    println!("Registered shuffle {shuffle_id}");

    // Push data
    let ipc_bytes = record_batches_to_ipc_bytes(&batches, &schema).unwrap();
    let buf_id = client
        .require_buffer(shuffle_id, ipc_bytes.len() as i32, vec![0], push_host, push_port)
        .await
        .expect("require_buffer failed");

    let block_id = client
        .send_shuffle_data(shuffle_id, buf_id, 0, ipc_bytes.clone(), 0, push_host, push_port)
        .await
        .expect("send_shuffle_data failed");
    println!("Pushed {} bytes (block_id={block_id})", ipc_bytes.len());

    // Report
    client
        .report_shuffle_result(shuffle_id, vec![(0, vec![block_id])], push_host, push_port)
        .await
        .expect("report_shuffle_result failed");

    // Read back
    let read_result = client
        .get_shuffle_data(shuffle_id, 0, push_host, push_port)
        .await
        .expect("get_shuffle_data failed");

    assert!(!read_result.data.is_empty(), "Read empty data");
    println!(
        "Read {} bytes, {} segments",
        read_result.data.len(),
        read_result.segments.len()
    );

    let read_batches =
        shuffle_read_to_record_batches(&read_result.data, &read_result.segments)
            .expect("deserialize failed");

    let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    println!("SUCCESS: {total_rows} rows roundtripped through testcontainers Riffle cluster");

    // Containers are automatically stopped and removed when dropped
    }
} // mod container_tests
