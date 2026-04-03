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

//! Integration test for the Riffle client against a local Riffle cluster.
//!
//! Requires a running Riffle cluster:
//! - Uniffle coordinator on localhost:21000
//! - Riffle shuffle server on localhost:21100
//!
//! Run with: cargo test -p ballista-riffle --test integration_test

use ballista_riffle::client::RiffleClient;
use ballista_riffle::config::RiffleConfig;
use ballista_riffle::serde::{ipc_bytes_to_record_batches, record_batches_to_ipc_bytes};

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

#[tokio::test]
#[ignore = "requires a running Riffle cluster (coordinator on localhost:21000, server on localhost:21100)"]
async fn test_riffle_client_lifecycle() {
    let schema = test_schema();
    let batches = test_batches(&schema);
    let app_id = format!("ballista-test-{}", uuid());
    let shuffle_id = 1;
    let num_partitions = 2;

    // 1. Connect to coordinator
    let config = RiffleConfig {
        coordinator_host: "127.0.0.1".to_string(),
        coordinator_port: 21000,
        app_id: app_id.clone(),
        ..Default::default()
    };

    let client = RiffleClient::connect(config)
        .await
        .expect("Failed to connect to coordinator");

    println!("Connected to Riffle coordinator");

    // 2. Get shuffle assignments
    let assignments = client
        .get_shuffle_assignments(shuffle_id, num_partitions)
        .await
        .expect("Failed to get assignments");

    assert!(!assignments.is_empty(), "No assignments returned");
    let server = assignments[0].server.first().expect("No server in assignment");
    let server_host = &server.ip;
    let server_port = server.port;
    println!("Got assignment: server={}:{}", server_host, server_port);

    // 3. Register shuffle with the server
    client
        .register_shuffle(shuffle_id, num_partitions as i32, server_host, server_port)
        .await
        .expect("Failed to register shuffle");

    println!("Registered shuffle {shuffle_id}");

    // 4. Serialize batches and push to partition 0
    let ipc_bytes = record_batches_to_ipc_bytes(&batches, &schema)
        .expect("Failed to serialize");

    println!("Serialized {} bytes for partition 0", ipc_bytes.len());

    let require_buffer_id = client
        .require_buffer(shuffle_id, ipc_bytes.len() as i32, vec![0], server_host, server_port)
        .await
        .expect("Failed to require buffer");

    println!("Got buffer id: {require_buffer_id}");

    let block_id = client
        .send_shuffle_data(
            shuffle_id,
            require_buffer_id,
            0, // partition_id
            ipc_bytes.clone(),
            0, // task_attempt_id
            server_host,
            server_port,
        )
        .await
        .expect("Failed to send shuffle data");

    println!("Pushed data to partition 0 (block_id={block_id})");

    // 5. Report shuffle result
    client
        .report_shuffle_result(
            shuffle_id,
            vec![(0, vec![block_id])],
            server_host,
            server_port,
        )
        .await
        .expect("Failed to report shuffle result");

    println!("Reported shuffle result");

    // 6. Commit (may not be supported by all Riffle versions)
    match client
        .commit_shuffle_task(shuffle_id, server_host, server_port)
        .await
    {
        Ok(_) => println!("Committed shuffle task"),
        Err(e) => println!("Commit not supported (non-fatal): {e}"),
    }

    // 6. Read back and deserialize using segment metadata
    let read_result = client
        .get_shuffle_data(shuffle_id, 0, server_host, server_port)
        .await
        .expect("Failed to read shuffle data");

    println!(
        "Read {} bytes from partition 0 ({} segments)",
        read_result.data.len(),
        read_result.segments.len()
    );

    assert!(!read_result.data.is_empty(), "Read empty data from Riffle");

    // Deserialize using segments (handles multi-mapper blocks correctly)
    let read_batches =
        ballista_riffle::serde::shuffle_read_to_record_batches(
            &read_result.data,
            &read_result.segments,
        )
        .expect("Failed to deserialize shuffle data");

    let total_rows: usize = read_batches.iter().map(|b| b.num_rows()).sum();
    println!("Deserialized {} batches, {} total rows", read_batches.len(), total_rows);
    assert_eq!(total_rows, 5, "Expected 5 rows (3 + 2 from two batches)");

    println!("SUCCESS: Full Riffle client lifecycle test passed!");
    println!("  - Connected to coordinator");
    println!("  - Got server assignments");
    println!("  - Registered shuffle");
    println!("  - Pushed {} bytes of Arrow IPC data", ipc_bytes.len());
    println!("  - Read back {} bytes ({} segments)", read_result.data.len(), read_result.segments.len());
    println!("  - Deserialized to {} batches, {} rows", read_batches.len(), total_rows);
}

#[tokio::test]
async fn test_serde_roundtrip() {
    let schema = test_schema();
    let batches = test_batches(&schema);

    let bytes = record_batches_to_ipc_bytes(&batches, &schema).unwrap();
    let result = ipc_bytes_to_record_batches(&bytes).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].num_rows(), 3);
    assert_eq!(result[1].num_rows(), 2);

    // Verify data integrity
    let ids = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

fn uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{ts}")
}
