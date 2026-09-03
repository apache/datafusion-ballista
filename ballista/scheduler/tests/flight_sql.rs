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

//! End-to-end coverage for the Arrow Flight SQL frontend against a real
//! (in-process) cluster: scheduler plus one executor, driven entirely through
//! the Flight SQL protocol.
//!
//! The pre-46.0.0 implementation shipped with no tests at all, and the bugs
//! that got it removed (#1012, #941, #839, #756) were exactly the ones an
//! end-to-end test catches: endpoints pointing somewhere the client cannot
//! reach, and results that fail to decode.

#![cfg(feature = "flight-sql")]

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetTables, SqlInfo};
use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::utils::{default_config_producer, default_session_builder};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::metrics::default_metrics_collector;
use ballista_scheduler::scheduler_process::start_grpc_service_with_listener;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::arrow::array::RecordBatch;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::TryStreamExt;
use tonic::transport::Channel;

/// Boots a scheduler serving Flight SQL plus one executor, and returns the
/// scheduler's URL.
async fn start_cluster() -> Result<String> {
    let mut config = SchedulerConfig::default()
        .with_scheduler_policy(TaskSchedulingPolicy::PullStaged);
    config.flight_sql = true;

    let cluster = BallistaCluster::new_memory(
        "localhost:50050",
        Arc::new(default_session_builder),
        Arc::new(default_config_producer),
    );

    let mut scheduler: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            "localhost:50050".to_owned(),
            cluster,
            BallistaCodec::default(),
            Arc::new(config),
            default_metrics_collector()?,
        );
    scheduler.init().await?;

    // Bind first so the test learns the port without racing another process
    // for it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(start_grpc_service_with_listener(listener, scheduler));

    let url = format!("http://{addr}");

    let scheduler_client = connect_with_retry(&url).await?;
    ballista_executor::new_standalone_executor(
        scheduler_client,
        2,
        BallistaCodec::default(),
    )
    .await?;

    Ok(url)
}

async fn connect_with_retry(url: &str) -> Result<SchedulerGrpcClient<Channel>> {
    for _ in 0..100 {
        if let Ok(client) = SchedulerGrpcClient::connect(url.to_string()).await {
            return Ok(client);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(BallistaError::General(format!(
        "scheduler at {url} did not come up"
    )))
}

async fn flight_sql_client(url: &str) -> Result<FlightSqlServiceClient<Channel>> {
    let channel = Channel::from_shared(url.to_string())
        .map_err(|e| BallistaError::General(e.to_string()))?
        .connect()
        .await
        .map_err(|e| BallistaError::General(e.to_string()))?;

    Ok(FlightSqlServiceClient::new(channel))
}

/// Collects every endpoint of a `FlightInfo`, which is what a real client does
/// and what proves the tickets we hand out are redeemable.
async fn collect(
    client: &mut FlightSqlServiceClient<Channel>,
    info: arrow_flight::FlightInfo,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint
            .ticket
            .ok_or_else(|| BallistaError::General("endpoint has no ticket".into()))?;
        let stream = client
            .do_get(ticket)
            .await
            .map_err(|e| BallistaError::General(e.to_string()))?;
        let mut collected: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| BallistaError::General(e.to_string()))?;
        batches.append(&mut collected);
    }
    Ok(batches)
}

fn write_csv(dir: &tempfile::TempDir) -> Result<String> {
    let path = dir.path().join("data.csv");
    std::fs::write(&path, "id,name\n1,alice\n2,bob\n3,carol\n")?;
    Ok(path.to_string_lossy().to_string())
}

/// The headline path: authenticate, register a table with DDL, run a query
/// that the cluster actually distributes, and read the results back through
/// the scheduler.
#[tokio::test]
async fn flight_sql_runs_a_distributed_query() -> Result<()> {
    let url = start_cluster().await?;
    let mut client = flight_sql_client(&url).await?;

    client
        .handshake("anonymous", "")
        .await
        .map_err(|e| BallistaError::General(format!("handshake failed: {e}")))?;

    let csv = tempfile::tempdir()?;
    let path = write_csv(&csv)?;

    // DDL runs on the scheduler and registers into the session catalog.
    let info = client
        .execute(
            format!(
                "CREATE EXTERNAL TABLE people STORED AS CSV LOCATION '{path}' \
                 OPTIONS ('format.has_header' 'true')"
            ),
            None,
        )
        .await
        .map_err(|e| BallistaError::General(format!("create table failed: {e}")))?;
    collect(&mut client, info).await?;

    let info = client
        .execute(
            "SELECT name FROM people WHERE id > 1 ORDER BY name".to_string(),
            None,
        )
        .await
        .map_err(|e| BallistaError::General(format!("query failed: {e}")))?;

    // Every endpoint must be redeemable on the connection we already have:
    // no executor address is allowed to leak to the client.
    assert!(
        info.endpoint.iter().all(|e| e.location.is_empty()),
        "endpoints must not advertise cluster-internal addresses: {:?}",
        info.endpoint
    );

    let batches = collect(&mut client, info).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "expected bob and carol, got {batches:?}");

    Ok(())
}

/// Drivers introspect before they query. Catalog answers must come from the
/// same session the query will run in, or the schema browser lies.
#[tokio::test]
async fn flight_sql_serves_catalog_metadata() -> Result<()> {
    let url = start_cluster().await?;
    let mut client = flight_sql_client(&url).await?;

    client
        .handshake("anonymous", "")
        .await
        .map_err(|e| BallistaError::General(format!("handshake failed: {e}")))?;

    let csv = tempfile::tempdir()?;
    let path = write_csv(&csv)?;
    let info = client
        .execute(
            format!(
                "CREATE EXTERNAL TABLE people STORED AS CSV LOCATION '{path}' \
                 OPTIONS ('format.has_header' 'true')"
            ),
            None,
        )
        .await
        .map_err(|e| BallistaError::General(e.to_string()))?;
    collect(&mut client, info).await?;

    let info = client
        .get_tables(CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: true,
        })
        .await
        .map_err(|e| BallistaError::General(e.to_string()))?;
    let batches = collect(&mut client, info).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "the table registered above should be listed");

    // `CommandGetSqlInfo` is what the Arrow Flight JDBC driver needs and what
    // the old implementation never implemented.
    let info = client
        .get_sql_info(vec![
            SqlInfo::FlightSqlServerName,
            SqlInfo::FlightSqlServerVersion,
        ])
        .await
        .map_err(|e| BallistaError::General(e.to_string()))?;
    let batches = collect(&mut client, info).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);

    Ok(())
}

/// A prepared statement is planned once and executed against the session it
/// was prepared in.
#[tokio::test]
async fn flight_sql_supports_prepared_statements() -> Result<()> {
    let url = start_cluster().await?;
    let mut client = flight_sql_client(&url).await?;

    client
        .handshake("anonymous", "")
        .await
        .map_err(|e| BallistaError::General(format!("handshake failed: {e}")))?;

    let csv = tempfile::tempdir()?;
    let path = write_csv(&csv)?;
    let info = client
        .execute(
            format!(
                "CREATE EXTERNAL TABLE people STORED AS CSV LOCATION '{path}' \
                 OPTIONS ('format.has_header' 'true')"
            ),
            None,
        )
        .await
        .map_err(|e| BallistaError::General(e.to_string()))?;
    collect(&mut client, info).await?;

    let mut prepared = client
        .prepare("SELECT id FROM people".to_string(), None)
        .await
        .map_err(|e| BallistaError::General(format!("prepare failed: {e}")))?;

    let info = prepared
        .execute()
        .await
        .map_err(|e| BallistaError::General(format!("execute failed: {e}")))?;
    let batches = collect(&mut client, info).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    prepared
        .close()
        .await
        .map_err(|e| BallistaError::General(format!("close failed: {e}")))?;

    Ok(())
}
