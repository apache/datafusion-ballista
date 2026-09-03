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
use ballista_core::error::BallistaError;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::utils::{
    GrpcClientConfig, create_grpc_client_connection, default_config_producer,
    default_session_builder,
};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::metrics::default_metrics_collector;
use ballista_scheduler::scheduler_process::start_grpc_service_with_listener;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::arrow::array::RecordBatch;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::TryStreamExt;
use tonic::transport::Channel;

/// Test errors are only ever printed, and the client, transport, and Ballista
/// error types all implement `Error` — so one boxed type spares every call site
/// a `map_err`.
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

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
    Err(BallistaError::General(format!("scheduler at {url} did not come up")).into())
}

/// Connects a Flight SQL client, authenticates, and registers a `people` table
/// for it to query.
///
/// The `TempDir` is returned because it owns the CSV the table points at; drop
/// it and the executors have nothing to read.
async fn client_with_people(
    url: &str,
) -> Result<(FlightSqlServiceClient<Channel>, tempfile::TempDir)> {
    let channel =
        create_grpc_client_connection(url.to_string(), &GrpcClientConfig::default())
            .await?;
    let mut client = FlightSqlServiceClient::new(channel);
    client.handshake("anonymous", "").await?;

    let csv = tempfile::tempdir()?;
    let path = csv.path().join("data.csv");
    std::fs::write(&path, "id,name\n1,alice\n2,bob\n3,carol\n")?;
    let path = path.to_string_lossy();

    // DDL runs on the scheduler and registers into the session catalog.
    let info = client
        .execute(
            format!(
                "CREATE EXTERNAL TABLE people STORED AS CSV LOCATION '{path}' \
                 OPTIONS ('format.has_header' 'true')"
            ),
            None,
        )
        .await?;
    collect(&mut client, info).await?;

    Ok((client, csv))
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
        let mut collected: Vec<RecordBatch> =
            client.do_get(ticket).await?.try_collect().await?;
        batches.append(&mut collected);
    }
    Ok(batches)
}

/// Total rows across every batch a query returned.
fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// The headline path: authenticate, register a table with DDL, run a query
/// that the cluster actually distributes, and read the results back through
/// the scheduler.
#[tokio::test]
async fn flight_sql_runs_a_distributed_query() -> Result {
    let url = start_cluster().await?;
    let (mut client, _csv) = client_with_people(&url).await?;

    let info = client
        .execute(
            "SELECT name FROM people WHERE id > 1 ORDER BY name".to_string(),
            None,
        )
        .await?;

    // Every endpoint must be redeemable on the connection we already have:
    // no executor address is allowed to leak to the client.
    assert!(
        info.endpoint.iter().all(|e| e.location.is_empty()),
        "endpoints must not advertise cluster-internal addresses: {:?}",
        info.endpoint
    );

    let batches = collect(&mut client, info).await?;
    assert_eq!(
        row_count(&batches),
        2,
        "expected bob and carol, got {batches:?}"
    );

    Ok(())
}

/// A table registered through DDL must still be there for the next request.
/// `QueryBackend::session` builds a fresh `SessionContext` every call, so this
/// only holds because the frontend caches it per session.
#[tokio::test]
async fn a_session_keeps_its_catalog_across_requests() -> Result {
    let url = start_cluster().await?;
    let (mut client, _csv) = client_with_people(&url).await?;

    let info = client
        .execute("SELECT id FROM people".to_string(), None)
        .await?;
    let batches = collect(&mut client, info).await?;
    assert_eq!(row_count(&batches), 3);

    Ok(())
}

/// Drivers introspect before they query. Catalog answers must come from the
/// same session the query will run in, or the schema browser lies.
#[tokio::test]
async fn flight_sql_serves_catalog_metadata() -> Result {
    let url = start_cluster().await?;
    let (mut client, _csv) = client_with_people(&url).await?;

    let info = client
        .get_tables(CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: true,
        })
        .await?;
    let batches = collect(&mut client, info).await?;
    assert_eq!(
        row_count(&batches),
        1,
        "the table registered above should be listed"
    );

    // `CommandGetSqlInfo` is what the Arrow Flight JDBC driver needs and what
    // the old implementation never implemented.
    let info = client
        .get_sql_info(vec![
            SqlInfo::FlightSqlServerName,
            SqlInfo::FlightSqlServerVersion,
        ])
        .await?;
    let batches = collect(&mut client, info).await?;
    assert_eq!(row_count(&batches), 2);

    Ok(())
}

/// A prepared statement is planned once and executed against the session it
/// was prepared in.
#[tokio::test]
async fn flight_sql_supports_prepared_statements() -> Result {
    let url = start_cluster().await?;
    let (mut client, _csv) = client_with_people(&url).await?;

    let mut prepared = client
        .prepare("SELECT id FROM people".to_string(), None)
        .await?;
    let info = prepared.execute().await?;
    let batches = collect(&mut client, info).await?;
    assert_eq!(row_count(&batches), 3);

    prepared.close().await?;

    Ok(())
}

/// `GetFlightInfo` promises a schema and `DoGet` has to deliver exactly it:
/// clients (ADBC among them) reject a stream whose schema differs from the one
/// they were given, down to per-field nullability.
///
/// DataFusion's logical and physical schemas disagree about nullability often
/// enough that this cannot be papered over -- `version()` is nullable in the
/// logical plan and not-null in the executed batches, and a list literal goes
/// the other way -- so the advertised schema has to come from the physical
/// side.
#[tokio::test]
async fn the_advertised_schema_matches_the_data() -> Result {
    let url = start_cluster().await?;
    let (mut client, _csv) = client_with_people(&url).await?;

    for sql in [
        "SELECT version()",
        "SELECT [1, 2, 3] AS lst",
        "SELECT id, name FROM people",
        // The scheduler answers EXPLAIN with a plan of its own, so its output
        // schema has to agree with the one a client is promised too.
        "EXPLAIN SELECT id FROM people",
    ] {
        let info = client.execute(sql.to_string(), None).await?;
        let advertised = info.clone().try_decode_schema()?;
        let batches = collect(&mut client, info).await?;
        let delivered = batches
            .first()
            .ok_or_else(|| BallistaError::General(format!("{sql} returned no data")))?
            .schema();

        assert_eq!(
            advertised, *delivered,
            "FlightInfo schema for `{sql}` does not match the schema of the data"
        );
    }

    Ok(())
}

/// `SHOW TABLES` and friends read `information_schema`, which is a streaming
/// table the physical codec cannot serialize. Distributing one produced a task
/// the executors never got and a status the client never saw, so `GetFlightInfo`
/// hung forever -- exactly what a BI tool does on connect.
#[tokio::test]
async fn catalog_queries_are_answered_without_distributing_them() -> Result {
    let url = start_cluster().await?;
    let (mut client, _csv) = client_with_people(&url).await?;

    for sql in [
        "SHOW TABLES",
        "SELECT table_name FROM information_schema.tables",
    ] {
        let run = async {
            let info = client.execute(sql.to_string(), None).await?;
            collect(&mut client, info).await
        };
        let batches = tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .map_err(|_| BallistaError::General(format!("`{sql}` hung")))??;

        assert!(
            row_count(&batches) > 0,
            "`{sql}` should list the session's tables, got {batches:?}"
        );
    }

    Ok(())
}
