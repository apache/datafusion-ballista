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

//! End-to-end distributed Iceberg write/read tests.
//!
//! Each test starts its own Iceberg REST catalog + MinIO with testcontainers
//! (see [`fixture`]), so a docker daemon is required — hence the
//! `integration-tests` feature gate:
//!
//! ```bash
//! cargo test -p iceberg-ballista --features integration-tests --test distributed_read_write
//! ```

#![cfg(feature = "integration-tests")]

mod fixture;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use ballista::datafusion::assert_batches_eq;
use ballista::datafusion::execution::{SessionState, SessionStateBuilder};
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::ConfigProducer;
use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::serde::protobuf::task_status;
use ballista_core::utils::{GrpcServerConfig, create_grpc_server};
use ballista_executor::new_standalone_executor_from_state;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::metrics::default_metrics_collector;
use ballista_scheduler::scheduler_server::{SchedulerServer, SessionBuilder};
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec,
};
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_ballista::{
    IcebergCatalogConfig, register_iceberg_catalog, register_iceberg_codecs,
    register_iceberg_table,
};
use iceberg_datafusion::IcebergTableProvider;

use crate::fixture::IcebergFixture;

/// Session state with the Iceberg codecs installed.
fn iceberg_session_state(config: SessionConfig) -> SessionState {
    SessionStateBuilder::new()
        .with_config(register_iceberg_codecs(config))
        .with_default_features()
        .build()
}

/// Runs a SQL statement to completion and returns its batches.
async fn run_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("plan sql")
        .collect()
        .await
        .expect("run sql")
}

/// Creates the test namespace. Each test owns its catalog, so this never races
/// another creator.
async fn create_namespace(catalog: &impl Catalog) -> NamespaceIdent {
    let namespace = NamespaceIdent::new("ballista_it".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    namespace
}

/// Creates `table_name` with `schema` (optionally partitioned) in the test
/// namespace and returns that namespace.
async fn create_table_with(
    props: &HashMap<String, String>,
    table_name: &str,
    schema: Schema,
    partition_spec: Option<UnboundPartitionSpec>,
) -> NamespaceIdent {
    let catalog = fixture::rest_catalog(props).await;
    let namespace = create_namespace(&catalog).await;

    let builder = TableCreation::builder()
        .name(table_name.to_string())
        .schema(schema)
        .properties(HashMap::new());
    let creation = match partition_spec {
        Some(spec) => builder.partition_spec(spec).build(),
        None => builder.build(),
    };
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");

    namespace
}

async fn create_table(
    props: &HashMap<String, String>,
    table_name: &str,
) -> NamespaceIdent {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String))
                .into(),
        ])
        .build()
        .unwrap();
    create_table_with(props, table_name, schema, None).await
}

/// Loads the table from the catalog and returns its current snapshot id.
async fn current_snapshot_id(
    props: &HashMap<String, String>,
    namespace: &NamespaceIdent,
    table_name: &str,
) -> i64 {
    fixture::rest_catalog(props)
        .await
        .load_table(&TableIdent::new(namespace.clone(), table_name.to_string()))
        .await
        .expect("load table")
        .metadata()
        .current_snapshot_id()
        .expect("table has a snapshot")
}

/// Creates a table partitioned by `region` (identity), so an INSERT injects a
/// partition-value expression (`PartitionExpr`) into the physical plan.
async fn create_partitioned_table(
    props: &HashMap<String, String>,
    table_name: &str,
) -> NamespaceIdent {
    // Optional (nullable) fields so the schema matches the nullable columns a
    // `VALUES` source produces; the partitioned-write path checks nullability.
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String))
                .into(),
        ])
        .build()
        .unwrap();
    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        // The REST catalog requires an explicit partition field-id (some
        // catalogs assign one automatically; REST does not).
        .add_partition_fields([UnboundPartitionField {
            source_id: 2,
            field_id: Some(1000),
            name: "region".to_string(),
            transform: Transform::Identity,
        }])
        .unwrap()
        .build();
    create_table_with(props, table_name, schema, Some(partition_spec)).await
}

/// The baseline: distributed INSERT and read-back on standalone Ballista.
///
/// Writes and reads are interleaved, because every query must plan against the
/// snapshot current at that moment — a provider that cached its table metadata
/// would keep serving the first read's empty snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_insert_and_read() {
    let _ = env_logger::builder().is_test(true).try_init();

    let fixture = IcebergFixture::start().await;
    let props = fixture.props();
    let table_name = "events".to_string();
    let namespace = create_table(&props, &table_name).await;

    let state = iceberg_session_state(
        SessionConfig::new_with_ballista()
            .with_target_partitions(2)
            .with_ballista_standalone_parallelism(2),
    );
    let ctx = SessionContext::standalone_with_state(state)
        .await
        .expect("start standalone ballista");

    let catalog_config = IcebergCatalogConfig::new("rest", "rest", props.clone());
    register_iceberg_table(
        &ctx,
        "events",
        catalog_config,
        namespace,
        table_name.clone(),
    )
    .await
    .expect("register iceberg table");

    // Read *before* any insert. This first read is what makes the re-reads below
    // meaningful: it forces the registered provider through a full plan/scan
    // cycle while the table is still empty, so a provider that froze its table
    // metadata here would keep returning this empty snapshot forever.
    assert_batches_eq!(
        ["+---+", "| n |", "+---+", "| 0 |", "+---+"],
        &run_sql(&ctx, "SELECT count(*) AS n FROM events").await
    );

    run_sql(
        &ctx,
        "INSERT INTO events VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    )
    .await;

    assert_batches_eq!(
        ["+---+", "| n |", "+---+", "| 3 |", "+---+"],
        &run_sql(&ctx, "SELECT count(*) AS n FROM events").await
    );

    assert_batches_eq!(
        [
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | alice |",
            "| 2  | bob   |",
            "| 3  | carol |",
            "+----+-------+",
        ],
        &run_sql(&ctx, "SELECT id, name FROM events ORDER BY id").await
    );

    // Insert again through the same registration and re-read. `scan` reloads
    // table metadata from the catalog every time, so each query plans against
    // the snapshot current at *that* moment — the codec then pins it for the
    // executors. A provider (or a cached catalog/table) that went stale after
    // the first read would still report 3 rows here.
    //
    // A `UNION ALL` of `VALUES` branches gives the write one input partition per
    // branch, so this one runs as concurrent writer tasks rather than a single
    // one — see [`multi_executor_write_fans_out_across_executors`].
    run_sql(
        &ctx,
        "INSERT INTO events SELECT * FROM (VALUES (4, 'dave')) \
         UNION ALL SELECT * FROM (VALUES (5, 'erin'))",
    )
    .await;

    // The re-read must observe the interposed insert: both the original and the
    // newly inserted rows.
    assert_batches_eq!(
        ["+---+", "| n |", "+---+", "| 5 |", "+---+"],
        &run_sql(&ctx, "SELECT count(*) AS n FROM events").await
    );

    assert_batches_eq!(
        [
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | alice |",
            "| 2  | bob   |",
            "| 3  | carol |",
            "| 4  | dave  |",
            "| 5  | erin  |",
            "+----+-------+",
        ],
        &run_sql(&ctx, "SELECT id, name FROM events ORDER BY id").await
    );

    // Catalog-level registration: mount the whole Iceberg catalog and read the
    // same table as `<catalog>.<namespace>.<table>`. The providers built through
    // the catalog carry the config too, so this distributed read exercises the
    // catalog/schema config-threading path end to end.
    register_iceberg_catalog(
        &ctx,
        "ice",
        IcebergCatalogConfig::new("rest", "rest", props),
    )
    .await
    .expect("register iceberg catalog");
    assert_batches_eq!(
        ["+---+", "| n |", "+---+", "| 5 |", "+---+"],
        &run_sql(
            &ctx,
            &format!("SELECT count(*) AS n FROM ice.ballista_it.{table_name}"),
        )
        .await
    );
}

/// Like `ballista_scheduler::standalone::new_standalone_scheduler_from_state`,
/// but also returns the [`BallistaCluster`] that helper discards: its job state
/// holds the execution graphs, which record who ran each task.
async fn start_scheduler_with_cluster(
    session_state: &SessionState,
) -> (SocketAddr, BallistaCluster) {
    let codec = BallistaCodec::new(
        session_state.config().ballista_logical_extension_codec(),
        session_state.config().ballista_physical_extension_codec(),
    );
    let max_message_size = session_state
        .config()
        .ballista_grpc_client_max_message_size();

    let session_config = session_state.config().clone();
    let state_for_builder = session_state.clone();
    let session_builder: SessionBuilder =
        Arc::new(move |_| Ok(state_for_builder.clone()));
    let config_producer: ConfigProducer = Arc::new(move || session_config.clone());

    let cluster =
        BallistaCluster::new_memory("localhost:50050", session_builder, config_producer);

    let mut scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            "localhost:50050".to_owned(),
            cluster.clone(),
            codec,
            Arc::new(
                SchedulerConfig::default()
                    .with_scheduler_policy(TaskSchedulingPolicy::PullStaged),
            ),
            default_metrics_collector().expect("metrics collector"),
        );
    scheduler_server
        .init()
        .await
        .expect("init scheduler server");

    let server = SchedulerGrpcServer::new(scheduler_server.clone())
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);
    let listener = tokio::net::TcpListener::bind("localhost:0")
        .await
        .expect("bind scheduler port");
    let addr = listener.local_addr().expect("scheduler address");
    tokio::spawn(
        create_grpc_server(&GrpcServerConfig::default())
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                listener,
            )),
    );
    (addr, cluster)
}

/// Executor ids that completed at least one task, across every job in the
/// cluster's state.
async fn executors_that_ran_tasks(cluster: &BallistaCluster) -> HashSet<String> {
    let job_state = cluster.job_state();
    let mut executors = HashSet::new();
    for job_id in job_state.get_all_jobs().await.expect("list jobs") {
        let graph = job_state
            .get_execution_graph(&job_id)
            .await
            .expect("fetch execution graph")
            .expect("job has an execution graph");
        for stage in graph.stages().values() {
            for task in stage.task_infos().unwrap_or_default() {
                if let task_status::Status::Successful(t) = &task.task_status {
                    executors.insert(t.executor_id.clone());
                }
            }
        }
    }
    executors
}

/// Executors in the multi-executor cluster, and task slots on each.
const N_EXECUTORS: usize = 2;
const SLOTS_PER_EXECUTOR: usize = 2;

/// One scheduler and [`N_EXECUTORS`] executors, all in-process but each its own
/// gRPC service. Returns a context connected to the scheduler, plus the cluster
/// handle that records task placement.
async fn start_multi_executor_cluster(
    state: SessionState,
) -> (SessionContext, BallistaCluster) {
    let (scheduler_addr, cluster) = start_scheduler_with_cluster(&state).await;
    let scheduler_url = format!("http://localhost:{}", scheduler_addr.port());

    // Bounded so a scheduler that never comes up fails the test instead of
    // hanging it.
    let mut attempts = 0;
    let scheduler_client = loop {
        match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
            Ok(client) => break client,
            Err(e) if attempts >= 100 => {
                panic!("scheduler at {scheduler_url} unreachable after 10s: {e}")
            }
            Err(_) => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    // Each executor is a separate service; the scheduler load-balances across
    // them via pull-based scheduling.
    for _ in 0..N_EXECUTORS {
        new_standalone_executor_from_state(
            scheduler_client.clone(),
            SLOTS_PER_EXECUTOR,
            &state,
        )
        .await
        .expect("start executor");
    }

    let ctx = SessionContext::remote_with_state(&scheduler_url, state)
        .await
        .expect("connect to scheduler");
    (ctx, cluster)
}

/// A **partitioned** write on a real multi-executor cluster: one scheduler,
/// several executors, a table partitioned by `region`, and a `UNION ALL` source
/// that gives the write one writer task per branch.
///
/// So the partition-value expression (`PartitionExpr`) must survive the codec,
/// and concurrent writers must still produce:
///
/// 1. exactly **one** atomic snapshot, not one per task,
/// 2. at least one data file per region, all within that snapshot,
/// 3. every input row exactly once — predicate pushdown included.
///
/// Task *placement* is asserted by
/// [`multi_executor_write_fans_out_across_executors`].
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_multi_executor_insert_commits_all_rows() {
    let _ = env_logger::builder().is_test(true).try_init();

    const REGIONS: [&str; 4] = ["a", "b", "c", "d"];
    // REGIONS.len() * 3.
    const TOTAL_ROWS: i32 = 12;
    // Rows per UNION ALL branch, so the write stage runs
    // TOTAL_ROWS / ROWS_PER_BRANCH = 6 concurrent writer tasks.
    const ROWS_PER_BRANCH: usize = 2;

    let fixture = IcebergFixture::start().await;
    let props = fixture.props();
    let table_name = "parallel_events".to_string();
    let namespace = create_partitioned_table(&props, &table_name).await;

    // --- Bring up one scheduler + N executors in-process (real multi-executor) ---
    let state = iceberg_session_state(SessionConfig::new_with_ballista());
    let (ctx, _cluster) = start_multi_executor_cluster(state).await;

    let catalog_config = IcebergCatalogConfig::new("rest", "rest", props.clone());
    register_iceberg_table(
        &ctx,
        "target",
        catalog_config,
        namespace.clone(),
        table_name.clone(),
    )
    .await
    .expect("register iceberg table");

    // --- Distributed, partitioned INSERT across the multi-executor cluster ---
    // A serializable VALUES source whose rows span every region, which is the
    // path that injects PartitionExpr into the physical plan, split into UNION
    // ALL branches so several writer tasks run at once. (A registered MemTable
    // would not work here — it's a custom TableProvider that Ballista cannot
    // serialize into the logical plan.)
    let source = (0..TOTAL_ROWS)
        .map(|i| {
            let region = REGIONS[i as usize % REGIONS.len()];
            format!("({}, '{region}')", i + 1)
        })
        .collect::<Vec<_>>()
        .chunks(ROWS_PER_BRANCH)
        .map(|branch| format!("SELECT * FROM (VALUES {})", branch.join(", ")))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    run_sql(&ctx, &format!("INSERT INTO target {source}")).await;

    // (1) The distributed write committed exactly once (a single atomic
    // snapshot), not one commit per task. Checked straight from the catalog so
    // it isolates write correctness from the distributed read-back below.
    let catalog = fixture::rest_catalog(&props).await;
    let table = catalog
        .load_table(&TableIdent::new(namespace.clone(), table_name.clone()))
        .await
        .expect("load committed table");
    assert_eq!(
        table.metadata().snapshots().count(),
        1,
        "the distributed write must produce exactly one atomic commit"
    );

    // (2) The writers produced at least one data file per region, all coalesced
    // into that single commit.
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("current snapshot");
    let mut data_files = 0usize;
    let manifest_list = table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .expect("load manifest list");
    for entry in manifest_list.entries() {
        let manifest = entry
            .load_manifest(table.file_io())
            .await
            .expect("load manifest");
        data_files += manifest.entries().len();
    }
    assert!(
        data_files >= REGIONS.len(),
        "partitioned write should produce at least one data file per region, got {data_files}"
    );

    // (3) Every row landed exactly once: the exact id set 1..=TOTAL_ROWS, no
    // lost or duplicated rows.
    assert_batches_eq!(
        ["+----+", "| n  |", "+----+", "| 12 |", "+----+"],
        &run_sql(&ctx, "SELECT count(*) AS n FROM target").await
    );

    assert_batches_eq!(
        [
            "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "| 4  |",
            "| 5  |", "| 6  |", "| 7  |", "| 8  |", "| 9  |", "| 10 |", "| 11 |",
            "| 12 |", "+----+",
        ],
        &run_sql(&ctx, "SELECT id FROM target ORDER BY id").await
    );

    // (4) Predicate pushdown survives serialization: a WHERE clause is pushed into
    // the distributed scan (and re-applied above it), and the result is correct.
    let half = TOTAL_ROWS / 2;
    assert_batches_eq!(
        [
            "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "| 4  |",
            "| 5  |", "| 6  |", "+----+",
        ],
        &run_sql(
            &ctx,
            &format!("SELECT id FROM target WHERE id <= {half} ORDER BY id"),
        )
        .await
    );
}

/// A distributed write must **fan out across the executors**, not merely run on
/// a cluster that has several.
///
/// Placement is invisible in the query result, so it is read from the scheduler:
/// each completed task records the executor that ran it, and the assertion fails
/// if one executor served the whole job.
///
/// Fanning out needs a multi-partition source. `IcebergWriteExec` reports
/// `benefits_from_input_partitioning = false`, so nothing repartitions on its
/// behalf — its parallelism is exactly its input's partition count, and one
/// `VALUES` is one partition. Hence the `UNION ALL`: one partition, and so one
/// write task, per branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multi_executor_write_fans_out_across_executors() {
    let _ = env_logger::builder().is_test(true).try_init();

    const REGIONS: [&str; 4] = ["a", "b", "c", "d"];
    const TOTAL_ROWS: i32 = 12;
    // Rows per UNION ALL branch, so the write stage has
    // TOTAL_ROWS / ROWS_PER_BRANCH = 6 tasks against the cluster's
    // N_EXECUTORS * SLOTS_PER_EXECUTOR = 4 slots.
    const ROWS_PER_BRANCH: usize = 2;

    let fixture = IcebergFixture::start().await;
    let props = fixture.props();
    let table_name = "fanout_events".to_string();
    let namespace = create_partitioned_table(&props, &table_name).await;

    let state = iceberg_session_state(SessionConfig::new_with_ballista());
    let (ctx, cluster) = start_multi_executor_cluster(state).await;

    let catalog_config = IcebergCatalogConfig::new("rest", "rest", props.clone());
    register_iceberg_table(
        &ctx,
        "target",
        catalog_config,
        namespace.clone(),
        table_name.clone(),
    )
    .await
    .expect("register iceberg table");

    let source = (0..TOTAL_ROWS)
        .map(|i| {
            let region = REGIONS[i as usize % REGIONS.len()];
            format!("({}, '{region}')", i + 1)
        })
        .collect::<Vec<_>>()
        .chunks(ROWS_PER_BRANCH)
        .map(|branch| format!("SELECT * FROM (VALUES {})", branch.join(", ")))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    run_sql(&ctx, &format!("INSERT INTO target {source}")).await;

    // Every executor completed at least one task. Timing-safe: the write stage
    // has more tasks than any one executor has slots, so a single executor can
    // never take the whole stage in one bind, and the other polls every 50ms.
    let executors_used = executors_that_ran_tasks(&cluster).await;
    assert_eq!(
        executors_used.len(),
        N_EXECUTORS,
        "every executor should have completed tasks, got {executors_used:?}"
    );

    // The parallel writers still commit exactly one snapshot with every row.
    let catalog = fixture::rest_catalog(&props).await;
    let table = catalog
        .load_table(&TableIdent::new(namespace.clone(), table_name.clone()))
        .await
        .expect("load committed table");
    assert_eq!(
        table.metadata().snapshots().count(),
        1,
        "the distributed write must produce exactly one atomic commit"
    );
    assert_batches_eq!(
        ["+----+", "| n  |", "+----+", "| 12 |", "+----+"],
        &run_sql(&ctx, "SELECT count(*) AS n FROM target").await
    );
}

/// A read pinned to an old snapshot must return that snapshot's rows *and* its
/// schema, even after the table's schema has moved on.
///
/// Executors never see the scheduler's table object; they reload metadata from
/// the catalog themselves. The pin must therefore survive both codecs to reach
/// them, or an executor describes historical rows with the current schema.
///
/// One table, five phases, covering both directions of drift:
///
/// 1. write `{id, name}` -> snapshot 1
/// 2. add `email`, write again -> snapshot 2; a current read sees all six rows
/// 3. read pinned to snapshot 1 — the table has a column that snapshot lacks
/// 4. drop `name`, leaving `{id, email}`
/// 5. read pinned to snapshot 2 — that snapshot has a column the table lacks
///
/// Phase 3 must precede the drop, while the schemas still differ. Phase 5 is the
/// one no executor gets right by accident: the current schema has no `name` to
/// project, though the pinned rows carry the values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_time_travel_pins_snapshot_schema() {
    let _ = env_logger::builder().is_test(true).try_init();

    let fixture = IcebergFixture::start().await;
    let props = fixture.props();
    let table_name = "timetravel".to_string();
    let namespace = create_table(&props, &table_name).await;

    let state = iceberg_session_state(
        SessionConfig::new_with_ballista()
            .with_target_partitions(2)
            .with_ballista_standalone_parallelism(2),
    );
    let ctx = SessionContext::standalone_with_state(state)
        .await
        .expect("start standalone ballista");

    let catalog_config = IcebergCatalogConfig::new("rest", "rest", props.clone());
    register_iceberg_table(
        &ctx,
        "events",
        catalog_config.clone(),
        namespace.clone(),
        table_name.clone(),
    )
    .await
    .expect("register iceberg table");

    // Snapshot 1: three rows under the original {id, name} schema.
    run_sql(
        &ctx,
        "INSERT INTO events VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    )
    .await;
    let snapshot_1 = current_snapshot_id(&props, &namespace, &table_name).await;

    // Evolve the schema: add an `email` column. Snapshot 1 keeps referencing the
    // two-column schema, so current and historical schemas now differ.
    let catalog: Arc<dyn Catalog> = Arc::new(fixture::rest_catalog(&props).await);
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
    let table = catalog.load_table(&table_ident).await.expect("load table");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "email",
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(tx)
        .expect("apply schema update");
    tx.commit(catalog.as_ref()).await.expect("commit schema");

    // A provider registered before the change still exposes the schema it was
    // built with, so writing the new column needs a freshly built provider.
    let evolved = IcebergTableProvider::try_new_with_config(
        catalog.clone(),
        catalog_config.clone(),
        namespace.clone(),
        table_name.clone(),
    )
    .await
    .expect("build evolved provider");
    ctx.register_table("events_v2", Arc::new(evolved))
        .expect("register evolved provider");

    // Snapshot 2: three more rows, now carrying `email`.
    run_sql(
        &ctx,
        "INSERT INTO events_v2 VALUES (4, 'dave', 'dave@x.test'), \
         (5, 'erin', 'erin@x.test'), (6, 'frank', 'frank@x.test')",
    )
    .await;
    let snapshot_2 = current_snapshot_id(&props, &namespace, &table_name).await;

    // Phase 2: the current distributed read sees all six rows under the evolved
    // schema (the header row asserts the schema; rows 1-3 predate `email`).
    assert_batches_eq!(
        [
            "+----+-------+--------------+",
            "| id | name  | email        |",
            "+----+-------+--------------+",
            "| 1  | alice |              |",
            "| 2  | bob   |              |",
            "| 3  | carol |              |",
            "| 4  | dave  | dave@x.test  |",
            "| 5  | erin  | erin@x.test  |",
            "| 6  | frank | frank@x.test |",
            "+----+-------+--------------+",
        ],
        &run_sql(&ctx, "SELECT * FROM events_v2 ORDER BY id").await
    );

    // Phase 3 — pin to snapshot 1, which has no `email` while the table does.
    // Must run before the drop below, or the two schemas match again and the
    // assertion would pass even for a scan that ignored the pin.
    let pinned_v1 = IcebergTableProvider::try_new_with_config(
        catalog.clone(),
        catalog_config.clone(),
        namespace.clone(),
        table_name.clone(),
    )
    .await
    .expect("build provider pinned to snapshot 1")
    .with_snapshot_id(Some(snapshot_1))
    .await
    .expect("pin snapshot 1");
    ctx.register_table("events_v1", Arc::new(pinned_v1))
        .expect("register provider pinned to snapshot 1");

    // The pinned read returns that snapshot's exact historical rows under the
    // schema in effect at that snapshot — no `email` column.
    assert_batches_eq!(
        [
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | alice |",
            "| 2  | bob   |",
            "| 3  | carol |",
            "+----+-------+",
        ],
        &run_sql(&ctx, "SELECT * FROM events_v1 ORDER BY id").await
    );

    // Phase 4 — drop `name`, leaving {id, email}. Snapshot 2 keeps referencing
    // the three-column schema.
    //
    // Dropping `email` instead would read more naturally here, but it hits an
    // iceberg-rust bug: `UpdateSchema` always emits `AddSchema` followed by
    // `SetCurrentSchema { schema_id: -1 }`, i.e. "make the last added schema
    // current". Removing `email` reproduces the table's original {id, name}
    // schema exactly, and the Java REST catalog treats `AddSchema` for a schema
    // it already knows as a no-op — so nothing is "last added" and the commit
    // fails with `Cannot set last added schema: no schema has been added`. Any
    // drop that returns a table to an earlier schema hits this; the fix is to
    // send the resolved schema id rather than -1 when the schema already exists.
    // Dropping `name` sidesteps it, since {id, email} is new.
    let table = catalog
        .load_table(&table_ident)
        .await
        .expect("reload table");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .delete_column("name")
        .apply(tx)
        .expect("apply column delete");
    tx.commit(catalog.as_ref())
        .await
        .expect("commit delete column");

    let dropped = IcebergTableProvider::try_new_with_config(
        catalog.clone(),
        catalog_config.clone(),
        namespace.clone(),
        table_name.clone(),
    )
    .await
    .expect("build provider after drop");
    ctx.register_table("events_v3", Arc::new(dropped))
        .expect("register provider after drop");

    // The current read reflects the dropped column: `name` is gone.
    assert_batches_eq!(
        [
            "+----+--------------+",
            "| id | email        |",
            "+----+--------------+",
            "| 1  |              |",
            "| 2  |              |",
            "| 3  |              |",
            "| 4  | dave@x.test  |",
            "| 5  | erin@x.test  |",
            "| 6  | frank@x.test |",
            "+----+--------------+",
        ],
        &run_sql(&ctx, "SELECT * FROM events_v3 ORDER BY id").await
    );

    // Phase 5 — pin to snapshot 2, which still has the `name` the table just
    // lost. An executor using the current schema has no `name` to project, so
    // the query fails outright even though those rows carry the values.
    let pinned_v2 = IcebergTableProvider::try_new_with_config(
        catalog,
        catalog_config,
        namespace,
        table_name,
    )
    .await
    .expect("build provider pinned to snapshot 2")
    .with_snapshot_id(Some(snapshot_2))
    .await
    .expect("pin snapshot 2");
    ctx.register_table("events_v2_pinned", Arc::new(pinned_v2))
        .expect("register provider pinned to snapshot 2");

    // The pinned read exposes the `name` column that existed at that snapshot,
    // with the dropped column's values still reading back — and rows written
    // before `email` existed read back null.
    assert_batches_eq!(
        [
            "+----+-------+--------------+",
            "| id | name  | email        |",
            "+----+-------+--------------+",
            "| 1  | alice |              |",
            "| 2  | bob   |              |",
            "| 3  | carol |              |",
            "| 4  | dave  | dave@x.test  |",
            "| 5  | erin  | erin@x.test  |",
            "| 6  | frank | frank@x.test |",
            "+----+-------+--------------+",
        ],
        &run_sql(&ctx, "SELECT * FROM events_v2_pinned ORDER BY id").await
    );
}
