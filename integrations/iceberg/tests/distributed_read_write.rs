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

//! End-to-end distributed Iceberg write/read test.
//!
//! This runs against the docker fixture in `integrations/iceberg/dev` (an
//! Iceberg REST catalog + MinIO), so it is behind the `integration-tests`
//! feature and is not part of a plain `cargo test`:
//!
//! ```bash
//! docker compose -f integrations/iceberg/dev/docker-compose.yaml up -d --wait
//! cargo test -p iceberg-ballista --features integration-tests --test distributed_read_write
//! ```
//!
//! The endpoints can be overridden with the `ICEBERG_REST_URI` and
//! `ICEBERG_S3_ENDPOINT` environment variables.

#![cfg(feature = "integration-tests")]

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::Int32Type;
use ballista::datafusion::execution::{SessionState, SessionStateBuilder};
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_executor::new_standalone_executor_from_state;
use ballista_scheduler::standalone::new_standalone_scheduler_from_state;
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec,
};
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_ballista::{
    IcebergCatalogConfig, register_iceberg_catalog, register_iceberg_codecs,
    register_iceberg_table,
};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::IcebergTableProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use tokio::sync::Mutex;

/// Serializes the catalog-mutating tests in this binary. The REST fixture is
/// backed by in-memory SQLite, which rejects concurrent commits with
/// `SQLITE_BUSY`. Each test still exercises parallelism *internally* (multiple
/// write tasks / executors); this only stops the two test cases from committing
/// to the catalog at the same time, so the suite is robust under a parallel test
/// harness (`cargo test`, `nextest`) without relying on `--test-threads=1`.
static CATALOG_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Table name unique per run, so reruns don't collide in the shared catalog.
fn unique_table_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}_{millis}")
}

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

/// Extracts the single `i64` value of the first column (a COUNT result).
fn single_i64(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .find_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0))
        })
        .expect("i64 column")
}

/// Flattens the first column of every batch into a `Vec<i32>`.
fn i32_values(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int32Type>().values().to_vec())
        .collect()
}

/// Flattens a named string column across all batches, preserving nulls.
fn string_values(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of(column).expect("column present");
            b.column(idx)
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn catalog_props() -> HashMap<String, String> {
    let rest_uri = std::env::var("ICEBERG_REST_URI")
        .unwrap_or_else(|_| "http://localhost:8181".to_string());
    let s3_endpoint = std::env::var("ICEBERG_S3_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    HashMap::from([
        ("uri".to_string(), rest_uri),
        ("s3.endpoint".to_string(), s3_endpoint),
        ("s3.access-key-id".to_string(), "admin".to_string()),
        ("s3.secret-access-key".to_string(), "password".to_string()),
        ("s3.region".to_string(), "us-east-1".to_string()),
        ("s3.path-style-access".to_string(), "true".to_string()),
    ])
}

async fn build_rest_catalog(props: &HashMap<String, String>) -> impl Catalog + use<> {
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", props.clone())
        .await
        .expect("build rest catalog")
}

/// Ensures the shared test namespace exists, tolerating a concurrent creator
/// (tests run in parallel and share this namespace).
async fn ensure_namespace(catalog: &impl Catalog) -> NamespaceIdent {
    let namespace = NamespaceIdent::new("ballista_it".to_string());
    if !catalog.namespace_exists(&namespace).await.unwrap()
        && let Err(e) = catalog.create_namespace(&namespace, HashMap::new()).await
        && !catalog.namespace_exists(&namespace).await.unwrap()
    {
        panic!("create namespace: {e}");
    }
    namespace
}

/// Creates `table_name` with `schema` (optionally partitioned) in the shared
/// test namespace and returns that namespace.
async fn create_table_with(
    props: &HashMap<String, String>,
    table_name: &str,
    schema: Schema,
    partition_spec: Option<UnboundPartitionSpec>,
) -> NamespaceIdent {
    let catalog = build_rest_catalog(props).await;
    let namespace = ensure_namespace(&catalog).await;

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

/// Column names of a result set, in order.
fn column_names(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .first()
        .expect("at least one batch")
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Loads the table from the catalog and returns its current snapshot id.
async fn current_snapshot_id(
    props: &HashMap<String, String>,
    namespace: &NamespaceIdent,
    table_name: &str,
) -> i64 {
    build_rest_catalog(props)
        .await
        .load_table(&TableIdent::new(namespace.clone(), table_name.to_string()))
        .await
        .expect("load table")
        .metadata()
        .current_snapshot_id()
        .expect("table has a snapshot")
}

/// Creates a table partitioned by `region` (identity). A distributed INSERT then
/// fans the rows out to one writer per region — exercising the partition-value
/// expression (`PartitionExpr`) serialization across the cluster.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_insert_and_read() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _catalog_guard = CATALOG_GUARD.lock().await;

    let props = catalog_props();
    let table_name = unique_table_name("events");
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
    let count = single_i64(&run_sql(&ctx, "SELECT count(*) AS n FROM events").await);
    assert_eq!(count, 0, "table is empty before the first insert");

    run_sql(
        &ctx,
        "INSERT INTO events VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    )
    .await;

    let count = single_i64(&run_sql(&ctx, "SELECT count(*) AS n FROM events").await);
    assert_eq!(count, 3, "expected 3 rows after distributed insert");

    let rows = run_sql(&ctx, "SELECT id, name FROM events ORDER BY id").await;
    assert_eq!(i32_values(&rows), vec![1, 2, 3]);

    // Insert again through the same registration and re-read. `scan` reloads
    // table metadata from the catalog every time, so each query plans against
    // the snapshot current at *that* moment — the codec then pins it for the
    // executors. A provider (or a cached catalog/table) that went stale after
    // the first read would still report 3 rows here.
    run_sql(&ctx, "INSERT INTO events VALUES (4, 'dave'), (5, 'erin')").await;

    let count = single_i64(&run_sql(&ctx, "SELECT count(*) AS n FROM events").await);
    assert_eq!(count, 5, "re-read must observe the interposed insert");

    let rows = run_sql(&ctx, "SELECT id, name FROM events ORDER BY id").await;
    assert_eq!(
        i32_values(&rows),
        vec![1, 2, 3, 4, 5],
        "re-read returns both the original and the newly inserted rows"
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
    let count = single_i64(
        &run_sql(
            &ctx,
            &format!("SELECT count(*) AS n FROM ice.ballista_it.{table_name}"),
        )
        .await,
    );
    assert_eq!(count, 5, "catalog-qualified distributed read");
}

/// Distributed correctness on a real multi-executor cluster, writing a
/// **partitioned** table.
///
/// Where [`distributed_insert_and_read`] uses standalone Ballista (one in-process
/// executor) and an unpartitioned table, this stands up a single scheduler with
/// **several in-process executors** and writes a table partitioned by `region`. A
/// partitioned write injects a partition-value expression (`PartitionExpr`) into
/// the physical plan, so this exercises that expression's serialization through
/// the codec on top of the plan-node serialization — and fans the rows out to one
/// writer per region across the executors.
///
/// The assertions target the correctness properties of a distributed, multi-writer
/// write:
///   1. the write commits exactly **one** atomic snapshot (not one per task),
///   2. the parallel writers contributed multiple data files (one per region), and
///   3. every input row lands exactly once (no loss, no duplication).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_multi_executor_insert_commits_all_rows() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _catalog_guard = CATALOG_GUARD.lock().await;

    const N_EXECUTORS: usize = 2;
    const SLOTS_PER_EXECUTOR: usize = 2;
    const WRITE_PARTITIONS: usize = 8;
    const REGIONS: [&str; 4] = ["a", "b", "c", "d"];
    // REGIONS.len() * 3.
    const TOTAL_ROWS: i32 = 12;

    let props = catalog_props();
    let table_name = unique_table_name("parallel_events");
    let namespace = create_partitioned_table(&props, &table_name).await;

    // --- Bring up one scheduler + N executors in-process (real multi-executor) ---
    let state = iceberg_session_state(
        SessionConfig::new_with_ballista().with_target_partitions(WRITE_PARTITIONS),
    );

    let scheduler_addr = new_standalone_scheduler_from_state(&state)
        .await
        .expect("start scheduler");
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
    // A serializable VALUES source whose rows span every region; the partitioned
    // write fans them out to one writer per region across the executors, which is
    // the path that injects PartitionExpr into the physical plan. (A registered
    // MemTable would not work here — it's a custom TableProvider that Ballista
    // cannot serialize into the logical plan.)
    let values = (0..TOTAL_ROWS)
        .map(|i| {
            let region = REGIONS[i as usize % REGIONS.len()];
            format!("({}, '{region}')", i + 1)
        })
        .collect::<Vec<_>>()
        .join(", ");
    run_sql(&ctx, &format!("INSERT INTO target VALUES {values}")).await;

    // (1) The distributed write committed exactly once (a single atomic
    // snapshot), not one commit per task. Checked straight from the catalog so
    // it isolates write correctness from the distributed read-back below.
    let catalog = build_rest_catalog(&props).await;
    let table = catalog
        .load_table(&TableIdent::new(namespace.clone(), table_name.clone()))
        .await
        .expect("load committed table");
    assert_eq!(
        table.metadata().snapshots().count(),
        1,
        "the distributed write must produce exactly one atomic commit"
    );

    // (2) The parallel writers each produced a data file (one per region), all
    // coalesced into that single commit.
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
        data_files >= 2,
        "partitioned write should produce multiple data files, got {data_files}"
    );

    // (3) Every row landed exactly once.
    let count = single_i64(&run_sql(&ctx, "SELECT count(*) AS n FROM target").await);
    assert_eq!(count as i32, TOTAL_ROWS, "row count after parallel insert");

    let ids = i32_values(&run_sql(&ctx, "SELECT id FROM target ORDER BY id").await);
    assert_eq!(
        ids,
        (1..=TOTAL_ROWS).collect::<Vec<_>>(),
        "exact id set after parallel insert (no lost or duplicated rows)"
    );

    // (4) Predicate pushdown survives serialization: a WHERE clause is pushed into
    // the distributed scan (and re-applied above it), and the result is correct.
    let half = TOTAL_ROWS / 2;
    let filtered_ids = i32_values(
        &run_sql(
            &ctx,
            &format!("SELECT id FROM target WHERE id <= {half} ORDER BY id"),
        )
        .await,
    );
    assert_eq!(
        filtered_ids,
        (1..=half).collect::<Vec<_>>(),
        "predicate-filtered distributed read"
    );
}

/// A distributed read pinned to an old snapshot must return that snapshot's rows
/// *and* that snapshot's schema — even after the table's schema has changed.
///
/// The executor never sees the scheduler's table object; it reloads metadata from
/// the catalog itself. So the pin has to survive both codecs to get there, and an
/// executor that used the table's *current* schema would describe the historical
/// rows wrongly.
///
/// One table, five phases, so both directions of schema drift are covered by a
/// single cluster:
///
/// 1. write `{id, name}` -> snapshot 1
/// 2. add `email`, write again -> snapshot 2; a current read sees all six rows
/// 3. read pinned to snapshot 1 — current schema has a column it lacks
/// 4. drop `name`, leaving `{id, email}`
/// 5. read pinned to snapshot 2 — that snapshot has a column current lacks
///
/// Phase 3 must come before the drop, while the schemas still differ. Phase 5 is
/// the one an executor can't get right by accident: the current schema has no
/// `name` to project, though the pinned snapshot's rows have the values.
///
/// Phase 4 drops `name` rather than `email` to work around an unrelated
/// iceberg-rust bug; see the comment there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_time_travel_pins_snapshot_schema() {
    let _ = env_logger::builder().is_test(true).try_init();
    let _catalog_guard = CATALOG_GUARD.lock().await;

    let props = catalog_props();
    let table_name = unique_table_name("timetravel");
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
    let catalog: Arc<dyn Catalog> = Arc::new(build_rest_catalog(&props).await);
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

    // The current distributed read sees all six rows under the evolved schema.
    let current = run_sql(&ctx, "SELECT * FROM events_v2 ORDER BY id").await;
    assert_eq!(
        column_names(&current),
        vec!["id", "name", "email"],
        "phase 2: current read exposes the evolved schema"
    );
    assert_eq!(i32_values(&current), vec![1, 2, 3, 4, 5, 6]);

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

    // The pinned read returns that snapshot's rows under that snapshot's schema.
    let pinned = run_sql(&ctx, "SELECT * FROM events_v1 ORDER BY id").await;
    assert_eq!(
        column_names(&pinned),
        vec!["id", "name"],
        "phase 3: pinned read exposes the schema in effect at that snapshot"
    );
    assert_eq!(
        i32_values(&pinned),
        vec![1, 2, 3],
        "phase 3: exact historical row set from the pinned snapshot"
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

    let current = run_sql(&ctx, "SELECT * FROM events_v3 ORDER BY id").await;
    assert_eq!(
        column_names(&current),
        vec!["id", "email"],
        "phase 4: current read reflects the dropped column"
    );
    assert_eq!(i32_values(&current), vec![1, 2, 3, 4, 5, 6]);

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

    let pinned = run_sql(&ctx, "SELECT * FROM events_v2_pinned ORDER BY id").await;
    assert_eq!(
        column_names(&pinned),
        vec!["id", "name", "email"],
        "phase 5: pinned read exposes the column that existed at that snapshot"
    );
    assert_eq!(i32_values(&pinned), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(
        string_values(&pinned, "name"),
        ["alice", "bob", "carol", "dave", "erin", "frank"]
            .map(|s| Some(s.to_string()))
            .to_vec(),
        "phase 5: the dropped column's values still read back from the pinned \
         snapshot"
    );
    assert_eq!(
        string_values(&pinned, "email"),
        vec![
            None,
            None,
            None,
            Some("dave@x.test".to_string()),
            Some("erin@x.test".to_string()),
            Some("frank@x.test".to_string()),
        ],
        "phase 5: rows written before `email` existed read back null"
    );
}
