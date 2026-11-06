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

// Each test binary compiles this module separately and uses a subset of it.
#![allow(dead_code)]

use std::io::Write;
use std::path::Path;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::config::BALLISTA_STANDALONE_PARALLELISM;
use ballista_core::serde::{
    BallistaCodec, protobuf::scheduler_grpc_client::SchedulerGrpcClient,
};
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_scheduler::SessionBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use tempfile::TempDir;

/// Returns the `testdata` directory of this crate.
pub fn test_data_dir() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .display()
        .to_string()
}

/// The `testdata` directory as an rstest fixture.
#[rstest::fixture]
pub fn test_data() -> String {
    test_data_dir()
}

/// The path of `alltypes_plain.parquet` in the `testdata` directory.
pub fn alltypes_parquet() -> String {
    format!("{}/alltypes_plain.parquet", test_data_dir())
}

/// Registers `alltypes_plain.parquet` as the table `name`.
pub async fn register_alltypes(ctx: &SessionContext, name: &str) {
    ctx.register_parquet(name, &alltypes_parquet(), Default::default())
        .await
        .unwrap();
}

/// The query of [`ALLTYPES_ID_GT_4`], over the table `test`.
pub const ALLTYPES_ID_GT_4_SQL: &str =
    "select string_col, timestamp_col from test where id > 4";

/// The result of [`ALLTYPES_ID_GT_4_SQL`], or of the same query over the
/// contents of `alltypes_plain.parquet`.
pub const ALLTYPES_ID_GT_4: [&str; 7] = [
    "+------------+---------------------+",
    "| string_col | timestamp_col       |",
    "+------------+---------------------+",
    "| 31         | 2009-03-01T00:01:00 |",
    "| 30         | 2009-04-01T00:00:00 |",
    "| 31         | 2009-04-01T00:01:00 |",
    "+------------+---------------------+",
];

/// A grouped count over the table `test`.
pub const ALLTYPES_GROUP_BY_SQL: &str = "select string_col, count(*) from test where id > 4 group by string_col order by string_col";

/// The result of [`ALLTYPES_GROUP_BY_SQL`].
pub const ALLTYPES_GROUP_BY: [&str; 6] = [
    "+------------+----------+",
    "| string_col | count(*) |",
    "+------------+----------+",
    "| 30         | 1        |",
    "| 31         | 2        |",
    "+------------+----------+",
];

/// A self-join of `alltypes_plain.parquet`, registered as `t0` and `t1`.
pub const ALLTYPES_JOIN_SQL: &str =
    "select t0.id from t0 join t1 on t0.id = t1.id order by t0.id desc limit 5";

/// The result of [`ALLTYPES_JOIN_SQL`].
pub const ALLTYPES_JOIN: [&str; 9] = [
    "+----+", "| id |", "+----+", "| 7  |", "| 6  |", "| 5  |", "| 4  |", "| 3  |",
    "+----+",
];

/// starts a ballista cluster for integration tests
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

/// Starts a scheduler and `executors` executors of `vcores` vcores each, all
/// in this process and all from `session_state`.
pub async fn setup_test_cluster_with_executors(
    session_state: &SessionState,
    executors: usize,
    vcores: usize,
) -> (String, u16) {
    let addr = ballista_scheduler::standalone::new_standalone_scheduler_from_state(
        session_state,
    )
    .await
    .expect("scheduler to be created");

    let host = "localhost".to_string();

    for _ in 0..executors {
        let scheduler =
            connect_to_scheduler(format!("http://{}:{}", host, addr.port())).await;
        ballista_executor::new_standalone_executor_from_state(
            scheduler,
            vcores,
            session_state,
        )
        .await
        .expect("executor to be created");
    }

    log::info!(
        "test scheduler with {executors} executors created at: {}:{}",
        host,
        addr.port()
    );

    (host, addr.port())
}

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

pub async fn standalone_context() -> SessionContext {
    SessionContext::standalone().await.unwrap()
}

/// A standalone context with `alltypes_plain.parquet` registered as `test`.
pub async fn alltypes_context() -> SessionContext {
    let ctx = standalone_context().await;
    register_alltypes(&ctx, "test").await;
    ctx
}

pub async fn remote_context() -> SessionContext {
    let (host, port) = setup_test_cluster().await;
    SessionContext::remote(&format!("df://{host}:{port}"))
        .await
        .unwrap()
}

pub async fn standalone_context_with_state() -> SessionContext {
    let state = state_with_config(SessionConfig::new_with_ballista());
    SessionContext::standalone_with_state(state).await.unwrap()
}

pub async fn remote_context_with_state() -> SessionContext {
    let state = state_with_config(SessionConfig::new_with_ballista());
    let (host, port) = setup_test_cluster_with_state(state.clone()).await;
    SessionContext::remote_with_state(&format!("df://{host}:{port}"), state)
        .await
        .unwrap()
}

/// A session state with DataFusion's default features and `config`.
pub fn state_with_config(config: SessionConfig) -> SessionState {
    SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build()
}

/// A Ballista session state with four target partitions and four standalone
/// executor vcores, so a stage of four partitions runs at once whatever the
/// machine's core count. `options` are set on top, in order.
pub fn ballista_state(options: &[(&str, &str)]) -> SessionState {
    let mut config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .set_str(BALLISTA_STANDALONE_PARALLELISM, "4");
    for (key, value) in options {
        config = config.set_str(key, value);
    }
    state_with_config(config)
}

/// A DataFusion session with four target partitions, used as the reference
/// for results from a Ballista session.
pub fn datafusion_context() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4))
}

/// Writes the tables of [`orders_customers_dir`] to `dir`. `orders` is split
/// over four files, so its scan has four partitions.
fn write_orders_customers(dir: &Path) {
    let orders = dir.join("orders");
    std::fs::create_dir(&orders).unwrap();
    for file in 0..4 {
        let mut out =
            std::fs::File::create(orders.join(format!("part-{file}.csv"))).unwrap();
        writeln!(out, "o_id,o_cust,o_amount").unwrap();
        for i in 0..250 {
            let id = file * 250 + i;
            writeln!(out, "{id},{},{}", id % 97, (id * 7) % 1000).unwrap();
        }
    }
    let customers = dir.join("customers");
    std::fs::create_dir(&customers).unwrap();
    let mut out = std::fs::File::create(customers.join("part-0.csv")).unwrap();
    writeln!(out, "c_id,c_name,c_region").unwrap();
    for id in 0..100 {
        writeln!(out, "{id},customer-{id},region-{}", id % 5).unwrap();
    }
}

/// A temporary directory holding an `orders` table of four files and a small
/// `customers` table, one directory each.
pub fn orders_customers_dir() -> TempDir {
    let dir = TempDir::new().unwrap();
    write_orders_customers(dir.path());
    dir
}

/// Registers the tables of [`orders_customers_dir`].
pub async fn register_orders_customers(ctx: &SessionContext, dir: &Path) {
    register_csv_tables(ctx, dir, &["orders", "customers"]).await;
}

/// Queries over the tables of [`orders_customers_dir`], each with whether its
/// result has no defined row order, so its rows are compared sorted. The first
/// three are a scan, an aggregate and a join.
pub const ORDERS_QUERIES: &[(&str, bool)] = &[
    // Scan and a collapse to one partition.
    ("SELECT count(*), sum(o_amount) FROM orders", false),
    // Hash repartition, aggregate, sort.
    (
        "SELECT o_cust, count(*) AS n, sum(o_amount) AS total \
         FROM orders GROUP BY o_cust ORDER BY o_cust",
        false,
    ),
    // Join (broadcast, since customers is small), aggregate, sort.
    (
        "SELECT c_region, count(*) AS n, sum(o_amount) AS total \
         FROM orders JOIN customers ON o_cust = c_id \
         GROUP BY c_region ORDER BY c_region",
        false,
    ),
    // Top-k over an aggregate.
    (
        "SELECT o_cust, sum(o_amount) AS total FROM orders \
         GROUP BY o_cust ORDER BY total DESC, o_cust LIMIT 5",
        false,
    ),
    // Filter with no exchange but the final merge.
    (
        "SELECT o_id, o_amount FROM orders WHERE o_amount > 990 ORDER BY o_id",
        false,
    ),
    // Union of two scans.
    (
        "SELECT o_id AS id FROM orders WHERE o_id < 10 \
         UNION ALL SELECT c_id AS id FROM customers WHERE c_id < 5",
        true,
    ),
    // Window function over a hash-partitioned input.
    (
        "SELECT o_cust, o_id, \
         row_number() OVER (PARTITION BY o_cust ORDER BY o_id) AS rn \
         FROM orders WHERE o_cust < 3 ORDER BY o_cust, o_id",
        false,
    ),
    // A scalar subquery, joined to every row: each task of the join's
    // stage reads the subquery's whole result.
    (
        "SELECT o_id, o_amount FROM orders \
         WHERE o_amount > (SELECT avg(o_amount) + 400 FROM orders) ORDER BY o_id",
        false,
    ),
];

/// A standalone cluster over [`ballista_state`] with `options`, with the
/// tables of [`orders_customers_dir`] registered.
pub async fn orders_customers_context(
    dir: &Path,
    options: &[(&str, &str)],
) -> SessionContext {
    let ctx = SessionContext::standalone_with_state(ballista_state(options))
        .await
        .unwrap();
    register_orders_customers(&ctx, dir).await;
    ctx
}

/// Registers each directory `dir/<name>` of header-bearing `.csv` files as
/// the table `<name>`.
pub async fn register_csv_tables(ctx: &SessionContext, dir: &Path, names: &[&str]) {
    for name in names {
        ctx.register_csv(
            *name,
            dir.join(name).to_str().unwrap(),
            CsvReadOptions::new()
                .has_header(true)
                .file_extension(".csv"),
        )
        .await
        .unwrap();
    }
}

/// Writes `batch` to the Parquet file `path`.
pub fn write_parquet_file(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// Runs `SET key = 'value'`.
pub async fn set_option(ctx: &SessionContext, key: &str, value: &str) {
    ctx.sql(&format!("SET {key} = '{value}'"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

/// The result of `sql` as a formatted table.
pub async fn collect_text(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

/// Runs `sql` on `ctx` and compares the formatted result, ignoring surrounding
/// whitespace, with `expected`. A difference, or a failed query, adds a
/// message that starts with `[label]` to `mismatches`.
pub async fn check_matches(
    ctx: &SessionContext,
    label: &str,
    sql: &str,
    expected: &str,
    mismatches: &mut Vec<String>,
) {
    match ctx.sql(sql).await.unwrap().collect().await {
        Ok(batches) => {
            let actual = pretty_format_batches(&batches).unwrap().to_string();
            if actual.trim() != expected.trim() {
                mismatches.push(format!(
                    "[{label}] expected:\n{expected}\nactual:\n{actual}"
                ));
            }
        }
        Err(error) => mismatches.push(format!("[{label}] failed: {error}")),
    }
}

/// The lines of the formatted table of the result of `sql`.
pub async fn collect_lines(ctx: &SessionContext, sql: &str) -> Vec<String> {
    collect_text(ctx, sql)
        .await
        .lines()
        .map(str::to_owned)
        .collect()
}

/// Asserts that the formatted `results` have the lines `expected`.
pub fn assert_result_eq(expected: Vec<&str>, results: &[RecordBatch]) {
    assert_eq!(
        expected,
        pretty_format_batches(results)
            .unwrap()
            .to_string()
            .trim()
            .lines()
            .collect::<Vec<&str>>()
    );
}

#[ctor::ctor(unsafe)]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista=debug,ballista_scheduler=debug,ballista_executor=debug")
        //.parse_filters("ballista=debug,ballista_scheduler-rs=debug,ballista_executor=debug,datafusion=debug")
        .is_test(true)
        .try_init();
}
