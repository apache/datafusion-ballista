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

//! Benchmark derived from TPC-H. This is not an official TPC-H benchmark.

use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use ballista::extension::SessionConfigExt;
use ballista::prelude::SessionContextExt;
use ballista_core::config::{LogRotationPolicy, TaskSchedulingPolicy};
use ballista_core::object_store::{
    runtime_env_with_s3_support, session_config_with_s3_support,
    session_state_with_s3_support,
};
use ballista_executor::executor_process::{
    ExecutorProcessConfig, start_executor_process,
};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use tokio::task::JoinHandle;
use tokio::time::sleep;

// ==================================================================================
// 1. TPC-H Schemas & Constants
// ==================================================================================

const TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

fn get_schema(table: &str) -> Schema {
    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),
        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),
        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),
        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),
        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),
        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),
        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),
        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),
        _ => unimplemented!("Schema for table {} not defined", table),
    }
}

// ==================================================================================
// 2. Test Logic
// ==================================================================================

/// Defines a test function for a specific TPC-H query number.
macro_rules! tpch_test {
    ($test_name:ident, $query_no:expr) => {
        #[test]
        fn $test_name() -> Result<(), Box<dyn std::error::Error>> {
            // We spawn a thread with a large stack to avoid overflow during complex plan optimization
            let thread_handle = std::thread::Builder::new()
                .name(stringify!($test_name).to_string())
                .stack_size(32 * 1024 * 1024)
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    runtime.block_on(run_single_query($query_no)).unwrap();
                })?;

            thread_handle.join().unwrap();
            Ok(())
        }
    };
}

async fn run_single_query(query_no: usize) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup Cluster
    // Note: Creating a new cluster for each test ensures isolation but is heavier.
    let cluster = TestCluster::new(2).await;

    // 2. Setup Client
    let config = SessionConfig::new_with_ballista()
        .with_ballista_job_name(&format!("TPCH Q{}", query_no))
        .with_target_partitions(4);

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .build();

    let connect_url =
        format!("df://{}:{}", cluster.scheduler_host, cluster.scheduler_port);
    let ctx = SessionContext::remote_with_state(&connect_url, state).await?;

    // 3. Register Data
    let workspace_root = find_workspace_root();
    let data_path = workspace_root.join("data");

    if !data_path.exists() {
        // If data doesn't exist, we can't fail the test in CI or it breaks the build unnecessarily.
        // We print a skip message. In a real rigorous env, you might want to panic here.
        println!(
            "SKIP: Data path not found at {:?}. Skipping execution.",
            data_path
        );
        return Ok(());
    }

    for &table in TABLES {
        let file_path = data_path.join(format!("{}.parquet", table));
        if file_path.exists() {
            let _schema = get_schema(table);
            ctx.register_parquet(
                table,
                file_path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        } else {
            // If a specific table file is missing, we warn but proceed,
            // as some queries might not need all tables.
            eprintln!("WARNING: Table '{}' not found at {:?}", table, file_path);
        }
    }

    // 3.1 Set custom parameters
    ctx.sql("SET datafusion.execution.parquet.schema_force_view_types = true")
        .await?
        .show()
        .await?;
    ctx.sql("SET ballista.grpc_client_max_message_size = 100") // 2MB = 2097152
        .await?
        .show()
        .await?;

    // 4. Find and Run Query
    let query_file = find_query_file(&workspace_root, query_no);
    if let Some(path) = query_file {
        let sql_content = fs::read_to_string(&path)?;
        let queries: Vec<String> = sql_content
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        println!("--- Running TPCH Q{} ---", query_no);
        for sql in queries {
            let start = Instant::now();
            let df = ctx.sql(&sql).await?;
            let batches = df.collect().await?;
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!(
                "Q{} finished: {} rows in {:.2?} ms",
                query_no,
                row_count,
                start.elapsed().as_millis()
            );
        }
    } else {
        println!("SKIP: Query file for Q{} not found.", query_no);
    }

    Ok(())
}

// ==================================================================================
// 3. Infrastructure (Cluster, Paths, Logging)
// ==================================================================================

struct TestCluster {
    _scheduler_handle: JoinHandle<()>,
    _executor_handles: Vec<JoinHandle<()>>,
    pub scheduler_host: String,
    pub scheduler_port: u16,
}

impl TestCluster {
    async fn new(num_executors: usize) -> Self {
        init_test_logging();

        let s_host = "127.0.0.1";
        let s_port = get_free_port();
        let s_addr = format!("{}:{}", s_host, s_port);
        // println!("Starting Scheduler at {}", s_addr);

        let config = SchedulerConfig::default()
            .with_hostname(s_host.to_string())
            .with_port(s_port)
            .with_override_config_producer(Arc::new(session_config_with_s3_support))
            .with_override_session_builder(Arc::new(session_state_with_s3_support));

        let cluster_obj = BallistaCluster::new_from_config(&config)
            .await
            .expect("Failed to create cluster struct");

        let scheduler_socket_addr = s_addr.parse().unwrap();
        let config_arc = Arc::new(config);

        let scheduler_handle = tokio::spawn(async move {
            let _ = start_server(cluster_obj, scheduler_socket_addr, config_arc).await;
        });

        wait_for_port(s_host, s_port, "Scheduler").await;

        let mut executor_handles = Vec::new();
        for _i in 0..num_executors {
            let grpc_port = get_free_port();
            let flight_port = get_free_port();
            let bind_host = "127.0.0.1";

            let executor_config = ExecutorProcessConfig {
                bind_host: bind_host.to_string(),
                external_host: None,
                port: flight_port,
                grpc_port,
                scheduler_host: s_host.to_string(),
                scheduler_port: s_port,
                scheduler_connect_timeout_seconds: 10,
                concurrent_tasks: 2,
                task_scheduling_policy: TaskSchedulingPolicy::PullStaged,
                log_dir: None,
                work_dir: None,
                special_mod_log_level: "info".to_string(),
                print_thread_info: false,
                log_rotation_policy: LogRotationPolicy::Never,
                job_data_ttl_seconds: 3600,
                job_data_clean_up_interval_seconds: 60,
                grpc_max_decoding_message_size: 16 * 1024 * 1024,
                grpc_max_encoding_message_size: 16 * 1024 * 1024,
                grpc_server_config: Default::default(),
                executor_heartbeat_interval_seconds: 60,
                override_execution_engine: None,
                override_function_registry: None,
                override_runtime_producer: Some(Arc::new(runtime_env_with_s3_support)),
                override_config_producer: Some(Arc::new(session_config_with_s3_support)),
                override_logical_codec: None,
                override_physical_codec: None,
                override_arrow_flight_service: None,
            };

            let handle = tokio::spawn(async move {
                let _ = start_executor_process(Arc::new(executor_config)).await;
            });

            wait_for_port(bind_host, flight_port, "Executor").await;
            executor_handles.push(handle);
        }

        TestCluster {
            _scheduler_handle: scheduler_handle,
            _executor_handles: executor_handles,
            scheduler_host: s_host.to_string(),
            scheduler_port: s_port,
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        self._scheduler_handle.abort();
        for h in &self._executor_handles {
            h.abort();
        }
    }
}

static INIT_LOGGING: Once = Once::new();

fn init_test_logging() {
    INIT_LOGGING.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info,ballista=debug,datafusion=info")
            .with_test_writer()
            .try_init();
    });
}

fn get_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_for_port(host: &str, port: u16, name: &str) {
    let start = Instant::now();
    let timeout = Duration::from_secs(10);
    while start.elapsed() < timeout {
        if TcpStream::connect((host, port)).is_ok() {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("Timeout waiting for {} to listen on port {}", name, port);
}

fn find_workspace_root() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if path.join("target").exists() || path.join("benchmarks").exists() {
            return path;
        }
        if !path.pop() {
            return std::env::current_dir().unwrap();
        }
    }
}

fn find_query_file(root: &Path, query_no: usize) -> Option<PathBuf> {
    let paths = vec![
        root.join(format!("benchmarks/queries/q{}.sql", query_no)),
        root.join(format!("queries/q{}.sql", query_no)),
    ];
    paths.into_iter().find(|p| p.exists())
}

tpch_test!(q1, 1);
tpch_test!(q2, 2);
tpch_test!(q3, 3);
tpch_test!(q4, 4);
tpch_test!(q5, 5);
tpch_test!(q6, 6);
tpch_test!(q7, 7);
tpch_test!(q8, 8);
tpch_test!(q9, 9);
tpch_test!(q10, 10);
tpch_test!(q11, 11);
tpch_test!(q12, 12);
tpch_test!(q13, 13);
tpch_test!(q14, 14);
tpch_test!(q15, 15);
tpch_test!(q16, 16);
tpch_test!(q17, 17);
tpch_test!(q18, 18);
tpch_test!(q19, 19);
tpch_test!(q20, 20);
tpch_test!(q21, 21);
tpch_test!(q22, 22);
