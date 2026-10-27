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

mod common;

/// End-to-end tests of `ballista.execution.mode = pipelined`, run against a
/// standalone cluster, which runs push-staged for a pipelined session. Each
/// query's results are compared with the same query run in blocking mode.
#[cfg(test)]
#[cfg(feature = "standalone")]
mod pipelined {
    use crate::common::setup_test_cluster_with_executors;
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::{
        BALLISTA_EXECUTION_MODE, BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ,
        BALLISTA_STANDALONE_PARALLELISM,
    };
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use futures::StreamExt;
    use std::io::Write;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Writes an `orders` table split over four files, so its scan has four
    /// partitions, and a small `customers` table.
    fn write_tables(dir: &Path) {
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
        let mut out = std::fs::File::create(dir.join("customers.csv")).unwrap();
        writeln!(out, "c_id,c_name,c_region").unwrap();
        for id in 0..100 {
            writeln!(out, "{id},customer-{id},region-{}", id % 5).unwrap();
        }
    }

    fn state(options: &[(&str, &str)]) -> SessionState {
        // A pipelined job wants a vcore per partition of its widest stage at
        // once, so give the standalone executor as many vcores as the plan
        // has partitions, whatever the machine's core count.
        let mut config = SessionConfig::new_with_ballista()
            .with_target_partitions(4)
            .set_str(BALLISTA_STANDALONE_PARALLELISM, "4");
        for (key, value) in options {
            config = config.set_str(key, value);
        }
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build()
    }

    /// A standalone cluster: one scheduler and one executor.
    async fn context(dir: &Path, options: &[(&str, &str)]) -> SessionContext {
        let ctx = SessionContext::standalone_with_state(state(options))
            .await
            .unwrap();
        register_tables(&ctx, dir).await;
        ctx
    }

    /// A scheduler with `executors` executors of `vcores` vcores each.
    async fn cluster_context(
        dir: &Path,
        options: &[(&str, &str)],
        executors: usize,
        vcores: usize,
    ) -> SessionContext {
        let state = state(options);
        let (host, port) =
            setup_test_cluster_with_executors(&state, executors, vcores).await;
        let ctx =
            SessionContext::remote_with_state(&format!("df://{host}:{port}"), state)
                .await
                .unwrap();
        register_tables(&ctx, dir).await;
        ctx
    }

    async fn register_tables(ctx: &SessionContext, dir: &Path) {
        ctx.register_csv(
            "orders",
            dir.join("orders").to_str().unwrap(),
            CsvReadOptions::new(),
        )
        .await
        .unwrap();
        ctx.register_csv(
            "customers",
            dir.join("customers.csv").to_str().unwrap(),
            CsvReadOptions::new(),
        )
        .await
        .unwrap();
    }

    async fn rows(ctx: &SessionContext, sql: &str, sorted: bool) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let text = pretty_format_batches(&batches).unwrap().to_string();
        let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
        if sorted {
            lines.sort();
        }
        lines
    }

    const QUERIES: &[(&str, bool)] = &[
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

    async fn check_matches_blocking(dir: &Path, pipelined: &SessionContext) {
        let blocking = context(dir, &[]).await;
        for (sql, sorted) in QUERIES {
            let expected = rows(&blocking, sql, *sorted).await;
            let actual = rows(pipelined, sql, *sorted).await;
            assert_eq!(actual, expected, "pipelined result differs for: {sql}");
        }
    }

    #[tokio::test]
    async fn pipelined_results_match_blocking() {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());
        let pipelined =
            context(dir.path(), &[(BALLISTA_EXECUTION_MODE, "pipelined")]).await;
        check_matches_blocking(dir.path(), &pipelined).await;
    }

    /// Readers normally take co-located producers straight from the
    /// executor's registry. Forcing remote reads sends every exchange through
    /// Arrow Flight instead.
    #[tokio::test]
    async fn pipelined_results_match_blocking_over_flight() {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());
        let pipelined = context(
            dir.path(),
            &[
                (BALLISTA_EXECUTION_MODE, "pipelined"),
                (BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ, "true"),
            ],
        )
        .await;
        check_matches_blocking(dir.path(), &pipelined).await;
    }

    /// Two executors of two vcores each, so every four-partition stage is
    /// split between them and its exchanges cross executors.
    #[tokio::test]
    async fn pipelined_results_match_blocking_across_executors() {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());
        let pipelined =
            cluster_context(dir.path(), &[(BALLISTA_EXECUTION_MODE, "pipelined")], 2, 2)
                .await;
        check_matches_blocking(dir.path(), &pipelined).await;
    }

    /// A client that stops reading part-way must not leave its job holding
    /// vcores: the next job, which needs every vcore, still runs.
    #[tokio::test]
    async fn dropping_the_results_early_frees_the_cluster() {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());
        // Small batches, so the scan stays blocked on its full queues.
        let ctx = context(
            dir.path(),
            &[
                (BALLISTA_EXECUTION_MODE, "pipelined"),
                ("datafusion.execution.batch_size", "8"),
            ],
        )
        .await;
        let mut stream = ctx
            .sql("SELECT o_id, o_amount FROM orders")
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        stream.next().await.unwrap().unwrap();
        drop(stream);

        let next = tokio::time::timeout(
            Duration::from_secs(60),
            rows(&ctx, "SELECT count(*) FROM orders", false),
        )
        .await
        .expect("the next job did not run: the dropped job kept its vcores");
        assert!(next.iter().any(|line| line.contains("1000")), "{next:?}");
    }

    #[tokio::test]
    async fn pipelined_limit_without_order_returns_the_limit() {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());
        let ctx = context(dir.path(), &[(BALLISTA_EXECUTION_MODE, "pipelined")]).await;
        let batches = ctx
            .sql("SELECT o_id FROM orders LIMIT 3")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[tokio::test]
    async fn pipelined_query_error_reaches_the_client() {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());
        let ctx = context(dir.path(), &[(BALLISTA_EXECUTION_MODE, "pipelined")]).await;
        let result = ctx
            .sql("SELECT o_id / (o_id - o_id) FROM orders")
            .await
            .unwrap()
            .collect()
            .await;
        let err = result.expect_err("dividing by zero should fail the query");
        // The task's error arrives as `Divide by zero error` or, from the
        // scheduler's failure report, as `ArrowError(DivideByZero)`.
        let text = err.to_string().to_lowercase().replace(' ', "");
        assert!(text.contains("dividebyzero"), "{err}");
    }

    #[tokio::test]
    async fn pipelined_mode_is_rejected_by_a_pull_staged_cluster() {
        // A standalone cluster for a blocking session runs pull-staged.
        let ctx = SessionContext::standalone().await.unwrap();
        ctx.sql(&format!("SET {BALLISTA_EXECUTION_MODE} = 'pipelined'"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let err = ctx
            .sql("SELECT 1")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("a pull-staged scheduler cannot run pipelined jobs");
        assert!(err.to_string().contains("push-staged"), "{err}");
    }

    #[test]
    fn config_constants_are_the_documented_keys() {
        // Guards the strings the tests above rely on.
        assert_eq!(BALLISTA_EXECUTION_MODE, "ballista.execution.mode");
        let config = SessionConfig::new_with_ballista()
            .set_str(BALLISTA_EXECUTION_MODE, "pipelined");
        assert_eq!(
            config
                .ballista_config()
                .execution_mode()
                .unwrap()
                .to_string(),
            "pipelined"
        );
    }
}
