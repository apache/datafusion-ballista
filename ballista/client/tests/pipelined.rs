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
/// query's results are compared with the same query run in staged mode.
#[cfg(test)]
#[cfg(feature = "standalone")]
mod pipelined {
    use crate::common::{
        ballista_state, collect_lines, orders_customers_context,
        register_orders_customers, set_option, setup_test_cluster_with_executors,
        write_orders_customers,
    };
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::{
        BALLISTA_EXECUTION_MODE, BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ,
    };
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::StreamExt;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;

    /// A scheduler with `executors` executors of `vcores` vcores each.
    async fn cluster_context(
        dir: &Path,
        options: &[(&str, &str)],
        executors: usize,
        vcores: usize,
    ) -> SessionContext {
        let state = ballista_state(options);
        let (host, port) =
            setup_test_cluster_with_executors(&state, executors, vcores).await;
        let ctx =
            SessionContext::remote_with_state(&format!("df://{host}:{port}"), state)
                .await
                .unwrap();
        register_orders_customers(&ctx, dir).await;
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str, sorted: bool) -> Vec<String> {
        let mut lines = collect_lines(ctx, sql).await;
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

    async fn check_matches_staged(dir: &Path, pipelined: &SessionContext) {
        let staged = orders_customers_context(dir, &[]).await;
        for (sql, sorted) in QUERIES {
            let expected = rows(&staged, sql, *sorted).await;
            let actual = rows(pipelined, sql, *sorted).await;
            assert_eq!(actual, expected, "pipelined result differs for: {sql}");
        }
    }

    #[tokio::test]
    async fn pipelined_results_match_staged() {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());
        let pipelined = orders_customers_context(
            dir.path(),
            &[(BALLISTA_EXECUTION_MODE, "pipelined")],
        )
        .await;
        check_matches_staged(dir.path(), &pipelined).await;
    }

    /// Readers normally take co-located producers straight from the
    /// executor's registry. Forcing remote reads sends every exchange through
    /// Arrow Flight instead.
    #[tokio::test]
    async fn pipelined_results_match_staged_over_flight() {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());
        let pipelined = orders_customers_context(
            dir.path(),
            &[
                (BALLISTA_EXECUTION_MODE, "pipelined"),
                (BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ, "true"),
            ],
        )
        .await;
        check_matches_staged(dir.path(), &pipelined).await;
    }

    /// Two executors of two vcores each, so every four-partition stage is
    /// split between them and its exchanges cross executors.
    #[tokio::test]
    async fn pipelined_results_match_staged_across_executors() {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());
        let pipelined =
            cluster_context(dir.path(), &[(BALLISTA_EXECUTION_MODE, "pipelined")], 2, 2)
                .await;
        check_matches_staged(dir.path(), &pipelined).await;
    }

    /// One session switches mode with `SET`: each query runs in the mode set
    /// when it is submitted, on the same cluster, with the same results.
    #[tokio::test]
    async fn one_session_switches_between_staged_and_pipelined() {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());
        // Started pipelined, so the standalone cluster runs push-staged.
        let ctx = orders_customers_context(
            dir.path(),
            &[(BALLISTA_EXECUTION_MODE, "pipelined")],
        )
        .await;
        for (sql, sorted) in QUERIES {
            set_option(&ctx, BALLISTA_EXECUTION_MODE, "staged").await;
            let staged = rows(&ctx, sql, *sorted).await;
            set_option(&ctx, BALLISTA_EXECUTION_MODE, "pipelined").await;
            let pipelined = rows(&ctx, sql, *sorted).await;
            assert_eq!(pipelined, staged, "pipelined result differs for: {sql}");
        }
    }

    /// A pipelined session builds a cached frame as a staged job, which
    /// leaves shuffle output to cache, and reads it back pipelined.
    #[tokio::test]
    async fn pipelined_session_caches_a_data_frame() {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());
        let ctx = orders_customers_context(
            dir.path(),
            &[(BALLISTA_EXECUTION_MODE, "pipelined")],
        )
        .await;
        set_option(&ctx, "ballista.cache.noop", "false").await;
        let sql = "SELECT o_cust, count(*) AS n, sum(o_amount) AS total \
                   FROM orders GROUP BY o_cust";
        let expected =
            rows(&orders_customers_context(dir.path(), &[]).await, sql, true).await;

        let cached = ctx.sql(sql).await.unwrap().cache().await.unwrap();
        for _ in 0..2 {
            let batches = cached.clone().collect().await.unwrap();
            let text = pretty_format_batches(&batches).unwrap().to_string();
            let mut actual: Vec<String> = text.lines().map(str::to_owned).collect();
            actual.sort();
            assert_eq!(actual, expected);
        }
    }

    /// A client that stops reading part-way must not leave its job holding
    /// vcores: the next job, which needs every vcore, still runs.
    #[tokio::test]
    async fn dropping_the_results_early_frees_the_cluster() {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());
        // Small batches, so the scan stays blocked on its full queues.
        let ctx = orders_customers_context(
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
        write_orders_customers(dir.path());
        let ctx = orders_customers_context(
            dir.path(),
            &[(BALLISTA_EXECUTION_MODE, "pipelined")],
        )
        .await;
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
        write_orders_customers(dir.path());
        let ctx = orders_customers_context(
            dir.path(),
            &[(BALLISTA_EXECUTION_MODE, "pipelined")],
        )
        .await;
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
        // A standalone cluster for a staged session runs pull-staged.
        let ctx = SessionContext::standalone().await.unwrap();
        set_option(&ctx, BALLISTA_EXECUTION_MODE, "pipelined").await;
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
