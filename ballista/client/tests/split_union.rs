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

/// End-to-end tests of `UNION ALL` queries whose stages are split between
/// tasks. Each task runs only the union branches it owns, so every other
/// branch is restricted to the task's share of it, often none. They must
/// return the rows DataFusion returns.
#[cfg(test)]
#[cfg(feature = "standalone")]
mod split_union {
    use crate::common::{
        ballista_state, collect_lines, datafusion_context, orders_customers_dir,
        register_alltypes, register_orders_customers,
    };
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK,
    };
    use datafusion::prelude::SessionContext;
    use rstest::rstest;
    use std::path::Path;

    /// The adaptive planner replaces a stage that gives no rows with an
    /// `EmptyExec` of that stage's partition count, so in most of these a
    /// union branch is an `EmptyExec` of several partitions.
    const QUERIES: &[&str] = &[
        // An aggregate that matches nothing, beside one that matches all.
        "SELECT id % 2 AS k, count(*) AS c FROM test WHERE id > 100 GROUP BY id % 2 \
         UNION ALL SELECT id % 2, count(*) FROM test GROUP BY id % 2",
        // The same, the other way round.
        "SELECT id % 2 AS k, count(*) AS c FROM test GROUP BY id % 2 \
         UNION ALL SELECT id % 2, count(*) FROM test WHERE id > 100 GROUP BY id % 2",
        // An empty middle branch of three.
        "SELECT o_cust AS k, count(*) AS c FROM orders GROUP BY o_cust \
         UNION ALL SELECT o_cust, count(*) FROM orders WHERE o_id < 0 GROUP BY o_cust \
         UNION ALL SELECT c_id, 1 FROM customers WHERE c_id < 5",
        // An aggregate over a union with an empty branch.
        "SELECT k, sum(c) AS s FROM ( \
           SELECT o_cust AS k, count(*) AS c FROM orders WHERE o_id < 0 GROUP BY o_cust \
           UNION ALL SELECT o_cust, count(*) FROM orders GROUP BY o_cust) \
         GROUP BY k",
        // A join that matches nothing below an aggregate.
        "SELECT c_region AS r, count(*) AS n FROM orders JOIN customers ON o_cust = c_id \
           WHERE o_id < 0 GROUP BY c_region \
         UNION ALL SELECT c_region, count(*) FROM customers GROUP BY c_region",
        // A literal beside an empty aggregate.
        "SELECT 7 AS k, 7 AS c \
         UNION ALL SELECT id % 2, count(*) FROM test WHERE id > 100 GROUP BY id % 2",
        // A union with an empty branch joined to a table.
        "SELECT x.k, t.c_name FROM ( \
           SELECT o_cust AS k FROM orders WHERE o_id < 0 GROUP BY o_cust \
           UNION ALL SELECT c_id FROM customers WHERE c_id < 3) x \
         JOIN customers t ON x.k = t.c_id",
        // A window over a union with an empty branch.
        "SELECT k, sum(c) OVER (PARTITION BY k) AS s FROM ( \
           SELECT id % 2 AS k, count(*) AS c FROM test WHERE id > 100 GROUP BY id % 2 \
           UNION ALL SELECT id % 2, count(*) FROM test GROUP BY id % 2)",
        // Scans, literals and values.
        "SELECT id FROM test UNION ALL SELECT id FROM test",
        "SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3",
        "SELECT 100 AS id UNION ALL SELECT id FROM test",
        "SELECT column1 AS id FROM (VALUES (10), (11)) UNION ALL SELECT id FROM test",
    ];

    async fn register(ctx: &SessionContext, dir: &Path) {
        register_alltypes(ctx, "test").await;
        register_orders_customers(ctx, dir).await;
    }

    /// The formatted rows of `sql`, sorted, since a union's rows come in no
    /// particular order.
    async fn sorted_lines(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let mut lines = collect_lines(ctx, sql).await;
        lines.sort();
        lines
    }

    #[rstest]
    #[tokio::test]
    async fn split_unions_return_the_rows_datafusion_returns(
        #[values(false, true)] adaptive: bool,
        #[values(1, 2)] max_partitions_per_task: usize,
    ) {
        let dir = orders_customers_dir();
        let reference = datafusion_context();
        register(&reference, dir.path()).await;
        let ctx = SessionContext::standalone_with_state(ballista_state(&[
            (BALLISTA_ADAPTIVE_PLANNER_ENABLED, &adaptive.to_string()),
            (
                BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK,
                &max_partitions_per_task.to_string(),
            ),
        ]))
        .await
        .unwrap();
        register(&ctx, dir.path()).await;

        for sql in QUERIES {
            assert_eq!(
                sorted_lines(&ctx, sql).await,
                sorted_lines(&reference, sql).await,
                "adaptive={adaptive} max_partitions_per_task={max_partitions_per_task} \
                 differs for: {sql}"
            );
        }
    }
}
