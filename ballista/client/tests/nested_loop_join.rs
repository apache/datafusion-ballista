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

#[cfg(test)]
#[cfg(feature = "standalone")]
mod nested_loop_join {
    use crate::common::{
        check_matches, collect_text, datafusion_context, orders_customers_context,
        orders_customers_dir, register_orders_customers,
    };

    // Non-equi self-joins of the four-partition `orders`, so DataFusion plans a
    // nested-loop join whose probe side has several partitions, for each join
    // type that emits build-side rows.
    const QUERIES: &[&str] = &[
        "SELECT a.o_id, b.o_id FROM orders a LEFT JOIN orders b \
         ON b.o_id < a.o_id AND b.o_id < 2 WHERE a.o_id < 3 ORDER BY 1, 2",
        "SELECT a.o_id, b.o_id FROM (SELECT * FROM orders WHERE o_id < 3) a \
         FULL JOIN (SELECT * FROM orders WHERE o_id < 3) b ON b.o_id + 1 < a.o_id \
         ORDER BY 1, 2",
        // One `b` row per file, so each `a` row matches in every task.
        "SELECT o_id FROM orders a WHERE o_id % 100 = 7 AND EXISTS \
         (SELECT 1 FROM orders b WHERE b.o_id < a.o_id AND b.o_id % 250 = 3) \
         ORDER BY 1",
        "SELECT o_id FROM orders a WHERE o_id < 3 OR EXISTS \
         (SELECT 1 FROM orders b WHERE b.o_id < a.o_id AND b.o_id < 2 \
         AND a.o_id > 997) ORDER BY 1",
        "SELECT o_id FROM orders a WHERE o_id < 3 AND NOT EXISTS \
         (SELECT 1 FROM orders b WHERE b.o_id < a.o_id AND b.o_id < 2) ORDER BY 1",
        // An aggregate above the join, so `EnsureRequirements` reruns over its stage.
        "SELECT a.o_cust, count(*) FROM orders a LEFT JOIN orders b \
         ON b.o_id < a.o_id AND b.o_id < 2 WHERE a.o_id < 300 \
         GROUP BY a.o_cust ORDER BY 1",
    ];

    #[tokio::test]
    async fn emits_build_rows_once() {
        let dir = orders_customers_dir();
        let df_ctx = datafusion_context();
        register_orders_customers(&df_ctx, dir.path()).await;

        let mut mismatches = vec![];
        // One partition per task, as on a cluster whose executors each take a
        // slice of the stage.
        let one_per_task = ("ballista.scheduler.max_partitions_per_task", "1");
        for (variant, options) in [
            ("adaptive", &[one_per_task][..]),
            (
                "static",
                &[one_per_task, ("ballista.planner.adaptive.enabled", "false")][..],
            ),
        ] {
            let ctx = orders_customers_context(dir.path(), options).await;
            for (i, sql) in QUERIES.iter().enumerate() {
                let expected = collect_text(&df_ctx, sql).await;
                let label = format!("{variant}/{i}");
                check_matches(&ctx, &label, sql, &expected, &mut mismatches).await;
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n\n"));
    }
}
