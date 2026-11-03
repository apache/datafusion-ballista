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

/// End-to-end tests of `ballista.planner.adaptive.reuse_exchange`. Queries
/// that read one subplan more than once run under adaptive query planning
/// with exchange reuse on and off, and must return the same rows as a run of
/// the static planner.
#[cfg(test)]
#[cfg(feature = "standalone")]
mod reuse_exchange {
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_ADAPTIVE_REUSE_EXCHANGE,
        BALLISTA_STANDALONE_PARALLELISM,
    };
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use rstest::rstest;
    use std::io::Write;
    use std::path::Path;
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

    async fn context(dir: &Path, options: &[(&str, &str)]) -> SessionContext {
        let ctx = SessionContext::standalone_with_state(state(options))
            .await
            .unwrap();
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
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        pretty_format_batches(&batches)
            .unwrap()
            .to_string()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// Each query reads the same aggregate, and so the same exchanges, more
    /// than once.
    const QUERIES: &[&str] = &[
        // A common table expression joined to itself.
        "WITH t AS (SELECT o_cust, count(*) AS n, sum(o_amount) AS total \
                    FROM orders GROUP BY o_cust) \
         SELECT a.o_cust, a.total, b.n \
         FROM t a JOIN t b ON a.o_cust = b.o_cust + 1 \
         ORDER BY a.o_cust",
        // A union of an aggregate with itself.
        "WITH t AS (SELECT o_cust, sum(o_amount) AS total \
                    FROM orders GROUP BY o_cust) \
         SELECT o_cust, total FROM t \
         UNION ALL SELECT o_cust, total FROM t \
         ORDER BY o_cust, total",
        // An aggregate compared with its own average, as TPC-H q15 does.
        "WITH t AS (SELECT o_cust, sum(o_amount) AS total \
                    FROM orders GROUP BY o_cust) \
         SELECT o_cust, total FROM t \
         WHERE total = (SELECT max(total) FROM t) ORDER BY o_cust",
        // A shared aggregate joined to a dimension table.
        "WITH t AS (SELECT o_cust, sum(o_amount) AS total \
                    FROM orders GROUP BY o_cust) \
         SELECT c_region, sum(a.total) AS s, count(b.total) AS c \
         FROM customers JOIN t a ON c_id = a.o_cust \
         JOIN t b ON a.o_cust = b.o_cust \
         GROUP BY c_region ORDER BY c_region",
    ];

    #[rstest]
    #[tokio::test]
    async fn repeated_subplans_return_the_same_rows(#[values(false, true)] reuse: bool) {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());

        // The static planner never shares a stage between readers.
        let reference =
            context(dir.path(), &[(BALLISTA_ADAPTIVE_PLANNER_ENABLED, "false")]).await;
        let ctx = context(
            dir.path(),
            &[
                (BALLISTA_ADAPTIVE_PLANNER_ENABLED, "true"),
                (BALLISTA_ADAPTIVE_REUSE_EXCHANGE, &reuse.to_string()),
            ],
        )
        .await;

        for sql in QUERIES {
            let expected = rows(&reference, sql).await;
            let actual = rows(&ctx, sql).await;
            assert_eq!(actual, expected, "reuse={reuse} differs for: {sql}");
        }
    }
}
