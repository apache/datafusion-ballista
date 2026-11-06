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

/// End-to-end tests of `ballista.planner.reuse_exchange.enabled`. Queries
/// that read one subplan more than once run under adaptive query planning
/// with exchange reuse on and off, and with the reader-side rules (coalesce
/// and skew join) on and off, and must return the same rows as a run of the
/// static planner.
#[cfg(test)]
#[cfg(feature = "standalone")]
mod reuse_exchange {
    use crate::common::{
        collect_lines, orders_customers_context, write_orders_customers,
    };
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_COALESCE_ENABLED,
        BALLISTA_REUSE_EXCHANGE_ENABLED, BALLISTA_SKEW_JOIN_ENABLED,
    };
    use rstest::rstest;
    use tempfile::TempDir;

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
        // An aggregate compared with its own maximum, as TPC-H q15 does.
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
    async fn repeated_subplans_return_the_same_rows(
        #[values(false, true)] reuse: bool,
        // Each reader of a shared stage decides its own coalesce and skew
        // join, which only run with these rules on.
        #[values(false, true)] reader_rules: bool,
    ) {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());

        // The static planner never shares a stage between readers.
        let reference = orders_customers_context(
            dir.path(),
            &[(BALLISTA_ADAPTIVE_PLANNER_ENABLED, "false")],
        )
        .await;
        let ctx = orders_customers_context(
            dir.path(),
            &[
                (BALLISTA_ADAPTIVE_PLANNER_ENABLED, "true"),
                (BALLISTA_REUSE_EXCHANGE_ENABLED, &reuse.to_string()),
                (BALLISTA_COALESCE_ENABLED, &reader_rules.to_string()),
                (BALLISTA_SKEW_JOIN_ENABLED, &reader_rules.to_string()),
            ],
        )
        .await;

        for sql in QUERIES {
            let expected = collect_lines(&reference, sql).await;
            let actual = collect_lines(&ctx, sql).await;
            assert_eq!(
                actual, expected,
                "reuse={reuse} reader_rules={reader_rules} differs for: {sql}"
            );
        }
    }
}
