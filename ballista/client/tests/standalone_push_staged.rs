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

/// End-to-end tests of a standalone cluster running push-staged, selected with
/// `ballista.standalone.task_scheduling_policy`. Each query's results are
/// compared with the same query run on a pull-staged standalone cluster.
#[cfg(test)]
#[cfg(feature = "standalone")]
mod standalone_push_staged {
    use crate::common::setup_test_cluster_with_executors;
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::{
        BALLISTA_STANDALONE_PARALLELISM, BALLISTA_STANDALONE_TASK_SCHEDULING_POLICY,
    };
    use ballista_core::extension::SessionConfigExt;
    use ballista_core::serde::protobuf::PollWorkParams;
    use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
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

    fn state(policy: &str) -> SessionState {
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(4)
            .set_str(BALLISTA_STANDALONE_PARALLELISM, "4")
            .set_str(BALLISTA_STANDALONE_TASK_SCHEDULING_POLICY, policy);
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build()
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

    async fn rows(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    const QUERIES: &[&str] = &[
        "SELECT count(*), sum(o_amount) FROM orders",
        "SELECT o_cust, count(*) AS n, sum(o_amount) AS total \
         FROM orders GROUP BY o_cust ORDER BY o_cust",
        "SELECT c_region, count(*) AS n, sum(o_amount) AS total \
         FROM orders JOIN customers ON o_cust = c_id \
         GROUP BY c_region ORDER BY c_region",
    ];

    async fn expected(dir: &Path) -> Vec<String> {
        let ctx = SessionContext::standalone_with_state(state("pull-staged"))
            .await
            .unwrap();
        register_tables(&ctx, dir).await;
        let mut out = Vec::new();
        for sql in QUERIES {
            out.push(rows(&ctx, sql).await);
        }
        out
    }

    #[tokio::test]
    async fn push_staged_standalone_matches_pull_staged() {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());
        let expected = expected(dir.path()).await;

        let ctx = SessionContext::standalone_with_state(state("push-staged"))
            .await
            .unwrap();
        register_tables(&ctx, dir.path()).await;
        for (sql, expected) in QUERIES.iter().zip(&expected) {
            assert_eq!(&rows(&ctx, sql).await, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn push_staged_cluster_of_two_executors_matches_pull_staged() {
        let dir = TempDir::new().unwrap();
        write_tables(dir.path());
        let expected = expected(dir.path()).await;

        let state = state("push-staged");
        let (host, port) = setup_test_cluster_with_executors(&state, 2, 2).await;

        // The results match pull-staged by design, so check the scheduler
        // really is push-staged: one of those rejects polling for work.
        let mut scheduler = SchedulerGrpcClient::connect(format!("http://{host}:{port}"))
            .await
            .unwrap();
        let status = scheduler
            .poll_work(PollWorkParams::default())
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);

        let ctx =
            SessionContext::remote_with_state(&format!("df://{host}:{port}"), state)
                .await
                .unwrap();
        register_tables(&ctx, dir.path()).await;
        for (sql, expected) in QUERIES.iter().zip(&expected) {
            assert_eq!(&rows(&ctx, sql).await, expected, "{sql}");
        }
    }
}
