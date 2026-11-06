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
    use crate::common::{
        ballista_state, collect_text, register_orders_customers,
        setup_test_cluster_with_executors, write_orders_customers,
    };
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::BALLISTA_STANDALONE_TASK_SCHEDULING_POLICY;
    use ballista_core::serde::protobuf::PollWorkParams;
    use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
    use datafusion::execution::SessionState;
    use datafusion::prelude::SessionContext;
    use std::path::Path;
    use tempfile::TempDir;

    fn state(policy: &str) -> SessionState {
        ballista_state(&[(BALLISTA_STANDALONE_TASK_SCHEDULING_POLICY, policy)])
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
        register_orders_customers(&ctx, dir).await;
        let mut out = Vec::new();
        for sql in QUERIES {
            out.push(collect_text(&ctx, sql).await);
        }
        out
    }

    #[tokio::test]
    async fn push_staged_standalone_matches_pull_staged() {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());
        let expected = expected(dir.path()).await;

        let ctx = SessionContext::standalone_with_state(state("push-staged"))
            .await
            .unwrap();
        register_orders_customers(&ctx, dir.path()).await;
        for (sql, expected) in QUERIES.iter().zip(&expected) {
            assert_eq!(&collect_text(&ctx, sql).await, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn push_staged_cluster_of_two_executors_matches_pull_staged() {
        let dir = TempDir::new().unwrap();
        write_orders_customers(dir.path());
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
        register_orders_customers(&ctx, dir.path()).await;
        for (sql, expected) in QUERIES.iter().zip(&expected) {
            assert_eq!(&collect_text(&ctx, sql).await, expected, "{sql}");
        }
    }
}
