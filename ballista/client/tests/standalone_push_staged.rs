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
        ORDERS_QUERIES, ballista_state, collect_text, orders_customers_dir,
        register_orders_customers, setup_test_cluster_with_executors,
    };
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::BALLISTA_STANDALONE_TASK_SCHEDULING_POLICY;
    use ballista_core::serde::protobuf::PollWorkParams;
    use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
    use datafusion::execution::SessionState;
    use datafusion::prelude::SessionContext;
    use std::path::Path;

    fn state(policy: &str) -> SessionState {
        ballista_state(&[(BALLISTA_STANDALONE_TASK_SCHEDULING_POLICY, policy)])
    }

    /// The scan, aggregate and join of [`ORDERS_QUERIES`], none of which has an
    /// unspecified row order.
    fn queries() -> impl Iterator<Item = &'static str> {
        ORDERS_QUERIES[..3].iter().map(|(sql, _)| *sql)
    }

    async fn expected(dir: &Path) -> Vec<String> {
        let ctx = SessionContext::standalone_with_state(state("pull-staged"))
            .await
            .unwrap();
        register_orders_customers(&ctx, dir).await;
        let mut out = Vec::new();
        for sql in queries() {
            out.push(collect_text(&ctx, sql).await);
        }
        out
    }

    #[tokio::test]
    async fn push_staged_standalone_matches_pull_staged() {
        let dir = orders_customers_dir();
        let expected = expected(dir.path()).await;

        let ctx = SessionContext::standalone_with_state(state("push-staged"))
            .await
            .unwrap();
        register_orders_customers(&ctx, dir.path()).await;
        for (sql, expected) in queries().zip(&expected) {
            assert_eq!(&collect_text(&ctx, sql).await, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn push_staged_cluster_of_two_executors_matches_pull_staged() {
        let dir = orders_customers_dir();
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
        for (sql, expected) in queries().zip(&expected) {
            assert_eq!(&collect_text(&ctx, sql).await, expected, "{sql}");
        }
    }
}
