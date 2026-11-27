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

/// A LIMIT directly over VALUES is absorbed into the memory scan's own
/// `fetch` by the optimizer, leaving no limit operator in the plan. The
/// scheduler's per-task partition restriction has to carry that fetch to
/// the executor, or the scan returns every row.
/// <https://github.com/apache/datafusion-ballista/issues/2961>
#[cfg(test)]
#[cfg(feature = "standalone")]
mod limit_memory_scan {
    use crate::common::ballista_state;
    use ballista::prelude::SessionContextExt;
    use ballista_core::config::BALLISTA_ADAPTIVE_PLANNER_ENABLED;
    use datafusion::prelude::SessionContext;

    const SQL: &str =
        "SELECT a FROM (VALUES (0), (1), (2), (3), (4), (5)) AS v(a) LIMIT 2";

    #[tokio::test]
    async fn limit_over_values_stops_at_the_limit() {
        let static_ctx =
            standalone_with(BALLISTA_ADAPTIVE_PLANNER_ENABLED, "false").await;
        assert_eq!(limit_rows(&static_ctx).await, 2);

        let adaptive_ctx =
            standalone_with(BALLISTA_ADAPTIVE_PLANNER_ENABLED, "true").await;
        assert_eq!(limit_rows(&adaptive_ctx).await, 2);
    }

    async fn standalone_with(option: &str, value: &str) -> SessionContext {
        let state = ballista_state(&[(option, value)]);
        SessionContext::standalone_with_state(state).await.unwrap()
    }

    async fn limit_rows(ctx: &SessionContext) -> usize {
        let batches = ctx.sql(SQL).await.unwrap().collect().await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    }
}
