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

//! A logical plan hook runs on the client each time a query is submitted.

mod common;

#[cfg(feature = "standalone")]
mod standalone {
    use std::sync::Arc;

    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_core::logical_plan_hook::LogicalPlanHook;
    use ballista_core::test_utils::logical_plan_hook::{AlwaysFail, CountRuns};
    use datafusion::assert_batches_eq;
    use datafusion::error::Result;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use crate::common::state_with_config;

    /// Standalone Ballista whose session runs `hook`.
    async fn context(hook: impl LogicalPlanHook + 'static) -> Result<SessionContext> {
        let config = SessionConfig::new_with_ballista()
            .with_ballista_logical_plan_hook(Arc::new(hook));
        SessionContext::standalone_with_state(state_with_config(config)).await
    }

    #[tokio::test]
    async fn should_run_the_hook_once_on_each_run_of_a_dataframe() -> Result<()> {
        let ctx = context(CountRuns::default()).await?;
        let df = ctx.sql("SELECT 0 AS run").await?;

        for run in ["| 1   |", "| 2   |"] {
            let result = df.clone().collect().await?;
            assert_batches_eq!(
                ["+-----+", "| run |", "+-----+", run, "+-----+"],
                &result
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_fail_a_query_with_its_hooks_error() -> Result<()> {
        let ctx = context(AlwaysFail).await?;

        for sql in ["SELECT 1", "EXPLAIN ANALYZE SELECT 1"] {
            let error = ctx.sql(sql).await?.collect().await.unwrap_err();
            assert_eq!(
                error.to_string(),
                "Execution error: rejected by AlwaysFail",
                "{sql}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_not_run_hooks_for_a_query_the_client_runs_itself() -> Result<()> {
        let ctx = context(AlwaysFail).await?;

        let sql = "SELECT table_name FROM information_schema.tables \
                   WHERE table_name = 'tables'";
        let result = ctx.sql(sql).await?.collect().await?;
        let expected = [
            "+------------+",
            "| table_name |",
            "+------------+",
            "| tables     |",
            "+------------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }
}
