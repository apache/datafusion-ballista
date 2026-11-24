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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_core::logical_plan_hook::LogicalPlanHook;
    use datafusion::assert_batches_eq;
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::common::exec_err;
    use datafusion::common::tree_node::Transformed;
    use datafusion::datasource::provider_as_source;
    use datafusion::error::Result;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use tempfile::TempDir;

    use crate::common::{register_csv_tables, state_with_config};

    /// Swaps each scan of the table `t` for the next of `versions` on every
    /// run, as a table format's hook would swap in the version its catalog
    /// serves when the query runs.
    #[derive(Debug)]
    struct NextVersion {
        versions: Vec<Arc<dyn TableProvider>>,
        runs: AtomicUsize,
    }

    #[async_trait]
    impl LogicalPlanHook for NextVersion {
        async fn before_submit(
            &self,
            plan: LogicalPlan,
            _session: &dyn Session,
        ) -> Result<LogicalPlan> {
            let run = self.runs.fetch_add(1, Ordering::SeqCst);
            let Some(version) = self.versions.get(run) else {
                return exec_err!("t has no version {run}");
            };
            plan.transform_up_with_subqueries(|node| match node {
                LogicalPlan::TableScan(mut scan) if scan.table_name.table() == "t" => {
                    scan.source = provider_as_source(Arc::clone(version));
                    Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
                }
                node => Ok(Transformed::no(node)),
            })
            .map(|transformed| transformed.data)
        }
    }

    /// Writes the CSV tables `t`, holding `0`, and `v1` and `v2`, holding the
    /// first one and two of `1, 2`, each with the column `id`.
    fn write_tables() -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, rows) in [("t", "0\n"), ("v1", "1\n"), ("v2", "1\n2\n")] {
            std::fs::create_dir(dir.path().join(name)).unwrap();
            let data = format!("id\n{rows}");
            std::fs::write(dir.path().join(name).join("data.csv"), data).unwrap();
        }
        dir
    }

    /// Standalone Ballista over `dir`'s `t`, whose hook swaps `t` for each of
    /// `versions` of `dir` in turn.
    async fn context(dir: &TempDir, versions: &[&str]) -> Result<SessionContext> {
        let local = SessionContext::new();
        register_csv_tables(&local, dir.path(), versions).await;
        let mut providers = Vec::new();
        for version in versions {
            providers.push(local.table_provider(*version).await?);
        }
        let hook = NextVersion {
            versions: providers,
            runs: AtomicUsize::new(0),
        };
        let config = SessionConfig::new_with_ballista()
            .with_ballista_logical_plan_hook(Arc::new(hook));
        let ctx =
            SessionContext::standalone_with_state(state_with_config(config)).await?;
        register_csv_tables(&ctx, dir.path(), &["t"]).await;
        Ok(ctx)
    }

    #[tokio::test]
    async fn should_run_the_hook_on_each_run_of_a_dataframe() -> Result<()> {
        let dir = write_tables();
        let ctx = context(&dir, &["v1", "v2"]).await?;
        let df = ctx.sql("SELECT id FROM t ORDER BY id").await?;

        let result = df.clone().collect().await?;
        let expected = ["+----+", "| id |", "+----+", "| 1  |", "+----+"];
        assert_batches_eq!(expected, &result);

        let result = df.collect().await?;
        let expected = ["+----+", "| id |", "+----+", "| 1  |", "| 2  |", "+----+"];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_fail_a_query_with_its_hooks_error() -> Result<()> {
        let dir = write_tables();
        let ctx = context(&dir, &[]).await?;

        let error = ctx
            .sql("SELECT id FROM t")
            .await?
            .collect()
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "Execution error: t has no version 0");

        // A query of `information_schema` alone runs on the client, without
        // hooks.
        let sql = "SELECT table_name FROM information_schema.tables \
                   WHERE table_name = 't'";
        let result = ctx.sql(sql).await?.collect().await?;
        let expected = [
            "+------------+",
            "| table_name |",
            "+------------+",
            "| t          |",
            "+------------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }
}
