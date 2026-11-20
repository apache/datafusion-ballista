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

//! Scans Iceberg tables with standalone Ballista.

mod common;

#[cfg(test)]
mod standalone {
    use std::path::Path;
    use std::sync::Arc;

    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK,
    };
    use ballista_iceberg::register_iceberg;
    use datafusion::datasource::TableType;
    use datafusion::error::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use iceberg::{Catalog, TableIdent};
    use iceberg_datafusion::to_datafusion_error;

    use crate::common::{assert_standalone_scans, catalog_table, insert, memory_catalog};

    #[tokio::test]
    async fn should_scan_latest_and_older_versions() -> Result<()> {
        let dir = tempfile::tempdir()?;
        assert_standalone_scans(memory_catalog(dir.path()).await).await
    }

    #[tokio::test]
    async fn should_read_a_views_latest_version() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (catalog, ctx) = view_context(dir.path()).await?;

        insert(&catalog, "(1, 'a')").await;
        ctx.sql("CREATE VIEW v AS SELECT * FROM t").await?;
        // A view over a view, reading `t` again in a subquery.
        ctx.sql("CREATE VIEW w AS SELECT * FROM v WHERE id IN (SELECT id FROM t)")
            .await?;
        insert(&catalog, "(2, 'b')").await;

        for view in ["v", "w"] {
            let sql = format!("SELECT * FROM {view} ORDER BY id");
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_batches_eq!(LATEST, &result);
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_read_a_dataframe_views_latest_version() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (catalog, ctx) = view_context(dir.path()).await?;

        insert(&catalog, "(1, 'a')").await;
        let df = ctx.sql("SELECT * FROM t").await?;
        ctx.register_table("v", df.clone().into_view())?;
        ctx.register_table("tv", df.into_temporary_view())?;
        insert(&catalog, "(2, 'b')").await;

        for (view, table_type) in [("v", TableType::View), ("tv", TableType::Temporary)] {
            let sql = format!("SELECT * FROM {view} ORDER BY id");
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_batches_eq!(LATEST, &result);
            assert_eq!(ctx.table_provider(view).await?.table_type(), table_type);
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_read_a_view_created_while_its_table_was_gone() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (catalog, ctx) = view_context(dir.path()).await?;

        insert(&catalog, "(1, 'a')").await;
        ctx.sql("CREATE VIEW v AS SELECT * FROM t").await?;
        // While `t` is gone, `w` is created over `v`, and reading it fails.
        rename_table(&catalog, "t", "gone").await;
        ctx.sql("CREATE VIEW w AS SELECT * FROM v").await?;
        assert_table_gone(&ctx, &catalog, "SELECT * FROM w").await;
        // Once `t` is back, `w` reads its latest version.
        rename_table(&catalog, "gone", "t").await;
        insert(&catalog, "(2, 'b')").await;
        let result = ctx
            .sql("SELECT * FROM w ORDER BY id")
            .await?
            .collect()
            .await?;
        assert_batches_eq!(LATEST, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_fail_to_read_a_table_that_is_gone() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (catalog, ctx) = view_context(dir.path()).await?;
        ctx.sql("CREATE VIEW v AS SELECT * FROM t").await?;
        rename_table(&catalog, "t", "gone").await;

        for sql in [
            "SELECT * FROM t",
            "SELECT * FROM t WHERE id = 1",
            "SELECT * FROM v",
            "SELECT * FROM v WHERE id = 1",
            "SELECT 1 AS x WHERE EXISTS (SELECT * FROM t)",
        ] {
            assert_table_gone(&ctx, &catalog, sql).await;
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_describe_drop_and_replace_while_a_table_is_gone() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (catalog, ctx) = view_context(dir.path()).await?;
        ctx.sql("CREATE VIEW v AS SELECT * FROM t").await?;
        ctx.sql("CREATE VIEW w AS SELECT * FROM t").await?;
        rename_table(&catalog, "t", "gone").await;

        let result = ctx.sql("DESCRIBE v").await?.collect().await?;
        let expected = [
            "+-------------+-----------+-------------+",
            "| column_name | data_type | is_nullable |",
            "+-------------+-----------+-------------+",
            "| id          | Int64     | NO          |",
            "| name        | Utf8      | YES         |",
            "+-------------+-----------+-------------+",
        ];
        assert_batches_eq!(expected, &result);

        ctx.sql("DROP VIEW v").await?;
        assert!(!ctx.table_exist("v")?);

        ctx.sql("CREATE OR REPLACE VIEW w AS SELECT 1 AS x").await?;
        let result = ctx.sql("SELECT * FROM w").await?.collect().await?;
        let expected = ["+---+", "| x |", "+---+", "| 1 |", "+---+"];
        assert_batches_eq!(expected, &result);

        ctx.sql("DROP TABLE t").await?;
        assert!(!ctx.table_exist("t")?);

        Ok(())
    }

    /// `ns.t` after the view tests' two commits.
    const LATEST: [&str; 6] = [
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 1  | a    |",
        "| 2  | b    |",
        "+----+------+",
    ];

    /// Standalone Ballista over an empty `ns.t` in a memory catalog in `dir`,
    /// registered as `t`.
    async fn view_context(dir: &Path) -> Result<(Arc<dyn Catalog>, SessionContext)> {
        let catalog = memory_catalog(dir).await;
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new_with_ballista())
            .with_default_features()
            .build();
        let ctx = SessionContext::standalone_with_state(register_iceberg(state)).await?;
        ctx.register_table("t", Arc::new(catalog_table(&catalog).await))?;
        Ok((catalog, ctx))
    }

    /// Checks that running `sql` fails while it's planned, with the error
    /// loading `ns.t` from `catalog` returns.
    async fn assert_table_gone(
        ctx: &SessionContext,
        catalog: &Arc<dyn Catalog>,
        sql: &str,
    ) {
        let error = ctx.sql(sql).await.unwrap().collect().await.unwrap_err();
        let load_error = catalog.load_table(&ident("t")).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "require_loaded_iceberg_tables\ncaused by\n{}",
                to_datafusion_error(load_error)
            ),
            "{sql}"
        );
    }

    /// Renames `ns.{from}` to `ns.{to}`, so a table registered from `ns.t` can
    /// be made to fail to load, and then to load again.
    async fn rename_table(catalog: &Arc<dyn Catalog>, from: &str, to: &str) {
        catalog
            .rename_table(&ident(from), &ident(to))
            .await
            .unwrap();
    }

    fn ident(name: &str) -> TableIdent {
        TableIdent::from_strs(["ns", name]).unwrap()
    }

    /// Standalone Ballista over `ns.t` holding `(1, 'a'), (2, 'b')`, splitting
    /// every stage into one task per partition, so each branch of a union runs
    /// in a task of its own.
    async fn split_stage_context(dir: &Path, adaptive: bool) -> Result<SessionContext> {
        let catalog = memory_catalog(dir).await;
        insert(&catalog, "(1, 'a'), (2, 'b')").await;
        let config = SessionConfig::new_with_ballista()
            .set_str(BALLISTA_ADAPTIVE_PLANNER_ENABLED, &adaptive.to_string())
            .set_str(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, "1");
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let ctx = SessionContext::standalone_with_state(register_iceberg(state)).await?;
        ctx.register_table("t", Arc::new(catalog_table(&catalog).await))?;
        Ok(ctx)
    }

    const UNION: &str = "SELECT id, name FROM t UNION ALL SELECT id, name FROM t";

    #[tokio::test]
    async fn should_union_scans_on_a_split_stage_with_the_static_planner() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = split_stage_context(dir.path(), false).await?;
        let expected = [
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 1  | a    |",
            "| 1  | a    |",
            "| 2  | b    |",
            "| 2  | b    |",
            "+----+------+",
        ];

        let result = ctx.sql(UNION).await?.collect().await?;
        assert_batches_sorted_eq!(expected, &result);

        // Each branch is sorted before the union, which the adaptive planner
        // gets wrong too (#2854).
        let sorted = format!("{UNION} ORDER BY id");
        let result = ctx.sql(&sorted).await?.collect().await?;
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    // A known limitation: the adaptive planner keeps both branches in the
    // union's stage, and the task that owns one branch can't restrict the
    // other branch's Iceberg scan to none of its partitions (#2852).
    #[tokio::test]
    async fn should_refuse_a_union_of_scans_on_a_split_stage_with_the_adaptive_planner()
    -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = split_stage_context(dir.path(), true).await?;

        let error = ctx.sql(UNION).await?.collect().await.unwrap_err();
        // The message starts with the job's id.
        let error = error.to_string();
        let (_, cause) = error.split_once("DataFusion error: ").unwrap();
        assert_eq!(
            cause,
            "Error during planning: cannot restrict `DataSourceExec: format=iceberg, \
             projection=[id, name]` to partitions [] of its 1: the scheduler does not \
             know how to restrict a `DataSourceExec`, and left alone it would number \
             its partitions from zero in every task. A node of your own can be \
             restricted by `SchedulerConfig::partition_sliceable_resolver`, for \
             instance by implementing `PartitionSliceable` and calling it from there."
        );

        Ok(())
    }
}
