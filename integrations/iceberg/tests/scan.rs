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
    use datafusion::error::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use iceberg_datafusion::IcebergStaticTableProvider;

    use crate::common::{assert_standalone_scans, catalog_table, insert, memory_catalog};

    #[tokio::test]
    async fn should_scan_latest_and_older_versions() -> Result<()> {
        let dir = tempfile::tempdir()?;
        assert_standalone_scans(memory_catalog(dir.path()).await).await
    }

    // A known limitation: a view stores its planned query, and tables are
    // pinned when a query is planned.
    #[tokio::test]
    async fn should_keep_a_views_version() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let catalog = memory_catalog(dir.path()).await;
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new_with_ballista())
            .with_default_features()
            .build();
        let ctx = SessionContext::standalone_with_state(register_iceberg(state)).await?;
        ctx.register_table("t", Arc::new(catalog_table(&catalog).await))?;

        insert(&catalog, "(1, 'a')").await;
        ctx.sql("CREATE VIEW v AS SELECT * FROM t").await?;
        insert(&catalog, "(2, 'b')").await;

        let result = ctx.sql("SELECT * FROM v").await?.collect().await?;
        let expected = [
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 1  | a    |",
            "+----+------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    /// Standalone Ballista over `ns.t` holding `(1, 'a'), (2, 'b')`, committed
    /// one row at a time, and `t_v1`, its first version holding `(1, 'a')`.
    /// Every stage is split into one task per partition, so each branch of a
    /// union runs in a task of its own.
    async fn split_stage_context(dir: &Path, adaptive: bool) -> Result<SessionContext> {
        let catalog = memory_catalog(dir).await;
        let first = insert(&catalog, "(1, 'a')").await;
        insert(&catalog, "(2, 'b')").await;
        let config = SessionConfig::new_with_ballista()
            .set_str(BALLISTA_ADAPTIVE_PLANNER_ENABLED, &adaptive.to_string())
            .set_str(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, "1");
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let ctx = SessionContext::standalone_with_state(register_iceberg(state)).await?;
        ctx.register_table("t", Arc::new(catalog_table(&catalog).await))?;
        let v1 = IcebergStaticTableProvider::try_new_from_table(first)?;
        ctx.register_table("t_v1", Arc::new(v1))?;
        Ok(ctx)
    }

    const UNION: &str = "SELECT id, name FROM t UNION ALL SELECT id, name FROM t";

    /// Runs a union of two scans, and the same union sorted, with each branch
    /// in a task of its own.
    async fn assert_union_on_a_split_stage(adaptive: bool) -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = split_stage_context(dir.path(), adaptive).await?;
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

        // Each branch is sorted before the union (#2854).
        let sorted = format!("{UNION} ORDER BY id");
        let result = ctx.sql(&sorted).await?.collect().await?;
        assert_batches_eq!(expected, &result);

        for (sql, expected) in UNIONS {
            let result = ctx.sql(sql).await?.collect().await?;
            assert_batches_sorted_eq!(*expected, &result);
        }

        Ok(())
    }

    /// Unions whose branches give different rows, so a task that ran another
    /// task's branch, or a branch twice, returns the wrong rows. Each is
    /// compared with its rows sorted.
    ///
    /// Not here: an `ORDER BY` over branches whose sort key is constant in
    /// each, such as a `count(*)` or a filter on `id = 1`, which the adaptive
    /// planner fails to plan (#2942).
    const UNIONS: &[(&str, &[&str])] = &[
        // A literal per branch.
        (
            "SELECT id, 'l' AS side FROM t UNION ALL SELECT id, 'r' AS side FROM t",
            &[
                "+----+------+",
                "| id | side |",
                "+----+------+",
                "| 1  | l    |",
                "| 1  | r    |",
                "| 2  | l    |",
                "| 2  | r    |",
                "+----+------+",
            ],
        ),
        // Two versions of the table, each its own pinned table.
        (
            "SELECT id, 'v1' AS v FROM t_v1 UNION ALL SELECT id, 'v2' AS v FROM t",
            &[
                "+----+----+",
                "| id | v  |",
                "+----+----+",
                "| 1  | v1 |",
                "| 1  | v2 |",
                "| 2  | v2 |",
                "+----+----+",
            ],
        ),
        // A branch that merges its input into one partition: a LIMIT. Which
        // row it keeps isn't defined, so count the rows.
        (
            "SELECT count(*) AS n FROM \
             (SELECT id FROM (SELECT id FROM t LIMIT 1) UNION ALL SELECT id FROM t)",
            &["+---+", "| n |", "+---+", "| 3 |", "+---+"],
        ),
        // The same with an ORDER BY in the branch: a top-k.
        (
            "SELECT id FROM (SELECT id FROM t ORDER BY id DESC LIMIT 1) \
             UNION ALL SELECT id + 10 FROM t",
            &[
                "+----+", "| id |", "+----+", "| 11 |", "| 12 |", "| 2  |", "+----+",
            ],
        ),
        // A branch that isn't an Iceberg scan.
        (
            "SELECT id FROM t UNION ALL SELECT column1 AS id FROM (VALUES (3))",
            &[
                "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "+----+",
            ],
        ),
    ];

    #[tokio::test]
    async fn should_union_scans_on_a_split_stage_with_the_static_planner() -> Result<()> {
        assert_union_on_a_split_stage(false).await
    }

    #[tokio::test]
    async fn should_union_scans_on_a_split_stage_with_the_adaptive_planner() -> Result<()>
    {
        assert_union_on_a_split_stage(true).await
    }
}
