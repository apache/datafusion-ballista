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
    use std::sync::Arc;

    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_iceberg::register_iceberg;
    use datafusion::assert_batches_eq;
    use datafusion::error::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

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
}
