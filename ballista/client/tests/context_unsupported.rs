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

/// # Tracking Unsupported Operations
///
/// It provides indication if/when datafusion
/// gets support for them
#[cfg(test)]
mod unsupported {
    use crate::common::{remote_context, standalone_context};
    use datafusion::prelude::*;
    use datafusion::{assert_batches_eq, prelude::SessionContext};
    use rstest::*;

    #[rstest::fixture]
    fn test_data() -> String {
        crate::common::example_test_data()
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    #[should_panic]
    async fn should_support_sql_create_table(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
    ) {
        ctx.sql("CREATE TABLE tbl_test (id INT, value INT)")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();

        // it does create table but it can't be queried
        let _result = ctx
            .sql("select * from tbl_test where id > 0")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    #[should_panic]
    async fn should_support_caching_data_frame(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) {
        ctx.sql("SET ballista.cache.noop = false")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();
        let df = ctx
            .read_parquet(
                &format!("{test_data}/alltypes_plain.parquet"),
                Default::default(),
            )
            .await
            .unwrap()
            .select_columns(&["id", "bool_col", "timestamp_col"])
            .unwrap()
            .filter(col("id").gt(lit(5)))
            .unwrap();

        let cached_df = df.cache().await.unwrap();
        let result = cached_df.collect().await.unwrap();

        let expected = [
            "+----+----------+---------------------+",
            "| id | bool_col | timestamp_col       |",
            "+----+----------+---------------------+",
            "| 6  | true     | 2009-04-01T00:00:00 |",
            "| 7  | false    | 2009-04-01T00:01:00 |",
            "+----+----------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_support_on_cache_collect(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
    ) -> datafusion::error::Result<()> {
        // opt out case, should fail
        ctx.sql("SET ballista.cache.noop = false")
            .await?
            .show()
            .await?;
        let cached_df = ctx.sql("SELECT 1").await?.cache().await?;

        // Collect fails because extension node is not handled for now by default query planner
        let result = cached_df.collect().await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("No installed planner was able to convert the custom node to an execution plan: BallistaCacheNode"),
            "Expected planner error, got: {err_msg}"
        );

        Ok(())
    }
}
