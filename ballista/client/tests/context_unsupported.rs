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
    use datafusion::prelude::SessionContext;
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
    async fn should_support_information_schema_query_reading_other_tables(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) -> datafusion::error::Result<()> {
        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;

        // information_schema can only be read on the client, and `test` should
        // be scanned by the cluster, so the query is refused rather than run
        // on the client
        let result = ctx
            .sql("select table_name, (select count(*) from test) as row_count from information_schema.tables where table_name = 'test'")
            .await?
            .collect()
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains(
                "cannot run a query that reads information_schema together with other tables"
            ),
            "Expected unsupported error, got: {err_msg}"
        );

        Ok(())
    }
}
