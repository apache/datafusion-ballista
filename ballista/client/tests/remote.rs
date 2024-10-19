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

#[cfg(test)]
mod remote {
    use ballista::extension::SessionContextExt;
    use datafusion::{assert_batches_eq, prelude::SessionContext};

    #[tokio::test]
    async fn should_execute_sql_show() -> datafusion::error::Result<()> {
        let (host, port) = crate::common::setup_test_cluster().await;
        let url = format!("df://{host}:{port}");

        let test_data = crate::common::example_test_data();
        let ctx: SessionContext = SessionContext::remote(&url).await?;

        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;

        let result = ctx
            .sql("select string_col, timestamp_col from test where id > 4")
            .await?
            .collect()
            .await?;
        let expected = vec![
            "+------------+---------------------+",
            "| string_col | timestamp_col       |",
            "+------------+---------------------+",
            "| 31         | 2009-03-01T00:01:00 |",
            "| 30         | 2009-04-01T00:00:00 |",
            "| 31         | 2009-04-01T00:01:00 |",
            "+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_sql_write() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();
        let (host, port) = crate::common::setup_test_cluster().await;
        let url = format!("df://{host}:{port}");

        let ctx: SessionContext = SessionContext::remote(&url).await?;

        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;
        let write_dir = tempfile::tempdir().expect("temporary directory to be created");
        let write_dir_path = write_dir
            .path()
            .to_str()
            .expect("path to be converted to str");

        ctx.sql("select * from test")
            .await?
            .write_parquet(&write_dir_path, Default::default(), Default::default())
            .await?;
        ctx.register_parquet("written_table", &write_dir_path, Default::default())
            .await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from written_table where id > 4")
            .await?
            .collect()
            .await?;
        let expected = vec![
            "+----+------------+---------------------+",
            "| id | string_col | timestamp_col       |",
            "+----+------------+---------------------+",
            "| 5  | 31         | 2009-03-01T00:01:00 |",
            "| 6  | 30         | 2009-04-01T00:00:00 |",
            "| 7  | 31         | 2009-04-01T00:01:00 |",
            "+----+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);
        Ok(())
    }

    #[tokio::test]
    async fn should_execute_show_tables() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let (host, port) = crate::common::setup_test_cluster().await;
        let url = format!("df://{host}:{port}");

        let ctx: SessionContext = SessionContext::remote(&url).await?;

        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;

        let result = ctx.sql("show tables").await?.collect().await?;
        //
        let expected = vec![
            "+---------------+--------------------+-------------+------------+",
            "| table_catalog | table_schema       | table_name  | table_type |",
            "+---------------+--------------------+-------------+------------+",
            "| datafusion    | public             | test        | BASE TABLE |",
            "| datafusion    | information_schema | tables      | VIEW       |",
            "| datafusion    | information_schema | views       | VIEW       |",
            "| datafusion    | information_schema | columns     | VIEW       |",
            "| datafusion    | information_schema | df_settings | VIEW       |",
            "| datafusion    | information_schema | schemata    | VIEW       |",
            "+---------------+--------------------+-------------+------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }
}
