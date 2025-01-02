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
mod supported {

    use crate::common::{remote_context, standalone_context};
    use ballista_core::config::BallistaConfig;
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
    async fn should_execute_sql_show(
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

        let result = ctx
            .sql("select string_col, timestamp_col from test where id > 4")
            .await?
            .collect()
            .await?;
        let expected = [
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
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_sql_show_configs(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
    ) -> datafusion::error::Result<()> {
        let result = ctx
            .sql("select name from information_schema.df_settings where name like 'datafusion.%' order by name limit 5")
            .await?
            .collect()
            .await?;
        //
        let expected = [
            "+------------------------------------------------------+",
            "| name                                                 |",
            "+------------------------------------------------------+",
            "| datafusion.catalog.create_default_catalog_and_schema |",
            "| datafusion.catalog.default_catalog                   |",
            "| datafusion.catalog.default_schema                    |",
            "| datafusion.catalog.format                            |",
            "| datafusion.catalog.has_header                        |",
            "+------------------------------------------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_sql_show_configs_ballista(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
    ) -> datafusion::error::Result<()> {
        let state = ctx.state();
        let ballista_config_extension =
            state.config().options().extensions.get::<BallistaConfig>();

        // ballista configuration should be registered with
        // session state
        assert!(ballista_config_extension.is_some());

        let result = ctx
            .sql("select name, value from information_schema.df_settings where name like 'ballista.%' order by name limit 2")
            .await?
            .collect()
            .await?;

        let expected = [
            "+---------------------------------------+----------+",
            "| name                                  | value    |",
            "+---------------------------------------+----------+",
            "| ballista.grpc_client_max_message_size | 16777216 |",
            "| ballista.job.name                     |          |",
            "+---------------------------------------+----------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_sql_set_configs(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
    ) -> datafusion::error::Result<()> {
        ctx.sql("SET ballista.job.name = 'Super Cool Ballista App'")
            .await?
            .show()
            .await?;

        let result = ctx
            .sql("select name, value from information_schema.df_settings where name like 'ballista.job.name' order by name limit 1")
            .await?
            .collect()
            .await?;

        let expected = [
            "+-------------------+-------------------------+",
            "| name              | value                   |",
            "+-------------------+-------------------------+",
            "| ballista.job.name | Super Cool Ballista App |",
            "+-------------------+-------------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    // select from ballista config
    // check for SET =
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_show_tables(
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

        let result = ctx.sql("show tables").await?.collect().await?;
        //
        let expected = [
            "+---------------+--------------------+-------------+------------+",
            "| table_catalog | table_schema       | table_name  | table_type |",
            "+---------------+--------------------+-------------+------------+",
            "| datafusion    | public             | test        | BASE TABLE |",
            "| datafusion    | information_schema | tables      | VIEW       |",
            "| datafusion    | information_schema | views       | VIEW       |",
            "| datafusion    | information_schema | columns     | VIEW       |",
            "| datafusion    | information_schema | df_settings | VIEW       |",
            "| datafusion    | information_schema | schemata    | VIEW       |",
            "| datafusion    | information_schema | routines    | VIEW       |",
            "| datafusion    | information_schema | parameters  | VIEW       |",
            "+---------------+--------------------+-------------+------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_sql_create_external_table(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) -> datafusion::error::Result<()> {
        ctx.sql(&format!("CREATE EXTERNAL TABLE tbl_test STORED AS PARQUET LOCATION '{}/alltypes_plain.parquet'", test_data, )).await?.show().await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from tbl_test where id > 4")
            .await?
            .collect()
            .await?;
        let expected = [
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

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_dataframe(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) -> datafusion::error::Result<()> {
        let df = ctx
            .read_parquet(
                &format!("{test_data}/alltypes_plain.parquet"),
                Default::default(),
            )
            .await?
            .select_columns(&["id", "bool_col", "timestamp_col"])?
            .filter(col("id").gt(lit(5)))?;

        let result = df.collect().await?;

        let expected = [
            "+----+----------+---------------------+",
            "| id | bool_col | timestamp_col       |",
            "+----+----------+---------------------+",
            "| 6  | true     | 2009-04-01T00:00:00 |",
            "| 7  | false    | 2009-04-01T00:01:00 |",
            "+----+----------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    #[cfg(not(windows))] // test is failing at windows, can't debug it
    async fn should_execute_sql_write(
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
        let write_dir = tempfile::tempdir().expect("temporary directory to be created");
        let write_dir_path = write_dir
            .path()
            .to_str()
            .expect("path to be converted to str");

        ctx.sql("select * from test")
            .await?
            .write_parquet(write_dir_path, Default::default(), Default::default())
            .await?;
        ctx.register_parquet("written_table", write_dir_path, Default::default())
            .await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from written_table where id > 4")
            .await?
            .collect()
            .await?;
        let expected = [
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

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_sql_app_name_show(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) -> datafusion::error::Result<()> {
        ctx.sql("SET ballista.job.name = 'Super Cool Ballista App'")
            .await?
            .show()
            .await?;

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
        let expected = [
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
}
