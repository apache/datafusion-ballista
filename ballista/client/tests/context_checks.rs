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

    use crate::common::{
        remote_context, remote_context_with_state, standalone_context,
        standalone_context_with_state,
    };
    use ballista_core::config::BallistaConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::*;
    use datafusion::{assert_batches_eq, prelude::SessionContext};
    use rstest::*;
    use std::path::PathBuf;

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

    // tests if client will collect statistics for
    // collect/show operation
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_collect_client_statistics_for_show(
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

        let plan = ctx
            .sql("select string_col, timestamp_col from test where id > 4")
            .await?
            .create_physical_plan()
            .await?;

        let result = collect(plan.clone(), ctx.task_ctx()).await?;

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

        let metrics = plan.metrics().unwrap();
        let rows = metrics.output_rows().unwrap();

        assert_eq!(3, rows);
        assert!(
            plan.metrics()
                .unwrap()
                .sum_by_name("transferred_bytes")
                .unwrap()
                .as_usize()
                > 0
        );

        // Verify timing metrics
        let job_execution_time = metrics.sum_by_name("job_execution_time_ms").unwrap();
        assert!(
            job_execution_time.as_usize() > 0,
            "job_execution_time_ms should be greater than 0"
        );

        let scheduling_time = metrics.sum_by_name("job_scheduling_in_ms").unwrap();
        assert!(
            scheduling_time.as_usize() > 0,
            "job_scheduling_in_ms should be non-negative"
        );

        let total_time = metrics.sum_by_name("total_query_time_ms").unwrap();
        assert!(
            total_time.as_usize() > 0,
            "total_query_time_ms should be greater than 0"
        );

        // Total time should be at least as long as execution time
        assert!(
            total_time.as_usize() >= job_execution_time.as_usize(),
            "total_query_time_ms should be >= job_execution_time_ms"
        );

        Ok(())
    }

    // tests if client will collect statistics for
    // insert operation
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_collect_client_statistics_for_insert(
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
        .await
        .unwrap();
        let write_dir = tempfile::tempdir().expect("temporary directory to be created");
        let write_dir_path = write_dir
            .path()
            .to_str()
            .expect("path to be converted to str");

        ctx.sql(&format!("CREATE EXTERNAL TABLE written_table (id INTEGER, string_col STRING, timestamp_col BIGINT) STORED AS PARQUET LOCATION '{write_dir_path}'")).await.unwrap().show().await.unwrap();

        let plan = ctx
            .sql("INSERT INTO written_table select  id, string_col, timestamp_col from test")
            .await?
            .create_physical_plan()
            .await?;

        let result = collect(plan.clone(), ctx.task_ctx()).await.unwrap();

        // INSERT operation should return only single row
        assert_eq!(1, plan.metrics().unwrap().output_rows().unwrap());
        assert!(
            plan.metrics()
                .unwrap()
                .sum_by_name("transferred_bytes")
                .unwrap()
                .as_usize()
                > 0
        );

        let expected = [
            "+-------+",
            "| count |",
            "+-------+",
            "| 8     |",
            "+-------+",
        ];

        assert_batches_eq!(expected, &result);

        // Verify timing metrics
        let metrics = plan.metrics().unwrap();
        let job_execution_time = metrics.sum_by_name("job_execution_time_ms").unwrap();
        assert!(
            job_execution_time.as_usize() > 0,
            "job_execution_time_ms should be greater than 0"
        );

        let total_time = metrics.sum_by_name("total_query_time_ms").unwrap();
        assert!(
            total_time.as_usize() >= job_execution_time.as_usize(),
            "total_query_time_ms should be >= job_execution_time_ms"
        );

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
            .sql("select name, value from information_schema.df_settings where name = 'ballista.job.name'")
            .await?
            .collect()
            .await?;

        let expected = [
            "+-------------------+-------+",
            "| name              | value |",
            "+-------------------+-------+",
            "| ballista.job.name |       |",
            "+-------------------+-------+",
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
        ctx.sql(&format!("CREATE EXTERNAL TABLE tbl_test STORED AS PARQUET LOCATION '{test_data}/alltypes_plain.parquet'", )).await?.show().await?;

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

    // test checks if this view types have been disabled in the configuration
    //
    // `datafusion.execution.parquet.schema_force_view_types` have been disabled
    // temporary as they could break shuffle reader/writer.
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_disable_view_types(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
    ) -> datafusion::error::Result<()> {
        let result = ctx
            .sql("select name, value from information_schema.df_settings where name like 'datafusion.execution.parquet.schema_force_view_types' or name like 'datafusion.sql_parser.map_string_types_to_utf8view' order by name limit 2")
            .await?
            .collect()
            .await?;

        let expected = [
            "+------------------------------------------------------+-------+",
            "| name                                                 | value |",
            "+------------------------------------------------------+-------+",
            "| datafusion.execution.parquet.schema_force_view_types | false |",
            "| datafusion.sql_parser.map_string_types_to_utf8view   | false |",
            "+------------------------------------------------------+-------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    // As mentioned in https://github.com/apache/datafusion-ballista/issues/1055
    // "Left/full outer join incorrect for CollectLeft / broadcast"
    //
    // In order to make correct results (decreasing performance) CollectLeft
    // has been disabled until fixed

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_disable_collect_left(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
    ) -> datafusion::error::Result<()> {
        let result = ctx
            .sql("select name, value from information_schema.df_settings where name in ('datafusion.optimizer.hash_join_single_partition_threshold', 'datafusion.optimizer.hash_join_single_partition_threshold_rows') order by name limit 2")
            .await?
            .collect()
            .await?;

        let expected = [
            "+----------------------------------------------------------------+-------+",
            "| name                                                           | value |",
            "+----------------------------------------------------------------+-------+",
            "| datafusion.optimizer.hash_join_single_partition_threshold      | 0     |",
            "| datafusion.optimizer.hash_join_single_partition_threshold_rows | 0     |",
            "+----------------------------------------------------------------+-------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_sql_show_with_url_table(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) -> datafusion::error::Result<()> {
        // tests check if this configuration option
        // will work
        let ctx = ctx.enable_url_table();

        let result = ctx
            .sql(&format!("select string_col, timestamp_col from '{test_data}/alltypes_plain.parquet' where id > 4"))
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
    async fn should_support_sql_insert_into(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) {
        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await
        .unwrap();
        let write_dir = tempfile::tempdir().expect("temporary directory to be created");
        let write_dir_path = write_dir
            .path()
            .to_str()
            .expect("path to be converted to str");

        ctx.sql(&format!("CREATE EXTERNAL TABLE written_table (id INTEGER, string_col STRING, timestamp_col BIGINT) STORED AS PARQUET LOCATION '{write_dir_path}'")).await.unwrap().show().await.unwrap();

        let _ = ctx
            .sql("INSERT INTO written_table select  id, string_col, timestamp_col from test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        ctx.deregister_table("written_table")
            .expect("table to be dropped");

        ctx.register_parquet("written_table_test", write_dir_path, Default::default())
            .await
            .unwrap();

        let result = ctx
            .sql("select id, string_col, timestamp_col from written_table_test where id > 4 order by id")
            .await.unwrap()
            .collect()
            .await.unwrap();

        let expected = [
            "+----+------------+---------------------+",
            "| id | string_col | timestamp_col       |",
            "+----+------------+---------------------+",
            "| 5  | 1          | 1235865660000000000 |",
            "| 6  | 0          | 1238544000000000000 |",
            "| 7  | 1          | 1238544060000000000 |",
            "+----+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[case::standalone_state(standalone_context_with_state())]
    #[case::remote_state(remote_context_with_state())]
    #[tokio::test]
    async fn should_execute_sql_write_read_roundtrip(
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

        let expected = [
            "+----+------------+",
            "| id | string_col |",
            "+----+------------+",
            "| 5  | 31         |",
            "| 6  | 30         |",
            "| 7  | 31         |",
            "+----+------------+",
        ];

        let write_dir = tempfile::tempdir().expect("temporary directory to be created");
        let write_dir_path = PathBuf::from(
            write_dir
                .path()
                .to_str()
                .expect("path to be converted to str"),
        );

        let parquet_file = write_dir_path.join("p_written_table.parquet");
        let parquet_file = parquet_file.to_str().expect("cannot create csv file");

        ctx.sql("select * from test")
            .await?
            .write_parquet(parquet_file, Default::default(), Default::default())
            .await?;

        ctx.register_parquet("p_written_table", parquet_file, Default::default())
            .await?;

        let result = ctx
            .sql("select id, string_col from p_written_table where id > 4")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(expected, &result);

        let csv_file = write_dir_path.join("c_written_table.csv");
        let csv_file = csv_file.to_str().expect("cannot create csv file");

        ctx.sql("select * from test")
            .await?
            .write_csv(csv_file, Default::default(), Default::default())
            .await?;

        ctx.register_csv("c_written_table", csv_file, Default::default())
            .await?;

        let result = ctx
            .sql("select id, string_col from c_written_table where id > 4")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(expected, &result);

        let json_file = write_dir_path.join("j_written_table.json");
        let json_file = json_file.to_str().expect("cannot create csv file");

        ctx.sql("select * from test")
            .await?
            .write_json(json_file, Default::default(), Default::default())
            .await?;

        ctx.register_json("j_written_table", json_file, Default::default())
            .await?;

        let result = ctx
            .sql("select id, string_col from j_written_table where id > 4")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[case::standalone_state(standalone_context_with_state())]
    #[case::remote_state(remote_context_with_state())]
    #[tokio::test]
    async fn should_execute_sql_show_multiple_times(
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

        let expected = [
            "+------------+---------------------+",
            "| string_col | timestamp_col       |",
            "+------------+---------------------+",
            "| 31         | 2009-03-01T00:01:00 |",
            "| 30         | 2009-04-01T00:00:00 |",
            "| 31         | 2009-04-01T00:01:00 |",
            "+------------+---------------------+",
        ];
        // there were cases when we break session context
        // with standalone setup. so subsequent query for the
        // same table fails with table does not exist
        for _ in 0..5 {
            let result = ctx
                .sql("select string_col, timestamp_col from test where id > 4")
                .await?
                .collect()
                .await?;

            assert_batches_eq!(expected, &result);
        }

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[case::standalone_state(standalone_context_with_state())]
    #[case::remote_state(remote_context_with_state())]
    #[tokio::test]
    async fn should_execute_group_by(
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

        let expected = [
            "+------------+----------+",
            "| string_col | count(*) |",
            "+------------+----------+",
            "| 30         | 1        |",
            "| 31         | 2        |",
            "+------------+----------+",
        ];

        let result = ctx
            .sql("select string_col, count(*) from test where id > 4 group by string_col order by string_col")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_force_local_read(
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

        ctx.sql("SET ballista.shuffle.force_remote_read = true")
            .await?
            .show()
            .await?;

        let result = ctx
            .sql("select name, value from information_schema.df_settings where name like 'ballista.shuffle.force_remote_read' order by name limit 1")
            .await?
            .collect()
            .await?;

        let expected = [
            "+------------------------------------+-------+",
            "| name                               | value |",
            "+------------------------------------+-------+",
            "| ballista.shuffle.force_remote_read | true  |",
            "+------------------------------------+-------+",
        ];

        assert_batches_eq!(expected, &result);

        let expected = [
            "+------------+----------+",
            "| string_col | count(*) |",
            "+------------+----------+",
            "| 30         | 1        |",
            "| 31         | 2        |",
            "+------------+----------+",
        ];

        let result = ctx
            .sql("select string_col, count(*) from test where id > 4 group by string_col order by string_col")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_force_local_read_with_flight(
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

        ctx.sql("SET ballista.shuffle.force_remote_read = true")
            .await?
            .show()
            .await?;

        ctx.sql("SET ballista.shuffle.remote_read_prefer_flight = true")
            .await?
            .show()
            .await?;

        let result = ctx
            .sql("select name, value from information_schema.df_settings where name like 'ballista.shuffle.remote_read_prefer_flight' order by name limit 1")
            .await?
            .collect()
            .await?;

        let expected = [
            "+--------------------------------------------+-------+",
            "| name                                       | value |",
            "+--------------------------------------------+-------+",
            "| ballista.shuffle.remote_read_prefer_flight | true  |",
            "+--------------------------------------------+-------+",
        ];

        assert_batches_eq!(expected, &result);

        let expected = [
            "+------------+----------+",
            "| string_col | count(*) |",
            "+------------+----------+",
            "| 30         | 1        |",
            "| 31         | 2        |",
            "+------------+----------+",
        ];

        let result = ctx
            .sql("select string_col, count(*) from test where id > 4 group by string_col order by string_col")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    // Sort Merge Join is supported since DF.v50
    // testing if it will work in ballista
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_support_sort_merge_join(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) -> datafusion::error::Result<()> {
        ctx.register_parquet(
            "t0",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;

        ctx.register_parquet(
            "t1",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;
        ctx.sql("SET datafusion.optimizer.prefer_hash_join = false")
            .await?
            .show()
            .await?;
        let result = ctx.sql(
            "select t0.id from t0 join t1 on t0.id = t1.id order by t0.id desc limit 5",
        )
        .await?
        .collect()
        .await?;

        let expected = [
            "+----+", "| id |", "+----+", "| 7  |", "| 6  |", "| 5  |", "| 4  |",
            "| 3  |", "+----+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn should_execute_explain_query_correctly(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
    ) {
        let result = ctx
            .sql("EXPLAIN select count(*), id from (select unnest([1,2,3,4,5]) as id) group by id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected: Vec<&str> = vec![
            "+------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| plan_type        | plan                                                                                                                                                                             |",
            "+------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| logical_plan     | Projection: count(Int64(1)) AS count(*), id                                                                                                                                      |",
            "|                  |   Aggregate: groupBy=[[id]], aggr=[[count(Int64(1))]]                                                                                                                            |",
            "|                  |     Projection: __unnest_placeholder(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)),depth=1) AS UNNEST(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5))) AS id |",
            "|                  |       Unnest: lists[__unnest_placeholder(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)))|depth=1] structs[]                                                            |",
            "|                  |         Projection: List([1, 2, 3, 4, 5]) AS __unnest_placeholder(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)))                                                      |",
            "|                  |           EmptyRelation: rows=1                                                                                                                                                  |",
            "| physical_plan    | ProjectionExec: expr=[count(Int64(1))@1 as count(*), id@0 as id]                                                                                                                 |",
            "|                  |   AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[count(Int64(1))]                                                                                                 |",
            "|                  |     RepartitionExec: partitioning=Hash([id@0], 16), input_partitions=1                                                                                                           |",
            "|                  |       AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[count(Int64(1))]                                                                                                      |",
            "|                  |         ProjectionExec: expr=[__unnest_placeholder(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)),depth=1)@0 as id]                                                    |",
            "|                  |           UnnestExec                                                                                                                                                             |",
            "|                  |             ProjectionExec: expr=[[1, 2, 3, 4, 5] as __unnest_placeholder(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)))]                                             |",
            "|                  |               PlaceholderRowExec                                                                                                                                                 |",
            "|                  |                                                                                                                                                                                  |",
            "| distributed_plan | =========ResolvedStage[stage_id=1.0, partitions=1]=========                                                                                                                      |",
            "|                  | ShuffleWriterExec: partitioning: Hash([id@0], 16)                                                                                                                                |",
            "|                  |   AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[count(Int64(1))]                                                                                                          |",
            "|                  |     ProjectionExec: expr=[__unnest_placeholder(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)),depth=1)@0 as id]                                                        |",
            "|                  |       UnnestExec                                                                                                                                                                 |",
            "|                  |         ProjectionExec: expr=[[1, 2, 3, 4, 5] as __unnest_placeholder(make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)))]                                                 |",
            "|                  |           PlaceholderRowExec                                                                                                                                                     |",
            "|                  |                                                                                                                                                                                  |",
            "|                  | =========UnResolvedStage[stage_id=2.0, children=1]=========                                                                                                                      |",
            "|                  | Inputs{1: StageOutput { partition_locations: {}, complete: false }}                                                                                                              |",
            "|                  | ShuffleWriterExec: partitioning: None                                                                                                                                            |",
            "|                  |   ProjectionExec: expr=[count(Int64(1))@1 as count(*), id@0 as id]                                                                                                               |",
            "|                  |     AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[count(Int64(1))]                                                                                               |",
            "|                  |       UnresolvedShuffleExec: partitioning: Hash([id@0], 16)                                                                                                                      |",
            "|                  |                                                                                                                                                                                  |",
            "|                  |                                                                                                                                                                                  |",
            "+------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &result);
    }
}
