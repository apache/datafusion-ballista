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
#[cfg(feature = "standalone")]
mod standalone {
    use ballista::{extension::SessionContextExt, prelude::*};
    use datafusion::prelude::*;
    use datafusion::{assert_batches_eq, prelude::SessionContext};

    #[tokio::test]
    async fn should_execute_sql_show() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx: SessionContext = SessionContext::standalone().await?;
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
    async fn should_execute_sql_show_configs() -> datafusion::error::Result<()> {
        let ctx: SessionContext = SessionContext::standalone().await?;

        let result = ctx
            .sql("select name from information_schema.df_settings where name like 'datafusion.%' order by name limit 5")
            .await?
            .collect()
            .await?;
        //
        let expected = vec![
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

    #[tokio::test]
    async fn should_execute_sql_show_configs_ballista() -> datafusion::error::Result<()> {
        let ctx: SessionContext = SessionContext::standalone().await?;
        let state = ctx.state();
        let ballista_config_extension =
            state.config().options().extensions.get::<BallistaConfig>();

        // ballista configuration should be registered with
        // session state
        assert!(ballista_config_extension.is_some());

        let result = ctx
            .sql("select name, value from information_schema.df_settings where name like 'ballista.%' order by name limit 5")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------------------------------------------------------+----------+",
            "| name                                                    | value    |",
            "+---------------------------------------------------------+----------+",
            "| ballista.batch.size                                     | 8192     |",
            "| ballista.collect_statistics                             | false    |",
            "| ballista.grpc_client_max_message_size                   | 16777216 |",
            "| ballista.job.name                                       |          |",
            "| ballista.optimizer.hash_join_single_partition_threshold | 1048576  |",
            "+---------------------------------------------------------+----------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_sql_set_configs() -> datafusion::error::Result<()> {
        let ctx: SessionContext = SessionContext::standalone().await?;

        ctx.sql("SET ballista.job.name = 'Super Cool Ballista App'")
            .await?
            .show()
            .await?;

        let result = ctx
            .sql("select name, value from information_schema.df_settings where name like 'ballista.job.name' order by name limit 1")
            .await?
            .collect()
            .await?;

        let expected = vec![
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

    #[tokio::test]
    async fn should_execute_show_tables() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx: SessionContext = SessionContext::standalone().await?;
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

    //
    // TODO: It calls scheduler to generate the plan, but no
    //       but there is no ShuffleRead/Write in physical_plan
    //
    // ShuffleWriterExec: None, metrics=[output_rows=2, input_rows=2, write_time=1.782295ms, repart_time=1ns]
    //   ExplainExec, metrics=[]
    //
    #[tokio::test]
    #[ignore = "It uses local files, will fail in CI"]
    async fn should_execute_sql_explain() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx: SessionContext = SessionContext::standalone().await?;
        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;

        let result = ctx
            .sql("EXPLAIN select count(*), id from test where id > 4 group by id")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| plan_type     | plan                                                                                                                                                                                                                                                                                             |",
            "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| logical_plan  | Projection: count(*), test.id                                                                                                                                                                                                                                                                    |",
            "|               |   Aggregate: groupBy=[[test.id]], aggr=[[count(Int64(1)) AS count(*)]]                                                                                                                                                                                                                           |",
            "|               |     Filter: test.id > Int32(4)                                                                                                                                                                                                                                                                   |",
            "|               |       TableScan: test projection=[id], partial_filters=[test.id > Int32(4)]                                                                                                                                                                                                                      |",
            "| physical_plan | ProjectionExec: expr=[count(*)@1 as count(*), id@0 as id]                                                                                                                                                                                                                                        |",
            "|               |   AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[count(*)]                                                                                                                                                                                                                        |",
            "|               |     CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                  |",
            "|               |       RepartitionExec: partitioning=Hash([id@0], 16), input_partitions=1                                                                                                                                                                                                                         |",
            "|               |         AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[count(*)]                                                                                                                                                                                                                           |",
            "|               |           CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                            |",
            "|               |             FilterExec: id@0 > 4                                                                                                                                                                                                                                                                 |",
            "|               |               ParquetExec: file_groups={1 group: [[Users/ballista/git/arrow-ballista/ballista/client/testdata/alltypes_plain.parquet]]}, projection=[id], predicate=id@0 > 4, pruning_predicate=CASE WHEN id_null_count@1 = id_row_count@2 THEN false ELSE id_max@0 > 4 END, required_guarantees=[] |",
            "|               |                                                                                                                                                                                                                                                                                                  |",
            "+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",        
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_sql_create_external_table() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx: SessionContext = SessionContext::standalone().await?;
        ctx.sql(&format!("CREATE EXTERNAL TABLE tbl_test STORED AS PARQUET LOCATION '{}/alltypes_plain.parquet'", test_data, )).await?.show().await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from tbl_test where id > 4")
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
    #[ignore = "Error serializing custom table - NotImplemented(LogicalExtensionCodec is not provided))"]
    async fn should_execute_sql_create_table() -> datafusion::error::Result<()> {
        let ctx: SessionContext = SessionContext::standalone().await?;
        ctx.sql(&format!("CREATE TABLE tbl_test (id INT, value INT)"))
            .await?
            .show()
            .await?;

        // it does create table but it can't be queried
        let _result = ctx
            .sql("select * from tbl_test where id > 0")
            .await?
            .collect()
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_dataframe() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx: SessionContext = SessionContext::standalone().await?;

        let df = ctx
            .read_parquet(
                &format!("{test_data}/alltypes_plain.parquet"),
                Default::default(),
            )
            .await?
            .select_columns(&["id", "bool_col", "timestamp_col"])?
            .filter(col("id").gt(lit(5)))?;

        let result = df.collect().await?;

        let expected = vec![
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

    #[tokio::test]
    #[ignore = "Error serializing custom table - NotImplemented(LogicalExtensionCodec is not provided))"]
    async fn should_execute_dataframe_cache() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx: SessionContext = SessionContext::standalone().await?;

        let df = ctx
            .read_parquet(
                &format!("{test_data}/alltypes_plain.parquet"),
                Default::default(),
            )
            .await?
            .select_columns(&["id", "bool_col", "timestamp_col"])?
            .filter(col("id").gt(lit(5)))?;

        let cached_df = df.cache().await?;
        let result = cached_df.collect().await?;

        let expected = vec![
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

    #[tokio::test]
    #[ignore = "Error: Internal(failed to serialize logical plan: Internal(LogicalPlan serde is not yet implemented for Dml))"]
    async fn should_execute_sql_insert() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx: SessionContext = SessionContext::standalone().await?;

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

        let _ = ctx
            .sql("INSERT INTO written_table select * from written_table")
            .await?
            .collect()
            .await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from written_table where id > 4 order by id")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+----+------------+---------------------+",
            "| id | string_col | timestamp_col       |",
            "+----+------------+---------------------+",
            "| 5  | 31         | 2009-03-01T00:01:00 |",
            "| 5  | 31         | 2009-03-01T00:01:00 |",
            "| 6  | 30         | 2009-04-01T00:00:00 |",
            "| 6  | 30         | 2009-04-01T00:00:00 |",
            "| 7  | 31         | 2009-04-01T00:01:00 |",
            "| 7  | 31         | 2009-04-01T00:01:00 |",
            "+----+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    #[cfg(not(windows))] // test is failing at windows, can't debug it
    async fn should_execute_sql_write() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx: SessionContext = SessionContext::standalone().await?;
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
}
