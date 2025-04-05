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
    async fn should_execute_explain_query_correctly(
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

        let result = ctx
            .sql("EXPLAIN select count(*), id from test where id > 4 group by id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

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
    #[should_panic]
    // "Error: Internal(failed to serialize logical plan: Internal(LogicalPlan serde is not yet implemented for Dml))"
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

        ctx.sql("select * from test")
            .await
            .unwrap()
            .write_parquet(write_dir_path, Default::default(), Default::default())
            .await
            .unwrap();

        ctx.register_parquet("written_table", write_dir_path, Default::default())
            .await
            .unwrap();

        let _ = ctx
            .sql("INSERT INTO written_table select * from written_table")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let result = ctx
            .sql("select id, string_col, timestamp_col from written_table where id > 4 order by id")
            .await.unwrap()
            .collect()
            .await.unwrap();

        let expected = [
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
    }
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    #[should_panic]
    // "Error preparing task definition: Unsupported plan and extension codec failed with unsupported plan type: NdJsonExec"
    async fn should_support_json_source(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) {
        let result = ctx
            .read_json(&format!("{test_data}/simple.json"), Default::default())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        #[rustfmt::skip]
        let expected = [
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "+---+"
        ];

        assert_batches_eq!(expected, &result);
    }
}
