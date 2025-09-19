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
            "+------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| plan_type        | plan                                                                                                                                                                                                                                                                                                     |",
            "+------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| logical_plan     | Projection: count(Int64(1)) AS count(*), test.id                                                                                                                                                                                                                                                         |",
            "|                  |   Aggregate: groupBy=[[test.id]], aggr=[[count(Int64(1))]]                                                                                                                                                                                                                                               |",
            "|                  |     Filter: test.id > Int32(4)                                                                                                                                                                                                                                                                           |",
            "|                  |       TableScan: test projection=[id], partial_filters=[test.id > Int32(4)]                                                                                                                                                                                                                              |",
            "| physical_plan    | ProjectionExec: expr=[count(Int64(1))@1 as count(*), id@0 as id]                                                                                                                                                                                                                                         |",
            "|                  |   AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[count(Int64(1))]                                                                                                                                                                                                                         |",
            "|                  |     CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                          |",
            "|                  |       RepartitionExec: partitioning=Hash([id@0], 16), input_partitions=1                                                                                                                                                                                                                                 |",
            "|                  |         AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[count(Int64(1))]                                                                                                                                                                                                                            |",
            "|                  |           CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                    |",
            "|                  |             FilterExec: id@0 > 4                                                                                                                                                                                                                                                                         |",
            "|                  |               DataSourceExec: file_groups={1 group: [[Users/ballista/git/datafusion-ballista/ballista/client/testdata/alltypes_plain.parquet]]}, projection=[id], file_type=parquet, predicate=id@0 > 4, pruning_predicate=id_null_count@1 != row_count@2 AND id_max@0 > 4, required_guarantees=[] |",
            "|                  |                                                                                                                                                                                                                                                                                                          |",
            "|                  |                                                                                                                                                                                                                                                                                                          |",
            "| distributed_plan | =========ResolvedStage[stage_id=1.0, partitions=1]=========                                                                                                                                                                                                                                              |",
            "|                  | ShuffleWriterExec: partitions:Some(Hash([Column { name: \"id\", index: 0 }], 16))                                                                                                                                                                                                                        |",
            "|                  |   AggregateExec: mode=Partial, gby=[id@0 as id], aggr=[count(Int64(1))]                                                                                                                                                                                                                                  |",
            "|                  |     CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                          |",
            "|                  |       FilterExec: id@0 > 4                                                                                                                                                                                                                                                                               |",
            "|                  |         DataSourceExec: file_groups={1 group: [[Users/ballista/git/datafusion-ballista/ballista/client/testdata/alltypes_plain.parquet]]}, projection=[id], file_type=parquet, predicate=id@0 > 4, pruning_predicate=id_null_count@1 != row_count@2 AND id_max@0 > 4, required_guarantees=[]       |",
            "|                  |                                                                                                                                                                                                                                                                                                          |",
            "|                  |                                                                                                                                                                                                                                                                                                          |",
            "|                  | =========UnResolvedStage[stage_id=2.0, children=1]=========                                                                                                                                                                                                                                              |",
            "|                  | Inputs{1: StageOutput { partition_locations: {}, complete: false }}                                                                                                                                                                                                                                      |",
            "|                  | ShuffleWriterExec: partitions:None                                                                                                                                                                                                                                                                       |",
            "|                  |   ProjectionExec: expr=[count(Int64(1))@1 as count(*), id@0 as id]                                                                                                                                                                                                                                       |",
            "|                  |     AggregateExec: mode=FinalPartitioned, gby=[id@0 as id], aggr=[count(Int64(1))]                                                                                                                                                                                                                       |",
            "|                  |       CoalesceBatchesExec: target_batch_size=8192                                                                                                                                                                                                                                                        |",
            "|                  |         UnresolvedShuffleExec: partitions=Hash([Column { name: \"id\", index: 0 }], 16)                                                                                                                                                                                                                  |",
            "|                  |                                                                                                                                                                                                                                                                                                          |",
            "|                  |                                                                                                                                                                                                                                                                                                          |",
            "+------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",    
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

    // at the moment sort merge join is not supported due to
    // serde issues. it should be supported with DF.50
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    #[should_panic]
    async fn should_support_sort_merge_join(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) {
        ctx.register_parquet(
            "t0",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await
        .unwrap();

        ctx.register_parquet(
            "t1",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await
        .unwrap();
        ctx.sql("SET datafusion.optimizer.prefer_hash_join = false")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();
        ctx.sql("select t0.id from t0 join t1 on t0.id = t1.id")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();
    }
}
