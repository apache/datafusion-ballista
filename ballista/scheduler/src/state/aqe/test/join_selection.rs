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

use crate::{
    assert_plan,
    state::aqe::{planner::AdaptivePlanner, test::mock_partitions_with_statistics},
};
use ballista_core::extension::SessionConfigExt;
use datafusion::{
    arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    },
    datasource::MemTable,
    execution::{SessionStateBuilder, config::SessionConfig, context::SessionContext},
};
use std::sync::Arc;

fn make_table(schema: Arc<Schema>) -> Arc<MemTable> {
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0])),
            Arc::new(Int32Array::from(vec![
                10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
            ])),
        ],
    )
    .unwrap();

    Arc::new(
        MemTable::try_new(
            schema,
            vec![
                vec![batch.clone()],
                vec![batch.clone()],
                vec![batch.clone()],
                vec![batch],
            ],
        )
        .unwrap(),
    )
}

fn make_ctx(prefer_hash_join: bool) -> SessionContext {
    let config = SessionConfig::new()
        // .set_u64(
        //     "datafusion.optimizer.hash_join_single_partition_threshold",
        //     0,
        // )
        // .set_u64(
        //     "datafusion.optimizer.hash_join_single_partition_threshold_rows",
        //     0,
        // )
        .set_bool("datafusion.optimizer.prefer_hash_join", prefer_hash_join)
        .with_target_partitions(4)
        .with_round_robin_repartition(false);
    let state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .build();
    SessionContext::new_with_state(state)
}

fn make_ctx_without_collect_left(prefer_hash_join: bool) -> SessionContext {
    let config = SessionConfig::new()
        .set_bool("datafusion.optimizer.prefer_hash_join", prefer_hash_join)
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            0,
        )
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            0,
        )
        // AQE join selection reads the broadcast cutoff from Ballista's own
        // config; a byte threshold of 0 disables CollectLeft promotion, keeping
        // these joins repartitioned.
        .with_ballista_broadcast_join_threshold_bytes(0)
        .with_target_partitions(4)
        .with_round_robin_repartition(false);
    let state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .build();
    SessionContext::new_with_state(state)
}

fn register_2tables(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]));
    ctx.register_table("t1", make_table(Arc::clone(&schema)))
        .unwrap();
    ctx.register_table("t2", make_table(schema)).unwrap();
}

fn register_3tables(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]));

    ctx.register_table("t1", make_table(Arc::clone(&schema)))
        .unwrap();
    ctx.register_table("t2", make_table(Arc::clone(&schema)))
        .unwrap();
    ctx.register_table("t3", make_table(schema)).unwrap();
}

#[tokio::test]
async fn test_hash_join_two_tables_coalesce() -> datafusion::common::Result<()> {
    let ctx = make_ctx(true);
    register_2tables(&ctx);

    let lp = ctx
        .sql("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();

    let planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".to_owned()).await?;
    let plan = planner.current_plan();
    // TODO: we could probably push datasource to read single partition
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending, stage_resolved=false
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)], projection=[id@0, val@2]
        ExchangeExec: partitioning=None, plan_id=1, stage_id=pending, stage_resolved=false, broadcast=true
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
        DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");
    Ok(())
}

// A `CollectLeft` join broadcasts (replicates) the build/left side to every
// probe task, each of which sees only its slice of the probe/right side. For a
// LEFT join that emits a null-padded row for every unmatched left row, each
// task would emit those rows independently, producing duplicate/spurious rows
// (apache/datafusion-ballista#1055). The resolver must therefore keep an
// unsafe-to-broadcast join type repartitioned instead of collecting it.
#[tokio::test]
async fn test_left_join_not_collected_left() -> datafusion::common::Result<()> {
    let ctx = make_ctx(true);
    register_2tables(&ctx);

    let lp = ctx
        .sql("SELECT t1.id, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();

    let planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".to_owned()).await?;
    let plan = planner.current_plan();
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@0 as id, val@2 as val]
        DynamicJoinSelectionExec: plan_id=0, join_type=Left, on=[(id@0, id@0)] repartitioned=true
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=pending, stage_resolved=false
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=pending, stage_resolved=false
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");
    Ok(())
}

#[tokio::test]
async fn test_hash_join_two_tables_repartition() -> datafusion::common::Result<()> {
    let ctx = make_ctx_without_collect_left(true);
    register_2tables(&ctx);

    let lp = ctx
        .sql("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();

    let mut planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".to_owned()).await?;

    let plan = planner.current_plan();
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@0 as id, val@2 as val]
        DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=true
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=pending, stage_resolved=false
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=pending, stage_resolved=false
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    let (stages, cancellable) = planner.actionable_stages()?;
    let stages = stages.unwrap();
    assert_eq!(2, stages.len());
    assert_eq!(0, cancellable.len());

    planner.finalise_stage_internal(0, mock_partitions_with_statistics())?;
    planner.finalise_stage_internal(1, mock_partitions_with_statistics())?;

    let plan = planner.current_plan();
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(id@0, id@0)], projection=[id@0, val@2]
        ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=0, stage_resolved=true
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
        ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=1, stage_resolved=true
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    let (stages, cancellable) = planner.actionable_stages()?;
    let stages = stages.unwrap();
    assert_eq!(1, stages.len());
    assert_eq!(0, cancellable.len());

    let plan = planner.current_plan();
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=true, plan_id=3, stage_id=2, stage_resolved=false
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(id@0, id@0)], projection=[id@0, val@2]
        ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=0, stage_resolved=true
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
        ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=1, stage_resolved=true
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}

#[tokio::test]
async fn test_sort_merge_join_two_tables_repartition() -> datafusion::common::Result<()> {
    let ctx = make_ctx_without_collect_left(false);
    register_2tables(&ctx);

    let lp = ctx
        .sql("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();

    let mut planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".to_owned()).await?;

    let plan = planner.current_plan();
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@0 as id, val@2 as val]
        DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=true
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=pending, stage_resolved=false
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=pending, stage_resolved=false
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    let (stages, cancellable) = planner.actionable_stages()?;
    let stages = stages.unwrap();
    assert_eq!(2, stages.len());
    assert_eq!(0, cancellable.len());

    planner.finalise_stage_internal(0, mock_partitions_with_statistics())?;
    planner.finalise_stage_internal(1, mock_partitions_with_statistics())?;

    let plan = planner.current_plan();
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@0 as id, val@2 as val]
        SortMergeJoinExec: join_type=Inner, on=[(id@0, id@0)]
          SortExec: expr=[id@0 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=0, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          SortExec: expr=[id@0 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=1, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    let (stages, cancellable) = planner.actionable_stages()?;
    let stages = stages.unwrap();
    assert_eq!(1, stages.len());
    assert_eq!(0, cancellable.len());

    let plan = planner.current_plan();
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=true, plan_id=3, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[id@0 as id, val@2 as val]
        SortMergeJoinExec: join_type=Inner, on=[(id@0, id@0)]
          SortExec: expr=[id@0 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=0, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          SortExec: expr=[id@0 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=1, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}

#[tokio::test]
async fn test_hash_join_three_tables_collect_left() -> datafusion::common::Result<()> {
    let ctx = make_ctx(true);
    register_3tables(&ctx);

    let lp = ctx
        .sql("SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let mut planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".to_owned()).await?;
    let plan = planner.current_plan();

    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@0 as id]
        DynamicJoinSelectionExec: plan_id=1, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)], projection=[id@0]
            ExchangeExec: partitioning=None, plan_id=2, stage_id=pending, stage_resolved=false, broadcast=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    let (stages, cancellable) = planner.actionable_stages()?;
    let stages = stages.unwrap();
    assert_eq!(1, stages.len());
    assert_eq!(0, cancellable.len());

    let plan = planner.current_plan();
    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@0 as id]
        DynamicJoinSelectionExec: plan_id=1, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)], projection=[id@0]
            ExchangeExec: partitioning=None, plan_id=2, stage_id=0, stage_resolved=false, broadcast=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    let stage = stages.first().unwrap();

    // plan for first stage
    assert_plan!(stage.plan.as_ref(), @ r"
    ShuffleWriterExec: partitioning: None
      DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    planner.finalise_stage_internal(0, mock_partitions_with_statistics())?;

    let (stages, cancellable) = planner.actionable_stages()?;
    let stages = stages.unwrap();
    assert_eq!(1, stages.len());
    assert_eq!(0, cancellable.len());

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)], projection=[id@0]
        ExchangeExec: partitioning=None, plan_id=4, stage_id=1, stage_resolved=false, broadcast=true
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)], projection=[id@0]
            ExchangeExec: partitioning=None, plan_id=2, stage_id=0, stage_resolved=true, broadcast=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
        DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}

#[tokio::test]
async fn test_hash_join_three_tables_repartition() -> datafusion::common::Result<()> {
    let ctx = make_ctx_without_collect_left(true);
    register_3tables(&ctx);

    let lp = ctx
        .sql("SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".to_owned()).await?;
    let plan = planner.current_plan();

    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=4, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@0 as id]
        DynamicJoinSelectionExec: plan_id=1, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
          ProjectionExec: expr=[id@0 as id]
            DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=true
              ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=pending, stage_resolved=false
                DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
              ExchangeExec: partitioning=Hash([id@0], 4), plan_id=3, stage_id=pending, stage_resolved=false
                DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}

#[tokio::test]
async fn test_sort_merge_join_three_tables_repartition() -> datafusion::common::Result<()>
{
    let ctx = make_ctx_without_collect_left(false);
    register_3tables(&ctx);

    let lp = ctx
        .sql("SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".to_owned()).await?;
    let plan = planner.current_plan();

    assert_plan!(plan, @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=4, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@0 as id]
        DynamicJoinSelectionExec: plan_id=1, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
          ProjectionExec: expr=[id@0 as id]
            DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=true
              ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=pending, stage_resolved=false
                DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
              ExchangeExec: partitioning=Hash([id@0], 4), plan_id=3, stage_id=pending, stage_resolved=false
                DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}
