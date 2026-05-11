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

//! End-to-end verification for the Pinot-style colocated-join optimizer.
//!
//! These tests exercise the full path: SQL → DataFusion physical plan →
//! AdaptivePlanner (which runs `ColocatedJoinRule` and
//! `BroadcastSmallSideRule` in sequence with the rest of the rule chain).
//!
//! The colocation case relies on `PartitionedTableProvider` to advertise
//! per-table hash bucketing; the unbucketed case asserts the plan is
//! unchanged so we know the rule is silent on tables without metadata.

use crate::assert_plan;
use crate::state::aqe::planner::AdaptivePlanner;
use ballista_core::extension::SessionConfigExt;
use ballista_core::partitioning::{
    HashDistribution, HashFn, PartitionedTableProvider,
};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

fn ctx() -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_round_robin_repartition(false)
        // Disable broadcast so these tests assert only the colocation /
        // sub-partition behavior — the default threshold (10 MB) would
        // promote our tiny inputs to CollectLeft and overshadow the rule
        // under test.
        .with_ballista_broadcast_join_threshold_bytes(0)
        // Upstream defaults to sort-merge join (issue #1648); opt back into
        // hash join so ColocatedJoinRule (which only matches HashJoinExec)
        // can fire on the planned join.
        .set_bool("datafusion.optimizer.prefer_hash_join", true);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

fn build_bucketed_table(
    schema: Arc<Schema>,
    buckets: usize,
    keys: &[&str],
) -> Arc<PartitionedTableProvider> {
    let parts: Vec<Vec<RecordBatch>> = (0..buckets)
        .map(|b| {
            let arrays: Vec<_> = schema
                .fields()
                .iter()
                .map(|_| {
                    Arc::new(Int32Array::from(vec![b as i32, b as i32 + 100])) as _
                })
                .collect();
            vec![RecordBatch::try_new(schema.clone(), arrays).unwrap()]
        })
        .collect();
    let inner: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(schema, parts).unwrap());
    let dist = HashDistribution::new(
        keys.iter().map(|s| s.to_string()).collect(),
        HashFn::Murmur3,
        buckets,
    );
    Arc::new(PartitionedTableProvider::new(inner, dist))
}

fn ab_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

#[tokio::test]
async fn colocated_join_emits_no_exchange() -> datafusion::error::Result<()> {
    let ctx = ctx();
    ctx.register_table("a", build_bucketed_table(ab_schema(), 4, &["id"]))?;
    ctx.register_table("b", build_bucketed_table(ab_schema(), 4, &["id"]))?;

    let plan = ctx
        .sql("select a.id, a.v, b.v from a inner join b on a.id = b.id")
        .await?
        .create_physical_plan()
        .await?;
    let planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "colo_inner".to_string())?;

    // Both inputs already satisfy the join's hash distribution, so the
    // optimizer should leave the plan exchange-free.
    assert_plan!(planner.current_plan(), @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=0, stage_id=pending, stage_resolved=false
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(id@0, id@0)], projection=[id@0, v@1, v@3]
        HashDistributedScanExec: keys=[id], hash_fn=murmur3, buckets=4
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
        HashDistributedScanExec: keys=[id], hash_fn=murmur3, buckets=4
          DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");
    Ok(())
}

#[tokio::test]
async fn divisor_join_inserts_sub_partition() -> datafusion::error::Result<()> {
    // Divisor case (8/4=2): the larger side wraps in BucketSubPartitionExec
    // and the join still avoids a network shuffle.
    let ctx = ctx();
    ctx.register_table("a", build_bucketed_table(ab_schema(), 8, &["id"]))?;
    ctx.register_table("b", build_bucketed_table(ab_schema(), 4, &["id"]))?;

    let plan = ctx
        .sql("select a.id, a.v, b.v from a inner join b on a.id = b.id")
        .await?
        .create_physical_plan()
        .await?;
    let planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "colo_div".to_string())?;

    assert_plan!(planner.current_plan(), @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=0, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[id@1 as id, v@2 as v, v@0 as v]
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(id@0, id@0)], projection=[v@1, id@2, v@3]
          HashDistributedScanExec: keys=[id], hash_fn=murmur3, buckets=4
            DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          BucketSubPartitionExec: out_buckets=4, factor=2
            HashDistributedScanExec: keys=[id], hash_fn=murmur3, buckets=8
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
    ");
    Ok(())
}

#[tokio::test]
async fn unbucketed_join_keeps_exchange() -> datafusion::error::Result<()> {
    // No PartitionedTableProvider here — the optimizer should be silent and
    // the standard ExchangeExec stage boundaries should remain in place.
    let ctx = ctx();
    let schema = ab_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
        ],
    )
    .unwrap();
    let provider: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(schema, vec![vec![batch.clone()], vec![batch]]).unwrap(),
    );
    ctx.register_table("a", provider.clone())?;
    ctx.register_table("b", provider)?;

    let plan = ctx
        .sql("select a.id, a.v, b.v from a inner join b on a.id = b.id")
        .await?
        .create_physical_plan()
        .await?;
    let planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "no_colo".to_string())?;

    assert_plan!(planner.current_plan(), @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending, stage_resolved=false
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(id@0, id@0)], projection=[id@0, v@1, v@3]
        ExchangeExec: partitioning=Hash([id@0], 4), plan_id=0, stage_id=pending, stage_resolved=false
          DataSourceExec: partitions=2, partition_sizes=[1, 1]
        ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=pending, stage_resolved=false
          DataSourceExec: partitions=2, partition_sizes=[1, 1]
    ");
    Ok(())
}
