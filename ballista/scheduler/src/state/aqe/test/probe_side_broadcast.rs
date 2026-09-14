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

//! A broadcast build side that measures larger than the probe side, so a
//! replan swaps it onto the probe side (the TPC-H q8 SF1000 stage 5 shape).

use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::stats_table::{estimated_statistics, sized_statistics};
use crate::state::aqe::test::{
    MB, ballista_ctx, mock_partitions_with_size, narrow_schema, register_stats_table,
};
use ballista_core::assert_plan;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Shuffle output of `partitions` partitions, each reporting `rows` and `bytes`.
fn locations(partitions: usize, rows: u64, bytes: u64) -> Vec<Vec<PartitionLocation>> {
    let template = mock_partitions_with_size(rows, bytes)
        .into_iter()
        .flatten()
        .next()
        .expect("a mock location");
    (0..partitions)
        .map(|i| {
            let mut location = template.clone();
            location.map_partition_id = i;
            location.partition_id.partition_id = i;
            vec![location]
        })
        .collect()
}

async fn planner_for(sql: &str) -> AdaptivePlanner {
    planner_over(sql, narrow_schema()).await
}

/// A planner for `sql` over `guessed_small`, estimated small enough to
/// broadcast, and `mid`, a larger table of known size.
async fn planner_over(sql: &str, schema: Arc<Schema>) -> AdaptivePlanner {
    let ctx = ballista_ctx();
    register_stats_table(
        &ctx,
        "guessed_small",
        schema.clone(),
        estimated_statistics(&schema, 100_000, 10 * MB),
    );
    register_stats_table(
        &ctx,
        "mid",
        schema.clone(),
        sized_statistics(&schema, 10_000_000, 500 * MB),
    );
    let lp = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
    AdaptivePlanner::try_new(&ctx, &lp, "test_job".into())
        .await
        .unwrap()
}

#[tokio::test]
async fn swapped_broadcast_is_read_partitioned_on_the_probe_side() {
    let mut planner = planner_for(
        "SELECT guessed_small.val FROM guessed_small JOIN mid ON guessed_small.id = mid.id",
    )
    .await;

    assert_plan!(planner.current_plan(), @ "
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending, stage_resolved=false
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)], projection=[val@1]
        ExchangeExec: partitioning=None, plan_id=1, stage_id=pending, stage_resolved=false, broadcast=true
          CooperativeExec
            StatsExec: partitions=4, rows=Inexact(100000), bytes=Inexact(10485760)
        CooperativeExec
          StatsExec: partitions=4, rows=Exact(10000000), bytes=Exact(524288000)
    ");
    planner.actionable_stages().unwrap();

    // The broadcast side measures 8 GB, so `join_selection` swaps it onto the
    // probe side, where it must no longer be read as a single partition.
    planner
        .finalise_stage_internal(0, locations(4, 50_000_000, 2_000 * MB as u64))
        .unwrap();
    planner.actionable_stages().unwrap();
    planner
        .finalise_stage_internal(1, locations(4, 10_000_000, 500 * MB as u64))
        .unwrap();

    let (stages, _) = planner.actionable_stages().unwrap();
    let stages = stages.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages[0].plan.as_ref(), @ "
    ShuffleWriterExec: partitioning: UnknownPartitioning(4)
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)], projection=[val@2]
        CoalescePartitionsExec
          ShuffleReaderExec: upstream_stage: 1, partitioning: UnknownPartitioning(4)
        ShuffleReaderExec: upstream_stage: 0, partitioning: UnknownPartitioning(4)
    ");
}

/// A broadcast stage stores one entry per partition, in partition order, so the
/// partitioned probe-side read gets the locations its tasks actually reported.
#[tokio::test]
async fn swapped_broadcast_reads_the_locations_its_tasks_reported() {
    let mut planner = planner_for(
        "SELECT guessed_small.val FROM guessed_small JOIN mid ON guessed_small.id = mid.id",
    )
    .await;
    planner.actionable_stages().unwrap();

    // Tasks report as they finish, so the stored order must come from the
    // partition ids rather than from arrival.
    let reported = locations(4, 1_000, 1_000_000).into_iter().rev().flatten();
    planner
        .update_exchange_locations(0, reported.collect())
        .unwrap();

    let stored = planner.take_stage_output_partitions(0).unwrap();
    let ids: Vec<Vec<usize>> = stored
        .iter()
        .map(|locs| locs.iter().map(|l| l.partition_id.partition_id).collect())
        .collect();
    assert_eq!(ids, vec![vec![0], vec![1], vec![2], vec![3]]);
}

/// A null-aware `NOT IN` join swaps to `RightAnti` like any other; its probe side
/// is still coalesced to a single task.
#[tokio::test]
async fn swapped_null_aware_broadcast_is_coalesced_on_the_probe_side() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("val", DataType::Int32, true),
    ]));
    let mut planner = planner_over(
        "SELECT val FROM guessed_small WHERE id NOT IN (SELECT id FROM mid)",
        schema,
    )
    .await;

    assert_plan!(planner.current_plan(), @ "
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(id@0, id@0)], projection=[val@1], null_aware
        ExchangeExec: partitioning=None, plan_id=1, stage_id=pending, stage_resolved=false, broadcast=true
          CooperativeExec
            StatsExec: partitions=4, rows=Inexact(100000), bytes=Inexact(10485760)
        CoalescePartitionsExec
          ExchangeExec: partitioning=None, plan_id=2, stage_id=pending, stage_resolved=false
            CooperativeExec
              StatsExec: partitions=4, rows=Exact(10000000), bytes=Exact(524288000)
    ");
    planner.actionable_stages().unwrap();

    planner
        .finalise_stage_internal(0, locations(4, 50_000_000, 2_000 * MB as u64))
        .unwrap();
    planner.actionable_stages().unwrap();
    planner
        .finalise_stage_internal(1, locations(4, 10_000_000, 500 * MB as u64))
        .unwrap();

    let (stages, _) = planner.actionable_stages().unwrap();
    let stages = stages.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages[0].plan.as_ref(), @ "
    ShuffleWriterExec: partitioning: UnknownPartitioning(1)
      HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(id@0, id@0)], projection=[val@1], null_aware
        CoalescePartitionsExec
          ShuffleReaderExec: upstream_stage: 1, partitioning: UnknownPartitioning(4)
        CoalescePartitionsExec
          ShuffleReaderExec: upstream_stage: 0, partitioning: UnknownPartitioning(4)
    ");
}
