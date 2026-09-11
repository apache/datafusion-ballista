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

//! Staging a join's build side, across the two passes it takes to pay off.
//!
//! The unit tests in `dynamic_join` pin the *decision* in isolation: given
//! these statistics, does the resolver choose to stage? What they cannot show
//! is the mechanism the feature exists for, which only appears across two
//! passes — shuffle the build side alone, read back what that shuffle actually
//! measured, then decide the join for real against a number instead of a guess.
//! These tests drive `AdaptivePlanner` through both passes and assert on the
//! plan that comes out of each.
//!
//! Sizes are declared ([`StatsTable`]) rather than materialised, because the
//! shape staging turns on is a build side whose size is an *estimate* sitting
//! over the broadcast budget. No table a test is willing to build produces that.

use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::stats_table::{
    StatsTable, estimated_statistics, sized_statistics,
};
use crate::state::aqe::test::{
    mock_partitions_with_size, mock_partitions_with_statistics,
};
use ballista_core::assert_plan;
use ballista_core::extension::SessionConfigExt;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Statistics;
use datafusion::execution::{
    SessionStateBuilder, config::SessionConfig, context::SessionContext,
};
use std::sync::Arc;

const MB: usize = 1024 * 1024;

/// A build side whose size is only a guess, sitting over the shipped 128 MB
/// broadcast budget but well inside the 32x window past which no measurement
/// could bring it back under. This is the TPC-H q8 `part` shape: a filtered
/// dimension scan the planner sizes with `default_filter_selectivity`.
const GUESSED_BUILD_BYTES: usize = 200 * MB;

/// A fact-table probe side, far past the 10x the ratio requires.
const FACT_PROBE_BYTES: usize = 40 * 1024 * MB;

/// A measured build size that still cannot be broadcast, for the fallthrough.
const MEASURED_TOO_LARGE_BYTES: u64 = (200 * MB) as u64;

fn join_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]))
}

/// A context carrying Ballista's shipped configuration, so these tests run
/// against the thresholds a deployment uses — including
/// `ballista.optimizer.stage_build_side`, which defaults to on.
fn ballista_ctx() -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_round_robin_repartition(false);
    let state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .build();
    SessionContext::new_with_state(state)
}

fn register(ctx: &SessionContext, name: &str, stats: Statistics) {
    ctx.register_table(name, Arc::new(StatsTable::new(join_schema(), stats, 4)))
        .unwrap();
}

/// The q8 shape: a guessed-large dimension side against a fact table that
/// dwarfs it, joined on the key.
fn q8_shaped_ctx() -> SessionContext {
    let ctx = ballista_ctx();
    let schema = join_schema();
    register(
        &ctx,
        "dim",
        estimated_statistics(&schema, 1_000_000, GUESSED_BUILD_BYTES),
    );
    register(
        &ctx,
        "fact",
        sized_statistics(&schema, 1_000_000_000, FACT_PROBE_BYTES),
    );
    ctx
}

async fn planner_for(ctx: &SessionContext, sql: &str) -> AdaptivePlanner {
    let lp = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
    AdaptivePlanner::try_new(ctx, &lp, "test_job".into())
        .await
        .unwrap()
}

const Q8_SHAPED_JOIN: &str = "SELECT dim.val FROM dim JOIN fact ON dim.id = fact.id";

/// The mechanism end to end: the first pass shuffles only the build side and
/// leaves the fact table alone, and once that one cheap stage reports what it
/// measured, the join resolves to a broadcast `CollectLeft` — so the fact table
/// is never shuffled at all.
#[tokio::test]
async fn stages_the_build_side_then_broadcasts_the_measured_result() {
    let ctx = q8_shaped_ctx();
    let mut planner = planner_for(&ctx, Q8_SHAPED_JOIN).await;

    // Pass one: an exchange on the build side only. The fact table is still a
    // bare scan, so no stage is created for it.
    assert_plan!(planner.current_plan(), @ "
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[val@1 as val]
        DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=false
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=pending, stage_resolved=false
            CooperativeExec
              StatsExec: partitions=4, rows=Inexact(1000000), bytes=Inexact(209715200)
          CooperativeExec
            StatsExec: partitions=4, rows=Exact(1000000000), bytes=Exact(42949672960)
    ");

    let (stages, cancellable) = planner.actionable_stages().unwrap();
    let stages = stages.unwrap();
    assert_eq!(1, stages.len(), "only the build side should be staged");
    assert_eq!(0, cancellable.len());

    // And that one stage is the build side alone.
    assert_plan!(stages.first().unwrap().plan.as_ref(), @ "
    SortShuffleWriterExec: partitioning=Hash([id@0], 4)
      CooperativeExec
        StatsExec: partitions=4, rows=Inexact(1000000), bytes=Inexact(209715200)
    ");

    // The staged shuffle reports 10 bytes, three orders of magnitude under the
    // estimate that kept the join partitioned.
    planner
        .finalise_stage_internal(0, mock_partitions_with_statistics())
        .unwrap();

    // Pass two: decided against the measurement, not the guess.
    assert_plan!(planner.current_plan(), @ "
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending, stage_resolved=false
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(id@0, id@0)], projection=[val@1]
        ExchangeExec: partitioning=None, plan_id=3, stage_id=0, stage_resolved=true, broadcast=true
          CooperativeExec
            StatsExec: partitions=4, rows=Inexact(1000000), bytes=Inexact(209715200)
        CooperativeExec
          StatsExec: partitions=4, rows=Exact(1000000000), bytes=Exact(42949672960)
    ");
}

/// The other half of the decision: when the measurement confirms the build side
/// really is too large, the join falls back to a partitioned one — and the
/// staged exchange is *reused* rather than wrapped, so each side ends up behind
/// exactly one exchange instead of a nested pair.
#[tokio::test]
async fn reuses_the_staged_exchange_when_the_build_side_measures_too_large() {
    let ctx = q8_shaped_ctx();
    let mut planner = planner_for(&ctx, Q8_SHAPED_JOIN).await;

    let (stages, _) = planner.actionable_stages().unwrap();
    assert_eq!(1, stages.unwrap().len());

    // This time the shuffle confirms the build side is over the budget.
    planner
        .finalise_stage_internal(
            0,
            mock_partitions_with_size(1_000_000, MEASURED_TOO_LARGE_BYTES),
        )
        .unwrap();

    // One exchange per side. The build side's is the staged one, already on the
    // join key and already resolved; nothing is nested inside anything.
    assert_plan!(planner.current_plan(), @ "
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[val@1 as val]
        DynamicJoinSelectionExec: plan_id=0, join_type=Inner, on=[(id@0, id@0)] repartitioned=true
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=0, stage_resolved=true
            CooperativeExec
              StatsExec: partitions=4, rows=Inexact(1000000), bytes=Inexact(209715200)
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=3, stage_id=pending, stage_resolved=false
            CooperativeExec
              StatsExec: partitions=4, rows=Exact(1000000000), bytes=Exact(42949672960)
    ");
}

/// A `Left` join can never be lowered to `CollectLeft` — the build side would
/// emit its unmatched rows once per probe task (#1055) — so measuring it cannot
/// change the decision, and staging would buy a serialised stage boundary for
/// nothing. Both sides must be shuffled on the first pass.
///
/// Worth pinning separately: `test_left_join_not_collected_left` covers the
/// broadcast half of this, but passes today only because its statistics do not
/// match the staging shape. These ones do.
#[tokio::test]
async fn does_not_stage_a_left_join() {
    let ctx = q8_shaped_ctx();
    let planner = planner_for(
        &ctx,
        "SELECT dim.val FROM dim LEFT JOIN fact ON dim.id = fact.id",
    )
    .await;

    assert_plan!(planner.current_plan(), @ "
    AdaptiveDatafusionExec: is_final=false, plan_id=3, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[val@1 as val]
        DynamicJoinSelectionExec: plan_id=0, join_type=Left, on=[(id@0, id@0)] repartitioned=true
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=1, stage_id=pending, stage_resolved=false
            CooperativeExec
              StatsExec: partitions=4, rows=Inexact(1000000), bytes=Inexact(209715200)
          ExchangeExec: partitioning=Hash([id@0], 4), plan_id=2, stage_id=pending, stage_resolved=false
            CooperativeExec
              StatsExec: partitions=4, rows=Exact(1000000000), bytes=Exact(42949672960)
    ");
}
