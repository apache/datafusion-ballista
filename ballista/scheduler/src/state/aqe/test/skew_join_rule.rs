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

//! Functional tests for [`OptimizeSkewedJoinRule`]: drive a hash- or
//! sort-merge join through `AdaptivePlanner`, finalize the upstream stages
//! with synthetic per-partition byte stats (one fat partition), then snapshot
//! the displayed plan tree so the rule's effect on the two leaf
//! `ExchangeExec`s is visible at the `skew_join=K' of M` field.
//!
//! Tests trace from inputs — small synthetic byte sizes are paired with
//! correspondingly small `skew_join` thresholds so the bin-pack outcome is
//! hand-traceable through `split_size_list_by_target_size`.

use crate::assert_plan;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{mock_batch, mock_schema};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::scheduler::{
    ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    PartitionId, PartitionLocation, PartitionStats,
};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

/// Build a session context with the Ballista extension and the skew-join
/// knobs forced to test-scale values.
///
/// Thresholds are tiny so we can drive the rule with hand-sized byte counts:
///   - `factor = 5.0` (Spark default)
///   - `threshold_bytes = 100` (vs 256 MiB in prod) — anything >100 may skew
///   - `advisory_bytes = 50` (vs 64 MiB in prod) — sub-shard target ≈ 50
///   - `small_factor = 0.2` (Spark default)
///
/// Also disables `optimizer.enable_join_dynamic_filter_pushdown` — the rule
/// enforces mutual exclusion with that option (see optimize_skewed_join.rs
/// for the rationale). Tests that want the rule to fire must keep it off.
fn skew_join_context(target_partitions: usize, enabled: bool) -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(target_partitions)
        .with_round_robin_repartition(false)
        .with_ballista_skew_join_enabled(enabled)
        .with_ballista_skew_join_skewed_partition_factor(5.0)
        .with_ballista_skew_join_skewed_partition_threshold_bytes(100)
        .with_ballista_skew_join_advisory_partition_bytes(50)
        .with_ballista_skew_join_small_partition_factor(0.2)
        .set_bool("datafusion.optimizer.enable_join_dynamic_filter_pushdown", false);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    SessionContext::new_with_state(state)
}

/// Register a MemTable with `n_partitions` partitions. Multi-partition
/// sources force `EnforceDistribution` to insert a hash repartition before
/// the join, which is what gives us `ExchangeExec` leaves to attach to.
fn register_partitioned_table(
    ctx: &SessionContext,
    name: &str,
    n_partitions: usize,
) -> datafusion::error::Result<()> {
    let data = (0..n_partitions)
        .map(|_| Ok(vec![mock_batch()?]))
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    let table = MemTable::try_new(mock_schema(), data)?;
    ctx.register_table(name, Arc::new(table))?;
    Ok(())
}

/// Build a `Vec<Vec<PartitionLocation>>` of length `per_mapper_bytes.len()`.
/// Each upstream partition `i` gets one mapper-output row of size
/// `per_mapper_bytes[i]`.
fn passthrough_partitions(per_mapper_bytes: &[u64]) -> Vec<Vec<PartitionLocation>> {
    per_mapper_bytes
        .iter()
        .enumerate()
        .map(|(idx, &bytes)| vec![synthetic_loc(idx, 0, bytes)])
        .collect()
}

/// Build a `Vec<Vec<PartitionLocation>>` where upstream `skewed_idx` has
/// `n_mappers_at_skew` mapper outputs (so the bin-packer can carve the
/// skewed upstream into sub-ranges), and every other upstream has one
/// mapper-output row at `non_skew_bytes`.
///
/// The skewed upstream's per-mapper bytes are uniform at
/// `skewed_per_mapper_bytes`, so the sub-shard count is deterministic:
/// `ceil(total / advisory_bytes)` give-or-take the tail-merge rule.
fn partitions_with_one_skewed(
    m: usize,
    skewed_idx: usize,
    n_mappers_at_skew: usize,
    skewed_per_mapper_bytes: u64,
    non_skew_bytes: u64,
) -> Vec<Vec<PartitionLocation>> {
    (0..m)
        .map(|i| {
            if i == skewed_idx {
                (0..n_mappers_at_skew)
                    .map(|m_idx| synthetic_loc(i, m_idx, skewed_per_mapper_bytes))
                    .collect()
            } else {
                vec![synthetic_loc(i, 0, non_skew_bytes)]
            }
        })
        .collect()
}

fn synthetic_loc(
    upstream_idx: usize,
    map_partition_id: usize,
    bytes: u64,
) -> PartitionLocation {
    PartitionLocation {
        map_partition_id,
        partition_id: PartitionId {
            job_id: "".to_string(),
            stage_id: 0,
            partition_id: upstream_idx,
        },
        executor_meta: ExecutorMetadata {
            id: "".to_string(),
            host: "".to_string(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification::default().with_task_slots(0),
            os_info: ExecutorOperatingSystemSpecification::default(),
        },
        partition_stats: PartitionStats::new(Some(1), None, Some(bytes)),
        file_id: None,
        is_sort_shuffle: false,
    }
}

/// Happy path: one side has a fat upstream partition. The other partitions
/// (and all of the other side's partitions) are well below threshold, so the
/// only skew is on left upstream 0.
///
/// Trace: M=4. Right (uniform `[10; 4]`, median=10) — none qualifies as
/// skewed (factor*median=50, threshold=100, none over 100). Left
/// `[600, 10, 10, 10]`, median=10 — only upstream 0 qualifies (600 > 50 AND
/// 600 > 100). Skewed upstream has 4 mappers @ 150 bytes each (total 600);
/// bin-pack at advisory=50: every mapper overshoots and flushes → 4 ranges
/// `[(0,1),(1,2),(2,3),(3,4)]`. Right upstream 0 stays passthrough `(0,1)`.
/// Per-upstream cartesian: left u0 = 4×1 = 4 pairs, u1/u2/u3 = 1×1 = 1 each.
/// K' = 4 + 3 = 7. Since K' > M (7 > 4), the rule attaches.
///
/// Both leaves get a `SkewJoinPlan { upstream_partition_count: 4, shards: 7 }`.
#[tokio::test]
async fn should_attach_skew_join_when_left_upstream_is_skewed()
-> datafusion::error::Result<()> {
    let ctx = skew_join_context(4, true);
    register_partitioned_table(&ctx, "t1", 4)?;
    register_partitioned_table(&ctx, "t2", 4)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(2, stages.len());

    // Left (stage 0): upstream 0 is fat — 4 mappers @ 150 bytes (= 600
    // total) while the other 3 upstreams are at 10 bytes each. Right: uniform 10.
    planner
        .finalise_stage_internal(0, partitions_with_one_skewed(4, 0, 4, 150, 10))?;
    planner.finalise_stage_internal(1, passthrough_partitions(&[10; 4]))?;

    // Surface the join stage so OptimizeSkewedJoinRule fires and attaches.
    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ "
    AdaptiveDatafusionExec: is_final=true, plan_id=2, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 4), plan_id=0, stage_id=0, stage_resolved=true, skew_join=7 of 4
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 4), plan_id=1, stage_id=1, stage_resolved=true, skew_join=7 of 4
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}

/// Disabled path: even with one fat partition, the rule short-circuits
/// at the first statement of `optimize()` and the plan flows through
/// untouched. Neither leaf gets `skew_join=`.
#[tokio::test]
async fn should_skip_skew_join_when_rule_disabled() -> datafusion::error::Result<()> {
    let ctx = skew_join_context(4, false);
    register_partitioned_table(&ctx, "t1", 4)?;
    register_partitioned_table(&ctx, "t2", 4)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let _ = planner.runnable_stages()?.unwrap();
    planner
        .finalise_stage_internal(0, partitions_with_one_skewed(4, 0, 4, 150, 10))?;
    planner.finalise_stage_internal(1, passthrough_partitions(&[10; 4]))?;

    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ "
    AdaptiveDatafusionExec: is_final=true, plan_id=2, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 4), plan_id=0, stage_id=0, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 4), plan_id=1, stage_id=1, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}

/// Below-threshold path: ratio meets `factor` but absolute bytes don't meet
/// the threshold (the dual guard). Rule bails — no skew_join attached.
///
/// Left `[80, 5, 5, 5]`: median=5, ratio=16× (well over 5.0) BUT
/// 80 < threshold=100. is_skewed returns false. Right uniform 5 → no skew.
/// Bail with "no upstream qualifies as skewed".
#[tokio::test]
async fn should_skip_skew_join_when_below_threshold() -> datafusion::error::Result<()> {
    let ctx = skew_join_context(4, true);
    register_partitioned_table(&ctx, "t1", 4)?;
    register_partitioned_table(&ctx, "t2", 4)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let _ = planner.runnable_stages()?.unwrap();
    planner.finalise_stage_internal(0, passthrough_partitions(&[80, 5, 5, 5]))?;
    planner.finalise_stage_internal(1, passthrough_partitions(&[5; 4]))?;

    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ "
    AdaptiveDatafusionExec: is_final=true, plan_id=2, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 4), plan_id=0, stage_id=0, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 4), plan_id=1, stage_id=1, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}

/// Uniform-bytes path: no upstream qualifies because all are at the same
/// size, so the ratio check (factor × median = same as each value) trivially
/// fails. Bail.
#[tokio::test]
async fn should_skip_skew_join_when_bytes_are_uniform() -> datafusion::error::Result<()>
{
    let ctx = skew_join_context(4, true);
    register_partitioned_table(&ctx, "t1", 4)?;
    register_partitioned_table(&ctx, "t2", 4)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let _ = planner.runnable_stages()?.unwrap();
    // Each upstream is well above threshold=100, but every value is identical
    // so median = each value, ratio is 1×, fails factor=5.
    planner.finalise_stage_internal(0, passthrough_partitions(&[500; 4]))?;
    planner.finalise_stage_internal(1, passthrough_partitions(&[500; 4]))?;

    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ "
    AdaptiveDatafusionExec: is_final=true, plan_id=2, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 4), plan_id=0, stage_id=0, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 4), plan_id=1, stage_id=1, stage_resolved=true
              DataSourceExec: partitions=4, partition_sizes=[1, 1, 1, 1]
    ");

    Ok(())
}

/// Sort-merge → hash join: same rule on the other join shape. Force hash
/// join (`prefer_hash_join=true`) and check the rule still fires on the leaf
/// Exchanges below the HashJoinExec.
#[tokio::test]
async fn should_attach_skew_join_on_hash_join() -> datafusion::error::Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_round_robin_repartition(false)
        .with_ballista_skew_join_enabled(true)
        .with_ballista_skew_join_skewed_partition_factor(5.0)
        .with_ballista_skew_join_skewed_partition_threshold_bytes(100)
        .with_ballista_skew_join_advisory_partition_bytes(50)
        .with_ballista_skew_join_small_partition_factor(0.2)
        .set_bool("datafusion.optimizer.prefer_hash_join", true)
        .set_bool("datafusion.optimizer.enable_join_dynamic_filter_pushdown", false);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    register_partitioned_table(&ctx, "t1", 4)?;
    register_partitioned_table(&ctx, "t2", 4)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let _ = planner.runnable_stages()?.unwrap();
    planner
        .finalise_stage_internal(0, partitions_with_one_skewed(4, 0, 4, 150, 10))?;
    planner.finalise_stage_internal(1, passthrough_partitions(&[10; 4]))?;

    let _ = planner.runnable_stages()?;

    // Both leaves get skew_join=7 of 4 — same K' as the SMJ case because
    // the upstream byte distribution and the rule's bin-packing are
    // identical. Join op above just differs.
    let display =
        datafusion::physical_plan::displayable(planner.current_plan()).indent(true);
    let rendered = display.to_string();
    assert!(
        rendered.contains("HashJoinExec: mode=Partitioned"),
        "expected HashJoinExec(Partitioned) in plan, got:\n{rendered}"
    );
    assert_eq!(
        rendered.matches("skew_join=7 of 4").count(),
        2,
        "both leaf ExchangeExecs should carry skew_join=7 of 4, got:\n{rendered}"
    );

    Ok(())
}

/// End-to-end: after the rule attaches `skew_join=K' of M` to both leaves,
/// the adapter must build both downstream `ShuffleReaderExec`s with K'
/// partitions. The next runnable stage's plan tree is the proof.
#[tokio::test]
async fn shuffle_readers_use_skew_join_kprime_when_rule_fires()
-> datafusion::error::Result<()> {
    let ctx = skew_join_context(4, true);
    register_partitioned_table(&ctx, "t1", 4)?;
    register_partitioned_table(&ctx, "t2", 4)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(2, stages.len());

    planner
        .finalise_stage_internal(0, partitions_with_one_skewed(4, 0, 4, 150, 10))?;
    planner.finalise_stage_internal(1, passthrough_partitions(&[10; 4]))?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());

    // The join stage's ShuffleReaderExecs now both expose K'=7 partitions,
    // declared as UnknownPartitioning — the skew rewrite is honest about
    // having broken the per-partition hash invariant. (Inside the stage,
    // each task still receives properly-paired (left-shard, right-shard)
    // input bundles, so the join above the reader still works.) The
    // skew-join carrier's decision has flowed through the adapter into
    // the runnable plan.
    let rendered =
        datafusion::physical_plan::displayable(stages[0].plan.as_ref())
            .indent(true)
            .to_string();
    assert_eq!(
        rendered.matches("ShuffleReaderExec: partitioning: UnknownPartitioning(7)").count(),
        2,
        "both readers should expose K'=7 UnknownPartitioning, got:\n{rendered}"
    );
    assert_eq!(
        rendered.matches("skew_join: 7 of 4").count(),
        2,
        "both readers should report the skew_join=7 of 4 source, got:\n{rendered}"
    );

    Ok(())
}

/// Mutual exclusion with `optimizer.enable_join_dynamic_filter_pushdown`:
/// even when skew_join.enabled=true AND the upstream byte distribution
/// would otherwise trigger a rewrite, the rule bails because DataFusion
/// 53.1's per-partition dynamic-filter routing is incompatible with the
/// split shards (see optimize_skewed_join.rs for the rationale). Neither
/// leaf gets a skew_join attached.
#[tokio::test]
async fn should_bail_when_dynamic_filter_pushdown_enabled()
-> datafusion::error::Result<()> {
    // Same setup as the happy-path test, but with the DF dynamic filter
    // option flipped back on. Skew distribution alone would otherwise
    // trigger the rule.
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_round_robin_repartition(false)
        .with_ballista_skew_join_enabled(true)
        .with_ballista_skew_join_skewed_partition_factor(5.0)
        .with_ballista_skew_join_skewed_partition_threshold_bytes(100)
        .with_ballista_skew_join_advisory_partition_bytes(50)
        .with_ballista_skew_join_small_partition_factor(0.2)
        .set_bool("datafusion.optimizer.enable_join_dynamic_filter_pushdown", true);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    register_partitioned_table(&ctx, "t1", 4)?;
    register_partitioned_table(&ctx, "t2", 4)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let _ = planner.runnable_stages()?.unwrap();
    planner
        .finalise_stage_internal(0, partitions_with_one_skewed(4, 0, 4, 150, 10))?;
    planner.finalise_stage_internal(1, passthrough_partitions(&[10; 4]))?;

    let _ = planner.runnable_stages()?;

    // Neither leaf carries skew_join=… — the mutual-exclusion guard fired.
    let rendered =
        datafusion::physical_plan::displayable(planner.current_plan())
            .indent(true)
            .to_string();
    assert_eq!(
        rendered.matches("skew_join=").count(),
        0,
        "rule must not attach skew_join when dynamic filter pushdown is on, got:\n{rendered}"
    );

    Ok(())
}

