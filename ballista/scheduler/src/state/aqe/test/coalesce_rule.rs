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

//! Functional tests for [`CoalescePartitionsRule`]: drive a query through
//! `AdaptivePlanner`, finalize the upstream stage with synthetic per-partition
//! byte stats, and snapshot the displayed plan tree so the rule's effect on
//! the leaf `ExchangeExec` is visible at the `coalesce=K of M` field.
//!
//! Each test uses small synthetic byte sizes paired with a small
//! `coalesce_target_partition_bytes` so the bin-pack outcome is hand-traceable
//! against `split_size_list_by_target_size`.

use crate::assert_plan;
use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::optimizer_rule::CoalescePartitionsRule;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{mock_batch, mock_schema, partitions_with_byte_sizes};
use ballista_core::execution_plans::{CoalescePlan, PartitionGroup};
use ballista_core::extension::SessionConfigExt;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

/// Build a session context with the Ballista config extension installed and
/// the coalesce-relevant knobs forced to specific values. The rule packs
/// directly toward `target_partition_bytes` (here 200 for test scale), so
/// every scenario below traces from inputs alone.
fn coalesce_context(target_partitions: usize, enabled: bool) -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(target_partitions)
        .with_round_robin_repartition(false)
        .with_ballista_coalesce_enabled(enabled)
        .with_ballista_coalesce_target_partition_bytes(200)
        .set_bool("datafusion.optimizer.prefer_hash_join", false)
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            0,
        )
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            0,
        );

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    SessionContext::new_with_state(state)
}

/// Register a MemTable with `n_partitions` partitions, each holding one copy
/// of `mock_batch`. Multi-partition sources force DataFusion's
/// `EnforceDistribution` to insert a hash repartition before partitioned
/// joins — without that, two 1-partition inputs satisfy `Partitioned` on a
/// single partition and no `ExchangeExec` shows up in the final plan.
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

/// Happy path: M=8 upstream partitions @ 50 bytes each, target=200.
/// Bin-pack trace (small_factor=0.2 → 40, merged_factor=1.2 → 240):
///   i=0..3 accumulate into bucket=200; i=4 overshoots, flush, start new;
///   i=5..7 accumulate into bucket=200; post-loop merge is rejected
///   (200 + 200 = 400, not below 240). Result: K=2.
/// Plan tree therefore shows `coalesce=2 of 8` on the leaf Exchange after
/// stage 0 finalizes.
#[tokio::test]
async fn should_attach_coalesce_when_partitions_pack_below_m()
-> datafusion::error::Result<()> {
    let ctx = coalesce_context(8, true);
    ctx.register_batch("t", mock_batch()?)?;

    let plan = ctx
        .sql("select min(a) as c0, c as c2 from t group by c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_from_plan(ctx.state().config(), plan, "test_job".into())?;

    // Before any stage finalizes the leaves are unresolved, so the rule
    // no-ops: `coalesce=none`.
    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[min(t.a)@1 as c0, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a)]
          ExchangeExec: partitioning=Hash([c@0], 8), plan_id=0, stage_id=pending, stage_resolved=false
            AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    // Surface the runnable stage so its id is registered before we finalize.
    let _ = planner.runnable_stages()?.unwrap();

    // Finalize stage 0 with 8 partitions of 50 bytes each (total = 400, target = 200).
    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[50; 8]))?;

    // Surface the next runnable stage — this is where `CoalescePartitionsRule`
    // fires per-stage on the downstream consumer and attaches the
    // `CoalescePlan` to plan_id=0.
    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=true, plan_id=1, stage_id=1, stage_resolved=false
      ProjectionExec: expr=[min(t.a)@1 as c0, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a)]
          ExchangeExec: partitioning=Hash([c@0], 8), plan_id=0, stage_id=0, stage_resolved=true, coalesce=2 of 8
            AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

/// Disabled path: same inputs as above but `ballista.planner.coalesce.enabled=false`.
/// The rule short-circuits at the first statement of `optimize()` and returns
/// the plan untouched, so the leaf Exchange's coalesce slot stays None.
#[tokio::test]
async fn should_skip_coalesce_when_rule_disabled() -> datafusion::error::Result<()> {
    let ctx = coalesce_context(8, false);
    ctx.register_batch("t", mock_batch()?)?;

    let plan = ctx
        .sql("select min(a) as c0, c as c2 from t group by c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_from_plan(ctx.state().config(), plan, "test_job".into())?;

    let _ = planner.runnable_stages()?.unwrap();
    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[50; 8]))?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[min(t.a)@1 as c0, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a)]
          ExchangeExec: partitioning=Hash([c@0], 8), plan_id=0, stage_id=0, stage_resolved=true
            AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

/// Degenerate K=M path: every partition is already at target. Bin-pack
/// flushes after each one (adding the next would exceed target=200), and
/// the post-flush merge is rejected (each bucket = 300, neither small nor
/// combinable). K = M = 8 → the rule treats it as no work and returns the
/// plan as-is.
#[tokio::test]
async fn should_skip_coalesce_when_partitions_are_full() -> datafusion::error::Result<()>
{
    let ctx = coalesce_context(8, true);
    ctx.register_batch("t", mock_batch()?)?;

    let plan = ctx
        .sql("select min(a) as c0, c as c2 from t group by c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_from_plan(ctx.state().config(), plan, "test_job".into())?;

    let _ = planner.runnable_stages()?.unwrap();
    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[300; 8]))?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending, stage_resolved=false
      ProjectionExec: expr=[min(t.a)@1 as c0, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a)]
          ExchangeExec: partitioning=Hash([c@0], 8), plan_id=0, stage_id=0, stage_resolved=true
            AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

/// Partitioned hash join: both sides shuffled by the join key, both leaf
/// Exchanges live in the same final-stage subtree. Per-leaf bytes
/// `[25; 8]` × 2 leaves → summed `[50; 8]` → bin-pack at target=200
/// collapses to K=2 (same trace as the happy-path test). Both leaves get
/// the SAME `CoalescePlan` so the join's partition-count requirement holds
/// across the rewrite.
#[tokio::test]
async fn should_attach_coalesce_to_both_sides_of_hash_join()
-> datafusion::error::Result<()> {
    let ctx = coalesce_context(8, true);
    register_partitioned_table(&ctx, "t1", 8)?;
    register_partitioned_table(&ctx, "t2", 8)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_from_plan(ctx.state().config(), plan, "test_job".into())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(2, stages.len());

    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[25; 8]))?;
    planner.finalise_stage_internal(1, partitions_with_byte_sizes(&[25; 8]))?;

    // Surface the join stage so `CoalescePartitionsRule` fires per-stage and
    // attaches the shared `CoalescePlan` to both leaf Exchanges.
    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=true, plan_id=2, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 8), plan_id=0, stage_id=0, stage_resolved=true, coalesce=2 of 8
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 8), plan_id=1, stage_id=1, stage_resolved=true, coalesce=2 of 8
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
    ");

    Ok(())
}

/// Two hash joins in one final stage: 3 upstream Exchanges feed the join
/// chain (t1 ⋈ t2 ⋈ t3 on a shared key). Per-leaf bytes `[16; 8]` × 3
/// leaves → summed `[48; 8]`. Bin-pack at target=200: 4 partitions fill
/// bucket to 192, 5th overshoots (240 > 200) and flushes; next bucket fills
/// remaining 4 to 192; post-loop merge rejected. K=2.
/// All three leaves get the same `CoalescePlan`.
#[tokio::test]
async fn should_attach_coalesce_to_all_three_legs_of_two_hash_joins()
-> datafusion::error::Result<()> {
    let ctx = coalesce_context(8, true);
    register_partitioned_table(&ctx, "t1", 8)?;
    register_partitioned_table(&ctx, "t2", 8)?;
    register_partitioned_table(&ctx, "t3", 8)?;

    let plan = ctx
        .sql(
            "select t1.a, t2.b, t3.c from t1 join t2 on t1.c = t2.c \
             join t3 on t1.c = t3.c",
        )
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_from_plan(ctx.state().config(), plan, "test_job".into())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(3, stages.len());

    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[16; 8]))?;
    planner.finalise_stage_internal(1, partitions_with_byte_sizes(&[16; 8]))?;
    planner.finalise_stage_internal(2, partitions_with_byte_sizes(&[16; 8]))?;

    // Surface the join stage so `CoalescePartitionsRule` fires per-stage and
    // attaches the same `CoalescePlan` to all three leaf Exchanges.
    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=true, plan_id=3, stage_id=3, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b, c@3 as c]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@0)]
          ProjectionExec: expr=[a@0 as a, c@1 as c, b@2 as b]
            SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
              SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
                ExchangeExec: partitioning=Hash([c@1], 8), plan_id=0, stage_id=0, stage_resolved=true, coalesce=2 of 8
                  DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
              SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
                ExchangeExec: partitioning=Hash([c@1], 8), plan_id=1, stage_id=1, stage_resolved=true, coalesce=2 of 8
                  DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
          SortExec: expr=[c@0 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@0], 8), plan_id=2, stage_id=2, stage_resolved=true, coalesce=2 of 8
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
    ");

    Ok(())
}

/// Sort-merge join: same shuffle structure as the hash-join case, but
/// DataFusion picks `SortMergeJoinExec` when `prefer_hash_join=false`. The
/// rule is structural — it walks down to the leaf Exchanges and attaches
/// `CoalescePlan` regardless of the parent join kind. Per-leaf bytes
/// `[25; 8]` × 2 leaves trace identically to the hash-join case → K=2.
#[tokio::test]
async fn should_attach_coalesce_to_both_sides_of_sort_merge_join()
-> datafusion::error::Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(8)
        .with_round_robin_repartition(false)
        .with_ballista_coalesce_enabled(true)
        .with_ballista_coalesce_target_partition_bytes(200)
        .set_bool("datafusion.optimizer.prefer_hash_join", false);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    register_partitioned_table(&ctx, "t1", 8)?;
    register_partitioned_table(&ctx, "t2", 8)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_from_plan(ctx.state().config(), plan, "test_job".into())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(2, stages.len());

    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[25; 8]))?;
    planner.finalise_stage_internal(1, partitions_with_byte_sizes(&[25; 8]))?;

    // Surface the join stage so `CoalescePartitionsRule` fires per-stage.
    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=true, plan_id=2, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 8), plan_id=0, stage_id=0, stage_resolved=true, coalesce=2 of 8
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 8), plan_id=1, stage_id=1, stage_resolved=true, coalesce=2 of 8
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
    ");

    Ok(())
}

/// End-to-end: after the rule attaches `coalesce=K of M` to a leaf Exchange,
/// the adapter must build the downstream `ShuffleReaderExec` with `K`
/// partitions instead of `M`. The next runnable stage's plan tree is the
/// proof — its `ShuffleReaderExec: partitioning: Hash([c@0], 2)` shows the
/// rule's decision has flowed through the adapter into the runnable plan,
/// not just sitting on the Exchange as metadata.
#[tokio::test]
async fn shuffle_reader_uses_coalesced_k_when_rule_fires() -> datafusion::error::Result<()>
{
    let ctx = coalesce_context(8, true);
    ctx.register_batch("t", mock_batch()?)?;

    let plan = ctx
        .sql("select min(a) as c0, max(b) as c1, c as c2 from t group by c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_from_plan(ctx.state().config(), plan, "test_job".into())?;

    // Stage 0 is the upstream shuffle writer, partitioning by `c` into M=8.
    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages[0].plan.as_ref(),  @ r"
    SortShuffleWriterExec: partitioning=Hash([c@0], 8)
      AggregateExec: mode=Partial, gby=[c@2 as c], aggr=[min(t.a), max(t.b)]
        DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    // Finalize stage 0 with 8 partitions × 50 bytes. Bin-pack at target=200
    // yields K=2 (same trace as the happy-path test).
    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[50; 8]))?;

    // Stage 1 is the final stage. Its `ShuffleReaderExec` exposes K=2
    // partitions — the rule's coalesce decision is now baked into the
    // adapter's reader-construction path.
    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages[0].plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: None
      ProjectionExec: expr=[min(t.a)@1 as c0, max(t.b)@2 as c1, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a), max(t.b)]
          ShuffleReaderExec: upstream_stage: 0, partitioning: Hash([c@0], 2), coalesce: 2 of 8
    ");

    Ok(())
}

// ---------------------------------------------------------------------------
// Rule-level tests.
//
// The cases below build the stage subtree directly rather than planning SQL,
// because their subject *is* the shape of the leaf set: a broadcast leaf beside
// shuffle leaves, and leaves that disagree on partition count. Both are awkward
// to coax out of a SQL planner and trivial to state directly.
// ---------------------------------------------------------------------------

/// The configuration the rule reads, at the byte scale the snapshot tests use.
fn rule_config() -> SessionConfig {
    SessionConfig::new_with_ballista()
        .with_ballista_coalesce_enabled(true)
        .with_ballista_coalesce_target_partition_bytes(200)
}

/// A resolved shuffle leaf declaring `m` partitions of `bytes` bytes each.
fn resolved_shuffle_leaf(m: usize, bytes: u64) -> Arc<ExchangeExec> {
    let exchange = ExchangeExec::new(
        Arc::new(EmptyExec::new(mock_schema())),
        Some(Partitioning::UnknownPartitioning(m)),
        0,
    );
    exchange.resolve_shuffle_partitions(partitions_with_byte_sizes(&vec![bytes; m]));
    Arc::new(exchange)
}

/// A resolved broadcast leaf whose upstream wrote `m` partitions. Note the
/// declared partition count is 1 while the resolved shape is `m`: that gap is
/// what the rule used to index off the end of.
fn resolved_broadcast_leaf(m: usize, bytes: u64) -> Arc<ExchangeExec> {
    let exchange = ExchangeExec::new_broadcast(
        Arc::new(EmptyExec::new(mock_schema())),
        None,
        1,
    );
    exchange.resolve_shuffle_partitions(partitions_with_byte_sizes(&vec![bytes; m]));
    Arc::new(exchange)
}

/// Wrap leaves in a final-stage root so the rule sees them as one stage's
/// alignment set. Takes `Arc<ExchangeExec>` so callers can keep a typed handle
/// on each leaf and read its decision back after the rule runs.
fn stage_over(leaves: Vec<Arc<ExchangeExec>>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        // Push, rather than cast: `as` cannot perform the unsizing coercion
        // from `Arc<ExchangeExec>` to `Arc<dyn ExecutionPlan>`.
        inputs.push(leaf);
    }
    let input: Arc<dyn ExecutionPlan> = if inputs.len() == 1 {
        inputs.pop().expect("one leaf")
    } else {
        UnionExec::try_new(inputs)?
    };
    Ok(Arc::new(AdaptiveDatafusionExec::new(99, input)))
}

/// `(K, M)` of a leaf's decision, or `None` when it has none.
fn decision(leaf: &ExchangeExec) -> Option<(usize, u32)> {
    leaf.coalesce()
        .map(|cp| (cp.groups.len(), cp.upstream_partition_count))
}

/// #2166: a broadcast leaf beside shuffle leaves. The broadcast leaf is
/// excluded from the alignment group rather than suppressing it, so the shuffle
/// leaves still coalesce, and the broadcast leaf itself is left alone.
///
/// Byte trace: two shuffle leaves at `[25; 8]` sum to `[50; 8]`; at target 200
/// that packs to K=2, the same trace as the hash-join snapshot above.
#[test]
fn should_coalesce_shuffle_leaves_beside_a_broadcast_leaf()
-> datafusion::error::Result<()> {
    let broadcast = resolved_broadcast_leaf(8, 25);
    let left = resolved_shuffle_leaf(8, 25);
    let right = resolved_shuffle_leaf(8, 25);
    let plan = stage_over(vec![broadcast.clone(), left.clone(), right.clone()])?;

    CoalescePartitionsRule.optimize(plan, rule_config().options())?;

    assert_eq!(decision(&left), Some((2, 8)));
    assert_eq!(decision(&right), Some((2, 8)));
    assert_eq!(decision(&broadcast), None);

    Ok(())
}

/// #2167: leaves with differing upstream partition counts. Each group packs
/// against its own M instead of the whole stage bailing.
///
/// Byte trace: the M=8 group is one leaf at `[50; 8]`, which packs 4 partitions
/// per 200-byte bucket into K=2. The M=4 group is one leaf at `[50; 4]`, which
/// sums to exactly 200 and never overshoots, so it packs into K=1.
#[test]
fn should_coalesce_each_partition_count_group_against_its_own_m()
-> datafusion::error::Result<()> {
    let eight = resolved_shuffle_leaf(8, 50);
    let four = resolved_shuffle_leaf(4, 50);
    let plan = stage_over(vec![eight.clone(), four.clone()])?;

    CoalescePartitionsRule.optimize(plan, rule_config().options())?;

    assert_eq!(decision(&eight), Some((2, 8)));
    assert_eq!(decision(&four), Some((1, 4)));

    Ok(())
}

/// A leaf whose group cannot be decided must not keep a decision an earlier
/// pass left on it. Without the clear, the unresolved leaf's sibling would read
/// as coalesced while the leaf itself read as not, and a join across them would
/// see mismatched partition counts.
#[test]
fn should_clear_a_stale_decision_when_the_group_becomes_undecidable()
-> datafusion::error::Result<()> {
    let resolved = resolved_shuffle_leaf(8, 50);
    let unresolved = Arc::new(ExchangeExec::new(
        Arc::new(EmptyExec::new(mock_schema())),
        Some(Partitioning::UnknownPartitioning(8)),
        2,
    ));

    // Stand in for a decision an earlier pass attached.
    let stale = Arc::new(CoalescePlan {
        upstream_partition_count: 8,
        groups: vec![
            PartitionGroup {
                upstream_indices: vec![0, 1, 2, 3],
            },
            PartitionGroup {
                upstream_indices: vec![4, 5, 6, 7],
            },
        ],
    });
    resolved.set_coalesce(Some(stale));

    let plan = stage_over(vec![resolved.clone(), unresolved.clone()])?;

    CoalescePartitionsRule.optimize(plan, rule_config().options())?;

    assert_eq!(decision(&resolved), None);
    assert_eq!(decision(&unresolved), None);

    Ok(())
}
