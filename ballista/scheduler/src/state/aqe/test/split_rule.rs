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

//! Functional tests for [`SplitPartitionsRule`].
//!
//! Mock helpers (`mock_batch`, `mock_schema`) come from the parent test
//! module. Byte-size fixtures use the algorithm's production defaults
//! (skew=5.0, min_split_bytes=64 MiB, max_factor=8) — see
//! `state/aqe/split/algorithm.rs` tests for unit-level coverage of the
//! decision function itself.

use crate::assert_plan;
use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::optimizer_rule::SplitPartitionsRule;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{mock_batch, mock_schema, partitions_with_bytes_and_files};
use ballista_core::extension::SessionConfigExt;
use datafusion::datasource::MemTable;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

/// Build a session context with split-rule knobs forced to specific values.
/// `skew_factor=5.0`, `max_factor=8` are the production defaults; the
/// `min_split_bytes` floor is set to `1` for tests so we can use small
/// synthetic byte sizes without hitting the production 64 MiB floor.
///
/// `prefer_hash_join`: when `Some(false)`, force the SQL planner to emit
/// `SortMergeJoinExec` instead of `HashJoinExec(Partitioned)`; `None` keeps
/// the DataFusion default.
fn split_context(
    target_partitions: usize,
    enabled: bool,
    prefer_hash_join: Option<bool>,
) -> SessionContext {
    let mut config = SessionConfig::new_with_ballista()
        .with_target_partitions(target_partitions)
        .with_round_robin_repartition(false)
        .with_ballista_split_enabled(enabled)
        .with_ballista_split_skew_factor(5.0)
        .with_ballista_split_min_split_bytes(1)
        .with_ballista_split_max_split_factor(8);
    if let Some(v) = prefer_hash_join {
        config = config.set_bool("datafusion.optimizer.prefer_hash_join", v);
    }

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    SessionContext::new_with_state(state)
}

/// As in `coalesce_rule.rs`: register a MemTable with `n_partitions` rows
/// so DataFusion's `EnforceDistribution` inserts the hash exchanges needed
/// for join plans.
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

/// Build a synthetic stage subtree:
///
/// ```text
/// AdaptiveDatafusionExec
///   ProjectionExec (pass-through, no distribution requirement)
///     ExchangeExec_leaf (with resolved shuffle_partitions)
/// ```
///
/// ProjectionExec's `required_input_distribution()` is
/// `UnspecifiedDistribution`, so the safety walk passes — this is the
/// shape needed to test the rule's slot-attachment path.
fn build_synthetic_stage(
    m: usize,
    bytes_and_files: &[(u64, usize)],
) -> datafusion::error::Result<(Arc<dyn ExecutionPlan>, Arc<ExchangeExec>)> {
    debug_assert_eq!(m, bytes_and_files.len());

    let schema = mock_schema();
    // Tiny in-memory source — content is irrelevant since the leaf is
    // resolved (the rule doesn't execute, just reads stats).
    let mem = Arc::new(MemorySourceConfig::try_new(
        &[vec![mock_batch()?]],
        schema.clone(),
        None,
    )?);
    let data_source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(mem));

    let leaf = Arc::new(ExchangeExec::new(
        data_source,
        Some(Partitioning::Hash(vec![Arc::new(Column::new("c", 2))], m)),
        /* plan_id */ 0,
    ));
    leaf.resolve_shuffle_partitions(partitions_with_bytes_and_files(bytes_and_files));

    // Project a single column to keep the plan compact in snapshots. This is
    // a no-op identity-style projection; it exists purely as the "non-hash-
    // requiring consumer" between the leaf and the root.
    let projection = Arc::new(ProjectionExec::try_new(
        vec![(Arc::new(Column::new("a", 0)) as _, "a".to_string())],
        leaf.clone() as Arc<dyn ExecutionPlan>,
    )?);

    let root = Arc::new(AdaptiveDatafusionExec::new(
        /* plan_id */ 1, projection,
    )) as Arc<dyn ExecutionPlan>;

    Ok((root, leaf))
}

// ---------- SQL-driven bail tests ----------

/// FinalPartitioned aggregate over a hash exchange: the rule's safety walk
/// sees `AggregateExec(FinalPartitioned)::required_input_distribution() ==
/// [HashPartitioned(...)]` and bails. Even with a clearly skewed byte
/// distribution and split enabled, no `split=` annotation appears on the
/// leaf Exchange. This is the v1-honest scope check — TPC-H Q2's skew sits
/// behind a FinalPartitioned aggregate, and v1 cannot help it.
#[tokio::test]
async fn bails_on_final_partitioned_aggregate() -> datafusion::error::Result<()> {
    let ctx = split_context(8, true, None);
    ctx.register_batch("t", mock_batch()?)?;

    let plan = ctx
        .sql("select min(a) as c0, c as c2 from t group by c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let _ = planner.runnable_stages()?.unwrap();
    // Heavy skew on partition 7: 4 KiB vs 1 byte everywhere else. The rule
    // would split this if the consumer allowed it; FinalPartitioned doesn't.
    planner.finalise_stage_internal(
        0,
        partitions_with_bytes_and_files(&[
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (4096, 4),
        ]),
    )?;

    let _ = planner.runnable_stages()?;

    // No `split=` annotation — the safety walk hit AggregateExec
    // (FinalPartitioned) and bailed.
    assert_plan!(planner.current_plan(),  @ "
    AdaptiveDatafusionExec: is_final=true, plan_id=1, stage_id=1, stage_resolved=false
      ProjectionExec: expr=[min(t.a)@1 as c0, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a)]
          ExchangeExec: partitioning=Hash([c@0], 8), plan_id=0, stage_id=0, stage_resolved=true
            AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

/// Partitioned hash join: `HashJoinExec(Partitioned)` requires hash-partitioned
/// input on both legs. Same safety walk as the aggregate test triggers — the
/// rule sees a hash distribution requirement and bails on both leg leaves
/// uniformly. Even though we apply heavy skew to one leg, no `split=` shows
/// up on either leaf.
#[tokio::test]
async fn bails_on_partitioned_hash_join() -> datafusion::error::Result<()> {
    let ctx = split_context(8, true, None);
    register_partitioned_table(&ctx, "t1", 8)?;
    register_partitioned_table(&ctx, "t2", 8)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(2, stages.len());

    planner.finalise_stage_internal(
        0,
        partitions_with_bytes_and_files(&[
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (4096, 4),
        ]),
    )?;
    planner.finalise_stage_internal(1, partitions_with_bytes_and_files(&[(1, 4); 8]))?;

    let _ = planner.runnable_stages()?;

    // Both join legs: `split=` absent (safety walk hit SortMergeJoinExec
    // /HashJoinExec partitioned requirement).
    assert_plan!(planner.current_plan(),  @ "
    AdaptiveDatafusionExec: is_final=true, plan_id=2, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 8), plan_id=0, stage_id=0, stage_resolved=true
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 8), plan_id=1, stage_id=1, stage_resolved=true
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
    ");

    Ok(())
}

/// Sort-merge join: same plan shape as the hash-join case when
/// `prefer_hash_join=false`. SortMergeJoinExec's
/// `required_input_distribution()` is also `HashPartitioned(...)`, so the
/// safety walk triggers the same bail.
#[tokio::test]
async fn bails_on_sort_merge_join() -> datafusion::error::Result<()> {
    let ctx = split_context(8, true, Some(false));
    register_partitioned_table(&ctx, "t1", 8)?;
    register_partitioned_table(&ctx, "t2", 8)?;

    let plan = ctx
        .sql("select t1.a, t2.b from t1 join t2 on t1.c = t2.c")
        .await?
        .create_physical_plan()
        .await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(2, stages.len());

    planner.finalise_stage_internal(
        0,
        partitions_with_bytes_and_files(&[
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (1, 4),
            (4096, 4),
        ]),
    )?;
    planner.finalise_stage_internal(1, partitions_with_bytes_and_files(&[(1, 4); 8]))?;

    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(),  @ "
    AdaptiveDatafusionExec: is_final=true, plan_id=2, stage_id=2, stage_resolved=false
      ProjectionExec: expr=[a@0 as a, b@2 as b]
        SortMergeJoinExec: join_type=Inner, on=[(c@1, c@1)]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 8), plan_id=0, stage_id=0, stage_resolved=true
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
          SortExec: expr=[c@1 ASC], preserve_partitioning=[true]
            ExchangeExec: partitioning=Hash([c@1], 8), plan_id=1, stage_id=1, stage_resolved=true
              DataSourceExec: partitions=8, partition_sizes=[1, 1, 1, 1, 1, 1, 1, 1]
    ");

    Ok(())
}

/// Disabled rule: even the synthetic safe shape doesn't trigger any split.
/// The rule returns its input plan verbatim at the first `bc.split_enabled()`
/// check, so the leaf's split slot stays `None`.
#[tokio::test]
async fn skips_when_disabled() -> datafusion::error::Result<()> {
    let ctx = split_context(8, /* enabled */ false, None);
    let config = ctx.state().config().options().clone();

    let (root, leaf) = build_synthetic_stage(4, &[(1, 4), (1, 4), (1, 4), (4096, 4)])?;

    let _ = SplitPartitionsRule.optimize(root, &config)?;
    assert!(
        leaf.split().is_none(),
        "leaf should have no SplitPlan when rule is disabled"
    );
    Ok(())
}

// ---------- Synthetic happy-path / decision tests ----------

/// Happy path: one upstream partition is ~4000× larger than the median and
/// has 4 files. The rule fires; the leaf's split slot holds a SplitPlan with
/// `K' = 3 passthroughs + 8 split shards = 11` (max_factor cap).
#[tokio::test]
async fn attaches_split_when_skewed_partition_has_multiple_files()
-> datafusion::error::Result<()> {
    let ctx = split_context(8, true, None);
    let config = ctx.state().config().options().clone();

    let (root, leaf) = build_synthetic_stage(4, &[(1, 4), (1, 4), (1, 4), (4096, 4)])?;

    let _ = SplitPartitionsRule.optimize(root, &config)?;
    let sp = leaf.split().expect("rule should have attached a SplitPlan");
    assert_eq!(sp.upstream_partition_count, 4, "M tracked from leaf");
    // 3 passthroughs + factor-8 split for the skewed idx = 11 shards.
    assert_eq!(sp.shards.len(), 11, "K' = 3*1 + 1*8");
    // Verify the split idx is 3 (the skewed one) with shards 0..8.
    let split_shards: Vec<_> = sp.shards.iter().filter(|s| s.split_factor > 1).collect();
    assert_eq!(split_shards.len(), 8);
    for (i, s) in split_shards.iter().enumerate() {
        assert_eq!(s.upstream_idx, 3);
        assert_eq!(s.shard_idx, i as u32);
        assert_eq!(s.split_factor, 8);
    }
    Ok(())
}

/// Single-file guard: even when the byte distribution would qualify as
/// skewed, an upstream partition with only one `PartitionLocation` can't be
/// sharded by file-list assignment in v1. The whole rule returns "no
/// decision" because every per-idx factor is 1 (the skewed idx is blocked
/// by `file_counts[i] <= 1`).
#[tokio::test]
async fn skips_when_single_file_in_skewed_idx() -> datafusion::error::Result<()> {
    let ctx = split_context(8, true, None);
    let config = ctx.state().config().options().clone();

    // partition 3 has 4096 bytes but only 1 file backing it.
    let (root, leaf) = build_synthetic_stage(4, &[(1, 4), (1, 4), (1, 4), (4096, 1)])?;

    let _ = SplitPartitionsRule.optimize(root, &config)?;
    assert!(
        leaf.split().is_none(),
        "single-file skewed partition cannot be split in v1"
    );
    Ok(())
}

/// Idempotence: a leaf that already has `split()` set is skipped on a
/// second pass. Same plan, same bytes, two consecutive `optimize` calls —
/// the slot from the first run is what the second observes, and the rule
/// short-circuits before recomputing.
#[tokio::test]
async fn idempotent_on_second_pass() -> datafusion::error::Result<()> {
    let ctx = split_context(8, true, None);
    let config = ctx.state().config().options().clone();

    let (root, leaf) = build_synthetic_stage(4, &[(1, 4), (1, 4), (1, 4), (4096, 4)])?;

    let plan_after_first = SplitPartitionsRule.optimize(root, &config)?;
    let sp_after_first = leaf.split().expect("first pass should attach SplitPlan");
    let kprime_first = sp_after_first.shards.len();

    // Second pass should observe the existing slot and bail without
    // overwriting. We compare Arc identity on the slot contents.
    let _ = SplitPartitionsRule.optimize(plan_after_first, &config)?;
    let sp_after_second = leaf.split().expect("second pass should not clear the slot");
    assert!(
        Arc::ptr_eq(&sp_after_first, &sp_after_second),
        "second pass should not overwrite the SplitPlan attached by the first"
    );
    assert_eq!(sp_after_second.shards.len(), kprime_first);
    Ok(())
}

/// Default-off sanity: when the rule is disabled, optimize returns the
/// input Arc verbatim (`Arc::ptr_eq` holds). This is the regression check
/// the verification plan calls "Gate 5".
#[tokio::test]
async fn default_off_returns_input_arc_verbatim() -> datafusion::error::Result<()> {
    let ctx = split_context(8, /* enabled */ false, None);
    let config = ctx.state().config().options().clone();

    let (root, _leaf) = build_synthetic_stage(4, &[(1, 4), (1, 4), (1, 4), (4096, 4)])?;

    let before = root.clone();
    let after = SplitPartitionsRule.optimize(root, &config)?;
    assert!(
        Arc::ptr_eq(&before, &after),
        "disabled rule must return the input Arc unchanged"
    );
    Ok(())
}
