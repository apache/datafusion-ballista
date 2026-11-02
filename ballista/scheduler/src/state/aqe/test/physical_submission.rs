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

//! Tests for adaptive planning of a submitted physical plan.
//!
//! A client that builds its own physical plan places the shuffles itself,
//! often with its own extension operators in between. The adaptive planner
//! must keep those shuffles as stage boundaries and must not make decisions
//! that assume it knows what an extension operator does with its input.

use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::coalesce_rule::partitions_with_byte_sizes;
use crate::state::aqe::test::mock_batch;
use ballista_core::assert_plan;
use ballista_core::extension::SessionConfigExt;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::fmt::Formatter;
use std::sync::Arc;

/// A session with Ballista's config extension installed, partition
/// coalescing switched on or off, and a small coalesce target so the
/// bin-pack is hand-traceable.
fn physical_context(coalesce_enabled: bool) -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(8)
        .with_round_robin_repartition(false)
        .with_ballista_coalesce_enabled(coalesce_enabled)
        .with_ballista_coalesce_target_partition_bytes(200);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_batch("t", mock_batch().unwrap()).unwrap();
    ctx
}

/// `scan -> hash repartition on `c` -> consumer`, built by hand the way a
/// client that owns its physical plan would.
async fn hand_built_plan(
    ctx: &SessionContext,
    consumer: impl FnOnce(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let scan = ctx.table("t").await?.create_physical_plan().await?;
    let shuffle: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        scan,
        Partitioning::Hash(vec![Arc::new(Column::new("c", 2))], 8),
    )?);
    Ok(consumer(shuffle))
}

/// A hash repartition placed by the client sits under an operator that does
/// not require it. DataFusion's `EnforceDistribution` would drop such a
/// repartition as unnecessary; the adaptive planner has to keep it as a stage
/// boundary, since the client put it there on purpose.
#[tokio::test]
async fn should_keep_submitted_shuffle_under_extension_operator()
-> datafusion::common::Result<()> {
    let ctx = physical_context(false);
    let plan =
        hand_built_plan(&ctx, |shuffle| Arc::new(OpaqueExec::new(shuffle))).await?;

    let planner =
        AdaptivePlanner::try_new_from_physical(&ctx, plan, "test_job".into(), None)?;

    assert_plan!(planner.plan.as_ref(), @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending, stage_resolved=false
      OpaqueExec
        ExchangeExec: partitioning=Hash([c@2], 8), plan_id=0, stage_id=pending, stage_resolved=false
          DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

/// A range repartition placed by the client is kept too. Nothing derives one
/// back from a requirement, and the consumer's partitions are only in key order
/// as a whole if each of them holds one range.
#[tokio::test]
async fn should_keep_submitted_range_shuffle_under_extension_operator()
-> datafusion::common::Result<()> {
    use datafusion::arrow::compute::SortOptions;
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::{
        LexOrdering, PhysicalSortExpr, RangePartitioning, SplitPoint,
    };

    let ctx = physical_context(false);
    let scan = ctx.table("t").await?.create_physical_plan().await?;
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("c", 2)),
        SortOptions::default(),
    )])
    .unwrap();
    let range = RangePartitioning::try_new(
        ordering,
        vec![SplitPoint::new(vec![ScalarValue::Int32(Some(2))])],
    )?;
    let shuffle: Arc<dyn ExecutionPlan> =
        Arc::new(RepartitionExec::try_new(scan, Partitioning::Range(range))?);
    let plan: Arc<dyn ExecutionPlan> = Arc::new(OpaqueExec::new(shuffle));

    let planner =
        AdaptivePlanner::try_new_from_physical(&ctx, plan, "test_job".into(), None)?;

    assert_plan!(planner.plan.as_ref(), @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending, stage_resolved=false
      OpaqueExec
        ExchangeExec: partitioning=Range([c@2 ASC], [(2)], 2), plan_id=0, stage_id=pending, stage_resolved=false
          DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

/// Coalescing a shuffle changes how many partitions its consumer sees. That
/// is only safe for operators the rule knows to be indifferent to the count;
/// an extension operator may not be, so the group it feeds is left alone.
#[tokio::test]
async fn should_not_coalesce_shuffle_feeding_extension_operator()
-> datafusion::common::Result<()> {
    let ctx = physical_context(true);
    let plan =
        hand_built_plan(&ctx, |shuffle| Arc::new(OpaqueExec::new(shuffle))).await?;

    let mut planner =
        AdaptivePlanner::try_new_from_physical(&ctx, plan, "test_job".into(), None)?;

    let _ = planner.runnable_stages()?.unwrap();
    // 8 partitions of 50 bytes against a 200-byte target packs to K=2, which
    // is what the rule attaches when the consumer is a DataFusion operator.
    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[50; 8]))?;
    let _ = planner.runnable_stages()?;

    assert_plan!(planner.current_plan(), @ r"
    AdaptiveDatafusionExec: is_final=true, plan_id=1, stage_id=1, stage_resolved=false
      OpaqueExec
        ExchangeExec: partitioning=Hash([c@2], 8), plan_id=0, stage_id=0, stage_resolved=true
          DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

/// A stand-in for a client's extension operator: passes its input through
/// and declares nothing about what it needs from it, like an opaque fragment
/// the planner cannot see into.
#[derive(Debug)]
struct OpaqueExec {
    input: Arc<dyn ExecutionPlan>,
}

impl OpaqueExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl DisplayAs for OpaqueExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OpaqueExec")
    }
}

impl ExecutionPlan for OpaqueExec {
    fn name(&self) -> &str {
        "OpaqueExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> datafusion::common::Result<TreeNodeRecursion>,
    ) -> datafusion::common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

/// Holds back (or releases) the stage of every exchange, as an embedder's rule
/// that wants to decide on a stage later would.
#[derive(Debug)]
struct HoldStages {
    inactive: bool,
}

impl datafusion::physical_optimizer::PhysicalOptimizerRule for HoldStages {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        use crate::state::aqe::execution_plan::ExchangeExec;
        use datafusion::common::tree_node::{Transformed, TreeNode};
        Ok(plan
            .transform_up(|node| {
                let Some(exchange) = node.downcast_ref::<ExchangeExec>() else {
                    return Ok(Transformed::no(node));
                };
                let held: Arc<dyn ExecutionPlan> =
                    Arc::new(exchange.with_inactive_stage(self.inactive));
                Ok(Transformed::yes(held))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "HoldStages"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// An embedder's rules: hold every stage while preparing the plan, and
/// optionally release them again when the plan is optimized.
#[derive(Debug)]
struct HoldThenRelease {
    release: bool,
}

impl crate::state::aqe::adaptive_rules::AdaptiveRules for HoldThenRelease {
    fn preparation_rules(
        &self,
        _plan_ids: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Vec<crate::state::aqe::adaptive_rules::AdaptiveRule> {
        vec![Arc::new(HoldStages { inactive: true })]
    }

    fn replan_rules(
        &self,
        _plan_ids: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Vec<crate::state::aqe::adaptive_rules::AdaptiveRule> {
        if self.release {
            vec![Arc::new(HoldStages { inactive: false })]
        } else {
            vec![]
        }
    }
}

/// An embedder's preparation rule runs after the submitted shuffles are
/// pinned, so it can hold their stages back, and a held stage doesn't run.
#[tokio::test]
async fn should_not_run_a_stage_an_embedder_rule_holds() -> datafusion::common::Result<()>
{
    let ctx = physical_context(false);
    let plan =
        hand_built_plan(&ctx, |shuffle| Arc::new(OpaqueExec::new(shuffle))).await?;

    let mut planner = AdaptivePlanner::try_new_from_physical(
        &ctx,
        plan,
        "test_job".into(),
        Some(&HoldThenRelease { release: false }),
    )?;

    let runnable = planner.identify_runnable_stages()?.unwrap_or_default();
    assert!(runnable.is_empty(), "a held stage must not run");
    Ok(())
}

/// An embedder's replan rules run whenever the plan is optimized, after its
/// preparation rules, so they can release a stage held back earlier.
#[tokio::test]
async fn should_run_a_stage_an_embedder_rule_releases() -> datafusion::common::Result<()>
{
    let ctx = physical_context(false);
    let plan =
        hand_built_plan(&ctx, |shuffle| Arc::new(OpaqueExec::new(shuffle))).await?;

    let mut planner = AdaptivePlanner::try_new_from_physical(
        &ctx,
        plan,
        "test_job".into(),
        Some(&HoldThenRelease { release: true }),
    )?;

    let runnable = planner.identify_runnable_stages()?.unwrap_or_default();
    assert_eq!(runnable.len(), 1, "the released stage must run");
    Ok(())
}
