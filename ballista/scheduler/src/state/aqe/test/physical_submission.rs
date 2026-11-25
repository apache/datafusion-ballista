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

use crate::state::aqe::adaptive_rules::AdaptiveRules;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{
    Embedder, Seen, coalesce_config, mock_batch, partitions_with_byte_sizes,
};
use crate::test_utils::PassThroughExec;
use ballista_core::assert_plan;
use ballista_core::execution_plans::CoalescePlan;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use datafusion::prelude::SessionContext;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// A session over [`coalesce_config`] with partition coalescing switched on
/// or off and the table `t` registered.
fn physical_context(coalesce_enabled: bool) -> SessionContext {
    let ctx = SessionContext::new_with_config(coalesce_config(8, coalesce_enabled));
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
    let plan = hand_built_plan(&ctx, |shuffle| {
        Arc::new(PassThroughExec::new("OpaqueExec", shuffle))
    })
    .await?;

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
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(PassThroughExec::new("OpaqueExec", shuffle));

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
    let plan = hand_built_plan(&ctx, |shuffle| {
        Arc::new(PassThroughExec::new("OpaqueExec", shuffle))
    })
    .await?;

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

/// A scheduler embedder's rules apply to a submitted physical plan as they do
/// to a planned logical one: its preparation rules run once, on the plan as
/// submitted, before the submitted shuffles are pinned as stage boundaries,
/// and its replan rules run when the job is planned and after each stage.
#[tokio::test]
async fn should_run_an_embedders_rules_on_a_submitted_plan()
-> datafusion::common::Result<()> {
    let seen = Arc::new(Seen::default());
    let embedder = Embedder {
        seen: Arc::clone(&seen),
        hold: false,
    };
    let ctx = physical_context(false);
    let plan = hand_built_plan(&ctx, |shuffle| {
        Arc::new(PassThroughExec::new("OpaqueExec", shuffle))
    })
    .await?;

    let mut planner = AdaptivePlanner::try_new_from_physical(
        &ctx,
        plan,
        "test_job".into(),
        Some(Arc::new(embedder)),
    )?;

    assert_eq!(seen.preparation_runs.load(Ordering::Relaxed), 1);
    assert!(
        seen.prepared_submitted_shuffle.load(Ordering::Relaxed),
        "a preparation rule should be given the plan as submitted"
    );
    assert!(
        !seen.prepared_stage_boundary.load(Ordering::Relaxed),
        "no stage boundary is placed before the preparation rules have run"
    );
    assert_eq!(seen.replan_runs.load(Ordering::Relaxed), 1);

    let _ = planner.runnable_stages()?.unwrap();
    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[50; 8]))?;

    assert_eq!(seen.preparation_runs.load(Ordering::Relaxed), 1);
    assert_eq!(seen.replan_runs.load(Ordering::Relaxed), 2);

    Ok(())
}

/// An operator over a descriptor with one partition per partition of the
/// operator, and an input with as many partitions, as Comet's
/// `CometFragmentExec` is. It can't be built over an input whose partition
/// count differs from its descriptor's.
#[derive(Debug)]
struct AlignedExec {
    children: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}

impl AlignedExec {
    fn try_new(
        descriptor: Arc<dyn ExecutionPlan>,
        input: Arc<dyn ExecutionPlan>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let partitions = descriptor.output_partitioning().partition_count();
        let actual = input.output_partitioning().partition_count();
        if actual != partitions {
            return internal_err!(
                "AlignedExec input has {actual} partitions, expected {partitions}"
            );
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            children: vec![descriptor, input],
            properties,
        }))
    }
}

impl DisplayAs for AlignedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "AlignedExec")
    }
}

impl ExecutionPlan for AlignedExec {
    fn name(&self) -> &str {
        "AlignedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let [descriptor, input] = <[_; 2]>::try_from(children)
            .map_err(|_| internal_datafusion_err!("AlignedExec has two children"))?;
        Self::try_new(descriptor, input)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        internal_err!("AlignedExec is only planned")
    }
}

/// An embedder whose operators, [`PassThroughExec`]s and [`AlignedExec`]s,
/// tolerate a coalesced partition count. With `rebuild_descriptors`, it
/// rebuilds an `AlignedExec`'s descriptor for the coalesced count.
#[derive(Debug)]
struct ToleratingEmbedder {
    rebuild_descriptors: bool,
}

impl AdaptiveRules for ToleratingEmbedder {
    fn tolerates_partition_count_change(&self, plan: &dyn ExecutionPlan) -> bool {
        plan.is::<PassThroughExec>() || plan.is::<AlignedExec>()
    }

    fn with_coalesced_children(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        coalesce: &CoalescePlan,
    ) -> Option<datafusion::common::Result<Arc<dyn ExecutionPlan>>> {
        if !self.rebuild_descriptors || !plan.is::<AlignedExec>() {
            return None;
        }
        let descriptor = Arc::new(
            EmptyExec::new(children[0].schema()).with_partitions(coalesce.groups.len()),
        );
        Some(AlignedExec::try_new(descriptor, Arc::clone(&children[1])))
    }
}

/// Plans `plan` with `embedder`'s rules, finishes its first stage with 8
/// partitions of 50 bytes, which pack to 2 at the 200-byte target, and
/// returns the stage that runs next.
fn next_stage_after_coalescing(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    embedder: ToleratingEmbedder,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let mut planner = AdaptivePlanner::try_new_from_physical(
        ctx,
        plan,
        "test_job".into(),
        Some(Arc::new(embedder)),
    )?;
    let _ = planner.runnable_stages()?.unwrap();
    planner.finalise_stage_internal(0, partitions_with_byte_sizes(&[50; 8]))?;
    let mut stages = planner.runnable_stages()?.unwrap();
    assert_eq!(stages.len(), 1);
    Ok(Arc::clone(&stages.remove(0).plan) as Arc<dyn ExecutionPlan>)
}

/// An extension operator the embedder's rules declare tolerant has the
/// shuffle it reads coalesced, as a DataFusion operator would.
#[tokio::test]
async fn should_coalesce_shuffle_feeding_tolerated_extension_operator()
-> datafusion::common::Result<()> {
    let ctx = physical_context(true);
    let plan = hand_built_plan(&ctx, |shuffle| {
        Arc::new(PassThroughExec::new("OpaqueExec", shuffle))
    })
    .await?;
    let embedder = ToleratingEmbedder {
        rebuild_descriptors: false,
    };

    let stage = next_stage_after_coalescing(&ctx, plan, embedder)?;

    assert_plan!(stage.as_ref(), @r"
    ShuffleWriterExec: partitioning: Hash([c@2], 2)
      OpaqueExec
        ShuffleReaderExec: upstream_stage: 0, partitioning: Hash([c@2], 2), coalesce: 2 of 8
    ");

    Ok(())
}

/// An operator with per-partition state beside its inputs is rebuilt for the
/// coalesced partition count by the embedder's rules.
#[tokio::test]
async fn should_rebuild_extension_operator_over_coalesced_shuffle()
-> datafusion::common::Result<()> {
    let ctx = physical_context(true);
    let plan = hand_built_plan(&ctx, |shuffle| {
        let descriptor = Arc::new(EmptyExec::new(shuffle.schema()).with_partitions(8));
        AlignedExec::try_new(descriptor, shuffle).unwrap()
    })
    .await?;
    let embedder = ToleratingEmbedder {
        rebuild_descriptors: true,
    };

    let stage = next_stage_after_coalescing(&ctx, plan, embedder)?;

    // `AlignedExec` checks that its descriptor has as many partitions as the
    // coalesced reader, so building it shows the descriptor was rebuilt.
    assert_plan!(stage.as_ref(), @r"
    ShuffleWriterExec: partitioning: UnknownPartitioning(2)
      AlignedExec
        EmptyExec
        ShuffleReaderExec: upstream_stage: 0, partitioning: Hash([c@2], 2), coalesce: 2 of 8
    ");

    Ok(())
}

/// Without the embedder rebuilding it, the operator is rebuilt over the
/// coalesced shuffle with its descriptor as it was, which it rejects.
#[tokio::test]
async fn should_fail_extension_operator_its_rules_do_not_rebuild()
-> datafusion::common::Result<()> {
    let ctx = physical_context(true);
    let plan = hand_built_plan(&ctx, |shuffle| {
        let descriptor = Arc::new(EmptyExec::new(shuffle.schema()).with_partitions(8));
        AlignedExec::try_new(descriptor, shuffle).unwrap()
    })
    .await?;
    let embedder = ToleratingEmbedder {
        rebuild_descriptors: false,
    };

    let err = next_stage_after_coalescing(&ctx, plan, embedder).unwrap_err();

    assert!(
        err.to_string()
            .contains("AlignedExec input has 2 partitions, expected 8"),
        "{err}"
    );

    Ok(())
}
