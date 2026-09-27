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

//! Reuse of structurally-identical shuffle exchanges in the distributed plan.
//!
//! Analog of Spark's `ReuseExchangeAndSubquery` rule. A repeated `ShuffleWriter`
//! subtree is materialized once; every consumer's `UnresolvedShuffleExec` is
//! rewired to the single surviving `stage_id`.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use ballista_core::error::Result;
use ballista_core::execution_plans::{ShuffleWriter, UnresolvedShuffleExec};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr_common::physical_expr::is_volatile;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::debug;

use crate::planner::create_shuffle_writer_with_config;

/// Produces a faithful byte key for a physical plan, or `None` if the plan
/// cannot be canonicalized, in which case it is never reused. A missing key
/// only costs a reuse opportunity, never a wrong result.
pub type Canonicalizer = dyn Fn(&Arc<dyn ExecutionPlan>) -> Option<Vec<u8>> + Send + Sync;

/// The production [`Canonicalizer`]: the plan's protobuf encoding under
/// `extension_codec`, the same encoding that ships stages to executors.
pub fn protobuf_canonical_key(
    plan: &Arc<dyn ExecutionPlan>,
    extension_codec: &dyn PhysicalExtensionCodec,
) -> Option<Vec<u8>> {
    physical_plan_to_bytes_with_extension_codec(plan.clone(), extension_codec)
        .ok()
        .map(Vec::from)
}

/// Deduplicate structurally-identical `ShuffleWriter` stages.
///
/// Stages are processed in ascending `stage_id` order. Because the planner
/// assigns ids bottom-up, a dependency stage is finalized before any dependent
/// stage is keyed, giving a single-pass fixed point: an inner shared subtree is
/// collapsed first, then the two outer subtrees — now carrying the same
/// collapsed inner id — serialize identically and collapse in turn.
///
/// The stage with the maximum id is the query root (the planner pushes it last)
/// and is never dropped; its refs are still rewritten.
pub fn reuse_shuffle_stages(
    mut stages: Vec<Arc<dyn ShuffleWriter>>,
    config: &ConfigOptions,
    canonical: &Canonicalizer,
) -> Result<Vec<Arc<dyn ShuffleWriter>>> {
    stages.sort_by_key(|s| s.stage_id());
    let Some(root_id) = stages.last().map(|s| s.stage_id()) else {
        return Ok(stages);
    };

    // dropped stage_id -> surviving representative stage_id
    let mut remap: HashMap<usize, usize> = HashMap::new();
    // canonical bytes -> surviving representative stage_id
    let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut kept: Vec<Arc<dyn ShuffleWriter>> = Vec::with_capacity(stages.len());

    for stage in stages {
        let stage_id = stage.stage_id();
        // Point this stage's inputs at the survivors of earlier merges.
        let child = stage.children()[0].clone();
        let rewritten_child = rewrite_shuffle_refs(child.clone(), &remap)?;
        let build = |id| {
            create_shuffle_writer_with_config(
                stage.job_id(),
                id,
                rewritten_child.clone(),
                stage.shuffle_output_partitioning().cloned(),
                config,
            )
        };

        // The root stage is the query output, so it is never merged. Neither is
        // a subtree with a volatile expression such as `random()`: it
        // serializes like its twin but must produce its own values.
        if stage_id != root_id && !has_volatile_expr(&rewritten_child)? {
            // Key on the writer with its id normalized away, so the key covers
            // input, partitioning and writer kind but not the stage id.
            let normalized: Arc<dyn ExecutionPlan> = build(0)?;
            if let Some(key) = canonical(&normalized) {
                match seen.entry(key) {
                    Entry::Occupied(rep) => {
                        let rep = *rep.get();
                        debug!("exchange reuse: stage {stage_id} reuses stage {rep}");
                        remap.insert(stage_id, rep);
                        continue;
                    }
                    Entry::Vacant(slot) => {
                        slot.insert(stage_id);
                    }
                }
            }
        }

        kept.push(if Arc::ptr_eq(&child, &rewritten_child) {
            stage
        } else {
            build(stage_id)?
        });
    }

    Ok(kept)
}

/// Rewrite every `UnresolvedShuffleExec` whose `stage_id` appears in `remap`,
/// swapping only its `stage_id`. Returns `plan` itself when nothing matches.
fn rewrite_shuffle_refs(
    plan: Arc<dyn ExecutionPlan>,
    remap: &HashMap<usize, usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|node| {
            if let Some(unresolved) = node.downcast_ref::<UnresolvedShuffleExec>()
                && let Some(&stage_id) = remap.get(&unresolved.stage_id)
            {
                let mut rewritten = unresolved.clone();
                rewritten.stage_id = stage_id;
                return Ok(Transformed::yes(Arc::new(rewritten)));
            }
            Ok(Transformed::no(node))
        })?
        .data)
}

/// Whether any operator in `plan` evaluates a volatile expression.
fn has_volatile_expr(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    let mut volatile = false;
    plan.apply(|node| {
        node.apply_expressions(&mut |expr| {
            if is_volatile(expr) {
                volatile = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
    })?;
    Ok(volatile)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{
        DefaultDistributedPlanner, DistributedPlanner, find_unresolved_shuffles,
    };
    use crate::state::execution_graph::ExecutionStageBuilder;
    use crate::test_utils::datafusion_test_context;
    use ballista_core::JobId;
    use ballista_core::serde::BallistaPhysicalExtensionCodec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{Partitioning, displayable};
    use datafusion::prelude::SessionConfig;
    use uuid::Uuid;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]))
    }

    fn hash(n: usize) -> Partitioning {
        Partitioning::Hash(vec![Arc::new(Column::new("k", 0))], n)
    }

    fn job() -> JobId {
        "job-1".to_string().into()
    }

    fn leaf() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    fn unresolved(stage_id: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnresolvedShuffleExec::new(stage_id, schema(), hash(4)))
    }

    /// Built through the same constructor the planner uses, so the stages
    /// these tests key on are the ones production would produce for the same
    /// partitioning.
    fn writer(
        stage_id: usize,
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
    ) -> Arc<dyn ShuffleWriter> {
        create_shuffle_writer_with_config(
            &job(),
            stage_id,
            input,
            partitioning,
            &ConfigOptions::default(),
        )
        .unwrap()
    }

    /// Stage ids referenced by the `UnresolvedShuffleExec`s in `plan`.
    fn ref_ids(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
        find_unresolved_shuffles(plan)
            .unwrap()
            .iter()
            .map(|u| u.stage_id)
            .collect()
    }

    /// Stub canonicalizer: key by indented Display plus the referenced stage
    /// ids, which Display omits. Making the key stage-id-sensitive is what
    /// forces the nested-duplicate test to exercise the rewrite-refs-before-
    /// keying step.
    fn display_key(p: &Arc<dyn ExecutionPlan>) -> Option<Vec<u8>> {
        let display = displayable(p.as_ref()).indent(false);
        Some(format!("{display}|refs={:?}", ref_ids(p)).into_bytes())
    }

    fn union(children: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        UnionExec::try_new(children).unwrap()
    }

    fn config() -> ConfigOptions {
        ConfigOptions::default()
    }

    /// Refs of the root stage `root_id` in `stages`.
    fn root_ref_ids(stages: &[Arc<dyn ShuffleWriter>], root_id: usize) -> Vec<usize> {
        let root: Arc<dyn ExecutionPlan> = stages
            .iter()
            .find(|s| s.stage_id() == root_id)
            .unwrap()
            .clone();
        ref_ids(&root)
    }

    #[test]
    fn protobuf_canonical_key_encodes_and_distinguishes() {
        let codec = BallistaPhysicalExtensionCodec::default();
        let key = |stage: Arc<dyn ShuffleWriter>| {
            let plan: Arc<dyn ExecutionPlan> = stage;
            protobuf_canonical_key(&plan, &codec)
        };
        let a = key(writer(1, leaf(), Some(hash(4))));
        assert!(a.is_some(), "a Ballista stage plan should encode");
        assert_eq!(a, key(writer(1, leaf(), Some(hash(4)))));
        assert_ne!(a, key(writer(1, leaf(), Some(hash(8)))));
    }

    #[test]
    fn identical_pair_collapses() {
        let stages = vec![
            writer(1, leaf(), Some(hash(4))),
            writer(2, leaf(), Some(hash(4))),
            writer(3, union(vec![unresolved(1), unresolved(2)]), None),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &display_key).unwrap();
        assert_eq!(out.len(), 2, "one duplicate exchange should be dropped");
        assert_eq!(root_ref_ids(&out, 3), vec![1, 1]);

        // Both consumer edges collapse into a single output link.
        let built = ExecutionStageBuilder::new(Arc::new(SessionConfig::new()))
            .build(out)
            .unwrap();
        assert_eq!(built[&1].output_links(), [3]);
    }

    #[test]
    fn distinct_partitioning_is_not_merged() {
        let stages = vec![
            writer(1, leaf(), Some(hash(4))),
            writer(2, leaf(), Some(hash(8))), // different partition count
            writer(3, union(vec![unresolved(1), unresolved(2)]), None),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &display_key).unwrap();
        assert_eq!(out.len(), 3, "different partitioning must not be merged");
    }

    #[test]
    fn nested_duplicate_collapses_in_one_pass() {
        // inner {1,3} identical; outer {2,4} identical once inner refs collapse.
        let stages = vec![
            writer(1, leaf(), Some(hash(4))),
            writer(2, unresolved(1), Some(hash(4))),
            writer(3, leaf(), Some(hash(4))),
            writer(4, unresolved(3), Some(hash(4))),
            writer(5, union(vec![unresolved(2), unresolved(4)]), None),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &display_key).unwrap();
        assert_eq!(out.len(), 3, "both inner and outer duplicates collapse");
        assert_eq!(root_ref_ids(&out, 5), vec![2, 2]);
    }

    #[test]
    fn root_is_never_dropped() {
        // stage 2 (root) is byte-identical to stage 1 but must be kept.
        let stages = vec![
            writer(1, leaf(), Some(hash(4))),
            writer(2, leaf(), Some(hash(4))),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &display_key).unwrap();
        assert_eq!(out.len(), 2, "the root stage is never merged away");
    }

    #[test]
    fn none_canonical_never_merges() {
        let never = |_: &Arc<dyn ExecutionPlan>| -> Option<Vec<u8>> { None };
        let stages = vec![
            writer(1, leaf(), Some(hash(4))),
            writer(2, leaf(), Some(hash(4))),
            writer(3, union(vec![unresolved(1), unresolved(2)]), None),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &never).unwrap();
        assert_eq!(out.len(), 3, "un-canonicalizable stages are never merged");
    }

    #[test]
    fn volatile_stages_are_not_merged() {
        use datafusion::common::DFSchema;
        use datafusion::execution::context::ExecutionProps;
        use datafusion::functions::math::expr_fn::random;
        use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
        use datafusion::physical_expr::create_physical_expr;
        use datafusion::physical_plan::projection::ProjectionExec;

        // Two `SELECT random() FROM t` subtrees look identical but must each
        // draw their own values.
        let random_projection = || -> Arc<dyn ExecutionPlan> {
            let df_schema = DFSchema::try_from(schema()).unwrap();
            let expr = create_physical_expr(
                &random(),
                &df_schema,
                &ExecutionProps::new(),
                &PhysicalPlanningContext::default(),
            )
            .unwrap();
            Arc::new(
                ProjectionExec::try_new(vec![(expr, "r".to_string())], leaf()).unwrap(),
            )
        };
        let stages = vec![
            writer(1, random_projection(), None),
            writer(2, random_projection(), None),
            writer(3, union(vec![unresolved(1), unresolved(2)]), None),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &display_key).unwrap();
        assert_eq!(out.len(), 3, "volatile stages must not be merged");
    }

    #[test]
    fn reused_stage_fans_out_to_distinct_consumers() {
        // Stages 1 and 2 are identical exchanges read by distinct consumers 3
        // and 4, which differ in their own partitioning. After reuse stage 1
        // feeds both, the shape of TPC-H q15's `revenue0`.
        let stages = vec![
            writer(1, leaf(), Some(hash(4))),
            writer(2, leaf(), Some(hash(4))),
            writer(3, unresolved(1), Some(hash(2))),
            writer(4, unresolved(2), Some(hash(3))),
            writer(5, union(vec![unresolved(3), unresolved(4)]), None),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &display_key).unwrap();

        let built = ExecutionStageBuilder::new(Arc::new(SessionConfig::new()))
            .build(out)
            .unwrap();
        let mut stage_ids: Vec<usize> = built.keys().copied().collect();
        stage_ids.sort_unstable();
        assert_eq!(stage_ids, vec![1, 3, 4, 5], "duplicate stage 2 was dropped");
        let mut links = built[&1].output_links().to_vec();
        links.sort_unstable();
        assert_eq!(links, vec![3, 4], "stage 1 feeds both consumers");
    }

    // --- TPC-H detection ---------------------------------------------------

    /// Plan a TPC-H query file and return its stage count before and after
    /// reuse. Unlike the plan-stability suite, this keys real `DataSourceExec`
    /// scans with the production codec.
    async fn stage_counts(query_num: usize) -> (usize, usize) {
        let ctx = datafusion_test_context("testdata").await.unwrap();
        let sql =
            std::fs::read_to_string(format!("../../benchmarks/queries/q{query_num}.sql"))
                .unwrap_or_else(|e| panic!("read q{query_num}.sql: {e}"));

        // Plan the last SELECT/WITH statement. Run any DDL before it as setup
        // (q15 creates a view) and ignore anything after it.
        let statements: Vec<&str> = sql
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        let query_idx = statements
            .iter()
            .rposition(|s| {
                let l = s.to_lowercase();
                l.starts_with("select") || l.starts_with("with")
            })
            .unwrap_or_else(|| panic!("q{query_num}: no SELECT/WITH statement found"));
        for stmt in &statements[..query_idx] {
            ctx.sql(stmt).await.unwrap().collect().await.unwrap();
        }
        let plan = ctx
            .sql(statements[query_idx])
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let job: JobId = Uuid::new_v4().to_string().into();
        let options = ctx.state().config().options().clone();
        let before = DefaultDistributedPlanner::new()
            .plan_query_stages(&job, plan, &options)
            .unwrap();
        let n_before = before.len();
        let codec = BallistaPhysicalExtensionCodec::default();
        let key = move |p: &Arc<dyn ExecutionPlan>| protobuf_canonical_key(p, &codec);
        let after = reuse_shuffle_stages(before, &options, &key).unwrap();
        (n_before, after.len())
    }

    #[tokio::test]
    async fn tpch_exchange_reuse_detected() {
        // (query, whether it has a structurally identical exchange to collapse)
        const EXPECT: &[(usize, bool)] = &[
            (2, true),
            (11, true),
            (14, false),
            (15, true),
            (17, false),
            (20, false),
            (21, false),
        ];

        for &(q, expect_reuse) in EXPECT {
            let (before, after) = stage_counts(q).await;
            assert_eq!(after < before, expect_reuse, "q{q}: {before} -> {after}");
        }
    }
}
