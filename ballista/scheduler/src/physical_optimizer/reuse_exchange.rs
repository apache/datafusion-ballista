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
//! rewired to the single surviving `stage_id`. See the design spec for details.

use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::error::Result;
use ballista_core::execution_plans::{ShuffleWriter, UnresolvedShuffleExec};
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::{ExecutionPlan, with_new_children_if_necessary};
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use log::debug;

use crate::planner::create_shuffle_writer_with_config;

/// Produces a faithful byte key for a physical plan, or `None` if the plan
/// cannot be canonicalized (in which case it is never reused — conservative).
pub type Canonicalizer<'a> =
    dyn Fn(&Arc<dyn ExecutionPlan>) -> Result<Option<Vec<u8>>> + 'a;

/// Reuse canonical key: serialize a plan to protobuf and use the encoded bytes
/// as its structural identity key. Returns `Ok(None)` (meaning "never reuse" —
/// a performance miss, never a wrong result) if the plan cannot be converted or
/// encoded by `extension_codec`. This is the single source of truth for the key;
/// both the scheduler's production canonicalizer and the plan-stability test
/// fixture call it so their reuse decisions cannot drift.
pub fn protobuf_canonical_key<U: AsExecutionPlan>(
    plan: &Arc<dyn ExecutionPlan>,
    extension_codec: &dyn PhysicalExtensionCodec,
) -> Result<Option<Vec<u8>>> {
    match U::try_from_physical_plan(plan.clone(), extension_codec) {
        Ok(node) => {
            let mut buf = Vec::new();
            match node.try_encode(&mut buf) {
                Ok(()) => Ok(Some(buf)),
                Err(_) => Ok(None),
            }
        }
        Err(_) => Ok(None),
    }
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
    stages: Vec<Arc<dyn ShuffleWriter>>,
    config: &ConfigOptions,
    canonical: &Canonicalizer<'_>,
) -> Result<Vec<Arc<dyn ShuffleWriter>>> {
    if stages.len() < 2 {
        return Ok(stages);
    }

    let root_id = stages.iter().map(|s| s.stage_id()).max().unwrap();

    let mut ordered = stages;
    ordered.sort_by_key(|s| s.stage_id());

    // dropped stage_id -> surviving representative stage_id
    let mut remap: HashMap<usize, usize> = HashMap::new();
    // canonical bytes -> surviving representative stage_id
    let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut kept: Vec<Arc<dyn ShuffleWriter>> = Vec::with_capacity(ordered.len());

    for stage in ordered {
        let stage_id = stage.stage_id();
        let job_id = stage.job_id().clone();
        let partitioning = stage.shuffle_output_partitioning().cloned();

        // 1. Rewrite nested UnresolvedShuffleExec refs using accumulated remap.
        let child = stage.children()[0].clone();
        let rewritten_child = rewrite_shuffle_refs(child, &remap)?;

        // 2. Rebuild this writer with the rewritten child (original stage_id).
        let rewritten_stage = create_shuffle_writer_with_config(
            &job_id,
            stage_id,
            rewritten_child.clone(),
            partitioning.clone(),
            config,
        )?;

        // The root stage is the query output; never dedup it.
        if stage_id == root_id {
            kept.push(rewritten_stage);
            continue;
        }

        // 3. Key = canonical bytes of a stage-id-normalized writer. Normalizing
        //    the id to 0 (never a real stage id — the planner starts at 1) means
        //    the key captures input + partitioning + writer kind but excludes the
        //    distinguishing stage_id.
        let normalized: Arc<dyn ExecutionPlan> = create_shuffle_writer_with_config(
            &job_id,
            0,
            rewritten_child,
            partitioning,
            config,
        )?;

        // Protobuf encoding is not guaranteed deterministic, but that's safe here:
        // a spurious key difference only costs a missed reuse, never a false
        // merge, since a `seen` hit is only ever used to confirm identity.
        match canonical(&normalized)? {
            Some(key) => match seen.get(&key) {
                Some(&rep) => {
                    debug!("exchange reuse: stage {stage_id} reuses stage {rep}");
                    remap.insert(stage_id, rep);
                }
                None => {
                    seen.insert(key, stage_id);
                    kept.push(rewritten_stage);
                }
            },
            None => kept.push(rewritten_stage),
        }
    }

    Ok(kept)
}

/// Recursively rewrite every `UnresolvedShuffleExec` whose `stage_id` appears in
/// `remap`, cloning the node and swapping only its `stage_id` (schema,
/// partitioning, broadcast and coalesce fields are preserved by the clone).
fn rewrite_shuffle_refs(
    plan: Arc<dyn ExecutionPlan>,
    remap: &HashMap<usize, usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(unresolved) = plan.downcast_ref::<UnresolvedShuffleExec>() {
        if let Some(&new_id) = remap.get(&unresolved.stage_id) {
            let mut rewritten = unresolved.clone();
            rewritten.stage_id = new_id;
            return Ok(Arc::new(rewritten));
        }
        return Ok(plan);
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let new_children = children
        .into_iter()
        .map(|c| rewrite_shuffle_refs(c.clone(), remap))
        .collect::<Result<Vec<_>>>()?;
    Ok(with_new_children_if_necessary(plan, new_children)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::JobId;
    use ballista_core::execution_plans::ShuffleWriterExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{Partitioning, displayable};
    use std::sync::Arc;

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

    fn writer(
        stage_id: usize,
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
    ) -> Arc<dyn ShuffleWriter> {
        Arc::new(
            ShuffleWriterExec::try_new(
                job(),
                stage_id,
                input,
                "".to_owned(),
                partitioning,
            )
            .unwrap(),
        )
    }

    fn collect_unresolved_ids(p: &Arc<dyn ExecutionPlan>, out: &mut Vec<usize>) {
        if let Some(u) = p.downcast_ref::<UnresolvedShuffleExec>() {
            out.push(u.stage_id);
        }
        for c in p.children() {
            collect_unresolved_ids(c, out);
        }
    }

    /// Stub canonicalizer: key by indented Display PLUS the concrete stage ids
    /// of any referenced `UnresolvedShuffleExec` (whose Display omits stage_id).
    /// Making the key stage-id-sensitive is what forces the nested-duplicate
    /// test to actually exercise the rewrite-refs-before-keying step.
    fn display_key(p: &Arc<dyn ExecutionPlan>) -> Result<Option<Vec<u8>>> {
        let mut ids = Vec::new();
        collect_unresolved_ids(p, &mut ids);
        let mut key = displayable(p.as_ref()).indent(false).to_string();
        key.push_str(&format!("|refs={ids:?}"));
        Ok(Some(key.into_bytes()))
    }

    fn union(children: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        UnionExec::try_new(children).unwrap()
    }

    fn config() -> ConfigOptions {
        ConfigOptions::default()
    }

    fn child_stage_ids(root: &Arc<dyn ShuffleWriter>) -> Vec<usize> {
        // root's writer child is the Union; collect its UnresolvedShuffleExec ids
        let union = root.children()[0].clone();
        union
            .children()
            .into_iter()
            .map(|c| {
                c.downcast_ref::<UnresolvedShuffleExec>()
                    .expect("expected UnresolvedShuffleExec under union")
                    .stage_id
            })
            .collect()
    }

    #[test]
    fn protobuf_canonical_key_encodes_and_distinguishes() {
        use ballista_core::serde::BallistaPhysicalExtensionCodec;
        use datafusion_proto::protobuf::PhysicalPlanNode;
        let codec = BallistaPhysicalExtensionCodec::default();
        // Reuse this module's `writer(...)` test helper (used by the
        // reuse_shuffle_stages tests, e.g. `identical_pair_collapses`) to build a
        // ShuffleWriter-rooted stage plan.
        let a: Arc<dyn ExecutionPlan> = writer(1, leaf(), Some(hash(4)));
        let b: Arc<dyn ExecutionPlan> = writer(1, leaf(), Some(hash(4)));
        let ka = protobuf_canonical_key::<PhysicalPlanNode>(&a, &codec).unwrap();
        let kb = protobuf_canonical_key::<PhysicalPlanNode>(&b, &codec).unwrap();
        assert!(
            ka.is_some(),
            "ballista stage plan should encode to Some(key)"
        );
        assert_eq!(ka, kb, "structurally identical stages produce equal keys");
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
        let root = out.iter().find(|s| s.stage_id() == 3).unwrap();
        assert_eq!(
            child_stage_ids(root),
            vec![1, 1],
            "both consumers point at stage 1"
        );
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
        let root = out.iter().find(|s| s.stage_id() == 5).unwrap();
        assert_eq!(
            child_stage_ids(root),
            vec![2, 2],
            "both outer consumers point at stage 2"
        );
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
        let never = |_: &Arc<dyn ExecutionPlan>| -> Result<Option<Vec<u8>>> { Ok(None) };
        let stages = vec![
            writer(1, leaf(), Some(hash(4))),
            writer(2, leaf(), Some(hash(4))),
            writer(3, union(vec![unresolved(1), unresolved(2)]), None),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &never).unwrap();
        assert_eq!(out.len(), 3, "un-canonicalizable stages are never merged");
    }

    #[test]
    fn reused_stage_fans_out_to_all_consumers() {
        use crate::state::execution_graph::ExecutionStageBuilder;
        use datafusion::prelude::SessionConfig;

        let stages = vec![
            writer(1, leaf(), Some(hash(4))),
            writer(2, leaf(), Some(hash(4))),
            writer(3, union(vec![unresolved(1), unresolved(2)]), None),
        ];
        let out = reuse_shuffle_stages(stages, &config(), &display_key).unwrap();

        let built = ExecutionStageBuilder::new(Arc::new(SessionConfig::new()))
            .build(out)
            .unwrap();
        // The final (union) stage consumes stage 1 twice → stage 1 has exactly
        // one output link (deduped) but stage 3 depends only on stage 1 now.
        // Assert stage 2 no longer exists and stage 1 feeds the root.
        assert!(!built.contains_key(&2), "duplicate stage 2 was dropped");
        assert!(built.contains_key(&1) && built.contains_key(&3));

        // Prove the fan-out: stage 1 (a leaf, no unresolved deps of its own) must
        // be a Resolved stage whose output_links point solely at the root stage 3,
        // with the two consumer edges deduped into a single link.
        match built.get(&1).expect("stage 1 present") {
            crate::state::execution_stage::ExecutionStage::Resolved(s) => {
                assert_eq!(
                    s.output_links,
                    vec![3],
                    "shared stage 1 must feed only the root stage 3"
                );
            }
            other => panic!("stage 1 should be a Resolved leaf stage, got {other:?}"),
        }
    }

    #[test]
    fn reused_stage_fans_out_to_distinct_consumers() {
        use crate::state::execution_graph::ExecutionStageBuilder;
        use datafusion::prelude::SessionConfig;

        // stage 1 and stage 2 are byte-identical exchanges; stage 3 and stage 4
        // are two DISTINCT consumer stages, each reading one of them (not a
        // single consumer referencing both, as in `..._fans_out_to_all_consumers`).
        // Their own output partitioning differs (hash(2) vs hash(3)) so they
        // stay distinct consumers themselves and aren't *also* deduped into
        // each other — the point being tested is that stage 1 alone (not 3/4)
        // ends up with two output links. After reuse, stage 2 is dropped and
        // stage 4 is rewritten to read stage 1, so stage 1 must fan out to two
        // different downstream stages — the production shape (e.g. TPC-H q15's
        // `revenue0` read by both the main FROM and a subquery).
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

        assert!(!built.contains_key(&2), "duplicate stage 2 was dropped");
        assert_eq!(built.len(), 4, "stages 1, 3, 4, 5 remain");

        match built.get(&1).expect("stage 1 present") {
            crate::state::execution_stage::ExecutionStage::Resolved(s) => {
                assert!(
                    s.output_links.len() == 2
                        && s.output_links.contains(&3)
                        && s.output_links.contains(&4),
                    "shared stage 1 must fan out to distinct consumer stages 3 and 4, got {:?}",
                    s.output_links
                );
            }
            other => panic!("stage 1 should be a Resolved leaf stage, got {other:?}"),
        }
    }

    // --- TPC-H detection ---------------------------------------------------

    use crate::planner::{DefaultDistributedPlanner, DistributedPlanner};
    use crate::test_utils::datafusion_test_context;
    use ballista_core::serde::BallistaCodec;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use uuid::Uuid;

    /// Real protobuf canonicalizer, matching the production closure. Delegates
    /// to the shared `protobuf_canonical_key` so there is a single source of
    /// truth for the key logic between production and this test module.
    fn protobuf_key(p: &Arc<dyn ExecutionPlan>) -> Result<Option<Vec<u8>>> {
        let codec = BallistaCodec::<
            datafusion_proto::protobuf::LogicalPlanNode,
            PhysicalPlanNode,
        >::default();
        protobuf_canonical_key::<PhysicalPlanNode>(p, codec.physical_extension_codec())
    }

    /// Plan a TPC-H query file and return (stages_before, stages_after_reuse).
    async fn stage_counts(query_num: usize) -> (usize, usize) {
        let ctx = datafusion_test_context("testdata").await.unwrap();
        let sql =
            std::fs::read_to_string(format!("../../benchmarks/queries/q{query_num}.sql"))
                .unwrap_or_else(|e| panic!("read q{query_num}.sql: {e}"));

        // A TPC-H file may contain leading DDL (e.g. CREATE VIEW) before the
        // real query and trailing DDL (e.g. DROP VIEW) after it. Plan the
        // last SELECT/WITH statement; run everything before it as setup;
        // ignore anything after it.
        let statements: Vec<&str> = sql
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        let query_idx = statements
            .iter()
            .rposition(|s| {
                let l = s.trim_start().to_lowercase();
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
            .into_optimized_plan()
            .unwrap();
        let plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        let mut planner = DefaultDistributedPlanner::new();
        let job: JobId = Uuid::new_v4().to_string().into();
        let options = ctx.state().config().options().clone();
        let before = planner.plan_query_stages(&job, plan, &options).unwrap();
        let n_before = before.len();
        let after = reuse_shuffle_stages(before, &options, &protobuf_key).unwrap();
        (n_before, after.len())
    }

    #[tokio::test]
    async fn tpch_exchange_reuse_detected() {
        // Populated from the empirical run of `explore_tpch_exchange_reuse`
        // (see task-4-report.md). `true` = query contains a
        // structurally-identical exchange that reuse must collapse.
        //
        // q15's file (`benchmarks/queries/q15.sql`) has 3 statements:
        // `CREATE VIEW revenue0 ...`, the real `SELECT ... FROM supplier,
        // revenue0 ...`, then `DROP VIEW revenue0;`. `stage_counts` now plans
        // the last SELECT/WITH statement (running the CREATE VIEW as setup
        // and ignoring the trailing DROP VIEW), and revenue0's two
        // references make it the flagship reuse case: 6 stages collapse to
        // 5.
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
            if expect_reuse {
                assert!(
                    after < before,
                    "q{q}: expected exchange reuse but stages stayed {before}",
                );
            } else {
                assert_eq!(
                    after, before,
                    "q{q}: expected no reuse but stages changed {before} -> {after}",
                );
            }
        }
        // Guard against a silent no-op regression: at least one TPC-H query must
        // exercise reuse.
        assert!(
            EXPECT.iter().any(|&(_, e)| e),
            "no TPC-H query exercises exchange reuse — see task-4-report.md",
        );
    }
}
