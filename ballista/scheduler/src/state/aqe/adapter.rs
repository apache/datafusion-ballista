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

use crate::planner::create_shuffle_writer_with_config;
use crate::state::aqe::execution_plan::{
    AdaptiveDatafusionExec, ExchangeExec, RangeRepartitionRouting,
};
use crate::state::aqe::planner::AdaptiveStageInfo;
use crate::state::execution_graph::StageOutput;
use ballista_core::JobId;
use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{
    RangeFilterExec, RangeShuffleReaderExec, ShuffleReaderExec, ShuffleWriter,
    ShuffleWriterExec,
};
use datafusion::common::exec_err;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    ExecutionPlanProperties, Partitioning, replace_children_if_necessary,
};
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    physical_plan::ExecutionPlan,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Mark the stage's writer when its output will be read back by an
/// ordering-preserving reader, so each task can merge its partitions before
/// writing and leave the consumer one source per task.
///
/// Only place that knows both sides: `build_reader` plants
/// `RangeShuffleReaderExec` for this same exchange under the conditions checked
/// here. The static planner never calls it, so it is never marked.
fn mark_merge_per_task(
    writer: Arc<dyn ShuffleWriter>,
    exchange: &ExchangeExec,
    config: &ConfigOptions,
) -> Arc<dyn ShuffleWriter> {
    let ballista = config.extensions.get::<BallistaConfig>();
    let enabled = ballista
        .map(|c| c.shuffle_merge_ordered_passthrough() && !c.coalesce_enabled())
        .unwrap_or(false);
    if !enabled || exchange.broadcast || exchange.input().output_ordering().is_none() {
        return writer;
    }
    let as_plan: &dyn ExecutionPlan = writer.as_ref();
    match as_plan.downcast_ref::<ShuffleWriterExec>() {
        Some(w) => Arc::new(w.clone().with_merge_per_task(true)),
        None => writer,
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BallistaAdapter {
    inputs: HashMap<usize, StageOutput>,
}

///
/// Used to transform plan nodes used in adaptive planning
/// to ballista specific nodes such as
/// ShuffleWriterExec/SortShuffleWriterExec and [ShuffleReaderExec]
///
impl BallistaAdapter {
    /// Build the reader that replaces `exchange`, recording its upstream
    /// stage as an input of this stage. `fetch` is a row limit from the
    /// consumer; only the ordered reader can honor it.
    fn build_reader(
        &mut self,
        exchange: &ExchangeExec,
        fetch: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = exchange.schema().clone();
        let partitions = exchange.shuffle_partitions().ok_or_else(|| {
            DataFusionError::Execution(
                "partitions have to be resolved at this point".to_string(),
            )
        })?;

        let stage_id = exchange.stage_id().ok_or_else(|| {
            DataFusionError::Execution(
                "stage ID has to be generated at this point".to_string(),
            )
        })?;
        let mut stage_output = StageOutput::new();
        for partition in partitions.iter().flatten().cloned() {
            stage_output.add_partition(partition);
        }
        stage_output.complete = true;
        self.inputs.insert(stage_id, stage_output);
        let partitioning = exchange.properties().partitioning.clone();

        Ok(match (exchange.coalesce(), exchange.broadcast) {
            (Some(cp), false) => {
                // Concatenate M-shape locations into K-shape per CoalescePlan.groups.
                let k_shape: Vec<Vec<_>> = cp
                    .groups
                    .iter()
                    .map(|pg| {
                        let mut concat = Vec::new();
                        for &idx in &pg.upstream_indices {
                            if let Some(inner) = partitions.get(idx as usize) {
                                concat.extend_from_slice(inner);
                            }
                        }
                        concat
                    })
                    .collect();
                let new_partitioning = match &partitioning {
                    Partitioning::Hash(keys, _m) => {
                        Partitioning::Hash(keys.clone(), cp.groups.len())
                    }
                    _ => Partitioning::UnknownPartitioning(cp.groups.len()),
                };
                Arc::new(ShuffleReaderExec::try_new_coalesced(
                    stage_id,
                    k_shape,
                    (*cp).clone(),
                    schema,
                    new_partitioning,
                )?)
            }
            (None, false) => {
                // Ordered-writer path: when the child declared an output
                // ordering, preserve it across the shuffle boundary with a
                // k-way merge instead of the arrival-order concat that the
                // regular reader does.
                if let Some(ordering) = exchange.input().output_ordering() {
                    Arc::new(
                        RangeShuffleReaderExec::try_new(
                            stage_id,
                            partitions,
                            schema,
                            ordering.clone(),
                        )?
                        .with_fetch_limit(fetch),
                    )
                } else {
                    Arc::new(ShuffleReaderExec::try_new(
                        stage_id,
                        partitions,
                        schema,
                        partitioning,
                    )?)
                }
            }
            (_, true) => Arc::new(ShuffleReaderExec::try_new_broadcast(
                stage_id,
                exchange.shuffle_partitions_flattened(),
                schema,
                exchange.input().output_partitioning().partition_count(),
            )?),
        })
    }

    fn transform_children(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        // A merge with a row limit on top of an exchange: build the reader
        // with the limit already set. Both merge on the same ordering, so the
        // consumer's first `n` rows come from the reader's first `n`.
        if let Some(spm) = plan.downcast_ref::<SortPreservingMergeExec>()
            && let Some(fetch) = spm.fetch()
            && let Some(exchange) = spm.input().downcast_ref::<ExchangeExec>()
        {
            let reader = self.build_reader(exchange, Some(fetch))?;
            return Ok(Transformed::yes(replace_children_if_necessary(
                plan,
                vec![reader],
            )?));
        }

        if let Some(exchange) = plan.downcast_ref::<ExchangeExec>() {
            return Ok(Transformed::yes(self.build_reader(exchange, None)?));
        }

        Ok(Transformed::no(plan))
    }

    /// Converts Adaptive plan to plan which ballista expects
    /// This is to be used to convert [ExchangeExec] to
    /// ShuffleWriterExec/SortShuffleWriterExec and [ShuffleReaderExec]
    pub fn adapt_to_ballista(
        plan: Arc<dyn ExecutionPlan>,
        job_id: &JobId,
        config: &ConfigOptions,
    ) -> datafusion::error::Result<AdaptiveStageInfo> {
        if let Some(root) = plan.downcast_ref::<ExchangeExec>() {
            let mut adapter = BallistaAdapter::default();
            resolve_range_filter_cuts(root.input())?;
            let plan = root
                .input()
                .clone()
                .transform_down(|e| adapter.transform_children(e))?
                .data;
            let stage_id = root.stage_id().ok_or_else(|| {
                DataFusionError::Execution(
                    "shuffle partitions have to be resolved at this point".to_string(),
                )
            })?;
            let partitioning = root.partitioning.clone();

            let writer = create_shuffle_writer_with_config(
                job_id,
                stage_id,
                plan,
                partitioning,
                config,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let writer = mark_merge_per_task(writer, root, config);

            Ok(AdaptiveStageInfo {
                plan: writer,
                inputs: adapter.inputs,
            })
        } else if let Some(root) = plan.downcast_ref::<AdaptiveDatafusionExec>() {
            let mut adapter = BallistaAdapter::default();
            resolve_range_filter_cuts(root.input())?;
            let plan = root
                .input()
                .clone()
                .transform_down(|e| adapter.transform_children(e))?
                .data;
            let stage_id = root.stage_id().ok_or_else(|| {
                DataFusionError::Execution(
                    "shuffle partitions have to be resolved at this point".to_string(),
                )
            })?;

            let writer =
                create_shuffle_writer_with_config(job_id, stage_id, plan, None, config)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(AdaptiveStageInfo {
                plan: writer,
                inputs: adapter.inputs,
            })
        } else {
            exec_err!(
                "Root exec expected to be either ExchangeExec or AdaptiveDatafusionExec"
            )
        }
    }
}

/// Walk `plan` and resolve every pending [`RangeFilterExec`]'s bounds from
/// its own descendant boundary `ExchangeExec`'s stored routing. Called at
/// `adapt_to_ballista` time, once the upstream stage's sketches have
/// merged into cuts and been parked on the boundary `ExchangeExec` via
/// `set_repartition_routing`.
///
/// Pairing is by tree structure — each RFE descends its own subtree to
/// the first `ExchangeExec` and takes that exchange's routing. Using
/// `PhysicalExpr::eq` on routing exprs alone would collide in
/// multi-legged shapes (e.g. SMJ with range-repartition on both sides
/// sharing the same `Column(name, idx)` after independent projections);
/// descent is unique by construction because every RFE has exactly one
/// child. The RFE's own `routing_expr` is then cross-checked against
/// the descendant exchange's for a plant-time invariant assert — if they
/// disagree, the rule wired the wrong RFE to the boundary.
///
/// RFE receives *unwidened* half-open ranges (`(cuts[k-1], cuts[k])` with ±∞
/// sentinels at ends). RFE widens by its own halos internally at
/// `resolve_bounds` time — see the separation-of-concerns note on
/// [`RangeFilterExec`]. The scheduler stays halo-blind at this boundary.
///
/// Errors if descent hits a fork (multi-child op) before reaching an
/// `ExchangeExec`, if the RFE has anything other than 1 child, if the
/// descendant `ExchangeExec` has no resolved routing yet, or if its
/// routing_expr disagrees with the RFE's — all four are plant-time or
/// stage-progress bugs.
fn resolve_range_filter_cuts(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<(), DataFusionError> {
    plan.apply(|node| {
        let Some(rf) = node.downcast_ref::<RangeFilterExec>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        if rf.raw_bounds().is_some() {
            return Ok(TreeNodeRecursion::Continue);
        }
        let children = rf.children();
        let [child] = children.as_slice() else {
            return datafusion::common::internal_err!(
                "RangeFilterExec must have exactly 1 child, got {}",
                children.len()
            );
        };
        let routing = descend_to_boundary_routing(child)?;
        if !rf.filter_expr().eq(&routing.routing_expr) {
            return datafusion::common::internal_err!(
                "RangeFilterExec filter_expr `{}` disagrees with its descendant \
                 boundary ExchangeExec's routing_expr `{}` — plant-time invariant \
                 broken",
                rf.filter_expr(),
                routing.routing_expr
            );
        }
        let raw_bounds = raw_bounds_from_cuts(&routing.cuts);
        rf.resolve_bounds(raw_bounds)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Descend the single-child spine below a `RangeFilterExec` until we
/// hit an `ExchangeExec`, and return its `range_repartition_routing`.
/// Forks in the spine are shape violations because a range-filter only
/// makes sense above a single-input boundary.
fn descend_to_boundary_routing(
    start: &Arc<dyn ExecutionPlan>,
) -> Result<RangeRepartitionRouting, DataFusionError> {
    let mut node = Arc::clone(start);
    loop {
        if let Some(exchange) = node.downcast_ref::<ExchangeExec>() {
            return exchange.range_repartition_routing().ok_or_else(|| {
                DataFusionError::Internal(
                    "RangeFilterExec's descendant ExchangeExec has no resolved \
                     range-repartition routing yet — stage progress skipped a step"
                        .into(),
                )
            });
        }
        let children = node.children();
        let [child] = children.as_slice() else {
            return datafusion::common::internal_err!(
                "RangeFilterExec descent hit a fork at `{}` ({} children) — cannot \
                 pair with a single boundary",
                node.name(),
                children.len()
            );
        };
        node = Arc::clone(*child);
    }
}

/// Project K-1 cuts to K half-open `(cuts[k-1], cuts[k])` ranges with `None`
/// sentinels at ±∞. This is the pure range-partitioning projection — no halo
/// arithmetic here (RFE widens internally at resolve time).
fn raw_bounds_from_cuts(
    cuts: &[ScalarValue],
) -> Vec<(Option<ScalarValue>, Option<ScalarValue>)> {
    let k = cuts.len() + 1;
    (0..k)
        .map(|i| {
            let lo = i.checked_sub(1).and_then(|j| cuts.get(j).cloned());
            let hi = cuts.get(i).cloned();
            (lo, hi)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::execution_plans::RangeShuffleReaderExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;

    fn f64_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]))
    }

    fn asc(schema: &Arc<Schema>, col: &str) -> PhysicalSortExpr {
        let column = Column::new_with_schema(col, schema).unwrap();
        PhysicalSortExpr::new_default(Arc::new(column))
    }

    /// When the exchange's child declares an output ordering, the adapter
    /// must plant `RangeShuffleReaderExec` so the shuffle boundary preserves
    /// sortedness via k-way merge.
    #[test]
    fn plants_range_reader_when_child_declares_ordering() {
        let schema = f64_schema();
        let empty: Vec<Vec<RecordBatch>> = vec![vec![]];
        let source =
            MemorySourceConfig::try_new_exec(&empty, schema.clone(), None).unwrap();
        let sort_lex = LexOrdering::new(vec![asc(&schema, "v")]).unwrap();
        let sorted =
            Arc::new(SortExec::new(sort_lex, source).with_preserve_partitioning(true))
                as Arc<dyn ExecutionPlan>;

        let exchange = ExchangeExec::new(sorted, None, 0);
        exchange.set_stage_id(1);
        exchange.resolve_shuffle_partitions(vec![vec![]]);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(exchange);

        let mut adapter = BallistaAdapter::default();
        let out = adapter.transform_children(plan).unwrap().data;

        assert!(
            out.downcast_ref::<RangeShuffleReaderExec>().is_some(),
            "expected RangeShuffleReaderExec but got {}",
            out.name()
        );
    }

    /// The unordered path must still plant the regular `ShuffleReaderExec`.
    #[test]
    fn plants_regular_reader_when_no_ordering() {
        let schema = f64_schema();
        let empty: Vec<Vec<RecordBatch>> = vec![vec![]];
        let source =
            MemorySourceConfig::try_new_exec(&empty, schema.clone(), None).unwrap();

        let exchange = ExchangeExec::new(source, None, 0);
        exchange.set_stage_id(1);
        exchange.resolve_shuffle_partitions(vec![vec![]]);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(exchange);

        let mut adapter = BallistaAdapter::default();
        let out = adapter.transform_children(plan).unwrap().data;

        assert!(
            out.downcast_ref::<ShuffleReaderExec>().is_some(),
            "expected ShuffleReaderExec but got {}",
            out.name()
        );
        assert!(out.downcast_ref::<RangeShuffleReaderExec>().is_none());
    }

    /// The consuming merge's row limit must reach the reader. The merge is
    /// rebuilt around the new reader, so a dropped limit is silent.
    #[test]
    fn pushes_consumer_limit_into_range_reader() {
        let schema = f64_schema();
        let empty: Vec<Vec<RecordBatch>> = vec![vec![]];
        let source =
            MemorySourceConfig::try_new_exec(&empty, schema.clone(), None).unwrap();
        let sort_lex = LexOrdering::new(vec![asc(&schema, "v")]).unwrap();
        let sorted = Arc::new(
            SortExec::new(sort_lex.clone(), source).with_preserve_partitioning(true),
        ) as Arc<dyn ExecutionPlan>;

        let exchange = ExchangeExec::new(sorted, None, 0);
        exchange.set_stage_id(1);
        exchange.resolve_shuffle_partitions(vec![vec![]]);
        let merge = Arc::new(
            SortPreservingMergeExec::new(sort_lex, Arc::new(exchange))
                .with_fetch(Some(7)),
        ) as Arc<dyn ExecutionPlan>;

        let mut adapter = BallistaAdapter::default();
        let out = adapter.transform_children(merge).unwrap().data;

        let children = out.children();
        let reader = children[0]
            .downcast_ref::<RangeShuffleReaderExec>()
            .unwrap_or_else(|| {
                panic!("expected a range reader, got {}", children[0].name())
            });
        assert_eq!(reader.fetch(), Some(7));
    }
}
