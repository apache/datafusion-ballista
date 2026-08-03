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
use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::planner::AdaptiveStageInfo;
use crate::state::execution_graph::StageOutput;
use ballista_core::JobId;
use ballista_core::execution_plans::{
    RangeFilterExec, RangeShuffleReaderExec, ShuffleReaderExec,
};
use datafusion::common::exec_err;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning};
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    physical_plan::ExecutionPlan,
};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

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
    fn transform_children(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exchange) = plan.downcast_ref::<ExchangeExec>() {
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

            let reader: Arc<dyn ExecutionPlan> =
                match (exchange.coalesce(), exchange.broadcast) {
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
                        // regular reader does. Fixes the silent RANGE-frame
                        // corruption in `docs/developer/parallel-range-window.md`.
                        if let Some(ordering) = exchange.input().output_ordering() {
                            Arc::new(RangeShuffleReaderExec::try_new(
                                stage_id,
                                partitions,
                                schema,
                                ordering.clone(),
                            )?)
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
                };
            // The adapter no longer injects a `RangeFilterExec` above the
            // reader. Rules that emit a range-repartition upstream (today:
            // `ParallelWindowRule`) are also responsible for planting the
            // read-side `RangeFilterExec`(s) at plan time with `cuts=None`;
            // the scheduler-side `resolve_range_filter_cuts` walker fills
            // in cuts once stage-0's sketches merge. If a range-routing
            // ExchangeExec reaches this point without a rule-planted
            // filter, `RangeFilterExec::execute()` fails loud rather than
            // straddling sub-parts silently corrupting downstream partial
            // aggregates.
            if exchange.range_repartition_routing().is_some() {
                debug!(
                    "range-repartition: ExchangeExec has resolved routing for stage {stage_id}; \
                     any rule-planted RangeFilterExec above should have been resolved by now"
                );
            }
            Ok(Transformed::yes(reader))
        } else {
            Ok(Transformed::no(plan))
        }
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

/// Walk `plan` and resolve every pending [`RangeFilterExec`]'s cuts from
/// its downstream [`ExchangeExec`]'s stored routing. Called at
/// `adapt_to_ballista` time, once stage-0's sketches have merged into
/// cuts and been parked on the boundary `ExchangeExec` via
/// `set_repartition_routing`.
///
/// Cross-referencing is by `routing_expr` equality — the rule plants both
/// wide (below SPM) and narrow (above BWAG) filters with the same routing
/// expression, both pointing at the same shuffle boundary.
///
/// Errors if a `RangeFilterExec` is still pending after the walk: the
/// rule promised cuts and the scheduler didn't deliver, which is either
/// a routing_expr mismatch or a stage-progress bug.
fn resolve_range_filter_cuts(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<(), DataFusionError> {
    let mut routings: Vec<(Arc<dyn PhysicalExpr>, Vec<f64>)> = Vec::new();
    plan.apply(|node| {
        if let Some(exchange) = node.downcast_ref::<ExchangeExec>()
            && let Some(routing) = exchange.range_repartition_routing()
        {
            routings.push((routing.routing_expr, routing.cuts));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    plan.apply(|node| {
        let Some(rf) = node.downcast_ref::<RangeFilterExec>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        if rf.cuts().is_some() {
            return Ok(TreeNodeRecursion::Continue);
        }
        let rf_expr = rf.routing_expr();
        let cuts = routings
            .iter()
            .find(|(expr, _)| expr.eq(rf_expr))
            .map(|(_, cuts)| cuts.clone())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "RangeFilterExec: no matching ExchangeExec routing for expr {rf_expr}"
                ))
            })?;
        let cuts_sv: Vec<ScalarValue> = cuts
            .into_iter()
            .map(|v| ScalarValue::Float64(Some(v)))
            .collect();
        rf.resolve_cuts(cuts_sv)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
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
}
