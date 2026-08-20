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

//! Module of helpers for executor-side capture of finalized window-aggregate state.
//!
//! DataFusion's `BoundedWindowAggExec` fires
//! [`WindowStateObserver::finalize_window_aggregate`] once per (output
//! partition, window expression, PARTITION BY tuple) as each group closes.
//! [`WindowStateCollector`] catches those and holds them until the executor
//! drains them at task completion. One task's captures are its contribution
//! to a cross-task prefix scan: the scheduler merges the contributions of
//! all prior partitions and bakes the result into a downstream
//! `PrefixMergeExec`.

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};

use datafusion::common::{Result, ScalarValue, internal_datafusion_err, internal_err};
use datafusion::physical_expr::window::{PartitionKey, PlainAggregateWindowExpr};
use datafusion::physical_plan::windows::{WindowExpr, WindowStateObserver};
use log::debug;

use crate::execution_plans::prefix_merge::FinalizedPartitionState;
use crate::serde::protobuf::WindowStateReport;

/// One finalized window-aggregate state, as DataFusion reported it.
/// Preserves all the info from the callback for storage.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedWindowState {
    /// Output partition index of the `BoundedWindowAggExec` stream that
    /// fired. Task-local: the scheduler restricted this task to a partition
    /// slice, so this indexes within the slice, not globally.
    pub partition_idx: usize,
    /// Position of the window expression in the exec's `window_expr()` list.
    ///
    /// DataFusion hands the callback an `&Arc<dyn WindowExpr>`; the collector
    /// resolves it back to this index against the list it was built from.
    pub window_expr_index: usize,
    /// The PARTITION BY tuple that closed. Empty when the window has no
    /// PARTITION BY, which is the only shape the prefix-scan rule plants
    /// today.
    pub partition_key: PartitionKey,
    /// `Accumulator::state` for the closed group: 1 element for SUM / COUNT /
    /// MIN / MAX, 2 for AVG's `(sum, count)`, 1 opaque `Binary` for
    /// sketch-backed aggregates like `approx_distinct`.
    pub state: Vec<ScalarValue>,
}

/// Captures every [`ObservedWindowState`] a `BoundedWindowAggExec` publishes.
///
/// Shared by `Arc` between the plan node that installs it and whatever drains
/// it after the task completes. Interior mutability is required because the
/// observer callback takes `&self`.
pub struct WindowStateCollector {
    /// The list the callback's `&Arc<dyn WindowExpr>` is resolved against.
    /// Must hold the same `Arc`s the observed exec does — the exec clones the
    /// `Vec` rather than the expressions when building its stream, so pointer
    /// identity survives.
    window_expr: Vec<Arc<dyn WindowExpr>>,
    observed: Mutex<Vec<ObservedWindowState>>,
}

impl WindowStateCollector {
    /// Build a collector resolving callbacks against `window_expr`.
    pub fn new(window_expr: Vec<Arc<dyn WindowExpr>>) -> Self {
        Self {
            window_expr,
            observed: Mutex::new(Vec::new()),
        }
    }

    /// Every state captured so far, in the order DataFusion published them.
    ///
    /// That is close order, not partition order: a group closing mid-stream
    /// precedes one closing at end-of-stream. Callers needing a specific
    /// order sort on [`ObservedWindowState`]'s fields.
    pub fn observed(&self) -> Vec<ObservedWindowState> {
        self.observed
            .lock()
            .expect("WindowStateCollector mutex poisoned")
            .clone()
    }
}

impl Debug for WindowStateCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // The expression list is already rendered by the installing
        // operator's DisplayAs; only the capture count adds anything.
        f.debug_struct("WindowStateCollector")
            .field("window_exprs", &self.window_expr.len())
            .field("observed", &self.observed.lock().map(|o| o.len()).ok())
            .finish()
    }
}

impl WindowStateObserver for WindowStateCollector {
    fn finalize_window_aggregate(
        &self,
        partition_idx: usize,
        window_expr: &Arc<dyn WindowExpr>,
        partition_key: &PartitionKey,
        state: Vec<ScalarValue>,
    ) -> Result<()> {
        let window_expr_index = self
            .window_expr
            .iter()
            .position(|candidate| Arc::ptr_eq(candidate, window_expr))
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "WindowStateCollector: callback for `{}`, which is not among \
                     the {} expressions this collector was built from",
                    window_expr.name(),
                    self.window_expr.len()
                )
            })?;
        let observation = ObservedWindowState {
            partition_idx,
            window_expr_index,
            partition_key: partition_key.clone(),
            state,
        };
        // Captures are otherwise invisible until they surface as corrected
        // values two stages later, so this is the cheapest place to see what
        // a task actually published.
        debug!(
            "WindowStateCollector: partition {} expr {} key {:?} state {:?}",
            observation.partition_idx,
            observation.window_expr_index,
            observation.partition_key,
            observation.state,
        );
        self.observed
            .lock()
            .map_err(|_| internal_datafusion_err!("WindowStateCollector mutex poisoned"))?
            .push(observation);
        Ok(())
    }
}

/// Encode one capture for the wire, stamped with the **global** partition it
/// belongs to.
///
/// The global id is supplied by the caller rather than read off the
/// observation: [`ObservedWindowState::partition_idx`] is task-local, and only
/// the stage's writer holds the slice-to-global mapping.
///
/// # Arguments
///
/// * `global_partition_id` - stage-global output partition for this capture
/// * `observed` - the capture itself
pub fn window_state_to_proto(
    global_partition_id: usize,
    observed: &ObservedWindowState,
) -> Result<WindowStateReport> {
    Ok(WindowStateReport {
        global_partition_id: global_partition_id as u32,
        window_expr_index: observed.window_expr_index as u32,
        partition_key: scalars_to_proto(&observed.partition_key, "partition key")?,
        state: scalars_to_proto(&observed.state, "accumulator state")?,
    })
}

/// Decode a wire report back into `(global partition id, expr index, key,
/// state)`. Reverses [`window_state_to_proto`].
pub fn window_state_from_proto(
    proto: &WindowStateReport,
) -> Result<(usize, usize, Vec<ScalarValue>, Vec<ScalarValue>)> {
    Ok((
        proto.global_partition_id as usize,
        proto.window_expr_index as usize,
        scalars_from_proto(&proto.partition_key, "partition key")?,
        scalars_from_proto(&proto.state, "accumulator state")?,
    ))
}

fn scalars_to_proto(
    scalars: &[ScalarValue],
    what: &str,
) -> Result<Vec<datafusion_proto_common::ScalarValue>> {
    scalars
        .iter()
        .map(datafusion_proto_common::ScalarValue::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| internal_datafusion_err!("failed to encode {what} to proto: {e:?}"))
}

fn scalars_from_proto(
    proto: &[datafusion_proto_common::ScalarValue],
    what: &str,
) -> Result<Vec<ScalarValue>> {
    proto
        .iter()
        .map(ScalarValue::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            internal_datafusion_err!("failed to decode {what} from proto: {e:?}")
        })
}

/// A window-state report tagged with the task that produced it.
///
/// The tag is what makes the stage's accumulated reports purgeable. Reports
/// are append-only, and a task that is reset — retried, or lost with its
/// executor — re-runs its partition slice and reports the same global
/// partitions again under a fresh `task_id`. Without dropping the original
/// attempt's entries, the stage would hold two states for one partition and
/// the prefix merge would double-count them, which is a wrong running
/// aggregate rather than a degraded one.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskWindowState {
    /// Producer task's `task_id` at the time it emitted the report.
    pub producer_task_id: usize,
    /// The report itself, already addressed by stage-global partition.
    pub report: WindowStateReport,
}

/// Prefix-scan per-partition window state into one carry-in per partition.
///
/// `out[k]` is the merge of every partition strictly before `k`, so `out[0]`
/// is empty (nothing precedes partition 0), `out[1]` is partition 0's state,
/// `out[2]` is partitions 0 and 1 merged, and so on. That is what a
/// downstream `PrefixMergeExec` adds to each partition's local running
/// aggregate to make it global.
///
/// Merging goes through the aggregate's own `Accumulator::merge_batch`
/// rather than any arithmetic here, which is what lets non-decomposable
/// aggregates work: two `approx_distinct` HLL sketches combine correctly
/// while two distinct *counts* could not.
///
/// Built from the running prefix rather than from scratch each time:
/// `out[k]` is `merge(out[k-1], state[k-1])`, which the monoid's
/// associativity makes equivalent to merging every prior partition. That is
/// two merges per partition — O(K) for K partitions — where merging all
/// priors independently would be O(K²).
///
/// A fresh accumulator per partition is still required: `Accumulator::state`
/// is a destructive read for several built-in aggregates and must not be
/// called twice, so one accumulator cannot be advanced across the scan.
/// Seeding the fresh one from the previous prefix's state is the same
/// round trip a two-phase aggregation makes, and is what DataFusion's own
/// cross-task prefix test exercises for `approx_distinct`.
///
/// # Arguments
///
/// * `reports` - every report the stage accumulated, in any order
/// * `window_expr` - the upstream window operator's expressions, positionally
///   matching each report's `window_expr_index`
/// * `partition_count` - K, the stage's global output partition count
pub fn prefix_merge_window_state(
    reports: &[TaskWindowState],
    window_expr: &[Arc<dyn WindowExpr>],
    partition_count: usize,
) -> Result<Vec<FinalizedPartitionState>> {
    // (global partition, window expression) -> that group's finalized state.
    let mut states: HashMap<(usize, usize), Vec<ScalarValue>> = HashMap::new();
    for tagged in reports {
        let (partition, expr_index, partition_key, state) =
            window_state_from_proto(&tagged.report)?;
        // `FinalizedPartitionState` carries no PARTITION BY dimension, so a
        // second group in one partition has nowhere to go. The prefix rule
        // only plants no-PARTITION-BY windows, and a window that has one
        // needs no prefix scan anyway — `BoundedWindowAggExec` asks for
        // `KeyPartitioned` input, so each partition's window is already
        // independent. This assertion keeps that gate honest rather than
        // silently merging two groups.
        if !partition_key.is_empty() {
            return internal_err!(
                "prefix merge: window state for partition {partition} carries a \
                 PARTITION BY key {partition_key:?}; only no-PARTITION-BY windows \
                 are supported"
            );
        }
        if partition >= partition_count {
            return internal_err!(
                "prefix merge: window state for partition {partition} exceeds the \
                 stage's {partition_count} partitions"
            );
        }
        if expr_index >= window_expr.len() {
            return internal_err!(
                "prefix merge: window state for expression {expr_index} exceeds the \
                 operator's {} expressions",
                window_expr.len()
            );
        }
        if states.insert((partition, expr_index), state).is_some() {
            // Retries are purged by producer task, so a duplicate here means
            // that purge failed. Merging both would double-count.
            return internal_err!(
                "prefix merge: duplicate window state for partition {partition}, \
                 expression {expr_index}"
            );
        }
    }

    let mut prefixes: Vec<FinalizedPartitionState> = Vec::with_capacity(partition_count);
    for partition in 0..partition_count {
        let mut per_expr = Vec::with_capacity(window_expr.len());
        for (expr_index, expr) in window_expr.iter().enumerate() {
            // Nothing precedes partition 0. Beyond that, the carry-in is the
            // previous carry-in merged with the previous partition's own
            // state; either may be absent when a non-aggregate window
            // function publishes nothing.
            let carried = partition
                .checked_sub(1)
                .and_then(|prior| prefixes[prior].slot(expr_index));
            let preceding = partition
                .checked_sub(1)
                .and_then(|prior| states.get(&(prior, expr_index)));
            if carried.is_none() && preceding.is_none() {
                per_expr.push(None);
                continue;
            }
            let Some(plain) = expr.as_any().downcast_ref::<PlainAggregateWindowExpr>()
            else {
                return internal_err!(
                    "prefix merge: expression {expr_index} published state but is not \
                     a plain aggregate window expression; only ever-expanding frames \
                     can be prefix-merged"
                );
            };
            let mut accumulator = plain.get_aggregate_expr().create_accumulator()?;
            for state in [carried, preceding].into_iter().flatten() {
                let arrays = state
                    .iter()
                    .map(|scalar| scalar.to_array_of_size(1))
                    .collect::<Result<Vec<_>>>()?;
                accumulator.merge_batch(&arrays)?;
            }
            per_expr.push(Some(accumulator.state()?));
        }
        prefixes.push(FinalizedPartitionState::new(per_expr));
    }
    Ok(prefixes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::{
        WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::windows::create_window_expr;

    /// `sum(v) OVER (ORDER BY v ROWS UNBOUNDED PRECEDING TO CURRENT ROW)` —
    /// the ever-expanding shape the prefix rule plants.
    fn running_sum_expr() -> Arc<dyn WindowExpr> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let v = col("v", schema.as_ref()).expect("column v");
        create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            "sum(v)".to_string(),
            &[Arc::clone(&v)],
            &[],
            &[PhysicalSortExpr {
                expr: v,
                options: SortOptions::default(),
            }],
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
            Arc::clone(&schema),
            false,
            false,
            None,
        )
        .expect("window expr")
    }

    fn report(global_partition_id: u32, sum: f64) -> TaskWindowState {
        TaskWindowState {
            producer_task_id: global_partition_id as usize,
            report: window_state_to_proto(
                global_partition_id as usize,
                &ObservedWindowState {
                    partition_idx: 0,
                    window_expr_index: 0,
                    partition_key: vec![],
                    state: vec![ScalarValue::Float64(Some(sum))],
                },
            )
            .expect("encode"),
        }
    }

    fn sums(prefixes: &[FinalizedPartitionState]) -> Vec<Option<f64>> {
        prefixes
            .iter()
            .map(|per_expr| match per_expr.slot(0) {
                None => None,
                Some(state) => match &state[0] {
                    ScalarValue::Float64(v) => *v,
                    other => panic!("unexpected sum state {other:?}"),
                },
            })
            .collect()
    }

    /// The carry-ins for the client e2e's input: four partitions holding
    /// 1..4, 5..8, 9..12, 13..16, so per-partition sums 10/26/42/58 and
    /// carry-ins 0 / 10 / 36 / 78.
    #[test]
    fn prefix_scan_accumulates_prior_partitions() -> Result<()> {
        let exprs = vec![running_sum_expr()];
        let reports = vec![
            report(0, 10.0),
            report(1, 26.0),
            report(2, 42.0),
            report(3, 58.0),
        ];
        let prefixes = prefix_merge_window_state(&reports, &exprs, 4)?;
        assert_eq!(
            sums(&prefixes),
            vec![None, Some(10.0), Some(36.0), Some(78.0)],
            "partition k must carry the merge of every partition before it"
        );
        Ok(())
    }

    /// Report order is close order, not partition order — a task's
    /// end-of-stream group closes after a mid-stream one, and tasks complete
    /// in any order. The scan must key on the reported partition, not on
    /// arrival.
    #[test]
    fn prefix_scan_is_independent_of_report_order() -> Result<()> {
        let exprs = vec![running_sum_expr()];
        let forward = prefix_merge_window_state(
            &[report(0, 10.0), report(1, 26.0), report(2, 42.0)],
            &exprs,
            3,
        )?;
        let shuffled = prefix_merge_window_state(
            &[report(2, 42.0), report(0, 10.0), report(1, 26.0)],
            &exprs,
            3,
        )?;
        assert_eq!(sums(&forward), sums(&shuffled));
        assert_eq!(sums(&forward), vec![None, Some(10.0), Some(36.0)]);
        Ok(())
    }

    /// A partition that received no rows closes no group, so it publishes no
    /// state. The carry-in must step over the gap rather than reset: building
    /// each prefix from the previous one makes that automatic, but it is the
    /// case the incremental form could plausibly get wrong.
    #[test]
    fn prefix_scan_carries_across_a_partition_with_no_state() -> Result<()> {
        let exprs = vec![running_sum_expr()];
        // Partition 1 is empty; 0, 2 and 3 report.
        let prefixes = prefix_merge_window_state(
            &[report(0, 10.0), report(2, 42.0), report(3, 58.0)],
            &exprs,
            4,
        )?;
        assert_eq!(
            sums(&prefixes),
            vec![None, Some(10.0), Some(10.0), Some(52.0)],
            "an empty partition must neither reset the carry-in nor contribute"
        );
        Ok(())
    }

    /// A duplicate means the producer-task purge failed; merging both would
    /// double-count silently.
    #[test]
    fn prefix_scan_rejects_duplicate_partition_state() {
        let exprs = vec![running_sum_expr()];
        let err =
            prefix_merge_window_state(&[report(0, 10.0), report(0, 10.0)], &exprs, 2)
                .expect_err("duplicate must be rejected");
        assert!(
            err.to_string().contains("duplicate window state"),
            "unexpected error: {err}"
        );
    }
}
