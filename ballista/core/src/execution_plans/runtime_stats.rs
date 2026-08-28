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

//! Passthrough operator that accumulates runtime statistics on the data
//! flowing through it. Two accessor families:
//!
//! - **Row count** — always tracked, per input partition. Written via
//!   `AtomicUsize::fetch_add` on the hot path (no cross-partition
//!   contention, no lock overhead).
//! - **Quantile sketch** — optional (only when `order_by` is set at
//!   construction). A per-partition `Mutex` around each sketch keeps writes
//!   off any shared lock.
//!
//! Timing is decoupled from correctness: both accessors are readable at
//! any point. Callers get whatever has flowed through so far — a
//! mid-stream snapshot for callers that decide the sample is accurate
//! enough, a post-drain snapshot for callers that want the full state
//! (typical after a blocking downstream like `SortExec`).
//!
//! The `order_by` field accepts the full `Vec<PhysicalSortExpr>` so
//! multi-key `ORDER BY` survives serde (tie-breakers get preserved for
//! downstream `SortExec` / `BoundedWindowAggExec` even though only the
//! first key drives the sketch today).
//!
//! The sketch is a [`SortKeySketch`], which covers any fixed-width key and
//! gives NULLs a position per each sort key's `SortOptions::nulls_first`.
//!
//! This PR lands the tap in isolation: nothing wires it into a plan yet,
//! and the executor doesn't yet ship the accumulated state back to the
//! scheduler. Those pieces arrive with the range-repartition operator
//! (which is the first consumer).

use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{
    Result, ScalarValue, Statistics, internal_datafusion_err, internal_err,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    Distribution, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream, StatisticsArgs, apply_expression_roots,
    statistics::ChildStats,
};
use futures::stream::StreamExt;
use log::debug;

use crate::execution_plans::range_filter::{widen_above, widen_below};
use crate::serde::protobuf::{RuntimeStatsPartitionEntry, RuntimeStatsReport};
use crate::serde::scheduler::PartitionLocation;
use crate::sort_key::{SortKeyCodec, SortKeySketch};

/// Streaming runtime-stats operator. See module-level docs.
pub struct RuntimeStatsExec {
    input: Arc<dyn ExecutionPlan>,
    /// Lexicographic ORDER BY carried through from the wrapping window
    /// operator when the caller wants quantile sketching. `None` when
    /// only row counting is needed.
    order_by: Option<Vec<PhysicalSortExpr>>,
    /// Always tracked, per input partition. Written via
    /// `AtomicUsize::fetch_add` on the hot path — no cross-partition
    /// contention, no lock overhead.
    row_counts: Arc<[AtomicUsize]>,
    /// Only allocated when `order_by` is `Some`. Sketches the first ORDER BY
    /// expression. `Mutex`-per-partition to keep writes off any shared lock.
    sort_key_sketches: Option<Arc<[Mutex<SortKeySketch>]>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl RuntimeStatsExec {
    /// Wrap `input`. If `order_by` is provided, its first entry drives the
    /// per-partition sketch; the full slice is preserved for serde and for
    /// downstream operators (`SortExec`, `BoundedWindowAggExec`) that need it.
    /// When `Some`, at least one expression is required — nothing to sketch on
    /// with an empty slice — and its type must be one [`SortKeyCodec`] encodes.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        order_by: Option<Vec<PhysicalSortExpr>>,
    ) -> Result<Self> {
        let codec = match &order_by {
            Some(exprs) => {
                let [first, ..] = exprs.as_slice() else {
                    return internal_err!(
                        "RuntimeStatsExec: order_by is Some but empty; pass None to skip sketching"
                    );
                };
                let schema = input.schema();
                let routing_type = first.expr.data_type(&schema)?;
                // What the codec covers is the only restriction: any
                // fixed-width type, nullable or not, in either direction.
                let Some(codec) = SortKeyCodec::try_new(&routing_type, first.options)
                else {
                    return internal_err!(
                        "RuntimeStatsExec: no sort-key encoding for {routing_type:?}"
                    );
                };
                Some(codec)
            }
            None => None,
        };
        let partition_count = input.output_partitioning().partition_count();
        let row_counts: Arc<[AtomicUsize]> = (0..partition_count)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into();
        let sort_key_sketches: Option<Arc<[Mutex<SortKeySketch>]>> = codec.map(|codec| {
            (0..partition_count)
                .map(|_| Mutex::new(SortKeySketch::new(codec.clone())))
                .collect::<Vec<_>>()
                .into()
        });
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            order_by,
            row_counts,
            sort_key_sketches,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Full ORDER BY carried through, or `None` if the operator was
    /// built in row-count-only mode.
    pub fn order_by(&self) -> Option<&[PhysicalSortExpr]> {
        self.order_by.as_deref()
    }

    /// Rows observed on `partition` so far. Cheap `Relaxed` load — the
    /// value is a running counter, monotonically non-decreasing.
    ///
    /// Errors on out-of-range partition. Callers pass a partition id
    /// they've already used with `execute`.
    pub fn row_count(&self, partition: usize) -> Result<usize> {
        let counter = self.row_counts.get(partition).ok_or_else(|| {
            internal_datafusion_err!(
                "RuntimeStatsExec: partition {} out of range (have {})",
                partition,
                self.row_counts.len()
            )
        })?;
        Ok(counter.load(Ordering::Relaxed))
    }

    /// Number of partition slots this operator was built with (matches
    /// its input's declared partition count). Every slot has its own
    /// row counter and — in sketch mode — its own sketch; a given task
    /// only fills the slot(s) it actually executes.
    pub fn partition_count(&self) -> usize {
        self.row_counts.len()
    }

    /// Rows observed across all partitions so far.
    pub fn total_row_count(&self) -> usize {
        self.row_counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }

    /// Snapshot of one partition's running [`SortKeySketch`]. `None` in
    /// row-count-only mode.
    ///
    /// Errors if `partition` ≥ input's partition count — callers pass a
    /// partition id they've already used with `execute`.
    pub fn sort_key_sketch(&self, partition: usize) -> Result<Option<SortKeySketch>> {
        let Some(sketches) = &self.sort_key_sketches else {
            return Ok(None);
        };
        let slot = sketches.get(partition).ok_or_else(|| {
            internal_datafusion_err!(
                "RuntimeStatsExec: partition {} out of range (have {})",
                partition,
                sketches.len()
            )
        })?;
        let guard = slot.lock().map_err(|e| {
            internal_datafusion_err!(
                "RuntimeStatsExec partition {}: sort-key sketch mutex poisoned: {e}",
                partition
            )
        })?;
        Ok(Some(guard.clone()))
    }

    /// All partitions merged into one [`SortKeySketch`]. `Ok(None)` in
    /// row-count-only mode.
    pub fn merged_sort_key_sketch(&self) -> Result<Option<SortKeySketch>> {
        let Some(sketches) = self.sort_key_sketches.as_ref() else {
            return Ok(None);
        };
        let mut merged: Option<SortKeySketch> = None;
        for (partition, slot) in sketches.iter().enumerate() {
            let snapshot = slot
                .lock()
                .map_err(|e| {
                    internal_datafusion_err!(
                        "RuntimeStatsExec partition {}: sort-key sketch mutex poisoned: {e}",
                        partition
                    )
                })?
                .clone();
            match &mut merged {
                // Every slot shares the codec built once in `try_new`, so
                // the codec-mismatch arm of `merge` can't fire here.
                Some(accumulated) => accumulated.merge(snapshot)?,
                None => merged = Some(snapshot),
            }
        }
        Ok(merged)
    }
}

impl Debug for RuntimeStatsExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeStatsExec")
            .field("order_by", &self.order_by)
            .finish()
    }
}

impl DisplayAs for RuntimeStatsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.order_by {
            Some(exprs) => {
                let routing = &exprs[0];
                write!(
                    f,
                    "RuntimeStatsExec: rows + sketch(routing={} {})",
                    routing.expr,
                    if routing.options.descending {
                        "desc"
                    } else {
                        "asc"
                    }
                )
            }
            None => write!(f, "RuntimeStatsExec: rows"),
        }
    }
}

impl ExecutionPlan for RuntimeStatsExec {
    fn name(&self) -> &str {
        "RuntimeStatsExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// Only the first ORDER BY expression, matching the `routing_expr` that
    /// `execute` evaluates per batch to feed the sketch. The remaining keys are
    /// carried so multi-key `ORDER BY` survives serde for downstream operators,
    /// which makes them ordering metadata rather than expressions this node
    /// evaluates, and the trait contract excludes those.
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(
            self.order_by
                .as_ref()
                .and_then(|exprs| exprs.first())
                .map(|sort_expr| &sort_expr.expr),
            f,
        )
    }

    /// Passthrough: no distribution requirement on the child.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    /// Passthrough: no ordering requirement on the child.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    /// Batches pass through unchanged, so input order is preserved.
    /// Overrides default `false`.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    /// Wrapping this operator doesn't change how the child benefits from
    /// its own input partitioning.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    /// Row count and per-column stats pass through unchanged.
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    /// Every input row is emitted exactly once. Overrides default `Unknown`.
    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!(
                "RuntimeStatsExec expects exactly one child, got {}",
                children.len()
            );
        };
        // Fresh counters + sketches on rebuild — planning-time
        // reshuffles shouldn't carry stale sample state through the
        // tree.
        Ok(Arc::new(RuntimeStatsExec::try_new(
            input.clone(),
            self.order_by.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.row_counts.len() {
            return internal_err!(
                "RuntimeStatsExec: partition {} out of range (have {})",
                partition,
                self.row_counts.len()
            );
        }
        let input_stream = self.input.execute(partition, ctx)?;
        let schema = self.schema();
        // Cloning `Arc`s so downstream operators observing the same
        // state share the counters/sketches. First ORDER BY expression,
        // if any, drives sketching.
        let row_counts = self.row_counts.clone();
        let sort_key_sketches = self.sort_key_sketches.clone();
        let routing_expr = self
            .order_by
            .as_ref()
            .and_then(|exprs| exprs.first())
            .map(|e| e.expr.clone());

        let baseline = BaselineMetrics::new(&self.metrics, partition);
        // Separates sketch cost from the rest so we can tell whether the
        // hot path in sketching mode is the sketch or the evaluate above it.
        let sketch_time =
            MetricBuilder::new(&self.metrics).subset_time("sketch_time", partition);
        let sketch_batches =
            MetricBuilder::new(&self.metrics).counter("sketch_batches", partition);

        let state = StreamState {
            input: input_stream,
            row_counts,
            sort_key_sketches,
            routing_expr,
            partition,
            baseline,
            sketch_time,
            sketch_batches,
        };
        let out = futures::stream::unfold(state, |mut state| async move {
            // Await the child first so upstream shuffle IO / parquet
            // reads aren't billed to our elapsed_compute.
            let next = state.input.next().await?;
            let elapsed = state.baseline.elapsed_compute().clone();
            let timer = elapsed.timer();
            let forwarded = next.and_then(|batch| {
                state.ingest(&batch)?;
                state.baseline.record_output(batch.num_rows());
                Ok(batch)
            });
            timer.done();
            Some((forwarded, state))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// Per-partition streaming state. Owns the input stream and the routing
/// expression; writes to its own row-count slot and (if sketching) its
/// own sketch slot.
struct StreamState {
    input: SendableRecordBatchStream,
    row_counts: Arc<[AtomicUsize]>,
    sort_key_sketches: Option<Arc<[Mutex<SortKeySketch>]>>,
    routing_expr: Option<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    partition: usize,
    baseline: BaselineMetrics,
    sketch_time: Time,
    sketch_batches: Count,
}

impl StreamState {
    /// Update per-partition counters and (if sketching) the digest.
    /// Returns `Err` on any failure path — evaluation error,
    /// materialisation error, or wrong result type — so the caller
    /// propagates to the output stream rather than emitting a batch the
    /// stats never observed.
    fn ingest(
        &mut self,
        batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<()> {
        let counter = self.row_counts.get(self.partition).ok_or_else(|| {
            internal_datafusion_err!(
                "RuntimeStatsExec: partition {} out of range (have {}) — \
                 execute() should have validated this",
                self.partition,
                self.row_counts.len()
            )
        })?;

        // Sketch first: any failure returns before we count rows that
        // never made it downstream. Both sketches read one evaluation of
        // the routing expression, so running them side by side costs a
        // second ingest and not a second evaluate.
        if let Some(routing_expr) = &self.routing_expr {
            let evaluated = routing_expr.evaluate(batch)?;
            let array = evaluated.into_array(batch.num_rows())?;
            if !array.is_empty() {
                self.sketch_batches.add(1);
            }

            if let Some(sketches) = &self.sort_key_sketches {
                let slot = sketches.get(self.partition).ok_or_else(|| {
                    internal_datafusion_err!(
                        "RuntimeStatsExec: partition {} out of range on sort-key sketch slot",
                        self.partition
                    )
                })?;
                let mut sketch = slot.lock().map_err(|e| {
                    internal_datafusion_err!(
                        "RuntimeStatsExec partition {}: sort-key sketch mutex poisoned: {e}",
                        self.partition
                    )
                })?;
                let sketch_timer = self.sketch_time.timer();
                sketch.ingest(array.as_ref())?;
                sketch_timer.done();
            }
        }

        counter.fetch_add(batch.num_rows(), Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for StreamState {
    fn drop(&mut self) {
        // End-of-stream introspection until the operator learns to
        // emit its stats upstream. Log-and-skip if invariants somehow
        // broke — panic in Drop is a process-abort footgun.
        let Some(counter) = self.row_counts.get(self.partition) else {
            log::error!(
                "RuntimeStatsExec partition {} missing row-count slot on Drop; \
                 skipping end-of-stream log",
                self.partition,
            );
            return;
        };
        let rows = counter.load(Ordering::Relaxed);
        if rows == 0 {
            return;
        }
        if let Some(slot) = self
            .sort_key_sketches
            .as_ref()
            .and_then(|sketches| sketches.get(self.partition))
        {
            match slot.lock() {
                Ok(sketch) => {
                    debug!(
                        "RuntimeStatsExec partition {}: sort-key sketch count={} nulls={} \
                         min={:?} max={:?}",
                        self.partition,
                        sketch.count(),
                        sketch.null_count(),
                        sketch.min(),
                        sketch.max(),
                    );
                }
                Err(e) => {
                    log::error!(
                        "RuntimeStatsExec partition {}: sort-key sketch mutex poisoned \
                         on Drop; skipping end-of-stream log: {e}",
                        self.partition,
                    );
                }
            }
        }
    }
}

/// Walk `plan` and collect one [`RuntimeStatsReport`]
/// per [`RuntimeStatsExec`] that remains valid at the plan's output.
/// "Valid" means reachable through single-child chains of distribution-
/// preserving operators only — see the `preserves_distribution`
/// whitelist in the range-repartition module. A stats-tap sitting
/// *below* an [`super::UnorderedRangeRepartitionExec`] (or any other
/// distribution-changing operator) is excluded automatically because
/// the walker stops at that boundary; its sketch describes data the
/// repartitioner then routed away and is no longer meaningful at the
/// plan's output.
///
/// Executors call this once per task at completion to package what to
/// return to the scheduler.
pub fn collect_reports(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<RuntimeStatsReport>> {
    use datafusion_proto::physical_plan::{
        DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
    };
    let codec = DefaultPhysicalExtensionCodec {};
    let converter = DefaultPhysicalProtoConverter {};
    let mut found: Vec<&RuntimeStatsExec> = Vec::new();
    collect_reachable_stats(plan, &mut found);
    found
        .into_iter()
        .map(|stats| stats_to_report(stats, &codec, &converter))
        .collect()
}

/// DFS `plan` through single-child chains only, descending through
/// distribution-preserving nodes and past any [`RuntimeStatsExec`] found
/// on the way. Stops at any branch, leaf, or non-whitelisted node.
/// Similar in shape to `range_repartition_common::find_runtime_stats`
/// but collects *all* reachable stats rather than returning the first
/// match keyed to a specific routing expression.
fn collect_reachable_stats<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    out: &mut Vec<&'a RuntimeStatsExec>,
) {
    if let Some(stats) = plan.downcast_ref::<RuntimeStatsExec>() {
        out.push(stats);
        // Continue descending — a plan could conceivably chain multiple
        // stats-taps; `preserves_distribution` still guards the recursion.
    } else if !super::plan_algebra::preserves_distribution(plan.as_ref()) {
        return;
    }
    let children = plan.children();
    let [only_child] = children.as_slice() else {
        return;
    };
    collect_reachable_stats(only_child, out);
}

fn stats_to_report(
    stats: &RuntimeStatsExec,
    codec: &dyn datafusion_proto::physical_plan::PhysicalExtensionCodec,
    converter: &datafusion_proto::physical_plan::DefaultPhysicalProtoConverter,
) -> Result<RuntimeStatsReport> {
    use datafusion_proto::physical_plan::to_proto::serialize_physical_sort_exprs;
    let order_by = match stats.order_by() {
        Some(order_by) => {
            serialize_physical_sort_exprs(order_by.iter().cloned(), codec, converter)?
        }
        None => Vec::new(),
    };
    // Iterate every partition slot the operator holds. Slots the task
    // didn't touch have row_count = 0 and an empty sketch; we still emit
    // them so the scheduler sees a shape-consistent view.
    let partition_count = stats.partition_count();
    let mut partitions = Vec::with_capacity(partition_count);
    for partition_id in 0..partition_count {
        let row_count = stats.row_count(partition_id)? as u64;
        // The router needs each partition's value range, never its
        // distribution — hence extremes here and one merged sketch below.
        let (key_min, key_max, null_count) = match stats.sort_key_sketch(partition_id)? {
            Some(sk) => (
                extreme_to_proto(sk.value_min()?)?,
                extreme_to_proto(sk.value_max()?)?,
                sk.null_count(),
            ),
            None => (Vec::new(), Vec::new(), 0),
        };
        partitions.push(RuntimeStatsPartitionEntry {
            partition_id: partition_id as u32,
            row_count,
            key_min,
            key_max,
            null_count,
        });
    }
    let sort_key_sketch = match stats.merged_sort_key_sketch()? {
        Some(sk) if sk.count() > 0 => Some(sk.to_proto()?),
        _ => None,
    };
    Ok(RuntimeStatsReport {
        order_by,
        partitions,
        sketch: sort_key_sketch,
    })
}

/// One extreme as the wire's `repeated ScalarValue`: a tuple with one element
/// per key column, empty when no value was observed.
fn extreme_to_proto(
    extreme: Option<datafusion::common::ScalarValue>,
) -> Result<Vec<datafusion_proto_common::ScalarValue>> {
    extreme
        .map(|value| {
            datafusion_proto_common::ScalarValue::try_from(&value).map_err(|e| {
                internal_datafusion_err!("failed to encode key extreme {value:?}: {e:?}")
            })
        })
        .transpose()
        .map(|encoded| encoded.into_iter().collect())
}

/// One group's merged view: sketches from every report sharing the same
/// `order_by` wire tag combined, plus the `partition_count - 1` quantile
/// cuts a globally-informed router would use for `partition_count`
/// output partitions.
#[derive(Debug, Clone)]
pub struct MergedRuntimeStats {
    /// How many `PhysicalSortExprNode`s were in the shared `order_by` tag.
    pub order_by_len: usize,
    /// Number of output partitions the router used (per-report
    /// `partitions.len()`, must agree across reports in the group).
    pub partition_count: usize,
    /// Number of `RuntimeStatsReport`s contributing to this group.
    pub task_count: usize,
    /// Sum of `row_count` across every partition entry in the group.
    pub total_rows: u64,
    /// Rows in the group whose key was NULL. `0` in row-count-only mode
    pub null_count: u64,
    /// `partition_count - 1` cut points at quantiles `i/partition_count`
    /// on the merged sketch. Empty when `partition_count < 2` or no
    /// non-empty sketches were merged.
    pub cuts: Vec<ScalarValue>,
    /// Merged sketch's minimum if at least one non-empty sketch contributed;
    /// `None` in row-count-only mode.
    pub min: Option<ScalarValue>,
    /// Merged sketch's maximum if at least one non-empty sketch contributed;
    /// `None` in row-count-only mode.
    pub max: Option<ScalarValue>,
    /// Which end of the order the NULL run occupies, from the `order_by` tag.
    /// Travels with the cuts because every consumer that routes by them also
    /// has to place the run, and re-deriving it invites the two to disagree.
    /// Meaningless when `cuts` is empty.
    pub nulls_first: bool,
}

/// Group `RuntimeStatsReport`s by `order_by` wire tag, merge the T-Digests
/// within each group, and return one [`MergedRuntimeStats`] per group.
pub fn merge_reports(reports: &[RuntimeStatsReport]) -> Result<Vec<MergedRuntimeStats>> {
    use prost::Message;
    use std::collections::HashMap;

    if reports.is_empty() {
        return Ok(Vec::new());
    }

    // Group by the bytes of the encoded `order_by`. Prost-encoding each
    // `PhysicalSortExprNode` and concatenating gives a stable, cheap
    // grouping key without needing `Hash` on the generated proto types.
    let mut groups: HashMap<Vec<u8>, Vec<&RuntimeStatsReport>> = HashMap::new();
    for report in reports {
        let mut group_key = Vec::new();
        for expr in &report.order_by {
            expr.encode(&mut group_key)
                .expect("Vec<u8> is an infallible sink for prost::Message::encode");
        }
        groups.entry(group_key).or_default().push(report);
    }

    let mut merged_groups = Vec::with_capacity(groups.len());
    for group in groups.into_values() {
        merged_groups.push(merge_group(&group)?);
    }
    Ok(merged_groups)
}

/// Merge one group of reports (all sharing the same `order_by` tag).
/// Kept separate from `merge_reports` so the group iteration reads as a
/// single fallible step per group.
fn merge_group(group: &[&RuntimeStatsReport]) -> Result<MergedRuntimeStats> {
    let [first, rest @ ..] = group else {
        // `merge_reports` only builds groups from `HashMap::entry().push()`,
        // so an empty group is unreachable. Surface as internal error
        // rather than panicking.
        return internal_err!(
            "runtime stats merge: empty group — merge_reports invariant broken"
        );
    };
    let partition_count = first.partitions.len();
    let task_count = group.len();

    // Every task ran the same stage plan, so partition counts must
    // agree. Mismatch = internal invariant break.
    for report in rest {
        if report.partitions.len() != partition_count {
            return internal_err!(
                "runtime stats merge: order_by_len={} mismatched partition \
                 counts across reports ({} vs {})",
                first.order_by.len(),
                partition_count,
                report.partitions.len()
            );
        }
    }

    let mut total_rows: u64 = 0;
    // The key's direction and NULL placement live once per report, in the
    // `order_by` tag every consumer already reads to know which expression a
    // sketch describes.
    let options = first.order_by.first().map(|sort| SortOptions {
        descending: !sort.asc,
        nulls_first: sort.nulls_first,
    });
    let mut merged: Option<SortKeySketch> = None;
    for report in group {
        for entry in &report.partitions {
            total_rows = total_rows.saturating_add(entry.row_count);
        }
        let Some(state) = report.sketch.as_ref() else {
            continue;
        };
        let Some(options) = options else {
            return internal_err!(
                "runtime stats merge: a report carries a sketch with an empty \
                 order_by tag, so the key's ordering is unknown"
            );
        };
        let sketch = SortKeySketch::try_from_proto(state, options)?;
        match &mut merged {
            Some(accumulated) => accumulated.merge(sketch)?,
            None => merged = Some(sketch),
        }
    }

    let Some(merged) = merged.filter(|sketch| sketch.count() > 0) else {
        return Ok(MergedRuntimeStats {
            order_by_len: first.order_by.len(),
            partition_count,
            task_count,
            total_rows,
            null_count: 0,
            cuts: Vec::new(),
            min: None,
            max: None,
            nulls_first: false,
        });
    };

    let cuts = merged.cuts(partition_count)?;
    let (min, max) = (merged.min()?, merged.max()?);
    Ok(MergedRuntimeStats {
        order_by_len: first.order_by.len(),
        partition_count,
        task_count,
        total_rows,
        null_count: merged.null_count(),
        cuts,
        min,
        max,
        nulls_first: options.is_some_and(|options| options.nulls_first),
    })
}

/// One producer task's runtime-stats report, kept alongside the
/// `producer_task_id` that emitted it. The scheduler stores these on
/// `RunningStage.runtime_stats_reports` so downstream stages can address
/// individual producer files as `(producer_task_id, partition_id)` pairs —
/// the partition_id inside a report is producer-local (0..K range-repartition
/// sub-parts), so the pair is what uniquely identifies a shuffle file across
/// producers.
#[derive(Debug, Clone)]
pub struct TaskRuntimeStats {
    /// Producer task's task_id at the time it emitted the report. Matches
    /// the `file_id` stamped on `ShuffleWritePartition` records.
    pub producer_task_id: usize,
    /// The report itself: per-partition row counts and (in sketch mode)
    /// quantile sketches for the routing expression.
    pub report: RuntimeStatsReport,
}

/// Walk the partition-preserving spine of `plan` for the
/// `UnorderedRangeRepartitionExec` or `OrderedRangeRepartitionExec` that
/// drives this stage's output partitioning, and return its routing
/// expression (`order_by[0].expr`).
///
/// The spine is the chain of partition-preserving ops (see
/// [`super::preserves_partitioning`]) between the stage root and the barrier
/// that sets the stage's output partitioning. Descent stops at any
/// non-preserving op (join, union, hash-agg, unknown node) — an RRE
/// below such a barrier drives a different logical partitioning that
/// this stage's output no longer carries.
///
/// `Ok(None)` means no range-repartition op drives this stage's
/// partitioning; `Err(_)` means one was found but its `order_by` was
/// empty (invariant break — a range repartition without a routing key
/// can't route anything), or the spine hit a partition-preserving node
/// with more than one child (shape bug in the whitelist).
pub fn repartition_routing_expr(
    plan: &dyn ExecutionPlan,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    if let Some(rre) = plan.downcast_ref::<super::UnorderedRangeRepartitionExec>() {
        return match rre.order_by() {
            [first, ..] => Ok(Some(first.expr.clone())),
            [] => internal_err!("UnorderedRangeRepartitionExec has empty ORDER BY"),
        };
    }
    if let Some(rre) = plan.downcast_ref::<super::OrderedRangeRepartitionExec>() {
        return match rre.order_by() {
            [first, ..] => Ok(Some(first.expr.clone())),
            [] => internal_err!("OrderedRangeRepartitionExec has empty ORDER BY"),
        };
    }
    if !super::preserves_partitioning(plan) {
        return Ok(None);
    }
    let children = plan.children();
    match children.as_slice() {
        [] => Ok(None),
        [child] => repartition_routing_expr(child.as_ref()),
        _ => internal_err!(
            "partition-preserving op `{}` has {} children — the whitelist \
             assumes single-child; expand the algorithm if this fires",
            plan.name(),
            children.len()
        ),
    }
}

/// Rebuild a stage's `Vec<Vec<PartitionLocation>>` under range-repartition
/// overlap semantics: for each producer file in `original_partitions`,
/// find its sketch (from `reports`), and route the file into every
/// downstream partition whose *halo-widened* range overlaps
/// `[sketch.min(), sketch.max()]`.
///
/// Downstream partition ranges follow the half-open convention, widened
/// by the downstream `RangeFilterExec`'s halos on each side:
/// - `k = 0`         → `(-∞,                  cuts[0]   + halo_hi)`
/// - `0 < k < K - 1` → `[cuts[k-1] - halo_lo, cuts[k]   + halo_hi)`
/// - `k = K - 1`     → `[cuts[K-2] - halo_lo, +∞)`
///
/// `[min, max]` overlaps `[lower, upper)` iff `max >= lower AND min < upper`.
///
/// `halo_lo`/`halo_hi` are `0.0` when the downstream stage has no halo
/// consumer (hash-agg, no-window range-repartition) — the check collapses
/// to raw cuts. When the downstream stage has a `RangeFilterExec` with
/// non-zero halo (bounded RANGE-frame windows), the caller passes the
/// widened halos so files straddling the halo band route to both sides.
/// Skipping this widening loses boundary rows from downstream window sums.
///
/// Files without a corresponding sketch (missing entirely, or present
/// with `count == 0`) are safe to skip only when `partition_stats.num_rows`
/// confirms the file is empty (`Some(0)`). If the file has rows or the
/// row count is unknown (`None`), silently skipping would lose data —
/// error out instead.
///
/// # Arguments
///
/// * `original_partitions` — passthrough shuffle output, `partitions[k]`
///   holds every file the writer produced for global partition `k`.
/// * `reports` — one per completed producer task; each carries the
///   per-sub-part sketches used for overlap lookup.
/// * `global_cuts` — K-1 monotone quantile cuts derived from merged
///   sketches; produce K downstream buckets.
/// * `halo_lo` / `halo_hi` — downstream `RangeFilterExec`'s halo widths
///   in the routing expression's units. `0.0` for non-halo consumers.
pub fn cut_partitions(
    original_partitions: Vec<Vec<PartitionLocation>>,
    reports: &[TaskRuntimeStats],
    global_cuts: &[ScalarValue],
    halo_lo: &ScalarValue,
    halo_hi: &ScalarValue,
    nulls_first: bool,
) -> Result<Vec<Vec<PartitionLocation>>> {
    use std::collections::HashMap;

    // Index sketches by (producer_task_id, sub_part_id). Under
    // ShuffleWriter(Passthrough) file_id == task_id, so PartitionLocation's
    // (file_id, partition_id.partition_id) is the same pair.
    // Only each file's value range is needed, never its distribution, which
    // is why the sketch beside these is merged once per report rather than
    // repeated per partition.
    let ranges: HashMap<(usize, u32), &RuntimeStatsPartitionEntry> =
        reports
            .iter()
            .flat_map(|stats| {
                stats.report.partitions.iter().map(move |entry| {
                    ((stats.producer_task_id, entry.partition_id), entry)
                })
            })
            .collect();

    debug_assert!(
        global_cuts.windows(2).all(|w| w[0] <= w[1]),
        "global_cuts must be non-decreasing: {global_cuts:?}"
    );

    let partition_count = global_cuts.len() + 1;
    let mut remapped: Vec<Vec<PartitionLocation>> = vec![Vec::new(); partition_count];
    for partition in original_partitions {
        for file in partition {
            let Some(task_id) = file.file_id else {
                return internal_err!(
                    "range-repartition remap: missing file_id (partition_id={})",
                    file.partition_id.partition_id
                );
            };
            let sub_part_id = file.partition_id.partition_id as u32;
            let entry = ranges.get(&(task_id as usize, sub_part_id));
            // The NULL run belongs wholly to the partition the `nulls_first`
            // end names. `key_min`/`key_max` are value extremes, so no
            // overlap check on them can find that partition — a file whose
            // values and NULLs belong to different consumers has to be
            // delivered to both, and is below. Every producer routes its own
            // NULLs to its own last slot, so without this only the one file
            // whose values happen to land on the run's partition delivers
            // them and the rest of the run is silently dropped.
            let null_part_idx = entry
                .is_some_and(|entry| entry.null_count > 0)
                .then(|| if nulls_first { 0 } else { partition_count - 1 });
            let range = entry.and_then(|entry| {
                let lo = entry.key_min.first()?;
                let hi = entry.key_max.first()?;
                Some((lo, hi))
            });
            let Some((min_proto, max_proto)) = range else {
                // No value range: an all-NULL file, or an empty one. Anything
                // else has rows this remap cannot address.
                match null_part_idx {
                    Some(part_idx) => remapped[part_idx].push(file),
                    None if file.partition_stats.num_rows != Some(0) => {
                        return internal_err!(
                            "range-repartition remap: file has num_rows={:?} but no usable key range (task_id={task_id}, sub_part_id={sub_part_id})",
                            file.partition_stats.num_rows
                        );
                    }
                    None => {}
                }
                continue;
            };
            // Bucket i has (lower, upper) = (cuts[i-1] - halo_lo, cuts[i] +
            // halo_hi) with ±∞ at the ends, and matches iff `sketch_max +
            // halo_lo >= cuts[i-1] && sketch_min - halo_hi < cuts[i]`.
            // Monotone cuts → the set of matching buckets is a contiguous
            // range [b_lo, b_hi], found by two partition_points over
            // `global_cuts` with the sketch shifted by the halos.
            let sketch_min = ScalarValue::try_from(min_proto).map_err(|e| {
                internal_datafusion_err!(
                    "range-repartition remap: undecodable key_min: {e:?}"
                )
            })?;
            let sketch_max = ScalarValue::try_from(max_proto).map_err(|e| {
                internal_datafusion_err!(
                    "range-repartition remap: undecodable key_max: {e:?}"
                )
            })?;
            // Typed widening with the same zero shortcut `RangeFilterExec`
            // uses, so a zero halo of one type cannot refuse a key of another.
            let reach_lo = widen_below(&sketch_min, halo_hi)?;
            let reach_hi = widen_above(&sketch_max, halo_lo)?;
            let b_lo = global_cuts.partition_point(|cut| cut <= &reach_lo);
            let b_hi = global_cuts.partition_point(|cut| cut <= &reach_hi);
            // Delivered for its NULL run only when the value range didn't
            // already reach that partition: two copies in one consumer would
            // be read twice and counted twice.
            if let Some(part) = null_part_idx.filter(|part| !(b_lo..=b_hi).contains(part))
            {
                remapped[part].push(file.clone());
            }
            for bucket in &mut remapped[b_lo..=b_hi] {
                bucket.push(file.clone());
            }
        }
    }
    Ok(remapped)
}

/// Merge `reports` and log each group's merged view at `debug!`
/// (`RUST_LOG` promotes when needed). Any merge error is logged at
/// `warn!` — the scheduler doesn't want telemetry loss to tank a query
/// whose data was already produced correctly. The scheduler calls this
/// once per stage-attempt at final-success.
pub fn log_merged_runtime_stats(
    job_id: &str,
    stage_id: usize,
    reports: &[TaskRuntimeStats],
) {
    let raw: Vec<RuntimeStatsReport> = reports.iter().map(|t| t.report.clone()).collect();
    let merged_groups = match merge_reports(&raw) {
        Ok(groups) => groups,
        Err(err) => {
            log::warn!(
                "runtime stats merge failed for job={job_id} stage={stage_id}: {err}"
            );
            return;
        }
    };
    for merged in merged_groups {
        match (merged.min, merged.max) {
            (Some(min), Some(max)) => log::debug!(
                "merged runtime stats: job={} stage={} order_by_len={} \
                 partition_count={} task_count={} total_rows={} cuts={:?} \
                 min={} max={}",
                job_id,
                stage_id,
                merged.order_by_len,
                merged.partition_count,
                merged.task_count,
                merged.total_rows,
                merged.cuts,
                min,
                max,
            ),
            _ => log::debug!(
                "merged runtime stats: job={} stage={} order_by_len={} \
                 partition_count={} task_count={} total_rows={} cuts=[] \
                 (no sketches)",
                job_id,
                stage_id,
                merged.order_by_len,
                merged.partition_count,
                merged.task_count,
                merged.total_rows,
            ),
        }
    }
}

#[cfg(test)]
mod stream_tests {
    //! End-to-end: build a small in-memory input, wrap it in
    //! `RuntimeStatsExec`, drain the stream, and verify the accumulated
    //! state matches the data that flowed through.

    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::common;
    use datafusion::physical_plan::expressions::col;
    use datafusion::prelude::SessionContext;

    fn schema_v_id() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Float64, false),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn batch(schema: &Arc<Schema>, v: Vec<Option<f64>>, id: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(v)),
                Arc::new(Int64Array::from(id)),
            ],
        )
        .unwrap()
    }

    /// Sketching mode: drain a single-partition input and verify the
    /// operator accumulated one row-count per input row and one sketch
    /// sample per non-null routing value.
    #[tokio::test]
    async fn execute_populates_sketch_and_row_count() {
        let schema = schema_v_id();
        let b1 = batch(
            &schema,
            vec![Some(1.0), Some(3.0), Some(5.0)],
            vec![10, 11, 12],
        );
        let b2 = batch(&schema, vec![Some(2.0), Some(4.0)], vec![20, 21]);

        let memory = Arc::new(
            MemorySourceConfig::try_new(&[vec![b1, b2]], schema.clone(), None).unwrap(),
        );
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(memory));

        let sort_expr = PhysicalSortExpr {
            expr: col("v", schema.as_ref()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        };
        let stats =
            Arc::new(RuntimeStatsExec::try_new(input, Some(vec![sort_expr])).unwrap());

        let ctx = SessionContext::new().task_ctx();
        let stream = stats.execute(0, ctx).unwrap();
        let output = common::collect(stream).await.unwrap();

        // Passthrough: same row count out as in.
        let out_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert_eq!(out_rows, 5);

        // Row-count accessor observed every batch.
        assert_eq!(stats.row_count(0).unwrap(), 5);
        assert_eq!(stats.total_row_count(), 5);

        // Sketch observed every routing value.
        let sort_key = stats.sort_key_sketch(0).unwrap().unwrap();
        assert_eq!(sort_key.count(), 5);
        assert_eq!(sort_key.null_count(), 0);
        assert_eq!(
            sort_key.min().unwrap(),
            Some(ScalarValue::Float64(Some(1.0)))
        );
        assert_eq!(
            sort_key.max().unwrap(),
            Some(ScalarValue::Float64(Some(5.0)))
        );
        assert_eq!(stats.merged_sort_key_sketch().unwrap().unwrap().count(), 5);
    }

    /// Volume through the operator, not just a handful of rows: past KLL's
    /// k=800 level-0 capacity so compaction actually runs, and ascending to
    /// match the post-`SortExec` position the operator occupies in a
    /// range-repartition plan.
    #[tokio::test]
    async fn the_sketch_sees_every_row_through_the_operator() {
        let schema = schema_v_id();
        const ROWS: i64 = 5_000;
        let batches: Vec<RecordBatch> = (0..5)
            .map(|chunk| {
                let lo = chunk * (ROWS / 5);
                let hi = lo + (ROWS / 5);
                batch(
                    &schema,
                    (lo..hi).map(|v| Some(v as f64)).collect(),
                    (lo..hi).collect(),
                )
            })
            .collect();

        let memory = Arc::new(
            MemorySourceConfig::try_new(&[batches], schema.clone(), None).unwrap(),
        );
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(memory));
        let sort_expr = PhysicalSortExpr {
            expr: col("v", schema.as_ref()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        };
        let stats =
            Arc::new(RuntimeStatsExec::try_new(input, Some(vec![sort_expr])).unwrap());

        let ctx = SessionContext::new().task_ctx();
        let stream = stats.execute(0, ctx).unwrap();
        common::collect(stream).await.unwrap();

        let sketch = stats.merged_sort_key_sketch().unwrap().unwrap();
        assert_eq!(sketch.count(), ROWS as u64);
        assert_eq!(sketch.null_count(), 0);
        // The extremes are tracked outside the compactor, so compaction
        // cannot have moved them however many times it ran.
        assert_eq!(sketch.min().unwrap(), Some(ScalarValue::Float64(Some(0.0))));
        assert_eq!(
            sketch.max().unwrap(),
            Some(ScalarValue::Float64(Some((ROWS - 1) as f64)))
        );
        // K-1 real, non-decreasing boundaries over what it saw.
        let cuts = sketch.cuts(8).unwrap();
        assert_eq!(cuts.len(), 7);
        assert!(cuts.iter().all(|cut| !cut.is_null()));
        assert!(cuts.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    /// Row-count-only mode (`order_by = None`): the operator still
    /// counts rows as they stream past, but the sketch accessors stay
    /// `None` no matter how much data flows through.
    #[tokio::test]
    async fn execute_row_count_only_no_sketch() {
        let schema = schema_v_id();
        let b1 = batch(&schema, vec![Some(1.0), Some(2.0)], vec![40, 41]);
        let b2 = batch(&schema, vec![Some(3.0)], vec![42]);

        let memory =
            Arc::new(MemorySourceConfig::try_new(&[vec![b1, b2]], schema, None).unwrap());
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(memory));

        let stats = Arc::new(RuntimeStatsExec::try_new(input, None).unwrap());

        let ctx = SessionContext::new().task_ctx();
        let stream = stats.execute(0, ctx).unwrap();
        let output = common::collect(stream).await.unwrap();

        assert_eq!(output.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert_eq!(stats.row_count(0).unwrap(), 3);
        assert!(
            stats.sort_key_sketch(0).unwrap().is_none(),
            "row-count-only mode must not allocate a sketch"
        );
        assert!(stats.merged_sort_key_sketch().unwrap().is_none());
    }
}

#[cfg(test)]
mod collect_tests {
    //! Walker behavior: which `RuntimeStatsExec`s does `collect_reports`
    //! see through the whitelist, and what do the emitted reports look
    //! like once the plan has been drained?

    use super::*;
    use crate::execution_plans::BufferExec;
    use crate::execution_plans::buffer::BufferMode;
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::common;
    use datafusion::physical_plan::expressions::col;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;

    fn schema_v() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]))
    }

    fn v_batch(schema: &Arc<Schema>, v: Vec<f64>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(v))])
            .unwrap()
    }

    fn v_input(schema: Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        let b1 = v_batch(&schema, vec![1.0, 3.0, 5.0]);
        let b2 = v_batch(&schema, vec![2.0, 4.0]);
        let memory =
            Arc::new(MemorySourceConfig::try_new(&[vec![b1, b2]], schema, None).unwrap());
        Arc::new(DataSourceExec::new(memory))
    }

    fn sort_expr_on_v(schema: &Arc<Schema>) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col("v", schema.as_ref()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }
    }

    /// Stats sit at plan root, sketching mode: `collect_reports` returns
    /// exactly one report whose partition entry carries the observed
    /// row_count and a populated sketch that survives the on-wire round-
    /// trip via `sketch_from_proto`.
    #[tokio::test]
    async fn collect_reports_finds_stats_and_ships_sketch() {
        let schema = schema_v();
        let input = v_input(schema.clone());
        let stats = Arc::new(
            RuntimeStatsExec::try_new(input, Some(vec![sort_expr_on_v(&schema)]))
                .unwrap(),
        );

        // Drive the stream so counters and the sketch actually fill.
        let ctx = SessionContext::new().task_ctx();
        let stream = stats.clone().execute(0, ctx).unwrap();
        let _ = common::collect(stream).await.unwrap();

        let plan: Arc<dyn ExecutionPlan> = stats;
        let reports = collect_reports(&plan).expect("collect_reports must succeed");
        let [report] = reports.as_slice() else {
            panic!(
                "expected exactly one report, got {} (order_by tags: {:?})",
                reports.len(),
                reports.iter().map(|r| r.order_by.len()).collect::<Vec<_>>()
            );
        };
        assert_eq!(report.order_by.len(), 1, "one sort expr encoded");
        let [entry] = report.partitions.as_slice() else {
            panic!(
                "expected one partition entry, got {}",
                report.partitions.len()
            );
        };
        assert_eq!(entry.partition_id, 0);
        assert_eq!(entry.row_count, 5);
        // The report's merged sketch is what routing reads; the entry
        // carries only the key range a file router needs.
        let state = report.sketch.as_ref().expect("sketch present in wire");
        let round_tripped = SortKeySketch::try_from_proto(
            state,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )
        .unwrap();
        assert_eq!(round_tripped.count(), 5);
        assert_eq!(
            round_tripped.min().unwrap(),
            Some(ScalarValue::Float64(Some(1.0)))
        );
        assert_eq!(
            round_tripped.max().unwrap(),
            Some(ScalarValue::Float64(Some(5.0)))
        );
        assert_eq!(entry.key_min.len(), 1, "entry carries its own range");
        assert_eq!(entry.key_max.len(), 1);
    }

    /// Row-count-only mode: report emitted, but its partition entry
    /// carries no sketch.
    #[tokio::test]
    async fn collect_reports_row_count_only_emits_report_without_sketch() {
        let schema = schema_v();
        let input = v_input(schema.clone());
        let stats = Arc::new(RuntimeStatsExec::try_new(input, None).unwrap());

        let ctx = SessionContext::new().task_ctx();
        let stream = stats.clone().execute(0, ctx).unwrap();
        let _ = common::collect(stream).await.unwrap();

        let plan: Arc<dyn ExecutionPlan> = stats;
        let reports = collect_reports(&plan).unwrap();
        let [report] = reports.as_slice() else {
            panic!("expected one report, got {}", reports.len());
        };
        assert!(report.order_by.is_empty());
        assert_eq!(report.partitions.len(), 1);
        assert!(report.sketch.is_none(), "no sketch in row-count-only mode");
        assert!(report.partitions[0].key_min.is_empty());
        assert_eq!(report.partitions[0].row_count, 5);
    }

    /// Whitelisted intermediary (`BufferExec` in Dam mode) between plan
    /// root and the stats-tap: walker still descends to it.
    #[tokio::test]
    async fn collect_reports_descends_through_whitelisted_op() {
        let schema = schema_v();
        let input = v_input(schema.clone());
        let stats = Arc::new(
            RuntimeStatsExec::try_new(input, Some(vec![sort_expr_on_v(&schema)]))
                .unwrap(),
        );
        let buffer: Arc<dyn ExecutionPlan> =
            Arc::new(BufferExec::try_new(stats, BufferMode::Dam).unwrap());

        // Drain via the outer plan so counters fill.
        let ctx = SessionContext::new().task_ctx();
        let stream = buffer.clone().execute(0, ctx).unwrap();
        let _ = common::collect(stream).await.unwrap();

        let reports = collect_reports(&buffer).unwrap();
        assert_eq!(reports.len(), 1, "buffer must not block the walker");
        assert_eq!(reports[0].partitions[0].row_count, 5);
    }

    /// A `SortExec` with `preserve_partitioning=false` collapses N→1;
    /// the whitelist excludes that variant explicitly. The walker
    /// stops at the collapse and doesn't reach the stats below.
    #[tokio::test]
    async fn collect_reports_stops_at_sort_that_collapses_partitions() {
        let schema = schema_v();
        let input = v_input(schema.clone());
        let stats: Arc<dyn ExecutionPlan> = Arc::new(
            RuntimeStatsExec::try_new(input, Some(vec![sort_expr_on_v(&schema)]))
                .unwrap(),
        );
        // Default SortExec has preserve_partitioning=false — the
        // whitelist path we're testing rejects it.
        let sort = SortExec::new(
            LexOrdering::new(vec![sort_expr_on_v(&schema)]).unwrap(),
            stats,
        );
        assert!(
            !sort.preserve_partitioning(),
            "test fixture assumes N→1 sort"
        );
        let plan: Arc<dyn ExecutionPlan> = Arc::new(sort);
        let reports = collect_reports(&plan).unwrap();
        assert!(
            reports.is_empty(),
            "N→1 sort must block the walker; got {} reports",
            reports.len()
        );
    }
}

#[cfg(test)]
mod merge_tests {
    //! Scheduler-side aggregation: given several `RuntimeStatsReport`s
    //! sharing an `order_by` tag, verify the merged view (total rows,
    //! cuts, min/max) reflects the union of the underlying samples.

    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::datatypes::DataType;

    use super::*;
    use crate::serde::protobuf::{RuntimeStatsPartitionEntry, RuntimeStatsReport};
    use datafusion_proto::protobuf::PhysicalSortExprNode;

    /// Build a report whose partition slots each carry a sketch made
    /// from that slot's `values`. Slot `slot_id` in the resulting
    /// report has `row_count = values[slot_id].len()` and a sketch
    /// over those values.
    /// The wire tag matching [`sketch_options`]. A report carrying a sketch
    /// must have one, since the tag is where the key's ordering lives.
    fn sketch_tag() -> Vec<PhysicalSortExprNode> {
        vec![PhysicalSortExprNode {
            expr: None,
            asc: true,
            nulls_first: true,
        }]
    }

    /// The ordering a fixture sketch is built under. Must match the tag the
    /// merge reads, since that is where direction and NULL placement live.
    fn sketch_options() -> SortOptions {
        SortOptions {
            descending: false,
            nulls_first: true,
        }
    }

    fn sketching_report(
        order_by: Vec<PhysicalSortExprNode>,
        values_per_slot: Vec<Vec<f64>>,
    ) -> RuntimeStatsReport {
        // One merged sketch per report, as the executor builds it, plus the
        // per-partition row counts.
        let codec = SortKeyCodec::try_new(&DataType::Float64, sketch_options()).unwrap();
        let mut merged = SortKeySketch::new(codec);
        let partitions = values_per_slot
            .into_iter()
            .enumerate()
            .map(|(slot_id, slot_values)| {
                let row_count = slot_values.len() as u64;
                merged
                    .ingest(&Float64Array::from(slot_values))
                    .expect("Float64 samples into a Float64 sketch");
                RuntimeStatsPartitionEntry {
                    partition_id: slot_id as u32,
                    row_count,
                    ..Default::default()
                }
            })
            .collect();
        let sketch = (merged.count() > 0).then(|| merged.to_proto().unwrap());
        RuntimeStatsReport {
            order_by,
            partitions,
            sketch,
        }
    }

    /// A report whose key is NULL in every row. The sketch retains nothing,
    /// so its NULL count is all that survives the round-trip.
    fn all_null_report(
        order_by: Vec<PhysicalSortExprNode>,
        nulls_per_slot: Vec<usize>,
    ) -> RuntimeStatsReport {
        let codec = SortKeyCodec::try_new(&DataType::Float64, sketch_options()).unwrap();
        let mut merged = SortKeySketch::new(codec);
        let partitions = nulls_per_slot
            .into_iter()
            .enumerate()
            .map(|(slot_id, nulls)| {
                merged
                    .ingest(&Float64Array::from(vec![None::<f64>; nulls]))
                    .expect("NULL Float64 samples into a Float64 sketch");
                RuntimeStatsPartitionEntry {
                    partition_id: slot_id as u32,
                    row_count: nulls as u64,
                    ..Default::default()
                }
            })
            .collect();
        RuntimeStatsReport {
            order_by,
            partitions,
            sketch: Some(merged.to_proto().unwrap()),
        }
    }

    fn only_group(reports: &[RuntimeStatsReport]) -> MergedRuntimeStats {
        let mut groups = merge_reports(reports).expect("merge should succeed");
        match groups.as_slice() {
            [_] => groups.remove(0),
            other => panic!("expected exactly one group, got {}", other.len()),
        }
    }

    /// A cut as the number the band assertions are written in terms of.
    fn as_f64(cut: &ScalarValue) -> f64 {
        match cut {
            ScalarValue::Float64(Some(v)) => *v,
            other => panic!("expected a Float64 cut, got {other:?}"),
        }
    }

    /// Two reports over disjoint value ranges — merged sketch spans the
    /// union, total_rows sums, and the partition_count=2 midpoint cut
    /// falls between the two ranges.
    #[test]
    fn merge_reports_combines_disjoint_ranges() {
        // Both reports share an empty `order_by` — we just need two
        // reports that land in the same group.
        let low_range = sketching_report(sketch_tag(), vec![vec![1.0, 2.0, 3.0], vec![]]);
        let high_range =
            sketching_report(sketch_tag(), vec![vec![], vec![10.0, 11.0, 12.0]]);

        let group = only_group(&[low_range, high_range]);
        assert_eq!(group.partition_count, 2);
        assert_eq!(group.task_count, 2);
        assert_eq!(group.total_rows, 6);
        let midpoint = match group.cuts.as_slice() {
            [midpoint] => as_f64(midpoint),
            other => panic!("expected exactly one cut, got {other:?}"),
        };
        assert!(
            (3.0..=10.0).contains(&midpoint),
            "midpoint cut should land between ranges (got {midpoint})"
        );
        assert_eq!(group.min.as_ref().map(as_f64), Some(1.0));
        assert_eq!(group.max.as_ref().map(as_f64), Some(12.0));
    }

    /// partition_count=4 cuts on a uniform [0, 100) sample land roughly
    /// at quartiles — verifies the quantile indices `i / partition_count`
    /// for `i in 1..partition_count`.
    #[test]
    fn merge_reports_partition_count_of_four_produces_three_quartile_cuts() {
        let uniform: Vec<f64> = (0..100).map(|value| value as f64).collect();
        // Single report, partition_count=4: each slot gets 25 uniform
        // samples.
        let values_per_slot = vec![
            uniform[0..25].to_vec(),
            uniform[25..50].to_vec(),
            uniform[50..75].to_vec(),
            uniform[75..100].to_vec(),
        ];
        let report = sketching_report(sketch_tag(), values_per_slot);

        let group = only_group(&[report]);
        assert_eq!(group.partition_count, 4);
        let (p25, p50, p75) = match group.cuts.as_slice() {
            [p25, p50, p75] => (as_f64(p25), as_f64(p50), as_f64(p75)),
            other => panic!("expected 3 cuts, got {other:?}"),
        };
        // Loose bounds — T-Digest quantile estimates aren't exact, but
        // must land in the expected quartile bands.
        assert!((10.0..40.0).contains(&p25), "p25 near 25, got {p25}");
        assert!((35.0..65.0).contains(&p50), "p50 near 50, got {p50}");
        assert!((60.0..90.0).contains(&p75), "p75 near 75, got {p75}");
    }

    /// Row-count-only reports (no sketches) still produce a group with
    /// summed `total_rows` — just empty `cuts` and `None` min/max.
    #[test]
    fn merge_reports_row_count_only_emits_empty_cuts() {
        let make_report = |row_counts: [u64; 2]| RuntimeStatsReport {
            order_by: vec![],
            partitions: vec![
                RuntimeStatsPartitionEntry {
                    partition_id: 0,
                    row_count: row_counts[0],
                    ..Default::default()
                },
                RuntimeStatsPartitionEntry {
                    partition_id: 1,
                    row_count: row_counts[1],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let group = only_group(&[make_report([100, 200]), make_report([300, 400])]);
        assert_eq!(group.partition_count, 2);
        assert_eq!(group.total_rows, 1000);
        assert!(group.cuts.is_empty());
        assert!(group.min.is_none());
        assert!(group.max.is_none());
    }

    /// A key that is NULL in every row has no value to cut on, so `cuts`
    /// comes back empty — the same answer a report carrying no sketch at all
    /// gives. The two have to stay distinguishable: one is a degenerate
    /// distribution whose every row belongs in a single partition, the other
    /// is a broken invariant a range-repartition stage must refuse to route
    /// on.
    #[test]
    fn merge_reports_distinguishes_an_all_null_key_from_a_missing_sketch() {
        let group = only_group(&[
            all_null_report(sketch_tag(), vec![3, 1]),
            all_null_report(sketch_tag(), vec![2, 4]),
        ]);

        assert_eq!(group.total_rows, 10);
        assert!(group.cuts.is_empty(), "no value to cut on");
        assert_eq!(group.null_count, group.total_rows, "every key was NULL");
    }

    /// Mismatched partition counts within a group surface as an error —
    /// the caller (scheduler / slice-D consumer) sees the invariant
    /// break rather than silently getting a partial merge.
    #[test]
    fn merge_reports_errors_on_mismatched_partition_counts() {
        let two_partitions = sketching_report(sketch_tag(), vec![vec![1.0], vec![2.0]]);
        let one_partition = sketching_report(sketch_tag(), vec![vec![3.0]]);
        let err = merge_reports(&[two_partitions, one_partition])
            .expect_err("mismatched partition counts must error");
        let message = err.to_string();
        assert!(
            message.contains("mismatched partition counts"),
            "expected mismatch error, got: {message}"
        );
    }

    /// A sketch the decoder cannot read surfaces as an error rather than
    /// getting silently dropped, which would size partitions from a
    /// population missing whatever that report observed.
    #[test]
    fn merge_reports_propagates_sketch_decode_errors() {
        let corrupt = crate::serde::protobuf::SortKeySketchState {
            k: 800,
            null_count: 0,
            key_min: vec![],
            key_max: vec![],
            levels: b"not an arrow stream".to_vec(),
        };
        let report = RuntimeStatsReport {
            order_by: sketch_tag(),
            partitions: vec![RuntimeStatsPartitionEntry {
                partition_id: 0,
                row_count: 1,
                ..Default::default()
            }],
            sketch: Some(corrupt),
        };
        let err = merge_reports(&[report])
            .expect_err("an undecodable sketch must surface as an error");
        assert!(!err.to_string().is_empty(), "got: {err}");
    }

    /// Empty input → empty output; ensures no panics or spurious groups.
    #[test]
    fn merge_reports_empty_input_is_empty_output() {
        assert!(merge_reports(&[]).unwrap().is_empty());
    }
}

#[cfg(test)]
mod plan_walker_tests {
    //! `range_repartition_routing_expr` — recursive walk that recognizes
    //! both `UnorderedRangeRepartitionExec` and `OrderedRangeRepartitionExec`
    //! at any depth.

    use super::*;
    use crate::execution_plans::{
        OrderedRangeRepartitionExec, UnorderedRangeRepartitionExec,
    };
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::expressions::col;
    use datafusion::physical_plan::sorts::sort::SortExec;

    fn v_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]))
    }

    fn v_source() -> Arc<dyn ExecutionPlan> {
        let schema = v_schema();
        let memory =
            Arc::new(MemorySourceConfig::try_new(&[vec![]], schema, None).unwrap());
        Arc::new(DataSourceExec::new(memory))
    }

    fn sort_expr_v() -> PhysicalSortExpr {
        let schema = v_schema();
        PhysicalSortExpr {
            expr: col("v", schema.as_ref()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }
    }

    fn urre_over_source(k: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            UnorderedRangeRepartitionExec::try_new(v_source(), vec![sort_expr_v()], k)
                .unwrap(),
        )
    }

    fn orre_over_source(k: usize) -> Arc<dyn ExecutionPlan> {
        // ORRE demands sorted input.
        let sort = Arc::new(SortExec::new(
            LexOrdering::new(vec![sort_expr_v()]).unwrap(),
            v_source(),
        ));
        Arc::new(
            OrderedRangeRepartitionExec::try_new(sort, vec![sort_expr_v()], k).unwrap(),
        )
    }

    #[test]
    fn range_repartition_routing_expr_bare_source_returns_none() {
        assert!(
            repartition_routing_expr(v_source().as_ref())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn range_repartition_routing_expr_urre_returns_first_order_by_expr() {
        let urre = urre_over_source(4);
        let expr = repartition_routing_expr(urre.as_ref())
            .unwrap()
            .expect("URRE at root should yield a routing expression");
        // The expression is the `v` column — verify by string equality since
        // PhysicalExpr doesn't implement PartialEq.
        assert_eq!(format!("{expr}"), format!("{}", sort_expr_v().expr));
    }

    #[test]
    fn range_repartition_routing_expr_orre_returns_first_order_by_expr() {
        let orre = orre_over_source(4);
        let expr = repartition_routing_expr(orre.as_ref())
            .unwrap()
            .expect("ORRE at root should yield a routing expression");
        assert_eq!(format!("{expr}"), format!("{}", sort_expr_v().expr));
    }

    #[test]
    fn range_repartition_routing_expr_descends_through_stats_wrapper() {
        // Canonical shape: RuntimeStatsExec above URRE — the walker must
        // recurse through the stats wrapper to find the URRE beneath.
        let urre = urre_over_source(4);
        let stats: Arc<dyn ExecutionPlan> =
            Arc::new(RuntimeStatsExec::try_new(urre, Some(vec![sort_expr_v()])).unwrap());
        let expr = repartition_routing_expr(stats.as_ref())
            .unwrap()
            .expect("walker must descend past RuntimeStatsExec");
        assert_eq!(format!("{expr}"), format!("{}", sort_expr_v().expr));
    }

    #[test]
    fn range_repartition_routing_expr_urre_nested_returns_expr() {
        // Two stats-wrapper layers to prove the walk descends more than once.
        let urre = urre_over_source(4);
        let inner_stats =
            Arc::new(RuntimeStatsExec::try_new(urre, Some(vec![sort_expr_v()])).unwrap());
        let outer: Arc<dyn ExecutionPlan> = Arc::new(
            RuntimeStatsExec::try_new(inner_stats, Some(vec![sort_expr_v()])).unwrap(),
        );
        assert!(repartition_routing_expr(outer.as_ref()).unwrap().is_some());
    }

    #[test]
    fn range_repartition_routing_expr_orre_nested_returns_expr() {
        let orre = orre_over_source(4);
        let inner_stats =
            Arc::new(RuntimeStatsExec::try_new(orre, Some(vec![sort_expr_v()])).unwrap());
        let outer: Arc<dyn ExecutionPlan> = Arc::new(
            RuntimeStatsExec::try_new(inner_stats, Some(vec![sort_expr_v()])).unwrap(),
        );
        assert!(repartition_routing_expr(outer.as_ref()).unwrap().is_some());
    }
}

#[cfg(test)]
mod overlap_remap_tests {
    //! `overlap_remap_partitions` — takes a passthrough
    //! `Vec<Vec<PartitionLocation>>` and rewrites it under overlap semantics
    //! computed from merged sketches. Straddling sub-parts appear in every
    //! downstream partition whose range they touch; bookkeeping mismatches
    //! surface as errors rather than silent misroutes.

    use super::*;
    use crate::serde::protobuf::{RuntimeStatsPartitionEntry, RuntimeStatsReport};
    use crate::serde::scheduler::{
        ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
        PartitionId, PartitionLocation, PartitionStats,
    };

    /// Build a `PartitionLocation` for a producer file identified by
    /// `(producer_task_id, sub_part_id)`. `file_id = producer_task_id`
    /// matches the passthrough writer's convention.
    fn location(sub_part_id: usize, producer_task_id: usize) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: 0,
            partition_id: PartitionId {
                job_id: "test-job".into(),
                stage_id: 0,
                partition_id: sub_part_id,
            },
            executor_meta: ExecutorMetadata {
                id: format!("exec-{producer_task_id}"),
                host: "".to_string(),
                port: 0,
                grpc_port: 0,
                specification: ExecutorSpecification::default().with_vcores(0),
                os_info: ExecutorOperatingSystemSpecification::default(),
            },
            // `Some(0)` — helper's default is "empty file". Tests that
            // need rows overwrite `.partition_stats` explicitly.
            partition_stats: PartitionStats::new(Some(0), None, None),
            file_id: Some(producer_task_id as u64),
            is_sort_shuffle: false,
        }
    }

    /// A zero halo of the routing key's type, which is what the scheduler
    /// passes for a consumer with no halo at all.
    const ZERO_HALO: ScalarValue = ScalarValue::Float64(Some(0.0));

    /// Cuts as `Float64` scalars, which is what merging produces.
    fn cuts_f64<const N: usize>(values: [f64; N]) -> Vec<ScalarValue> {
        values
            .into_iter()
            .map(|v| ScalarValue::Float64(Some(v)))
            .collect()
    }

    /// Build a report whose sub-parts carry the key range routing reads.
    /// Slot `sub_part_id` covers `values[sub_part_id]`.
    fn sketch_report(
        producer_task_id: usize,
        values_per_sub_part: Vec<Vec<f64>>,
    ) -> TaskRuntimeStats {
        let scalar = |v: f64| {
            vec![
                datafusion_proto_common::ScalarValue::try_from(&ScalarValue::Float64(
                    Some(v),
                ))
                .unwrap(),
            ]
        };
        let partitions = values_per_sub_part
            .into_iter()
            .enumerate()
            .map(|(sub_part_id, samples)| {
                let extremes = samples.iter().copied().fold(
                    None::<(f64, f64)>,
                    |acc, v| match acc {
                        None => Some((v, v)),
                        Some((lo, hi)) => Some((lo.min(v), hi.max(v))),
                    },
                );
                let (key_min, key_max) = match extremes {
                    Some((lo, hi)) => (scalar(lo), scalar(hi)),
                    None => (Vec::new(), Vec::new()),
                };
                RuntimeStatsPartitionEntry {
                    partition_id: sub_part_id as u32,
                    row_count: samples.len() as u64,
                    key_min,
                    key_max,
                    ..Default::default()
                }
            })
            .collect();
        TaskRuntimeStats {
            producer_task_id,
            report: RuntimeStatsReport {
                order_by: vec![],
                partitions,
                ..Default::default()
            },
        }
    }

    /// Two producers with disjoint value ranges + one downstream cut →
    /// each downstream partition gets exactly one producer's files.
    #[test]
    fn overlap_remap_disjoint_producers_route_to_single_partition() {
        // Producer 100 covers [0, 10); producer 200 covers [20, 30).
        let reports = vec![
            sketch_report(100, vec![vec![0.0, 5.0, 9.0]]),
            sketch_report(200, vec![vec![20.0, 25.0, 29.0]]),
        ];
        // Cut at 15 → partition 0 = (-∞, 15), partition 1 = [15, +∞).
        let cuts = cuts_f64([15.0]);
        // Passthrough map: both producers wrote to sub_part_id=0.
        let original_partitions = vec![vec![location(0, 100), location(0, 200)]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .unwrap();
        assert_eq!(remapped.len(), 2, "K = cuts.len() + 1");
        // Partition 0: only producer 100.
        assert_eq!(remapped[0].len(), 1);
        assert_eq!(remapped[0][0].file_id, Some(100));
        // Partition 1: only producer 200.
        assert_eq!(remapped[1].len(), 1);
        assert_eq!(remapped[1][0].file_id, Some(200));
    }

    /// A straddling sub-part — one whose sketched [min, max] spans the cut
    /// — appears in BOTH downstream partitions' lists. This is the case
    /// RangeFilterExec exists to clean up.
    #[test]
    fn overlap_remap_straddling_producer_appears_in_both_partitions() {
        // Producer 300 covers [5, 25) — straddles the cut at 15.
        let reports = vec![sketch_report(300, vec![vec![5.0, 15.0, 25.0]])];
        let cuts = cuts_f64([15.0]);
        let original_partitions = vec![vec![location(0, 300)]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .unwrap();
        assert_eq!(remapped.len(), 2);
        assert_eq!(remapped[0].len(), 1, "straddler in partition 0");
        assert_eq!(remapped[0][0].file_id, Some(300));
        assert_eq!(remapped[1].len(), 1, "straddler in partition 1");
        assert_eq!(remapped[1][0].file_id, Some(300));
    }

    /// [`sketch_report`] with a NULL count per sub-part, taken as
    /// `(values, nulls)` pairs so the two can't fall out of step.
    fn sketch_report_with_nulls(
        producer_task_id: usize,
        per_sub_part: Vec<(Vec<f64>, u64)>,
    ) -> TaskRuntimeStats {
        let null_counts: Vec<u64> = per_sub_part.iter().map(|(_, n)| *n).collect();
        let mut stats = sketch_report(
            producer_task_id,
            per_sub_part.into_iter().map(|(values, _)| values).collect(),
        );
        for (entry, nulls) in stats.report.partitions.iter_mut().zip(null_counts) {
            entry.null_count = nulls;
            entry.row_count += nulls;
        }
        stats
    }

    /// A file holding both values and NULLs belongs to two consumers: the one
    /// its value range overlaps, and the one holding the NULL run. Reported
    /// extremes are value extremes, so an overlap check alone never finds the
    /// second — and every producer writes its own NULLs to its own last slot,
    /// so dropping it loses all but one producer's share of the run.
    #[test]
    fn overlap_remap_mixed_file_reaches_both_its_range_and_the_null_run() {
        // Values [1, 2] fall left of the cut; the run sorts last, so it
        // belongs to partition 1.
        let reports = vec![sketch_report_with_nulls(100, vec![(vec![1.0, 2.0], 2)])];
        let cuts = cuts_f64([10.0]);
        let original_partitions = vec![vec![location(0, 100)]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            false,
        )
        .unwrap();
        assert_eq!(remapped[0].len(), 1, "values [1, 2] land left of the cut");
        assert_eq!(remapped[1].len(), 1, "the NULL run sorts last");
    }

    /// Same, mirrored: `nulls_first` puts the run in partition 0 while the
    /// values belong to the last partition.
    #[test]
    fn overlap_remap_mixed_file_reaches_the_null_run_at_the_front() {
        let reports = vec![sketch_report_with_nulls(100, vec![(vec![20.0, 25.0], 2)])];
        let cuts = cuts_f64([10.0]);
        let original_partitions = vec![vec![location(0, 100)]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .unwrap();
        assert_eq!(remapped[0].len(), 1, "the NULL run sorts first");
        assert_eq!(
            remapped[1].len(),
            1,
            "values [20, 25] land right of the cut"
        );
    }

    /// When the value range already reaches the partition holding the run,
    /// the file must be delivered once and not twice — a second copy in one
    /// consumer is read twice and so counted twice.
    #[test]
    fn overlap_remap_mixed_file_in_the_run_partition_is_delivered_once() {
        let reports = vec![sketch_report_with_nulls(100, vec![(vec![20.0, 25.0], 2)])];
        let cuts = cuts_f64([10.0]);
        let original_partitions = vec![vec![location(0, 100)]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            false,
        )
        .unwrap();
        assert!(remapped[0].is_empty(), "no values left of the cut");
        assert_eq!(
            remapped[1].len(),
            1,
            "values and run share partition 1, so one delivery"
        );
    }

    /// An all-NULL file has a row count but no value range, so the run is the
    /// only thing placing it.
    #[test]
    fn overlap_remap_all_null_file_goes_only_to_the_null_run() {
        let reports = vec![sketch_report_with_nulls(100, vec![(vec![], 3)])];
        let cuts = cuts_f64([10.0]);
        let mut only_nulls = location(0, 100);
        only_nulls.partition_stats = PartitionStats::new(Some(3), None, None);

        let remapped = cut_partitions(
            vec![vec![only_nulls]],
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            false,
        )
        .unwrap();
        assert!(remapped[0].is_empty(), "no values to place");
        assert_eq!(remapped[1].len(), 1, "the run sorts last");
    }

    /// A producer file without `file_id` means the writer that produced it
    /// wasn't the passthrough writer — remap can't identify the file, so
    /// error rather than silently misroute.
    #[test]
    fn overlap_remap_missing_file_id_errors() {
        let reports = vec![sketch_report(100, vec![vec![1.0, 2.0, 3.0]])];
        let cuts = cuts_f64([10.0]);
        // Loc has file_id=None — invalid for URRE/ORRE stages.
        let mut bad = location(0, 100);
        bad.file_id = None;
        let original_partitions = vec![vec![bad]];

        let err = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .expect_err("missing file_id must surface as an error");
        assert!(
            err.to_string().contains("missing file_id"),
            "unexpected error: {err}"
        );
    }

    /// A report entry with no corresponding PartitionLocation is silently
    /// ignored — the walk is driven by `original_partitions`, so orphan
    /// reports never get consulted. `SuccessfulTask` bundles the shuffle
    /// files and their runtime-stats report together, so this scenario
    /// shouldn't happen in practice; the shape just doesn't need to
    /// enforce it.
    #[test]
    fn orphan_report_is_silently_ignored() {
        // Report from producer 100, but original_partitions only has
        // producer 200 — no file to route.
        let reports = vec![sketch_report(100, vec![vec![1.0, 2.0, 3.0]])];
        let cuts = cuts_f64([10.0]);
        let original_partitions = vec![vec![location(0, 200)]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .unwrap();
        assert_eq!(remapped.len(), 2);
        assert!(remapped[0].is_empty());
        assert!(remapped[1].is_empty());
    }

    /// Empty-sketch entries contribute no locations when the file is
    /// itself empty (num_rows == 0). No data lost, no error.
    #[test]
    fn overlap_remap_empty_sketches_produce_empty_partitions() {
        let reports = vec![sketch_report(100, vec![vec![]])];
        let cuts = cuts_f64([10.0]);
        let original_partitions = vec![vec![location(0, 100)]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .unwrap();
        assert_eq!(remapped.len(), 2);
        assert!(remapped[0].is_empty());
        assert!(remapped[1].is_empty());
    }

    /// A file with rows but no matching sketch cannot be routed by
    /// overlap — silently skipping would drop rows. Surface as an error.
    #[test]
    fn missing_sketch_with_rows_errors() {
        let reports = vec![sketch_report(100, vec![vec![1.0, 2.0]])];
        let cuts = cuts_f64([10.0]);
        // File 200 has 5 rows but no report entry exists for it.
        let mut orphan = location(0, 200);
        orphan.partition_stats = PartitionStats::new(Some(5), None, None);
        let original_partitions = vec![vec![orphan]];

        let err = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .expect_err("file with rows but no sketch must error");
        let msg = err.to_string();
        assert!(
            msg.contains("num_rows=Some(5)") && msg.contains("no usable key range"),
            "unexpected error: {msg}"
        );
    }

    /// Multi-cut layout: with K = 4 buckets and cuts [10, 20, 30], a sketch
    /// covering [15, 25] must land in exactly buckets 1 and 2 — verifying the
    /// `partition_point` range walk lines up with the original inclusive
    /// `lower ≤ sketch_max` / exclusive `sketch_min < upper` semantics.
    #[test]
    fn overlap_remap_multi_cut_range_matches_bucket_semantics() {
        let reports = vec![
            // A: covers only bucket 0
            sketch_report(1, vec![vec![1.0, 5.0, 9.0]]),
            // B: straddles cuts[0]=10 → buckets 0 and 1
            sketch_report(2, vec![vec![5.0, 10.0, 15.0]]),
            // C: fully inside bucket 1
            sketch_report(3, vec![vec![11.0, 15.0, 19.0]]),
            // D: sketch_min == cuts[1] → excluded from bucket 1 (upper is exclusive),
            //    included in buckets 2 and 3
            sketch_report(4, vec![vec![20.0, 25.0, 35.0]]),
            // E: covers only the last bucket
            sketch_report(5, vec![vec![31.0, 40.0, 50.0]]),
            // F: spans the whole range → every bucket
            sketch_report(6, vec![vec![0.0, 20.0, 100.0]]),
        ];
        let cuts = cuts_f64([10.0, 20.0, 30.0]);
        let original_partitions = vec![vec![
            location(0, 1),
            location(0, 2),
            location(0, 3),
            location(0, 4),
            location(0, 5),
            location(0, 6),
        ]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .unwrap();
        assert_eq!(remapped.len(), 4);
        let ids = |b: &[PartitionLocation]| {
            let mut v: Vec<u64> = b.iter().map(|l| l.file_id.unwrap()).collect();
            v.sort();
            v
        };
        assert_eq!(ids(&remapped[0]), vec![1u64, 2, 6]);
        assert_eq!(ids(&remapped[1]), vec![2u64, 3, 6]);
        assert_eq!(ids(&remapped[2]), vec![4u64, 6]);
        assert_eq!(ids(&remapped[3]), vec![4u64, 5, 6]);
    }

    /// A file with an unknown row count (`None`) and no usable sketch
    /// also errors — we can't confirm the file is empty, so we can't
    /// safely skip it.
    #[test]
    fn missing_sketch_with_unknown_rows_errors() {
        let reports: Vec<TaskRuntimeStats> = vec![];
        let cuts = cuts_f64([10.0]);
        let mut orphan = location(0, 100);
        orphan.partition_stats = PartitionStats::default(); // num_rows = None
        let original_partitions = vec![vec![orphan]];

        let err = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &ZERO_HALO,
            &ZERO_HALO,
            true,
        )
        .expect_err("file with unknown rows but no sketch must error");
        let msg = err.to_string();
        assert!(
            msg.contains("num_rows=None") && msg.contains("no usable key range"),
            "unexpected error: {msg}"
        );
    }

    /// Halo widening lets each partition see files that sit within
    /// `[halo_lo, halo_hi]` of its raw cut range — the downstream
    /// `RangeFilterExec`'s frame-context rows come from those files, and
    /// missing any of them causes RANGE-frame window sums to drop rows
    /// at boundaries.
    ///
    /// The K=5 layout with `halo_lo != halo_hi` proves three things at
    /// once: (a) `halo_lo` widens downward, (b) `halo_hi` widens upward,
    /// (c) the halo band stays *local* — it does not bleed across two
    /// cut hops to far-away partitions.
    #[test]
    fn overlap_remap_halo_band_widens_both_sides_without_bleeding_to_far_partitions() {
        // K=5, asymmetric halos so we can tell halo_lo and halo_hi apart.
        let cuts = cuts_f64([10.0, 20.0, 30.0, 40.0]);
        let halo_lo = ScalarValue::Float64(Some(1.0));
        let halo_hi = ScalarValue::Float64(Some(2.0));
        // Effective partition ranges:
        //   P0: (-∞, 12)   P1: [9, 22)   P2: [19, 32)   P3: [29, 42)   P4: [39, +∞)
        let reports = vec![
            // 100 sits deep inside P0 — far from P1's halo, stays P0-only.
            sketch_report(100, vec![vec![5.0, 6.0]]),
            // 200 is entirely below cut 20 but within halo_lo=1 of it —
            // routes to P1 (own bucket) AND P2 (halo band from below).
            // Must NOT reach P0 (two cut hops away).
            sketch_report(200, vec![vec![18.0, 19.0]]),
            // 300 sits cleanly inside P2 — no halo participation.
            sketch_report(300, vec![vec![25.0, 26.0]]),
            // 400 is entirely above cut 30 but within halo_hi=2 of it —
            // routes to P2 (halo band from above) AND P3 (own bucket).
            // Must NOT reach P4 (two cut hops away).
            sketch_report(400, vec![vec![31.0, 32.0]]),
            // 500 sits deep inside P4 — far from P3's halo, stays P4-only.
            sketch_report(500, vec![vec![45.0, 46.0]]),
        ];
        let original_partitions = vec![vec![
            location(0, 100),
            location(0, 200),
            location(0, 300),
            location(0, 400),
            location(0, 500),
        ]];

        let remapped = cut_partitions(
            original_partitions,
            &reports,
            &cuts,
            &halo_lo,
            &halo_hi,
            true,
        )
        .unwrap();
        let ids = |b: &[PartitionLocation]| {
            let mut v: Vec<u64> = b.iter().map(|l| l.file_id.unwrap()).collect();
            v.sort();
            v
        };
        assert_eq!(remapped.len(), 5);
        assert_eq!(
            ids(&remapped[0]),
            vec![100u64],
            "P0 sees only its own bucket — 200's halo band belongs to P1/P2, not here",
        );
        assert_eq!(
            ids(&remapped[1]),
            vec![200u64],
            "P1 sees its own straddler (200) below cut 20",
        );
        assert_eq!(
            ids(&remapped[2]),
            vec![200u64, 300, 400],
            "P2 (middle) sees siblings from BOTH halo bands — 200 via halo_lo, 400 via halo_hi — plus its own 300",
        );
        assert_eq!(
            ids(&remapped[3]),
            vec![400u64],
            "P3 sees its own straddler (400) above cut 30",
        );
        assert_eq!(
            ids(&remapped[4]),
            vec![500u64],
            "P4 sees only its own bucket — 400's halo band belongs to P2/P3, not here",
        );
    }
}
