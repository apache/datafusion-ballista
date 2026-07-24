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
//!   construction). Per-partition `Mutex<TDigest>` keeps writes off any
//!   shared lock.
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
//! TODO: swap `TDigest` for a generic-over-`Ord` KLL sketch. TDigest is
//! `Float64`-only, single-column, and has no representation for NULLs;
//! a KLL implementation would sketch the full `Vec<PhysicalSortExpr>`
//! (composite keys, non-numeric types) and position NULLs per each
//! sort key's `SortOptions::nulls_first`, letting the operator drop
//! both the "first expression, `Float64` only" restriction and the
//! not-null-routing-column requirement enforced at construction today.
//!
//! This PR lands the tap in isolation: nothing wires it into a plan yet,
//! and the executor doesn't yet ship the accumulated state back to the
//! scheduler. Those pieces arrive with the range-repartition operator
//! (which is the first consumer).

use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Result, Statistics, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{Distribution, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_functions_aggregate_common::tdigest::TDigest;
use futures::stream::StreamExt;
use log::debug;

/// T-Digest centroid budget. 100 is DataFusion's default and gives ~1%
/// quantile error, plenty of margin over the sub-partition counts we
/// expect at bin-pack time.
const TDIGEST_MAX_SIZE: usize = 100;

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
    /// Only allocated when `order_by` is `Some`. Sketches over the first
    /// ORDER BY expression's `Float64` values. `Mutex`-per-partition to
    /// keep writes off any shared lock.
    sketches: Option<Arc<[Mutex<TDigest>]>>,
    properties: Arc<PlanProperties>,
}

impl RuntimeStatsExec {
    /// Wrap `input`. If `order_by` is provided, its first entry drives
    /// the per-partition T-Digest; the full slice is preserved for serde
    /// and for downstream operators (`SortExec`, `BoundedWindowAggExec`)
    /// that need it. When `Some`, at least one expression is required —
    /// nothing to sketch on with an empty slice — and the first
    /// expression must evaluate to a non-nullable `Float64` (T-Digest is
    /// `Float64`-only and has no NULL slot; the KLL swap will lift both
    /// restrictions).
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        order_by: Option<Vec<PhysicalSortExpr>>,
    ) -> Result<Self> {
        if let Some(exprs) = &order_by {
            let [first, ..] = exprs.as_slice() else {
                return internal_err!(
                    "RuntimeStatsExec: order_by is Some but empty; pass None to skip sketching"
                );
            };
            let schema = input.schema();
            let routing_type = first.expr.data_type(&schema)?;
            if routing_type != DataType::Float64 {
                return internal_err!(
                    "RuntimeStatsExec: routing expression must be Float64, got {routing_type:?}"
                );
            }
            if first.expr.nullable(&schema)? {
                return internal_err!(
                    "RuntimeStatsExec: routing expression must be non-nullable; \
                     T-Digest has no NULL slot (lifts with the KLL swap)"
                );
            }
        }
        let partition_count = input.output_partitioning().partition_count();
        let row_counts: Arc<[AtomicUsize]> = (0..partition_count)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into();
        let sketches: Option<Arc<[Mutex<TDigest>]>> = order_by.as_ref().map(|_| {
            (0..partition_count)
                .map(|_| Mutex::new(TDigest::new(TDIGEST_MAX_SIZE)))
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
            sketches,
            properties,
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

    /// Snapshot of one partition's running quantile sketch. Returns
    /// `None` when the operator was built in row-count-only mode (no
    /// `order_by`). Cheap clone (a `Vec<Centroid>` of size
    /// ≤ `TDIGEST_MAX_SIZE`).
    ///
    /// Errors if `partition` ≥ input's partition count — callers pass a
    /// partition id they've already used with `execute`.
    pub fn quantile_sketch(&self, partition: usize) -> Result<Option<TDigest>> {
        let Some(sketches) = &self.sketches else {
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
                "RuntimeStatsExec partition {}: sketch mutex poisoned: {e}",
                partition
            )
        })?;
        Ok(Some(guard.clone()))
    }

    /// All partitions merged into one sketch. `Ok(None)` in
    /// row-count-only mode.
    pub fn merged_quantile_sketch(&self) -> Result<Option<TDigest>> {
        let Some(sketches) = self.sketches.as_ref() else {
            return Ok(None);
        };
        let snapshots: Vec<TDigest> = sketches
            .iter()
            .enumerate()
            .map(|(partition, m)| {
                let guard = m.lock().map_err(|e| {
                    internal_datafusion_err!(
                        "RuntimeStatsExec partition {}: sketch mutex poisoned: {e}",
                        partition
                    )
                })?;
                Ok(guard.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(TDigest::merge_digests(snapshots.iter())))
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
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    /// Every input row is emitted exactly once. Overrides default `Unknown`.
    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
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
        let sketches = self.sketches.clone();
        let routing_expr = self
            .order_by
            .as_ref()
            .and_then(|exprs| exprs.first())
            .map(|e| e.expr.clone());

        let state = StreamState {
            input: input_stream,
            row_counts,
            sketches,
            routing_expr,
            partition,
        };
        let out = futures::stream::unfold(state, |mut state| async move {
            let batch = state.input.next().await?;
            let forwarded = batch.and_then(|batch| {
                state.ingest(&batch)?;
                Ok(batch)
            });
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
    sketches: Option<Arc<[Mutex<TDigest>]>>,
    routing_expr: Option<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    partition: usize,
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
        // never made it downstream. With Float64 validated at
        // construction, the downcast is belt-and-braces — evaluate()
        // itself can still fail for expr-internal reasons.
        if let (Some(sketches), Some(routing_expr)) = (&self.sketches, &self.routing_expr)
        {
            let evaluated = routing_expr.evaluate(batch)?;
            let array = evaluated.into_array(batch.num_rows())?;
            let f64_arr =
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "RuntimeStatsExec partition {}: routing expr produced {:?}, \
                         expected Float64",
                            self.partition,
                            array.data_type()
                        )
                    })?;
            // Construction rejects nullable routing exprs, so nulls
            // shouldn't reach us; keep the flatten as cheap defense
            // against a Field/data mismatch. NULLs are still forwarded
            // downstream and counted — they just can't enter the
            // sketch until the KLL swap gives us a `nulls_first`-aware
            // slot.
            let values: Vec<f64> = f64_arr.iter().flatten().collect();
            if !values.is_empty() {
                let slot = sketches.get(self.partition).ok_or_else(|| {
                    internal_datafusion_err!(
                        "RuntimeStatsExec: partition {} out of range on sketch slot",
                        self.partition
                    )
                })?;
                let mut sketch = slot.lock().map_err(|e| {
                    internal_datafusion_err!(
                        "RuntimeStatsExec partition {}: sketch mutex poisoned: {e}",
                        self.partition
                    )
                })?;
                *sketch = sketch.merge_unsorted_f64(values);
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
        match self.sketches.as_ref().and_then(|s| s.get(self.partition)) {
            Some(slot) => match slot.lock() {
                Ok(sketch) => {
                    debug!(
                        "RuntimeStatsExec partition {}: rows={} T-Digest count={} min={} max={}",
                        self.partition,
                        rows,
                        sketch.count(),
                        sketch.min(),
                        sketch.max(),
                    );
                }
                Err(e) => {
                    log::error!(
                        "RuntimeStatsExec partition {}: sketch mutex poisoned on Drop; \
                         skipping end-of-stream log: {e}",
                        self.partition,
                    );
                }
            },
            None => {
                debug!(
                    "RuntimeStatsExec partition {}: rows={}",
                    self.partition, rows
                );
            }
        }
    }
}

/// Walk `plan` and collect one [`crate::serde::protobuf::RuntimeStatsReport`]
/// per [`RuntimeStatsExec`] that remains valid at the plan's output.
/// "Valid" means reachable through single-child chains of distribution-
/// preserving operators only — see
/// [`super::range_repartition_common::preserves_distribution`]. A stats-
/// tap sitting *below* an [`super::UnorderedRangeRepartitionExec`] (or
/// any distribution-changing operator) is excluded automatically because
/// the walker stops at that boundary; its sketch describes data the
/// repartitioner then routed away and is no longer meaningful at the
/// plan's output.
///
/// Executors call this once per task at completion to package what to
/// return to the scheduler.
pub fn collect_reports(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<crate::serde::protobuf::RuntimeStatsReport>> {
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
    } else if !super::range_repartition_common::preserves_distribution(plan.as_ref()) {
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
) -> Result<crate::serde::protobuf::RuntimeStatsReport> {
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
        let sketch = match stats.quantile_sketch(partition_id)? {
            Some(sk) if sk.count() > 0.0 => Some(sketch_to_proto(&sk)?),
            _ => None,
        };
        partitions.push(crate::serde::protobuf::RuntimeStatsPartitionEntry {
            partition_id: partition_id as u32,
            row_count,
            sketch,
        });
    }
    Ok(crate::serde::protobuf::RuntimeStatsReport {
        order_by,
        partitions,
    })
}

/// Serialize a T-Digest to the on-wire
/// [`crate::serde::protobuf::QuantileSketchState`].
///
/// Wraps `TDigest::to_scalar_state()` — the 6-element canonical form
/// `(max_size, sum, count, max, min, centroids_as_list)` — each element
/// encoded via `datafusion_proto_common::ScalarValue::try_from`.
pub fn sketch_to_proto(
    sketch: &TDigest,
) -> Result<crate::serde::protobuf::QuantileSketchState> {
    let state = sketch.to_scalar_state();
    let proto_state = state
        .iter()
        .map(datafusion_proto_common::ScalarValue::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            internal_datafusion_err!("failed to encode TDigest to proto: {e:?}")
        })?;
    Ok(crate::serde::protobuf::QuantileSketchState { state: proto_state })
}

/// Deserialize a [`crate::serde::protobuf::QuantileSketchState`] into a
/// T-Digest.
///
/// Reverses [`sketch_to_proto`]. Guards against corrupted wire input by
/// checking the element count before calling
/// `TDigest::from_scalar_state`, which would panic on invalid shape.
pub fn sketch_from_proto(
    proto: &crate::serde::protobuf::QuantileSketchState,
) -> Result<TDigest> {
    let scalars = proto
        .state
        .iter()
        .map(datafusion::common::ScalarValue::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            internal_datafusion_err!(
                "failed to decode QuantileSketchState scalars: {e:?}"
            )
        })?;
    if scalars.len() != 6 {
        return internal_err!(
            "QuantileSketchState: expected 6 elements per TDigest::to_scalar_state, got {} \
             — likely wire corruption",
            scalars.len()
        );
    }
    Ok(TDigest::from_scalar_state(&scalars))
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
    /// `partition_count - 1` cut points at quantiles `i/partition_count`
    /// on the merged T-Digest. Empty when `partition_count < 2` or no
    /// non-empty sketches were merged.
    pub cuts: Vec<f64>,
    /// Merged T-Digest's `min()` if at least one non-empty sketch
    /// contributed; `None` in row-count-only mode.
    pub min: Option<f64>,
    /// Merged T-Digest's `max()` if at least one non-empty sketch
    /// contributed; `None` in row-count-only mode.
    pub max: Option<f64>,
}

/// Group `RuntimeStatsReport`s by `order_by` wire tag, merge the T-Digests
/// within each group, and return one [`MergedRuntimeStats`] per group.
///
/// Errors surface planner / wire invariants the caller can't quietly
/// paper over: mismatched partition counts within a group (the planner
/// gave two tasks in the same stage different plans), or a sketch that
/// won't decode (wire corruption). The caller decides whether to log
/// and drop, propagate to the user, or fail the stage.
pub fn merge_reports(
    reports: &[crate::serde::protobuf::RuntimeStatsReport],
) -> Result<Vec<MergedRuntimeStats>> {
    use prost::Message;
    use std::collections::HashMap;

    if reports.is_empty() {
        return Ok(Vec::new());
    }

    // Group by the bytes of the encoded `order_by`. Prost-encoding each
    // `PhysicalSortExprNode` and concatenating gives a stable, cheap
    // grouping key without needing `Hash` on the generated proto types.
    let mut groups: HashMap<Vec<u8>, Vec<&crate::serde::protobuf::RuntimeStatsReport>> =
        HashMap::new();
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
fn merge_group(
    group: &[&crate::serde::protobuf::RuntimeStatsReport],
) -> Result<MergedRuntimeStats> {
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
    let mut sketches: Vec<TDigest> = Vec::new();
    for report in group {
        for entry in &report.partitions {
            total_rows = total_rows.saturating_add(entry.row_count);
            if let Some(proto_sketch) = entry.sketch.as_ref() {
                let sketch = sketch_from_proto(proto_sketch)?;
                if sketch.count() > 0.0 {
                    sketches.push(sketch);
                }
            }
        }
    }

    if sketches.is_empty() {
        return Ok(MergedRuntimeStats {
            order_by_len: first.order_by.len(),
            partition_count,
            task_count,
            total_rows,
            cuts: Vec::new(),
            min: None,
            max: None,
        });
    }

    let merged_sketch = TDigest::merge_digests(sketches.iter());
    let cuts: Vec<f64> = if partition_count > 1 {
        (1..partition_count)
            .map(|cut_index| {
                merged_sketch.estimate_quantile(cut_index as f64 / partition_count as f64)
            })
            .collect()
    } else {
        Vec::new()
    };
    Ok(MergedRuntimeStats {
        order_by_len: first.order_by.len(),
        partition_count,
        task_count,
        total_rows,
        cuts,
        min: Some(merged_sketch.min()),
        max: Some(merged_sketch.max()),
    })
}

/// Merge `reports` and log each group's merged view at `debug!`
/// (`RUST_LOG` promotes when needed). Any merge error is logged at
/// `warn!` — the scheduler doesn't want telemetry loss to tank a query
/// whose data was already produced correctly. The scheduler calls this
/// once per stage-attempt at final-success.
pub fn log_merged_runtime_stats(
    job_id: &str,
    stage_id: usize,
    reports: &[crate::serde::protobuf::RuntimeStatsReport],
) {
    let merged_groups = match merge_reports(reports) {
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
mod wire_tests {
    use super::*;

    /// Round-trip a populated T-Digest through the wire. Count / min /
    /// max should survive unchanged; quantile queries should agree to
    /// within floating-point equality since serde is lossless.
    #[test]
    fn tdigest_wire_roundtrip_preserves_populated_sketch() {
        let mut original = TDigest::new(100);
        original = original
            .merge_unsorted_f64(vec![1.0, 5.0, 10.0, 20.0, 30.0, 50.0, 75.0, 100.0]);
        let proto = sketch_to_proto(&original).unwrap();
        let decoded = sketch_from_proto(&proto).unwrap();
        assert_eq!(decoded.count(), original.count());
        assert_eq!(decoded.min(), original.min());
        assert_eq!(decoded.max(), original.max());
        // Quantile agreement at the median.
        assert_eq!(
            decoded.estimate_quantile(0.5),
            original.estimate_quantile(0.5),
        );
    }

    /// Empty T-Digest — zero centroids, no samples — still survives
    /// the round-trip. This is the case an executor hits when a
    /// `RuntimeStatsExec` was present in the plan but no batches
    /// flowed through (e.g. empty input partition).
    #[test]
    fn tdigest_wire_roundtrip_preserves_empty_sketch() {
        let original = TDigest::new(100);
        let proto = sketch_to_proto(&original).unwrap();
        let decoded = sketch_from_proto(&proto).unwrap();
        assert_eq!(decoded.count(), 0.0);
        assert_eq!(decoded.max_size(), original.max_size());
    }

    /// Corrupted wire input (wrong element count) is caught before
    /// `TDigest::from_scalar_state` gets a chance to panic.
    #[test]
    fn sketch_from_proto_rejects_wrong_shape() {
        use datafusion::common::ScalarValue;
        let proto = crate::serde::protobuf::QuantileSketchState {
            state: (0..3)
                .map(|_| {
                    datafusion_proto_common::ScalarValue::try_from(&ScalarValue::Float64(
                        Some(0.0),
                    ))
                    .unwrap()
                })
                .collect(),
        };
        let err = sketch_from_proto(&proto)
            .expect_err("wrong-count wire input must be rejected before decode");
        assert!(
            err.to_string().contains("expected 6 elements"),
            "got: {err}"
        );
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

        // Sketch observed every non-null routing value.
        let sketch = stats.quantile_sketch(0).unwrap().unwrap();
        assert_eq!(sketch.count(), 5.0);
        assert_eq!(sketch.min(), 1.0);
        assert_eq!(sketch.max(), 5.0);
        // Merged over the (single) partition matches the per-partition view.
        assert_eq!(
            stats.merged_quantile_sketch().unwrap().unwrap().count(),
            5.0
        );
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
            stats.quantile_sketch(0).unwrap().is_none(),
            "row-count-only mode must not allocate a sketch"
        );
        assert!(stats.merged_quantile_sketch().unwrap().is_none());
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
        let proto_sketch = entry.sketch.as_ref().expect("sketch present in wire");
        let round_tripped = sketch_from_proto(proto_sketch).unwrap();
        assert_eq!(round_tripped.count(), 5.0);
        assert_eq!(round_tripped.min(), 1.0);
        assert_eq!(round_tripped.max(), 5.0);
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
        assert!(
            report.partitions[0].sketch.is_none(),
            "no sketch in row-count-only mode"
        );
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

    use super::*;
    use crate::serde::protobuf::{RuntimeStatsPartitionEntry, RuntimeStatsReport};
    use datafusion_proto::protobuf::PhysicalSortExprNode;

    /// Build a report whose partition slots each carry a sketch made
    /// from that slot's `values`. Slot `slot_id` in the resulting
    /// report has `row_count = values[slot_id].len()` and a sketch
    /// over those values.
    fn sketching_report(
        order_by: Vec<PhysicalSortExprNode>,
        values_per_slot: Vec<Vec<f64>>,
    ) -> RuntimeStatsReport {
        let partitions = values_per_slot
            .into_iter()
            .enumerate()
            .map(|(slot_id, slot_values)| {
                let row_count = slot_values.len() as u64;
                let sketch = if slot_values.is_empty() {
                    None
                } else {
                    let digest = TDigest::new(100).merge_unsorted_f64(slot_values);
                    Some(sketch_to_proto(&digest).unwrap())
                };
                RuntimeStatsPartitionEntry {
                    partition_id: slot_id as u32,
                    row_count,
                    sketch,
                }
            })
            .collect();
        RuntimeStatsReport {
            order_by,
            partitions,
        }
    }

    fn only_group(reports: &[RuntimeStatsReport]) -> MergedRuntimeStats {
        let mut groups = merge_reports(reports).expect("merge should succeed");
        match groups.as_slice() {
            [_] => groups.remove(0),
            other => panic!("expected exactly one group, got {}", other.len()),
        }
    }

    /// Two reports over disjoint value ranges — merged sketch spans the
    /// union, total_rows sums, and the partition_count=2 midpoint cut
    /// falls between the two ranges.
    #[test]
    fn merge_reports_combines_disjoint_ranges() {
        // Both reports share an empty `order_by` — we just need two
        // reports that land in the same group.
        let low_range = sketching_report(vec![], vec![vec![1.0, 2.0, 3.0], vec![]]);
        let high_range = sketching_report(vec![], vec![vec![], vec![10.0, 11.0, 12.0]]);

        let group = only_group(&[low_range, high_range]);
        assert_eq!(group.partition_count, 2);
        assert_eq!(group.task_count, 2);
        assert_eq!(group.total_rows, 6);
        let midpoint = match group.cuts.as_slice() {
            [midpoint] => *midpoint,
            other => panic!("expected exactly one cut, got {other:?}"),
        };
        assert!(
            (3.0..=10.0).contains(&midpoint),
            "midpoint cut should land between ranges (got {midpoint})"
        );
        assert_eq!(group.min, Some(1.0));
        assert_eq!(group.max, Some(12.0));
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
        let report = sketching_report(vec![], values_per_slot);

        let group = only_group(&[report]);
        assert_eq!(group.partition_count, 4);
        let (p25, p50, p75) = match group.cuts.as_slice() {
            [p25, p50, p75] => (*p25, *p50, *p75),
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
                    sketch: None,
                },
                RuntimeStatsPartitionEntry {
                    partition_id: 1,
                    row_count: row_counts[1],
                    sketch: None,
                },
            ],
        };
        let group = only_group(&[make_report([100, 200]), make_report([300, 400])]);
        assert_eq!(group.partition_count, 2);
        assert_eq!(group.total_rows, 1000);
        assert!(group.cuts.is_empty());
        assert!(group.min.is_none());
        assert!(group.max.is_none());
    }

    /// Mismatched partition counts within a group surface as an error —
    /// the caller (scheduler / slice-D consumer) sees the invariant
    /// break rather than silently getting a partial merge.
    #[test]
    fn merge_reports_errors_on_mismatched_partition_counts() {
        let two_partitions = sketching_report(vec![], vec![vec![1.0], vec![2.0]]);
        let one_partition = sketching_report(vec![], vec![vec![3.0]]);
        let err = merge_reports(&[two_partitions, one_partition])
            .expect_err("mismatched partition counts must error");
        let message = err.to_string();
        assert!(
            message.contains("mismatched partition counts"),
            "expected mismatch error, got: {message}"
        );
    }

    /// A wire-corrupted sketch — one whose scalar-state length is wrong
    /// — surfaces the underlying `sketch_from_proto` error rather than
    /// getting silently dropped.
    #[test]
    fn merge_reports_propagates_sketch_decode_errors() {
        use crate::serde::protobuf::QuantileSketchState;
        use datafusion::common::ScalarValue;

        // Six scalars is the valid shape; three is a corrupted wire.
        let corrupt_sketch = QuantileSketchState {
            state: (0..3)
                .map(|_| {
                    datafusion_proto_common::ScalarValue::try_from(&ScalarValue::Float64(
                        Some(0.0),
                    ))
                    .unwrap()
                })
                .collect(),
        };
        let report = RuntimeStatsReport {
            order_by: vec![],
            partitions: vec![RuntimeStatsPartitionEntry {
                partition_id: 0,
                row_count: 1,
                sketch: Some(corrupt_sketch),
            }],
        };
        let err = merge_reports(&[report])
            .expect_err("corrupt sketch must surface as an error");
        assert!(
            err.to_string().contains("expected 6 elements"),
            "expected shape-error propagation, got: {err}"
        );
    }

    /// Empty input → empty output; ensures no panics or spurious groups.
    #[test]
    fn merge_reports_empty_input_is_empty_output() {
        assert!(merge_reports(&[]).unwrap().is_empty());
    }
}
