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
use datafusion::physical_expr::PhysicalSortExpr;
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
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
