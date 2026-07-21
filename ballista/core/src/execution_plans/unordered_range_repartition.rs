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

//! Value-range router over unordered inputs. Reads `P` input partitions with
//! no sort assumption, evaluates the first ORDER BY expression per row, and
//! routes each row to one of `K` output partitions under the half-open
//! convention: partition `p` owns `[cut[p-1], cut[p])` with virtual `-∞`/`+∞`
//! sentinels on the ends.
//!
//! # Dynamic discovery
//!
//! The whole point of this operator is that boundaries are *discovered at
//! runtime*, not baked in at plan time. On the first batch to arrive from
//! any input partition, the operator walks its own child subtree to find a
//! sibling [`RuntimeStatsExec`], snapshots its `merged_quantile_sketch()`,
//! and computes `K - 1` quantile cuts at `1/K, 2/K, ..., (K-1)/K`. All
//! batches (including the one that triggered the snapshot) then route
//! through those cuts.
//!
//! # Fallback
//!
//! If no sibling [`RuntimeStatsExec`] exists, or it's in row-count-only mode
//! (no sketch), or the sketch is empty, discovery returns an empty cut set
//! and every row lands in output partition 0 — the natural single-bucket
//! outcome of `boundaries.len() + 1 = 1`. Runtime routing must never crash;
//! degraded-but-alive beats the alternative, and downstream sees an empty
//! stream on the K-1 partitions that got no data.
//!
//! # Type generality
//!
//! The impl hardcodes Float64 downcast internally (that's what DataFusion's
//! T-Digest speaks). The public API and the sibling [`RuntimeStatsExec`]
//! stay type-agnostic; widening to other `Ord` `ScalarValue` types replaces
//! the downcast + boundary computation, no API break.
//!
//! Sibling `OrderedRangeRepartitionExec` (not yet built) handles the sorted
//! case (N sorted → M sorted range-disjoint via k-way merge). See
//! `docs/source/contributors-guide/parallel-window-kll-adaptive.md`.
//!
//! [`RuntimeStatsExec`]: crate::execution_plans::RuntimeStatsExec

use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::execution_plans::range_repartition_common::{
    broadcast_error, discover_cuts, split_batch_by_range,
};

/// Per-output-partition channel capacity. Small = tight backpressure; the
/// classic double-buffering shape (one batch in-flight while consumer works
/// on another). When a downstream drain lags, `send().await` suspends the
/// scatter task, which suspends its input read, which propagates upstream.
/// Bump if profiling shows scatter tasks blocked on `send` more than the
/// downstream is meaningfully doing work.
const CHANNEL_CAPACITY: usize = 2;

/// Value-range router over unordered inputs. See the module-level docs.
pub struct UnorderedRangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    /// Lexicographic ORDER BY carried through from the wrapping window
    /// operator. `try_new` guarantees at least one element; the first entry
    /// (a `Float64` column, until we widen) drives routing.
    order_by: Vec<PhysicalSortExpr>,
    /// K — number of output partitions. K=1 collapses all P inputs to a
    /// single bucket (the same shape discovery-failure fallback produces);
    /// larger K spreads rows by value range.
    output_partitions: usize,
    /// Lazy channel setup guarded by `Mutex`. First `execute()` call spawns
    /// the P input-reader tasks and creates the K channels; subsequent
    /// `execute(p)` calls take `channels[p]`.
    state: Arc<Mutex<DispatchState>>,
    properties: Arc<PlanProperties>,
}

/// Per-exec-instance lazy state. Owns the K receivers between setup and the
/// K per-partition takes.
struct DispatchState {
    /// K slots. Each holds `Some(receiver)` after setup, `None` once its
    /// output partition has been consumed.
    receivers: Vec<Option<mpsc::Receiver<Result<RecordBatch>>>>,
    initialized: bool,
}

impl UnorderedRangeRepartitionExec {
    /// Wrap `input`. `order_by` must be non-empty and its first entry must
    /// evaluate to `Float64` against `input.schema()`. `output_partitions`
    /// is K; any value works, K=1 gives a coalesce-shaped passthrough.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        order_by: Vec<PhysicalSortExpr>,
        output_partitions: usize,
        // TODO: support RANGE & ROW halos
    ) -> Result<Self> {
        let [routing, ..] = order_by.as_slice() else {
            return internal_err!(
                "UnorderedRangeRepartitionExec requires at least one ORDER BY expression"
            );
        };
        let schema = input.schema();
        let routing_type = routing.expr.data_type(&schema)?;
        if !matches!(routing_type, DataType::Float64) {
            // TODO: support all continuous primitives
            return internal_err!(
                "UnorderedRangeRepartitionExec routing expression `{}` must be Float64, got {:?}",
                routing.expr,
                routing_type
            );
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(output_partitions),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        let state = Arc::new(Mutex::new(DispatchState {
            receivers: Vec::new(),
            initialized: false,
        }));
        Ok(Self {
            input,
            order_by,
            output_partitions,
            state,
            properties,
        })
    }

    /// Full ORDER BY carried through from the wrapping window operator.
    pub fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    /// K — the fixed output partition count.
    pub fn output_partitions(&self) -> usize {
        self.output_partitions
    }
}

impl Debug for UnorderedRangeRepartitionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnorderedRangeRepartitionExec")
            .field("order_by", &self.order_by)
            .field("output_partitions", &self.output_partitions)
            .finish()
    }
}

impl DisplayAs for UnorderedRangeRepartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let routing = &self.order_by[0];
        write!(
            f,
            "UnorderedRangeRepartitionExec: routing={} {} → {} partitions",
            routing.expr,
            if routing.options.descending {
                "desc"
            } else {
                "asc"
            },
            self.output_partitions,
        )
    }
}

impl ExecutionPlan for UnorderedRangeRepartitionExec {
    fn name(&self) -> &str {
        "UnorderedRangeRepartitionExec"
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!(
                "UnorderedRangeRepartitionExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(UnorderedRangeRepartitionExec::try_new(
            input.clone(),
            self.order_by.clone(),
            self.output_partitions,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| internal_datafusion_err!("dispatch state mutex poisoned"))?;
        if !state.initialized {
            let mut senders = Vec::with_capacity(self.output_partitions);
            let mut receivers = Vec::with_capacity(self.output_partitions);
            for _ in 0..self.output_partitions {
                let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                senders.push(tx);
                receivers.push(Some(rx));
            }
            state.receivers = receivers;
            state.initialized = true;
            let senders: Arc<[mpsc::Sender<Result<RecordBatch>>]> = senders.into();
            // Empty `Vec<f64>` = discovery failed = single-bucket fallback.
            // Populated once, on the first batch, by whichever scatter task
            // wins the `OnceLock::get_or_init` race.
            let cuts_cell: Arc<OnceLock<Vec<f64>>> = Arc::new(OnceLock::new());
            let input_partitions = self.input.output_partitioning().partition_count();
            let routing_expr = self.order_by[0].expr.clone(); // TODO: KLL for multi-column?
            for input_partition in 0..input_partitions {
                let child = self.input.clone();
                let senders = senders.clone();
                let cuts_cell = cuts_cell.clone();
                let routing_expr = routing_expr.clone();
                let ctx = ctx.clone();
                let output_partitions = self.output_partitions;
                tokio::spawn(async move {
                    scatter_input_partition(
                        child,
                        input_partition,
                        ctx,
                        routing_expr,
                        senders,
                        cuts_cell,
                        output_partitions,
                    )
                    .await;
                });
            }
        }
        let Some(slot) = state.receivers.get_mut(partition) else {
            return internal_err!(
                "UnorderedRangeRepartitionExec: partition {} out of range (have {})",
                partition,
                self.output_partitions
            );
        };
        let receiver = slot.take().ok_or_else(|| {
            internal_datafusion_err!(
                "UnorderedRangeRepartitionExec: execute({partition}) called twice \
                 on the same instance"
            )
        })?;
        drop(state);
        let stream = ReceiverStream::new(receiver);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// One background task per input partition. Reads the child's batches; on
/// the first batch, the shared `OnceLock` learns the value cuts (by walking
/// the child tree for `RuntimeStatsExec` and snapshotting its sketch).
/// Every batch — including the first — is then dispatched via
/// `split_batch_by_range`, which handles the degenerate empty-cuts case
/// (all rows to partition 0) transparently.
async fn scatter_input_partition(
    child: Arc<dyn ExecutionPlan>,
    input_partition: usize,
    ctx: Arc<TaskContext>,
    routing_expr: Arc<dyn PhysicalExpr>,
    senders: Arc<[mpsc::Sender<Result<RecordBatch>>]>,
    cuts_cell: Arc<OnceLock<Vec<f64>>>,
    output_partitions: usize,
) {
    let mut stream = match child.execute(input_partition, ctx) {
        Ok(s) => s,
        Err(err) => {
            broadcast_error(&senders, err).await;
            return;
        }
    };
    while let Some(batch_result) = stream.next().await {
        // Every downstream consumer dropped its receiver — DRR itself was
        // dropped, or every output partition had a `LIMIT` above and hit it.
        // Either way, no one's listening; stop reading input so this scatter
        // task exits promptly (drops its `stream`, which propagates upstream).
        if senders.iter().all(|s| s.is_closed()) {
            return;
        }
        let batch = match batch_result {
            Ok(b) => b,
            Err(err) => {
                broadcast_error(&senders, err).await;
                return;
            }
        };
        let cuts = cuts_cell.get_or_init(|| {
            discover_cuts(&child, routing_expr.as_ref(), output_partitions)
        });
        // TODO(perf): `split_batch_by_range` materialises K sub-batches per
        // input batch via `take_arrays` — one copy per row into a fresh
        // allocation. Unlike the ordered variant we can't slice
        // contiguous ranges (input isn't sorted), but an
        // `Arc<RecordBatch>` broadcast + receiver-side filter would skip
        // the scatter-side allocations at the cost of duplicating the
        // filter work K times. Worth measuring under skew.
        match split_batch_by_range(&batch, &routing_expr, cuts) {
            Ok(splits) => {
                for (output, sub) in splits.into_iter().enumerate() {
                    if sub.num_rows() == 0 {
                        continue;
                    }
                    // `send().await` is where the backpressure lives:
                    // suspends when the channel is at capacity, which
                    // suspends this scatter task, which suspends its
                    // input read → propagates upstream. Errors mean the
                    // downstream dropped its receiver; keep forwarding
                    // to the other outputs.
                    let _ = senders[output].send(Ok(sub)).await;
                }
            }
            Err(err) => {
                broadcast_error(&senders, err).await;
                return;
            }
        }
    }
    // All senders drop with this task → receivers see EOF.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::RuntimeStatsExec;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn schema_v2_id() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("v2", DataType::Float64, false),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn batch(schema: &Arc<Schema>, keys: Vec<f64>, ids: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(keys)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
        .unwrap()
    }

    fn asc(schema: &Schema, name: &str) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col(name, schema).unwrap(),
            options: Default::default(),
        }
    }

    // ---------- try_new: constructor validation -------------------------

    fn empty_input(schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap()
    }

    #[test]
    fn try_new_rejects_empty_order_by() {
        let schema = schema_v2_id();
        let err = UnorderedRangeRepartitionExec::try_new(empty_input(&schema), vec![], 3)
            .expect_err("empty order_by must be rejected");
        assert!(
            err.to_string().contains("at least one ORDER BY expression"),
            "error should name the missing invariant, got: {err}"
        );
    }

    #[test]
    fn try_new_rejects_non_float64_routing_key() {
        let schema = schema_v2_id();
        let err = UnorderedRangeRepartitionExec::try_new(
            empty_input(&schema),
            vec![asc(&schema, "id")], // Int64
            3,
        )
        .expect_err("Int64 routing key must be rejected");
        assert!(
            err.to_string().contains("must be Float64"),
            "error should name the type mismatch, got: {err}"
        );
    }

    #[test]
    fn output_partitioning_reflects_k() {
        let schema = schema_v2_id();
        let exec = UnorderedRangeRepartitionExec::try_new(
            empty_input(&schema),
            vec![asc(&schema, "v2")],
            4,
        )
        .unwrap();
        assert_eq!(exec.properties().output_partitioning().partition_count(), 4);
    }

    // ---------- execute(): end-to-end routing over Scan → Stats → DRR --

    fn mem_input(
        schema: &Arc<Schema>,
        partitions: Vec<Vec<(f64, i64)>>,
    ) -> Arc<dyn ExecutionPlan> {
        let batches: Vec<Vec<RecordBatch>> = partitions
            .into_iter()
            .map(|rows| {
                let (keys, ids): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
                vec![batch(schema, keys, ids)]
            })
            .collect();
        MemorySourceConfig::try_new_exec(&batches, schema.clone(), None).unwrap()
    }

    /// Wrap `source` in a `RuntimeStatsExec` (sketch mode on `v2`), then in
    /// a DRR — matches the Stage-1 layout minus the Dam.
    fn source_stats_drr(
        schema: &Arc<Schema>,
        source: Arc<dyn ExecutionPlan>,
        k: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let stats = Arc::new(
            RuntimeStatsExec::try_new(source, Some(vec![asc(schema, "v2")])).unwrap(),
        );
        Arc::new(
            UnorderedRangeRepartitionExec::try_new(stats, vec![asc(schema, "v2")], k)
                .unwrap(),
        )
    }

    /// Drain all K output partitions **concurrently** — otherwise the
    /// bounded scatter channels can deadlock: while a test sequentially
    /// polls partition 0, scatter tasks fill channels 1..K-1 to
    /// `CHANNEL_CAPACITY` and block on send, never dropping their partition-0
    /// senders. In production K downstream tasks run concurrently and this
    /// pattern never arises.
    async fn drain_concurrent<T: Send + 'static>(
        exec: Arc<dyn ExecutionPlan>,
        ctx: Arc<SessionContext>,
        column: usize,
        extract: fn(&RecordBatch, usize) -> Vec<T>,
    ) -> Vec<Vec<T>> {
        let output_partitions = exec.output_partitioning().partition_count();
        let streams: Vec<_> = (0..output_partitions)
            .map(|p| exec.execute(p, ctx.task_ctx()).unwrap())
            .collect();
        let handles: Vec<_> = streams
            .into_iter()
            .map(|stream| {
                tokio::spawn(async move {
                    let batches: Vec<RecordBatch> =
                        <_ as futures::stream::StreamExt>::collect::<
                            Vec<Result<RecordBatch>>,
                        >(stream)
                        .await
                        .into_iter()
                        .map(|r| r.unwrap())
                        .collect();
                    batches.iter().flat_map(|b| extract(b, column)).collect()
                })
            })
            .collect();
        let mut per_output = Vec::with_capacity(output_partitions);
        for handle in handles {
            per_output.push(handle.await.unwrap());
        }
        per_output
    }

    fn extract_ids(batch: &RecordBatch, column: usize) -> Vec<i64> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect()
    }

    fn extract_keys(batch: &RecordBatch, column: usize) -> Vec<f64> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect()
    }

    async fn ids_per_output(
        exec: Arc<dyn ExecutionPlan>,
        ctx: Arc<SessionContext>,
    ) -> Vec<Vec<i64>> {
        drain_concurrent(exec, ctx, 1, extract_ids).await
    }

    async fn keys_per_output(
        exec: Arc<dyn ExecutionPlan>,
        ctx: Arc<SessionContext>,
    ) -> Vec<Vec<f64>> {
        drain_concurrent(exec, ctx, 0, extract_keys).await
    }

    fn session() -> Arc<SessionContext> {
        Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ))
    }

    /// Random-uniform-ish input over Scan → RuntimeStatsExec → DRR: verify
    /// every row lands in *some* output, and no row lands in more than one.
    #[tokio::test]
    async fn end_to_end_conserves_rows() {
        let schema = schema_v2_id();
        // 20 rows, roughly evenly spread across [0, 100).
        let rows: Vec<(f64, i64)> =
            (0..20).map(|i| ((i as f64) * 5.0, i as i64)).collect();
        let source = mem_input(&schema, vec![rows]);
        let exec = source_stats_drr(&schema, source, 4);
        let per_output = ids_per_output(exec, session()).await;
        let total: usize = per_output.iter().map(|v| v.len()).sum();
        assert_eq!(total, 20, "every row must land in exactly one output");
    }

    /// The load-bearing property: outputs are range-disjoint. For every
    /// adjacent pair `(p, p+1)`, `max(partition[p]) <= min(partition[p+1])`.
    #[tokio::test]
    async fn end_to_end_partitions_are_range_disjoint() {
        let schema = schema_v2_id();
        let rows: Vec<(f64, i64)> =
            (0..40).map(|i| ((i as f64) * 2.5, i as i64)).collect();
        let source = mem_input(&schema, vec![rows]);
        let exec = source_stats_drr(&schema, source, 4);
        let keys = keys_per_output(exec, session()).await;
        for window in keys.windows(2) {
            let [left, right] = window else {
                unreachable!()
            };
            if left.is_empty() || right.is_empty() {
                continue;
            }
            let left_max = left.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let right_min = right.iter().cloned().fold(f64::INFINITY, f64::min);
            assert!(
                left_max <= right_min,
                "range-disjoint invariant broken: max(left)={left_max} > min(right)={right_min}"
            );
        }
    }

    /// Multiple input partitions still route correctly. Distinct values in
    /// each input partition; conservation must hold across all of them.
    #[tokio::test]
    async fn end_to_end_multiple_input_partitions() {
        let schema = schema_v2_id();
        let partitions = vec![
            (0..10).map(|i| (i as f64, i as i64)).collect(),
            (0..10).map(|i| (i as f64 + 20.0, 100 + i as i64)).collect(),
            (0..10).map(|i| (i as f64 + 50.0, 200 + i as i64)).collect(),
        ];
        let source = mem_input(&schema, partitions);
        let exec = source_stats_drr(&schema, source, 4);
        let per_output = ids_per_output(exec, session()).await;
        let total: usize = per_output.iter().map(|v| v.len()).sum();
        assert_eq!(total, 30);
    }

    /// Fallback: without a sibling `RuntimeStatsExec`, every batch funnels
    /// to `FALLBACK_PARTITION`. No crash, no error, no data loss.
    #[tokio::test]
    async fn end_to_end_fallback_when_no_runtime_stats() {
        let schema = schema_v2_id();
        let rows: Vec<(f64, i64)> = (0..15).map(|i| (i as f64, i as i64)).collect();
        let source = mem_input(&schema, vec![rows]);
        // No RuntimeStatsExec in the tree — DRR built directly on the source.
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            UnorderedRangeRepartitionExec::try_new(source, vec![asc(&schema, "v2")], 4)
                .unwrap(),
        );
        let per_output = ids_per_output(exec, session()).await;
        assert_eq!(per_output.len(), 4);
        // Empty cuts → single-bucket fallback → everything on partition 0.
        assert_eq!(per_output[0].len(), 15);
        assert_eq!(per_output[1].len(), 0);
        assert_eq!(per_output[2].len(), 0);
        assert_eq!(per_output[3].len(), 0);
    }

    /// Fallback also fires when the sibling `RuntimeStatsExec` is in
    /// row-count-only mode (no sketch to read cuts from).
    #[tokio::test]
    async fn end_to_end_fallback_when_stats_has_no_sketch() {
        let schema = schema_v2_id();
        let rows: Vec<(f64, i64)> = (0..12).map(|i| (i as f64, i as i64)).collect();
        let source = mem_input(&schema, vec![rows]);
        let stats = Arc::new(RuntimeStatsExec::try_new(source, None).unwrap()); // row-count only
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            UnorderedRangeRepartitionExec::try_new(stats, vec![asc(&schema, "v2")], 3)
                .unwrap(),
        );
        let per_output = ids_per_output(exec, session()).await;
        assert_eq!(per_output.len(), 3);
        assert_eq!(per_output[0].len(), 12);
        assert_eq!(per_output[1].len(), 0);
        assert_eq!(per_output[2].len(), 0);
    }

    /// A `RuntimeStatsExec` hiding behind a branching node (here `UnionExec`
    /// with two children) is *not* what we want — its sketch reflects only
    /// one side of the branch, not the data actually flowing through DRR.
    /// Discovery must refuse to descend and fall back cleanly.
    #[tokio::test]
    async fn end_to_end_fallback_when_runtime_stats_hides_behind_branch() {
        use datafusion::physical_plan::union::UnionExec;
        let schema = schema_v2_id();
        // Branch A: has a RuntimeStatsExec sketching v2, values in [0, 5).
        let branch_a = mem_input(
            &schema,
            vec![(0..5).map(|i| (i as f64, i as i64)).collect()],
        );
        let stats_on_branch_a = Arc::new(
            RuntimeStatsExec::try_new(branch_a, Some(vec![asc(&schema, "v2")])).unwrap(),
        );
        // Branch B: no stats; values shifted to [100, 105) so a "wrong"
        // sketch (only branch A) would send them all to the top partition.
        let branch_b = mem_input(
            &schema,
            vec![(0..5).map(|i| (100.0 + i as f64, 100 + i as i64)).collect()],
        );
        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![stats_on_branch_a, branch_b]).unwrap();
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            UnorderedRangeRepartitionExec::try_new(union, vec![asc(&schema, "v2")], 3)
                .unwrap(),
        );
        let per_output = ids_per_output(exec, session()).await;
        // Empty cuts → single-bucket fallback → all on partition 0. If
        // find_runtime_stats had descended into the union, branch A's tiny
        // sketch would have spread rows across partitions instead.
        assert_eq!(per_output.len(), 3);
        assert_eq!(per_output[0].len(), 10);
        assert_eq!(per_output[1].len(), 0);
        assert_eq!(per_output[2].len(), 0);
    }

    /// `BufferExec` is on the distribution-preserving whitelist: the walker
    /// must traverse it to reach the matching `RuntimeStatsExec`, and cuts
    /// come out non-empty (data lands across multiple output partitions,
    /// not funneled to bucket 0).
    #[tokio::test]
    async fn end_to_end_walker_descends_through_buffer() {
        use crate::execution_plans::{BufferExec, BufferMode};
        let schema = schema_v2_id();
        let rows: Vec<(f64, i64)> = (0..40).map(|i| (i as f64, i as i64)).collect();
        let source = mem_input(&schema, vec![rows]);
        let stats = Arc::new(
            RuntimeStatsExec::try_new(source, Some(vec![asc(&schema, "v2")])).unwrap(),
        );
        let buffer = Arc::new(BufferExec::try_new(stats, BufferMode::Dam).unwrap());
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            UnorderedRangeRepartitionExec::try_new(buffer, vec![asc(&schema, "v2")], 4)
                .unwrap(),
        );
        let per_output = ids_per_output(exec, session()).await;
        let non_empty = per_output.iter().filter(|v| !v.is_empty()).count();
        assert!(
            non_empty > 1,
            "walker should have traversed BufferExec, populated cuts, and \
             spread rows across multiple outputs — got per_output={per_output:?}"
        );
    }

    /// `FilterExec` is *not* on the whitelist: it drops rows, so any upstream
    /// sketch has stale distribution info. The walker must refuse to descend
    /// through it → single-bucket fallback (all rows to partition 0).
    #[tokio::test]
    async fn end_to_end_walker_refuses_filter() {
        use datafusion::physical_expr::expressions::{Literal, binary};
        use datafusion::physical_plan::filter::FilterExec;
        use datafusion::scalar::ScalarValue;
        let schema = schema_v2_id();
        let rows: Vec<(f64, i64)> = (0..15).map(|i| (i as f64, i as i64)).collect();
        let source = mem_input(&schema, vec![rows]);
        let stats = Arc::new(
            RuntimeStatsExec::try_new(source, Some(vec![asc(&schema, "v2")])).unwrap(),
        );
        // `v2 >= 0` — passes every row (we just want a FilterExec node in the
        // chain so the walker has to decide whether to descend past it).
        let predicate = binary(
            col("v2", schema.as_ref()).unwrap(),
            datafusion::logical_expr::Operator::GtEq,
            Arc::new(Literal::new(ScalarValue::Float64(Some(0.0)))),
            schema.as_ref(),
        )
        .unwrap();
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, stats).unwrap());
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            UnorderedRangeRepartitionExec::try_new(filter, vec![asc(&schema, "v2")], 4)
                .unwrap(),
        );
        let per_output = ids_per_output(exec, session()).await;
        // Every row funnels to bucket 0 — walker refused to descend past
        // FilterExec, so cuts came back empty.
        assert_eq!(per_output.len(), 4);
        assert_eq!(per_output[0].len(), 15);
        assert_eq!(per_output[1].len(), 0);
        assert_eq!(per_output[2].len(), 0);
        assert_eq!(per_output[3].len(), 0);
    }

    /// First execute(p) takes channels[p]; second call must error rather than
    /// silently returning nothing. Uses the concurrent-drain helper because
    /// bounded scatter channels would deadlock under sequential collection.
    #[tokio::test]
    async fn execute_same_partition_twice_errors() {
        let schema = schema_v2_id();
        let source = mem_input(&schema, vec![vec![(5.0, 0)]]);
        let exec = source_stats_drr(&schema, source, 3);
        let ctx = session();
        let _ = ids_per_output(exec.clone(), ctx.clone()).await;
        let err = match exec.execute(0, ctx.task_ctx()) {
            Ok(_) => panic!("second execute on same partition must error"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("execute(0) called twice"),
            "error should name the invariant, got: {err}"
        );
    }
}
