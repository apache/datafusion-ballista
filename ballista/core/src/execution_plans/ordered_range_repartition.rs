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

//! Value-range router over N locally-sorted overlapping input partitions.
//! Redistributes them into K range-disjoint output partitions where each
//! output is fully sorted on the routing expression. Concatenating the K
//! outputs in order yields a globally-sorted stream.
//!
//! ```text
//!    N sorted input                                              outputs (K = 4, each sorted)
//!    partitions
//!         ─┐                                                     ┌─▶ 0: sorted, key < c₁
//!         ─┼──▶ RuntimeStatsExec ──▶ SortExec/preserve ──▶ OrderedRRE ─┼─▶ 1: sorted, c₁ ≤ key < c₂
//!         ─┤    (T-Digest tap)      (per-partition sort)   (K = 4)    ├─▶ 2: sorted, c₂ ≤ key < c₃
//!         ─┘                                                 │        └─▶ 3: sorted, c₃ ≤ key
//!                     ▲                                      │
//!                     └──────────── walker ◀─────────────────┘
//!                                   (descends past sort-preserving
//!                                    nodes, matches on routing expr,
//!                                    reads K−1 quantile cuts)
//!
//!    Inside OrderedRRE: N × K channels + K k-way merges.
//!    Each of N scatter tasks splits its sorted input by the K-1 cuts
//!    (K sub-batches per batch) and forwards each sub-batch to its output's
//!    sender. Each of K outputs runs a `StreamingMerge` across the N
//!    sub-streams targeting it → the output stream is sorted.
//! ```
//!
//! # Contrast with `UnorderedRangeRepartitionExec`
//!
//! The two operators share the same runtime discovery mechanism (walk child
//! subtree → find matching `RuntimeStatsExec` → quantile cuts from T-Digest,
//! see [`crate::execution_plans::range_repartition_common`]). They diverge
//! on execution model:
//!
//! - **Unordered**: N scatter tasks → K channels → K raw receivers. Rows
//!   from different sources arrive interleaved, order is lost.
//! - **Ordered**: N scatter tasks → N × K channels → K [`StreamingMerge`]
//!   streams. Each output p performs a heap-based k-way merge across N
//!   sorted sub-streams (one per input source, pre-filtered to output p's
//!   value range). Battle-tested merger via DataFusion's `SortPreservingMerge`
//!   internals; no custom heap code here.
//!
//! # Discovery + fallback
//!
//! Identical to Unordered: walk `self.input`'s subtree through single-child
//! chains and the distribution-preserving whitelist; snapshot the matching
//! T-Digest on first batch; empty cut set = single-bucket fallback where all
//! rows land in output 0.
//!
//! # Ordering claim
//!
//! Constructor requires `input.output_ordering()` to lead with the routing
//! expression — otherwise the merger would produce garbled output.
//! [`PlanProperties::eq_properties`] declares each output partition sorted
//! on `order_by`, letting downstream operators (BWAG, HaloDrop) rely on the
//! claim without inserting a redundant `SortExec`.
//!
//! [`StreamingMerge`]: datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder

use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{Result, Statistics, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, OrderingRequirements, Partitioning,
    PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::execution_plans::range_repartition_common::{
    discover_cuts, guarded_scatter, split_batch_by_range,
};

/// Per-output-partition channel capacity, per input source. Matches the
/// unordered variant's default; see the discussion there. Total buffered
/// batches at rest ≤ `N × K × CHANNEL_CAPACITY`.
const CHANNEL_CAPACITY: usize = 2;

/// Value-range router that preserves the input's sort order. See the
/// module-level docs.
pub struct OrderedRangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    /// Lexicographic ORDER BY. `try_new` guarantees the first entry evaluates
    /// to `Float64` and matches the input's declared output ordering.
    order_by: Vec<PhysicalSortExpr>,
    /// K — number of output partitions.
    output_partitions: usize,
    /// Lazy channel + merge-stream setup guarded by `Mutex`. First
    /// `execute()` call spawns the N input-reader tasks, wires N × K
    /// channels, and constructs K `StreamingMerge` streams — one per
    /// output partition. Subsequent `execute(p)` calls take
    /// `merged_streams[p]`.
    state: Arc<Mutex<DispatchState>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

/// Per-exec-instance lazy state.
struct DispatchState {
    /// K slots. Each holds `Some(stream)` after setup, `None` once its
    /// output partition has been consumed. Each stream is a k-way merge
    /// (via `StreamingMerge`) across N sorted sub-streams — one per input
    /// source, filtered to the output's value range on the scatter side.
    merged_streams: Vec<Option<SendableRecordBatchStream>>,
    /// N scatter tasks (one per input partition). Each runs its scatter
    /// body inside `guarded_scatter`, which converts panic/error/clean-end
    /// into an explicit signal on every output channel. `SpawnedTask`
    /// aborts its inner tokio task on drop, so holding these here ties the
    /// background work's lifetime to this exec's — dropping the exec
    /// cancels all scatter work.
    _drop_helper: Vec<SpawnedTask<()>>,
    initialized: bool,
}

impl OrderedRangeRepartitionExec {
    /// Wrap `input`. `order_by` must be non-empty, the first entry must
    /// evaluate to `Float64`, and `input.output_ordering()` must lead with
    /// the same expression (otherwise the merger produces garbled output).
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        order_by: Vec<PhysicalSortExpr>,
        output_partitions: usize,
        // TODO: support RANGE & ROW halos
    ) -> Result<Self> {
        let [routing, ..] = order_by.as_slice() else {
            return internal_err!(
                "OrderedRangeRepartitionExec requires at least one ORDER BY expression"
            );
        };
        let schema = input.schema();
        let routing_type = routing.expr.data_type(&schema)?;
        if !matches!(routing_type, DataType::Float64) {
            // TODO: support all continuous primitives
            return internal_err!(
                "OrderedRangeRepartitionExec routing expression `{}` must be Float64, got {:?}",
                routing.expr,
                routing_type
            );
        }
        // TODO: fixed by KLL — a NULL-aware sketch lifts this restriction and
        // lets `split_batch_by_range` honor SortOptions::nulls_first properly.
        if routing.expr.nullable(&schema)? {
            return internal_err!(
                "OrderedRangeRepartitionExec: routing expression `{}` must be non-nullable",
                routing.expr
            );
        }
        // Input MUST claim to be sorted on our routing expression — otherwise
        // the k-way merge produces garbled output. Sortedness of individual
        // input partitions is enforced by the operator upstream (`SortExec`
        // with `preserve_partitioning=true`); this check verifies the plan
        // node declares that property.
        let input_first_sort = input.output_ordering().map(|ordering| ordering.first());
        let Some(input_first) = input_first_sort else {
            return internal_err!(
                "OrderedRangeRepartitionExec requires sorted input — child plan claims no ordering"
            );
        };
        if input_first.expr.as_ref() != routing.expr.as_ref() {
            return internal_err!(
                "OrderedRangeRepartitionExec: input's first sort key `{}` does not match \
                 routing expression `{}`",
                input_first.expr,
                routing.expr
            );
        }
        // Advertise each output partition as sorted on `order_by`. Downstream
        // operators (BWAG, HaloDrop) rely on this claim to skip redundant
        // Sort insertions.
        let eq_properties = EquivalenceProperties::new_with_orderings(
            schema,
            vec![LexOrdering::new(order_by.clone()).ok_or_else(|| {
                internal_datafusion_err!(
                    "order_by is non-empty but LexOrdering rejected it"
                )
            })?],
        );
        let properties = Arc::new(
            PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(output_partitions),
                input.pipeline_behavior(),
                input.boundedness(),
            )
            // Scatter tasks eagerly drive their inputs the moment `execute()`
            // is first called (not on-demand from downstream polls), and their
            // per-output channels are the yield points the scheduler needs to
            // trade CPU across tasks — same combination DataFusion's
            // RepartitionExec uses.
            .with_evaluation_type(EvaluationType::Eager)
            .with_scheduling_type(SchedulingType::Cooperative),
        );
        let state = Arc::new(Mutex::new(DispatchState {
            merged_streams: Vec::new(),
            _drop_helper: Vec::new(),
            initialized: false,
        }));
        Ok(Self {
            input,
            order_by,
            output_partitions,
            state,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
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

impl Debug for OrderedRangeRepartitionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedRangeRepartitionExec")
            .field("order_by", &self.order_by)
            .field("output_partitions", &self.output_partitions)
            .finish()
    }
}

impl DisplayAs for OrderedRangeRepartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let routing = &self.order_by[0];
        write!(
            f,
            "OrderedRangeRepartitionExec: routing={} {} → {} sorted partitions",
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

impl ExecutionPlan for OrderedRangeRepartitionExec {
    fn name(&self) -> &str {
        "OrderedRangeRepartitionExec"
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

    /// Input distribution is irrelevant — the operator re-routes every row
    /// by value range regardless of how the child organized them.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    /// The child MUST be sorted on the routing expression (and the full
    /// `order_by` lex): the per-output `StreamingMerge` assumes each of its N
    /// sub-streams is locally sorted. `try_new` also enforces this, but
    /// declaring it here lets the optimizer see the requirement and avoid
    /// inserting a redundant `SortExec` above us.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let Some(lex) = LexOrdering::new(self.order_by.clone()) else {
            return vec![None];
        };
        vec![Some(OrderingRequirements::new(lex.into()))]
    }

    /// Each output partition is a k-way merge across N sorted sub-streams
    /// pre-filtered to that output's value range. The relative order within
    /// each output faithfully preserves the child's sort — that's the whole
    /// point (contrast the Unordered variant, which returns `false`).
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    /// Extra input repartitioning above us buys nothing — we already read
    /// all N input partitions concurrently and scatter them to K outputs.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    /// Row count is preserved but redistributed across K new output
    /// partitions; per-partition breakdown depends on the runtime sketch,
    /// which isn't known at plan time.
    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
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
                "OrderedRangeRepartitionExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(OrderedRangeRepartitionExec::try_new(
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
            self.initialize_state(&mut state, &ctx)?;
        }
        let Some(slot) = state.merged_streams.get_mut(partition) else {
            return internal_err!(
                "OrderedRangeRepartitionExec: partition {} out of range (have {})",
                partition,
                self.output_partitions
            );
        };
        slot.take().ok_or_else(|| {
            internal_datafusion_err!(
                "OrderedRangeRepartitionExec: execute({partition}) called twice \
                 on the same instance"
            )
        })
    }
}

impl OrderedRangeRepartitionExec {
    /// First-execute() setup: create N × K channels, spawn N scatter tasks,
    /// build K `StreamingMerge` streams (one per output partition). Called
    /// once, under the state `Mutex`.
    fn initialize_state(
        &self,
        state: &mut DispatchState,
        ctx: &Arc<TaskContext>,
    ) -> Result<()> {
        let input_partitions = self.input.output_partitioning().partition_count();
        let schema = self.schema();
        let lex_ordering = LexOrdering::new(self.order_by.clone()).ok_or_else(|| {
            internal_datafusion_err!("order_by is non-empty but LexOrdering rejected it")
        })?;

        // Wire N × K channels. Two layouts of the same channels:
        //   • senders_per_input[i][p]: scatter task i's handle to output p.
        //   • receivers_per_output[p]: N receivers feeding output p's merge.
        let mut senders_per_input: Vec<Vec<mpsc::Sender<Result<RecordBatch>>>> =
            Vec::with_capacity(input_partitions);
        let mut receivers_per_output: Vec<Vec<mpsc::Receiver<Result<RecordBatch>>>> = (0
            ..self.output_partitions)
            .map(|_| Vec::with_capacity(input_partitions))
            .collect();
        for _ in 0..input_partitions {
            let mut senders = Vec::with_capacity(self.output_partitions);
            for per_output_receivers in receivers_per_output.iter_mut() {
                let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                senders.push(tx);
                per_output_receivers.push(rx);
            }
            senders_per_input.push(senders);
        }

        // Empty `Vec<f64>` = discovery failed = single-bucket fallback.
        // Populated once, on the first batch, by whichever scatter task
        // wins the `OnceLock::get_or_init` race.
        let cuts_cell: Arc<OnceLock<Vec<f64>>> = Arc::new(OnceLock::new());
        let routing_expr = self.order_by[0].expr.clone();
        let mut drop_helper = Vec::with_capacity(input_partitions);
        for (input_partition, senders) in senders_per_input.into_iter().enumerate() {
            let child = self.input.clone();
            let cuts_cell = cuts_cell.clone();
            let routing_expr = routing_expr.clone();
            let ctx = ctx.clone();
            let output_partitions = self.output_partitions;
            // Move senders into an `Arc<[_]>` so scatter and guard can share
            // the same slice — scatter uses them to forward sub-batches;
            // guard uses them to broadcast panics/errors.
            let scatter_senders: Arc<[mpsc::Sender<Result<RecordBatch>>]> =
                senders.into();
            let guard_senders = scatter_senders.clone();
            // `guarded_scatter` wraps the body in `catch_unwind`: a panic
            // inside `scatter_input_partition` (or anything it calls) becomes
            // a broadcast error rather than a silent sender-drop that
            // downstream would misread as clean EOF.
            drop_helper.push(SpawnedTask::spawn(guarded_scatter(
                scatter_input_partition(
                    child,
                    input_partition,
                    ctx,
                    routing_expr,
                    scatter_senders,
                    cuts_cell,
                    output_partitions,
                ),
                guard_senders,
            )));
        }
        state._drop_helper = drop_helper;

        // Build one `StreamingMerge` per output partition. Each merges N
        // sub-streams (from each input source's channel to this output)
        // in sort order. `StreamingMergeBuilder` is DataFusion's SPM
        // internals — battle-tested cursor + heap machinery.
        let mut merged_streams = Vec::with_capacity(self.output_partitions);
        for (output_partition, receivers) in receivers_per_output.into_iter().enumerate()
        {
            let sub_streams: Vec<SendableRecordBatchStream> = receivers
                .into_iter()
                .map(|rx| {
                    Box::pin(RecordBatchStreamAdapter::new(
                        schema.clone(),
                        ReceiverStream::new(rx),
                    )) as SendableRecordBatchStream
                })
                .collect();
            let metrics = BaselineMetrics::new(&self.metrics, output_partition);
            let merged = StreamingMergeBuilder::new()
                .with_streams(sub_streams)
                .with_schema(schema.clone())
                .with_expressions(&lex_ordering)
                .with_batch_size(ctx.session_config().batch_size())
                .with_metrics(metrics)
                .with_reservation(
                    datafusion::execution::memory_pool::MemoryConsumer::new(format!(
                        "OrderedRangeRepartitionExec[out={output_partition}]"
                    ))
                    .register(ctx.memory_pool()),
                )
                .build()?;
            merged_streams.push(Some(merged));
        }
        state.merged_streams = merged_streams;
        state.initialized = true;
        Ok(())
    }
}

/// One background task per input partition. Reads the sorted input; on the
/// first batch, the shared `OnceLock` learns the value cuts. Each batch is
/// then dispatched via `split_batch_by_range` to K per-input senders (one
/// per output partition).
///
/// TODO: this is the naive path. Two follow-ups worth measuring:
///   1. **Binary search on the batch head/tail against the cuts** to find
///      slice boundaries in O(log K) per batch instead of per-row
///      evaluation. Legal only because input is sorted — this operator's
///      whole precondition.
///   2. **Send zero-copy Arrow slices** (`batch.slice(start, len)`) instead
///      of materialising via `take_arrays`. Turns per-batch scatter into a
///      few Arc bumps rather than N × K allocations under skew.
async fn scatter_input_partition(
    child: Arc<dyn ExecutionPlan>,
    input_partition: usize,
    ctx: Arc<TaskContext>,
    routing_expr: Arc<dyn PhysicalExpr>,
    senders: Arc<[mpsc::Sender<Result<RecordBatch>>]>,
    cuts_cell: Arc<OnceLock<Vec<f64>>>,
    output_partitions: usize,
) -> Result<()> {
    let mut stream = child.execute(input_partition, ctx)?;
    while let Some(batch_result) = stream.next().await {
        // Every downstream merger dropped its receiver — no one's listening.
        // Stop reading so the input stream drops and backpressure propagates.
        if senders.iter().all(|s| s.is_closed()) {
            return Ok(());
        }
        let batch = batch_result?;
        let cuts = cuts_cell.get_or_init(|| {
            discover_cuts(&child, routing_expr.as_ref(), output_partitions)
        });
        // TODO(perf): input is sorted — this per-row `split_batch_by_range`
        // is legal but wasteful. Two follow-ups worth measuring:
        //   1. Binary-search batch head/tail against cuts to find slice
        //      boundaries in O(log K) per batch instead of per-row.
        //   2. Send zero-copy `batch.slice(start, len)` instead of
        //      `take_arrays`-materialised sub-batches. Arc bumps replace
        //      allocations under skew.
        let splits = split_batch_by_range(&batch, &routing_expr, cuts)?;
        for (output, sub) in splits.into_iter().enumerate() {
            if sub.num_rows() == 0 {
                continue;
            }
            // `send().await` provides backpressure: full channel suspends
            // this task → suspends input read → propagates upstream. Error
            // on send means downstream dropped its receiver; keep forwarding
            // to other outputs.
            let _ = senders[output].send(Ok(sub)).await;
        }
    }
    // Senders drop with this task → each output's merger sees EOF on that
    // sub-stream. When all N sub-streams for output p reach EOF, the merger
    // emits its final rows and returns None.
    Ok(())
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
    use datafusion::physical_plan::sorts::sort::SortExec;
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

    fn session() -> Arc<SessionContext> {
        Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ))
    }

    /// Feed pre-sorted per-partition test data through a `SortExec`
    /// (preserve-partitioning) so the DRR's input carries the ordering
    /// claim, then wrap in `RuntimeStatsExec` for the sketch source, then
    /// the Ordered operator on top.
    fn build_pipeline(
        schema: &Arc<Schema>,
        partitions: Vec<Vec<(f64, i64)>>,
        k: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let batches: Vec<Vec<RecordBatch>> = partitions
            .into_iter()
            .map(|rows| {
                let (keys, ids): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
                vec![batch(schema, keys, ids)]
            })
            .collect();
        let source =
            MemorySourceConfig::try_new_exec(&batches, schema.clone(), None).unwrap();
        let sort_expr = asc(schema, "v2");
        let sort_lex = LexOrdering::new(vec![sort_expr.clone()]).unwrap();
        let sorted =
            Arc::new(SortExec::new(sort_lex, source).with_preserve_partitioning(true))
                as Arc<dyn ExecutionPlan>;
        let stats = Arc::new(
            RuntimeStatsExec::try_new(sorted, Some(vec![sort_expr.clone()])).unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        Arc::new(OrderedRangeRepartitionExec::try_new(stats, vec![sort_expr], k).unwrap())
    }

    async fn drain_concurrent(
        exec: Arc<dyn ExecutionPlan>,
        ctx: Arc<SessionContext>,
    ) -> Vec<Vec<(f64, i64)>> {
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
                    batches
                        .iter()
                        .flat_map(|b| {
                            let keys = b
                                .column(0)
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap();
                            let ids = b
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            (0..b.num_rows())
                                .map(|i| (keys.value(i), ids.value(i)))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut per_output = Vec::with_capacity(output_partitions);
        for handle in handles {
            per_output.push(handle.await.unwrap());
        }
        per_output
    }

    // ---------- Constructor validation ----------------------------------

    fn empty_input(schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap()
    }

    #[test]
    fn try_new_rejects_empty_order_by() {
        let schema = schema_v2_id();
        let err = OrderedRangeRepartitionExec::try_new(empty_input(&schema), vec![], 4)
            .expect_err("empty order_by must be rejected");
        assert!(
            err.to_string().contains("at least one ORDER BY expression"),
            "got: {err}"
        );
    }

    #[test]
    fn try_new_rejects_unsorted_input() {
        let schema = schema_v2_id();
        // MemorySourceConfig with no declared ordering — output_ordering() is None.
        let err = OrderedRangeRepartitionExec::try_new(
            empty_input(&schema),
            vec![asc(&schema, "v2")],
            4,
        )
        .expect_err("input without ordering claim must be rejected");
        assert!(
            err.to_string().contains("child plan claims no ordering"),
            "got: {err}"
        );
    }

    #[test]
    fn try_new_rejects_nullable_routing_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v2", DataType::Float64, true), // nullable
            Field::new("id", DataType::Int64, false),
        ]));
        let err = OrderedRangeRepartitionExec::try_new(
            empty_input(&schema),
            vec![asc(&schema, "v2")],
            3,
        )
        .expect_err("nullable routing key must be rejected");
        assert!(
            err.to_string().contains("must be non-nullable"),
            "error should name the nullability constraint, got: {err}"
        );
    }

    #[test]
    fn try_new_rejects_mismatched_sort_key() {
        let schema = schema_v2_id();
        // Sort input on `id` (Int64); DRR tries to route on `v2`.
        let source = empty_input(&schema);
        let id_sort = LexOrdering::new(vec![asc(&schema, "id")]).unwrap();
        let sorted_on_id =
            Arc::new(SortExec::new(id_sort, source).with_preserve_partitioning(true))
                as Arc<dyn ExecutionPlan>;
        let err = OrderedRangeRepartitionExec::try_new(
            sorted_on_id,
            vec![asc(&schema, "v2")],
            4,
        )
        .expect_err("mismatched sort key must be rejected");
        assert!(
            err.to_string().contains("does not match routing"),
            "got: {err}"
        );
    }

    // ---------- End-to-end -----------------------------------------------

    #[tokio::test]
    async fn end_to_end_conserves_rows_across_two_overlapping_partitions() {
        let schema = schema_v2_id();
        // Two partitions that overlap in value space (both cover [0, 40)):
        //   partition A: 0.0, 2.0, 4.0, ..., 38.0 (20 rows, even ids 0..20)
        //   partition B: 1.0, 3.0, 5.0, ..., 39.0 (20 rows, ids 100..120)
        let part_a: Vec<(f64, i64)> =
            (0..20).map(|i| ((i * 2) as f64, i as i64)).collect();
        let part_b: Vec<(f64, i64)> = (0..20)
            .map(|i| ((i * 2 + 1) as f64, 100 + i as i64))
            .collect();
        let exec = build_pipeline(&schema, vec![part_a, part_b], 4);
        let per_output = drain_concurrent(exec, session()).await;
        let total: usize = per_output.iter().map(|v| v.len()).sum();
        assert_eq!(total, 40, "no row lost or duplicated");
    }

    /// Each output partition is *fully sorted* on the routing key —
    /// that's the load-bearing claim of the operator.
    #[tokio::test]
    async fn end_to_end_each_output_is_sorted() {
        let schema = schema_v2_id();
        let part_a: Vec<(f64, i64)> =
            (0..25).map(|i| ((i * 2) as f64, i as i64)).collect();
        let part_b: Vec<(f64, i64)> = (0..25)
            .map(|i| ((i * 2 + 1) as f64, 100 + i as i64))
            .collect();
        let exec = build_pipeline(&schema, vec![part_a, part_b], 4);
        let per_output = drain_concurrent(exec, session()).await;
        for (p, rows) in per_output.iter().enumerate() {
            for pair in rows.windows(2) {
                let [(left, _), (right, _)] = pair else {
                    unreachable!()
                };
                assert!(
                    left <= right,
                    "output partition {p} not sorted: {left} > {right}"
                );
            }
        }
    }

    /// Outputs must be range-disjoint — for every adjacent (p, p+1),
    /// max(p) <= min(p+1). Combined with per-output sortedness this means
    /// concatenating the K outputs produces a globally-sorted stream.
    #[tokio::test]
    async fn end_to_end_outputs_are_range_disjoint() {
        let schema = schema_v2_id();
        let part_a: Vec<(f64, i64)> =
            (0..30).map(|i| ((i * 2) as f64, i as i64)).collect();
        let part_b: Vec<(f64, i64)> = (0..30)
            .map(|i| ((i * 2 + 1) as f64, 100 + i as i64))
            .collect();
        let part_c: Vec<(f64, i64)> = (0..30)
            .map(|i| (((i * 2) + 100) as f64, 200 + i as i64))
            .collect();
        let exec = build_pipeline(&schema, vec![part_a, part_b, part_c], 4);
        let per_output = drain_concurrent(exec, session()).await;
        for window in per_output.windows(2) {
            let [left, right] = window else {
                unreachable!()
            };
            if left.is_empty() || right.is_empty() {
                continue;
            }
            let left_max = left
                .iter()
                .map(|(k, _)| *k)
                .fold(f64::NEG_INFINITY, f64::max);
            let right_min = right.iter().map(|(k, _)| *k).fold(f64::INFINITY, f64::min);
            assert!(
                left_max <= right_min,
                "range-disjoint invariant broken: max(left)={left_max} > min(right)={right_min}"
            );
        }
    }

    /// Without a matching `RuntimeStatsExec` in the chain, discovery
    /// returns empty cuts → single-bucket fallback → all rows to output 0.
    #[tokio::test]
    async fn end_to_end_fallback_when_no_runtime_stats() {
        let schema = schema_v2_id();
        // Sort chain (needed for ordering claim) but no RuntimeStatsExec.
        let rows: Vec<(f64, i64)> = (0..15).map(|i| (i as f64, i as i64)).collect();
        let source = MemorySourceConfig::try_new_exec(
            &[vec![batch(
                &schema,
                rows.iter().map(|(k, _)| *k).collect(),
                rows.iter().map(|(_, id)| *id).collect(),
            )]],
            schema.clone(),
            None,
        )
        .unwrap();
        let sort_expr = asc(&schema, "v2");
        let sort_lex = LexOrdering::new(vec![sort_expr.clone()]).unwrap();
        let sorted: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(sort_lex, source).with_preserve_partitioning(true));
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            OrderedRangeRepartitionExec::try_new(sorted, vec![sort_expr], 4).unwrap(),
        );
        let per_output = drain_concurrent(exec, session()).await;
        assert_eq!(per_output.len(), 4);
        assert_eq!(per_output[0].len(), 15);
        assert_eq!(per_output[1].len(), 0);
        assert_eq!(per_output[2].len(), 0);
        assert_eq!(per_output[3].len(), 0);
    }

    #[tokio::test]
    async fn execute_same_partition_twice_errors() {
        let schema = schema_v2_id();
        let exec = build_pipeline(&schema, vec![vec![(1.0, 0)]], 3);
        let ctx = session();
        let _ = drain_concurrent(exec.clone(), ctx.clone()).await;
        let err = match exec.execute(0, ctx.task_ctx()) {
            Ok(_) => panic!("second execute on same partition must error"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("execute(0) called twice"),
            "got: {err}"
        );
    }

    /// A panic inside the scatter task's polling of the child stream must
    /// surface to every output partition as an `Err(..)` — not a silent EOF.
    /// `guarded_scatter`'s `catch_unwind` wraps the panic into a
    /// `DataFusionError` broadcast across all K senders, and the payload
    /// downcast preserves the original panic message.
    #[tokio::test]
    async fn scatter_task_panic_surfaces_as_error() {
        use crate::execution_plans::range_repartition_common::test_util::{
            PanickingSourceExec, SYNTHETIC_PANIC_MESSAGE,
        };
        let schema = schema_v2_id();
        // `OrderedRangeRepartitionExec::try_new` requires the child to
        // declare an ordering; give the panic source one on `v2`.
        let source: Arc<dyn ExecutionPlan> = Arc::new(
            PanickingSourceExec::with_ordering(&schema, vec![asc(&schema, "v2")]),
        );
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            OrderedRangeRepartitionExec::try_new(source, vec![asc(&schema, "v2")], 3)
                .unwrap(),
        );
        let ctx = session();
        let output_partitions = exec.output_partitioning().partition_count();
        let streams: Vec<_> = (0..output_partitions)
            .map(|p| exec.execute(p, ctx.task_ctx()).unwrap())
            .collect();
        let mut outputs_with_err = 0;
        let mut outputs_carrying_original_message = 0;
        for stream in streams {
            let results: Vec<Result<RecordBatch>> =
                <_ as futures::stream::StreamExt>::collect(stream).await;
            let mut err_seen = false;
            let mut msg_seen = false;
            for r in results {
                if let Err(err) = r {
                    let text = err.to_string();
                    if text.contains("panicked") {
                        err_seen = true;
                    }
                    if text.contains(SYNTHETIC_PANIC_MESSAGE) {
                        msg_seen = true;
                    }
                }
            }
            if err_seen {
                outputs_with_err += 1;
            }
            if msg_seen {
                outputs_carrying_original_message += 1;
            }
        }
        // Every output partition must see the panic — not a spurious EOF —
        // and the original panic message must survive the payload downcast.
        assert_eq!(
            outputs_with_err, output_partitions,
            "all output partitions must surface the scatter panic as Err"
        );
        assert_eq!(
            outputs_carrying_original_message, output_partitions,
            "original panic message must survive catch_unwind payload extraction"
        );
    }
}
