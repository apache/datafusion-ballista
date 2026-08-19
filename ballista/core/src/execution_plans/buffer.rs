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

//! Generic flow-control operator that DataFusion doesn't have (yet).
//! Buffers upstream batches with a policy-configurable release trigger,
//! then drains buffered rows and passes remaining input through.
//!
//! Currently supports one mode:
//!
//! - **`BufferMode::Dam`** — buffer input while a `MemoryReservation`
//!   grows against DataFusion's `MemoryPool`. Break the dam when
//!   `try_grow` refuses further allocation (any memory pressure — this
//!   operator, or contention from elsewhere in the query). No
//!   hardcoded byte threshold: the pool decides pressure, same mechanism
//!   [`SortExec`] uses to trigger spill (`sort.rs:887-907`).
//!
//! Growth path (intentionally not built today; documented so the shape
//! and future mode names are visible to reviewers):
//!
//! - `BufferMode::PassThrough` — noop, present so upstream rules can
//!   drop the operator without restructuring the plan.
//! - `BufferMode::BufferAll` — buffer until end-of-stream, then drain.
//! - `BufferMode::SpillOnPressure` — same as `Dam`, but writes to disk
//!   when the pool refuses rather than releasing to downstream. Fills
//!   the "generic materialise/spill primitive" gap in DataFusion.
//! - `BufferMode::StageBoundary` — the buffer IS a shuffle write.
//!
//! Only `Dam` is implemented for now. The other variants are named so
//! that when they land, they slot in without renaming or restructuring.
//!
//! ## How this pairs with `RuntimeStatsExec`
//!
//! Sketch-based routing has a chicken-and-egg problem: the router
//! wants to pick cut points from a `RuntimeStatsExec` sketch, but a
//! streaming pipeline pulls one batch through the tap and hands it
//! straight to the router — the router runs with a one-batch sample.
//!
//! `BufferMode::Dam` inserted between `RuntimeStatsExec` and the
//! sketch-consuming router closes the gap without a plan-time size
//! guess: batches accumulate above the router, feeding the tap's
//! T-Digest as they go, until the memory pool refuses further growth.
//! At that point the dam breaks, the sketch already reflects
//! representative data, the router picks cuts, and buffered batches
//! (plus the input remainder) drain through in order. The pool — not
//! a magic byte count in code — decides "enough sample".
//!
//! Ships in isolation here: nothing wires `BufferExec` into a plan
//! yet. It lands alongside the parallel-window detection rule and
//! the range-repartition operators that consume the sketch.
//!
//! Ultimately this operator should land upstream in DataFusion so
//! every blocking operator (`SortExec`, `HashAggregateExec`, joins,
//! ...) can share the same materialise/spill primitive instead of
//! each rolling its own.
//!
//! [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec

use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::{Distribution, OrderingRequirements};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{Stream, StreamExt, TryStreamExt};

/// How the operator decides when to release buffered input.
///
/// Only `Dam` is implemented today; the other variants are documented
/// here (rather than only in the module docstring) so that when they
/// land, the growth path is visible right at the enum definition.
///
/// ```ignore
/// // Aspirational; NOT implemented today. Each is a natural evolution
/// // of the shape we already have — same operator, different release
/// // policy.
/// pub enum BufferMode {
///     Dam,                          // ← implemented
///
///     /// Pass batches through unchanged. Present so a rewrite rule can
///     /// disable the operator without restructuring the plan tree.
///     PassThrough,
///
///     /// Buffer until end-of-stream, then drain. Equivalent to what
///     /// `SortExec` does today but without the sort — the "generic
///     /// materialise" primitive DataFusion doesn't have.
///     BufferAll,
///
///     /// Same trigger as `Dam` (memory-pool pressure), but on
///     /// pressure spill buffered batches to disk instead of releasing
///     /// downstream. Fills the "generic spill" primitive gap; would
///     /// share plumbing with `SortExec::spill` machinery.
///     SpillOnPressure,
///
///     /// Buffer until an explicit row/byte threshold OR pool pressure,
///     /// then spill. The two-trigger variant of the above for callers
///     /// who want a hard cap on in-memory buffering time.
///     BufferAndSpill { max_rows: usize },
/// }
/// ```
#[derive(Debug, Clone)]
pub enum BufferMode {
    /// Buffer batches while a `MemoryReservation` grows against
    /// DataFusion's `MemoryPool`. The dam breaks when
    /// `MemoryPool::try_grow` refuses further allocation (pressure from
    /// anywhere in the query). After the dam breaks, buffered batches
    /// drain in order and the remaining input passes through unchanged;
    /// the reservation shrinks per-batch as the drain yields, so pool
    /// accounting tracks what's still live in this operator right up
    /// until the last batch leaves.
    ///
    /// No user-tunable byte threshold — the pool decides pressure,
    /// matching `SortExec::reserve_memory_for_batch_and_maybe_spill`.
    Dam,
}

/// Generic flow-control operator. See module-level docs.
pub struct BufferExec {
    input: Arc<dyn ExecutionPlan>,
    mode: BufferMode,
    properties: Arc<PlanProperties>,
}

impl BufferExec {
    /// Wrap `input` with the given buffering `mode`.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, mode: BufferMode) -> Result<Self> {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            mode,
            properties,
        })
    }

    /// The configured buffering policy.
    pub fn mode(&self) -> &BufferMode {
        &self.mode
    }
}

impl Debug for BufferExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferExec")
            .field("mode", &self.mode)
            .finish()
    }
}

impl DisplayAs for BufferExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.mode {
            BufferMode::Dam => write!(f, "BufferExec: mode=Dam"),
        }
    }
}

impl ExecutionPlan for BufferExec {
    fn name(&self) -> &str {
        "BufferExec"
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
                "BufferExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(BufferExec::try_new(
            input.clone(),
            self.mode.clone(),
        )?))
    }

    /// Passthrough: no distribution requirement on the child.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    /// Passthrough: no ordering requirement on the child.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    /// Buffered batches drain in insertion order and the remainder passes
    /// through unchanged, so input order is preserved. Overrides default `false`.
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

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input_stream = self.input.execute(partition, ctx.clone())?;
        match &self.mode {
            BufferMode::Dam => {
                // Register a per-partition consumer against the runtime's
                // memory pool. Same shape SortExec uses; participates in
                // whatever pool policy is configured (Greedy, FairSpill,
                // Unbounded, ...) without picking numbers ourselves.
                let reservation = MemoryConsumer::new(format!(
                    "BufferExec::Dam[partition={partition}]"
                ))
                .register(&ctx.runtime_env().memory_pool);
                let out = build_dam_stream(input_stream, reservation);
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
            }
        }
    }
}

/// Build the async stream that implements `BufferMode::Dam`.
///
/// Semantics (mirrors `SortExec::reserve_memory_for_batch_and_maybe_spill`):
/// 1. For each incoming batch, `reservation.try_grow(batch.get_array_memory_size())`.
///    - `Ok`: append to buffer.
///    - `Err` and buffer empty: propagate the OOM (nothing to release).
///    - `Err` and buffer non-empty: **dam breaks**; drain buffer, then
///      passthrough. Reservation shrinks per-batch as the buffer drains.
/// 2. On stream-end without dam-break: drain buffer, done.
///
/// The reservation is held by the drain stream itself (not the fill
/// closure), and each yielded batch shrinks it by the exact amount that
/// was grown at fill time — so pool accounting stays in sync with what's
/// physically buffered right up until the last batch leaves. The final
/// `Drop` on the reservation happens after the last batch is consumed
/// downstream.
fn build_dam_stream(
    mut input: SendableRecordBatchStream,
    reservation: MemoryReservation,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    futures::stream::once(async move {
        // (size_at_fill, batch) — storing the fill-time size guarantees
        // shrink amounts match grow amounts exactly, even if
        // `get_array_memory_size()` ever changed between calls.
        let mut buffered: VecDeque<(usize, RecordBatch)> = VecDeque::new();
        let mut dam_broken = false;

        // Fill phase: read and buffer until pool pressure or end-of-stream.
        while let Some(batch) = input.next().await {
            let batch = batch?;
            let size = batch.get_array_memory_size();
            match reservation.try_grow(size) {
                Ok(_) => buffered.push_back((size, batch)),
                Err(err) => {
                    if buffered.is_empty() {
                        return Err(oom_context(err));
                    }
                    // Dam breaks: this batch was pulled but not reserved;
                    // let it ride through with the buffered rows (accepting
                    // a one-batch reservation overshoot at the moment of
                    // break — same as SortExec's behaviour on the batch
                    // that triggers spill).
                    buffered.push_back((0, batch));
                    dam_broken = true;
                    break;
                }
            }
        }

        // Drain phase: yield each buffered batch and shrink the reservation
        // by the amount it took at fill time. `unfold` owns the reservation,
        // so the final `Drop` fires after the last batch has been consumed
        // downstream — no window where the reservation is released while
        // batches are still live in this operator.
        let drained = futures::stream::unfold(
            (buffered, reservation),
            |(mut buf, res)| async move {
                let (size, batch) = buf.pop_front()?;
                res.shrink(size);
                Some((Ok::<_, DataFusionError>(batch), (buf, res)))
            },
        );
        let out: futures::stream::BoxStream<'static, Result<RecordBatch>> = if dam_broken
        {
            drained.chain(input).boxed()
        } else {
            drained.boxed()
        };
        Ok::<_, DataFusionError>(out)
    })
    .try_flatten()
}

/// Wrap an OOM error with the same context message SortExec uses so the
/// operator surfaces uniform "tune your memory limit" advice.
fn oom_context(e: DataFusionError) -> DataFusionError {
    match e {
        DataFusionError::ResourcesExhausted(_) => e.context(
            "Not enough memory to buffer a single batch in BufferExec::Dam. \
             Consider increasing 'datafusion.runtime.memory_limit'.",
        ),
        _ => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;

    fn one_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batches(rows_per: usize, num_batches: usize) -> Vec<RecordBatch> {
        (0..num_batches)
            .map(|b| {
                let start = (b * rows_per) as i64;
                let array = Int64Array::from_iter_values(start..start + rows_per as i64);
                RecordBatch::try_new(one_col_schema(), vec![Arc::new(array)]).unwrap()
            })
            .collect()
    }

    fn memory_source(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = one_col_schema();
        let src =
            MemorySourceConfig::try_new(&[batches], schema, None).expect("mem source");
        Arc::new(DataSourceExec::new(Arc::new(src)))
    }

    /// Runtime context whose memory pool has a hard `pool_bytes` limit.
    /// The dam is triggered by pool refusal, so a tight pool is how we
    /// make tests exercise the dam-break branch without hardcoded
    /// thresholds.
    fn ctx_with_pool_limit(pool_bytes: usize) -> Arc<TaskContext> {
        let pool = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .expect("runtime");
        let cfg = SessionConfig::new();
        let session = SessionContext::new_with_config_rt(cfg, runtime);
        session.task_ctx()
    }

    async fn collect_rows(
        plan: Arc<dyn ExecutionPlan>,
        ctx: Arc<TaskContext>,
    ) -> Result<usize> {
        let stream = plan.execute(0, ctx)?;
        let out: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(out.iter().map(|b| b.num_rows()).sum())
    }

    /// Input fits within the pool → everything buffers, drains at
    /// end-of-stream. No rows lost.
    #[tokio::test]
    async fn dam_drains_full_buffer_when_pool_has_headroom() -> Result<()> {
        let src = memory_source(batches(100, 10));
        let dam = BufferExec::try_new(src, BufferMode::Dam)?;
        // 10 MB pool — trivially fits ~800 bytes per batch × 10.
        let ctx = ctx_with_pool_limit(10 * 1024 * 1024);
        assert_eq!(collect_rows(Arc::new(dam), ctx).await?, 1000);
        Ok(())
    }

    /// Pool tight enough that reservation growth fails partway through
    /// the stream → dam breaks, buffered rows plus the remainder all
    /// pass through. No rows lost, none duplicated.
    #[tokio::test]
    async fn dam_breaks_on_pool_pressure_and_passes_remainder() -> Result<()> {
        // 200 rows/batch of Int64 ≈ 1.6 KB payload per batch, but arrow's
        // reported memory footprint per batch here is ~1-2 KB. A ~5 KB
        // pool buffers 2-3 batches, then try_grow refuses.
        let src = memory_source(batches(200, 50));
        let dam = BufferExec::try_new(src, BufferMode::Dam)?;
        let ctx = ctx_with_pool_limit(5 * 1024);
        assert_eq!(collect_rows(Arc::new(dam), ctx).await?, 200 * 50);
        Ok(())
    }

    /// Pool so tight that even the first batch's reservation fails →
    /// operator surfaces OOM (mirrors SortExec behaviour when there's
    /// nothing already buffered that could be spilled/released).
    #[tokio::test]
    async fn dam_propagates_oom_when_first_batch_wont_fit() -> Result<()> {
        let src = memory_source(batches(1000, 5));
        let dam = BufferExec::try_new(src, BufferMode::Dam)?;
        // 1-byte pool — no batch can be reserved.
        let ctx = ctx_with_pool_limit(1);
        let result = collect_rows(Arc::new(dam), ctx).await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("BufferExec::Dam") || e.to_string().contains("memory")),
            "expected OOM, got {result:?}"
        );
        Ok(())
    }

    /// Pool accounting stays in sync with what's physically buffered:
    /// after fill, `reserved()` equals the sum of buffered batch sizes;
    /// after each yielded batch, `reserved()` drops by that batch's
    /// size; after stream end, `reserved()` is 0.
    ///
    /// Directly pins the invariant behind the reviewer's concern —
    /// regressing to "drop the reservation at end of fill" would leave
    /// `reserved()` at 0 mid-drain while batches were still live.
    #[tokio::test]
    async fn dam_reservation_shrinks_incrementally_during_drain() -> Result<()> {
        let src_batches = batches(100, 5);
        let sizes: Vec<usize> = src_batches
            .iter()
            .map(|b| b.get_array_memory_size())
            .collect();
        let total: usize = sizes.iter().sum();

        let src = memory_source(src_batches);
        let dam: Arc<dyn ExecutionPlan> =
            Arc::new(BufferExec::try_new(src, BufferMode::Dam)?);

        // Hold our own handle to the pool so we can observe `reserved()`.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10 * 1024 * 1024));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build_arc()
            .expect("runtime");
        let session = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let ctx = session.task_ctx();

        let mut stream = dam.execute(0, ctx)?;

        // Pulling the first item drives the whole fill phase (reserve all
        // batches), then yields batch 0 after shrinking by sizes[0].
        let first = stream.next().await.expect("first batch")?;
        assert_eq!(first.num_rows(), 100);
        let mut expected = total - sizes[0];
        assert_eq!(
            pool.reserved(),
            expected,
            "after fill + first drain: total {total} - sizes[0] {}",
            sizes[0]
        );

        for (i, size) in sizes.iter().enumerate().skip(1) {
            let batch = stream.next().await.expect("more batches")?;
            assert_eq!(batch.num_rows(), 100, "batch {i}");
            expected -= size;
            assert_eq!(pool.reserved(), expected, "after batch {i}");
        }

        assert!(stream.next().await.is_none(), "stream exhausted");
        assert_eq!(pool.reserved(), 0, "reservation released after stream end");
        Ok(())
    }
}
