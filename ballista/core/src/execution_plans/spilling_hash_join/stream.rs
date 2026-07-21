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

// The bounded-memory execution stream for `SpillingHashJoinExec`. Both sides
// are hash-partitioned into a fixed number of sub-partitions ("buckets"); a
// bucket is either kept resident or spilled to disk, so peak memory is bounded
// by the runtime `MemoryPool` during the build/probe phases and by a single
// bucket during the drain phase.
//
// The stream runs in three phases:
//
//   1. Build. Drain the left (build) side. Each bucket's batches are buffered
//      resident while the pool's `MemoryReservation` grants the space; when a
//      `try_grow` is rejected, the largest resident not-yet-spilled bucket is
//      evicted to a per-bucket build spill file (freeing its reservation), and
//      the current batch is retried — it may itself become the victim and land
//      on disk. Once the pool cannot hold a bucket, that whole bucket is
//      spilled for the rest of the build. Resident buckets are finalized into
//      `ProbeTable`s; spilled buckets stay on disk.
//
//   2. Probe. Stream the right (probe) side. A probe sub-batch whose bucket is
//      resident is probed immediately and its output emitted; a probe sub-batch
//      whose bucket is spilled is appended to a per-bucket probe spill file.
//
//   3. Drain. After the probe side ends, resident tables are dropped and the
//      reservation freed, then each spilled bucket is processed one at a time:
//      its build spill is read back and concatenated, and the reservation is
//      grown for it. When it fits, its hashes are recomputed by re-partitioning
//      the concatenated build (the fixed seed guarantees all rows land in the
//      one bucket, so the recomputed hashes match build time), a `ProbeTable`
//      is built, and its probe spill is streamed through it. When a bucket's
//      build side will not fit, it is re-partitioned — build and probe together,
//      under a depth-varied seed so equal keys still co-locate — into finer
//      sub-buckets that are drained recursively, one resident table at a time.
//      A build side for a single join key that exceeds the whole pool is
//      irreducible skew and fails with a clean error rather than an OOM. The
//      table is dropped before the next bucket, so only one (sub-)bucket's
//      build side is resident at a time, at any recursion depth.
//
// The stream is a single `futures::stream::once` future (do the async build
// phase) flattened via `try_flatten` into a `stream::unfold` that carries the
// probe and drain phases. Nothing polls the probe side until the build side is
// fully drained, which is exactly the ordering an inner hash join requires.

use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::execution::disk_manager::DiskManager;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryReservation,
};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, TryStreamExt, stream};

use super::hash_table::{ProbeTable, assemble_output};
use super::partitioner::{RowPartitioner, seed_for_depth};
use super::spill::{JoinSpillReader, JoinSpillWriter};

/// Maximum drain-phase re-partition depth. A bucket that still will not fit
/// after this many splits is treated as irreducible skew and reported as a
/// clean error rather than recursed on forever.
const MAX_DRAIN_DEPTH: usize = 8;

/// Spill counters for one `SpillingHashJoinExec` partition, published on the
/// operator's `ExecutionPlanMetricsSet`.
#[derive(Clone)]
struct SpillMetrics {
    /// Number of batches appended to a build- or probe-side spill file.
    spill_count: Count,
    /// Sum of `RecordBatch::get_array_memory_size` over all spilled batches.
    spilled_bytes: Count,
    /// Number of times a spilled bucket's build side did not fit in memory
    /// during drain and was recursively re-partitioned into finer sub-buckets.
    repartition_count: Count,
}

impl SpillMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            spill_count: MetricBuilder::new(metrics).counter("spill_count", partition),
            spilled_bytes: MetricBuilder::new(metrics)
                .counter("spilled_bytes", partition),
            repartition_count: MetricBuilder::new(metrics)
                .counter("repartition_count", partition),
        }
    }

    /// Records one spilled batch.
    fn record(&self, batch: &RecordBatch) {
        self.spill_count.add(1);
        self.spilled_bytes.add(batch.get_array_memory_size());
    }

    /// Records one drain-phase recursive re-partition of an oversized bucket.
    fn record_repartition(&self) {
        self.repartition_count.add(1);
    }
}

/// Builds the `SendableRecordBatchStream` for `SpillingHashJoinExec::execute`.
///
/// `output_schema` is the join's output schema (left columns ++ right columns),
/// independent of `left`/`right`'s own schemas — it is what the returned
/// stream's `schema()` reports.
#[allow(clippy::too_many_arguments)]
pub fn execute_join(
    output_schema: SchemaRef,
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    left_keys: Vec<PhysicalExprRef>,
    right_keys: Vec<PhysicalExprRef>,
    num_sub_partitions: usize,
    context: Arc<TaskContext>,
    metrics: &ExecutionPlanMetricsSet,
    partition: usize,
) -> SendableRecordBatchStream {
    let adapter_schema = Arc::clone(&output_schema);
    let spill_metrics = SpillMetrics::new(metrics, partition);

    let joined = stream::once(build_then_stream(
        left,
        right,
        left_keys,
        right_keys,
        num_sub_partitions,
        output_schema,
        context,
        spill_metrics,
    ))
    .try_flatten();

    Box::pin(RecordBatchStreamAdapter::new(adapter_schema, joined))
}

/// Runs the build phase (async, drains `left`), then hands back the stream
/// that carries the probe and drain phases.
#[allow(clippy::too_many_arguments)]
async fn build_then_stream(
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    left_keys: Vec<PhysicalExprRef>,
    right_keys: Vec<PhysicalExprRef>,
    num_sub_partitions: usize,
    output_schema: SchemaRef,
    context: Arc<TaskContext>,
    spill_metrics: SpillMetrics,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send> {
    let runtime = context.runtime_env();
    let mut reservation = MemoryConsumer::new(format!(
        "SpillingHashJoin[{}]",
        output_schema.fields().len()
    ))
    .with_can_spill(true)
    .register(&runtime.memory_pool);

    let left_schema = left.schema();
    let right_schema = right.schema();

    // Two independent spill sets: build-side batches (build schema) and
    // probe-side batches (probe schema).
    let mut build_spill =
        JoinSpillWriter::new(Arc::clone(&left_schema), Arc::clone(&runtime.disk_manager));
    let probe_spill = JoinSpillWriter::new(
        Arc::clone(&right_schema),
        Arc::clone(&runtime.disk_manager),
    );

    let partitioner_left = RowPartitioner::new(left_keys.clone(), num_sub_partitions);
    let partitioner_right = RowPartitioner::new(right_keys.clone(), num_sub_partitions);

    // The pool's total capacity, used during drain to tell irreducible
    // single-key skew (a build side larger than the whole pool) apart from a
    // grow that merely lost a race with other resident buckets.
    let pool_limit = match runtime.memory_pool.memory_limit() {
        MemoryLimit::Finite(bytes) => Some(bytes),
        MemoryLimit::Infinite | MemoryLimit::Unknown => None,
    };

    let (tables, spilled) = build_phase(
        left,
        &left_keys,
        &left_schema,
        num_sub_partitions,
        &mut reservation,
        &mut build_spill,
        &spill_metrics,
        &partitioner_left,
    )
    .await?;

    let state = JoinState {
        phase: Phase::Probe,
        right,
        left_keys,
        right_keys,
        left_schema,
        right_schema,
        output_schema,
        num_sub: num_sub_partitions,
        pool_limit,
        disk_manager: Arc::clone(&runtime.disk_manager),
        tables,
        spilled,
        reservation,
        build_spill,
        probe_spill,
        partitioner_left,
        partitioner_right,
        spill_metrics,
        pending: VecDeque::new(),
        drain_buckets: VecDeque::new(),
        current: None,
    };

    Ok(join_stream(state))
}

/// Drains `left` into resident `ProbeTable`s and a build spill, spilling the
/// largest resident bucket under memory pressure.
///
/// Returns one entry per bucket: `Some(table)` for resident non-empty buckets,
/// `None` for empty or spilled buckets. `spilled[b]` is `true` iff bucket `b`
/// was spilled (its batches live in `build_spill`).
#[allow(clippy::too_many_arguments)]
async fn build_phase(
    mut left: SendableRecordBatchStream,
    left_keys: &[PhysicalExprRef],
    left_schema: &SchemaRef,
    num_sub_partitions: usize,
    reservation: &mut MemoryReservation,
    build_spill: &mut JoinSpillWriter,
    spill_metrics: &SpillMetrics,
    partitioner_left: &RowPartitioner,
) -> Result<(Vec<Option<ProbeTable>>, Vec<bool>)> {
    let mut bucket_batches: Vec<Vec<RecordBatch>> = vec![Vec::new(); num_sub_partitions];
    let mut bucket_hashes: Vec<Vec<u64>> = vec![Vec::new(); num_sub_partitions];
    let mut resident_bytes: Vec<usize> = vec![0; num_sub_partitions];
    let mut spilled: Vec<bool> = vec![false; num_sub_partitions];

    while let Some(batch) = left.next().await {
        let batch = batch?;
        for pb in partitioner_left.partition(&batch)? {
            let bucket = pb.bucket;

            // Already-spilled bucket: straight to disk, no reservation.
            if spilled[bucket] {
                build_spill.append(bucket, &pb.batch)?;
                spill_metrics.record(&pb.batch);
                continue;
            }

            let size = pb.batch.get_array_memory_size();
            loop {
                if reservation.try_grow(size).is_ok() {
                    bucket_batches[bucket].push(pb.batch);
                    bucket_hashes[bucket].extend(pb.hashes);
                    resident_bytes[bucket] += size;
                    break;
                }

                // Pool rejected the grow: evict the largest resident
                // not-yet-spilled bucket to free space, then retry.
                match largest_resident(&resident_bytes, &spilled) {
                    Some(victim) => {
                        let freed = resident_bytes[victim];
                        for b in bucket_batches[victim].drain(..) {
                            build_spill.append(victim, &b)?;
                            spill_metrics.record(&b);
                        }
                        bucket_hashes[victim].clear();
                        reservation.shrink(freed);
                        resident_bytes[victim] = 0;
                        spilled[victim] = true;

                        if victim == bucket {
                            // This batch's own bucket was the victim; the batch
                            // now belongs on disk with the rest of the bucket.
                            build_spill.append(bucket, &pb.batch)?;
                            spill_metrics.record(&pb.batch);
                            break;
                        }
                        // Otherwise loop and retry `try_grow` for this batch.
                    }
                    None => {
                        // Nothing resident to evict and the pool still won't
                        // grant the space: spill this batch directly and mark
                        // its bucket spilled for the remainder of the build.
                        build_spill.append(bucket, &pb.batch)?;
                        spill_metrics.record(&pb.batch);
                        spilled[bucket] = true;
                        break;
                    }
                }
            }
        }
    }

    // Finalize resident buckets into probe tables; spilled buckets stay on disk.
    let mut tables: Vec<Option<ProbeTable>> = Vec::with_capacity(num_sub_partitions);
    for bucket in 0..num_sub_partitions {
        if spilled[bucket] {
            tables.push(None);
            continue;
        }
        let batches = std::mem::take(&mut bucket_batches[bucket]);
        if batches.is_empty() {
            tables.push(None);
            continue;
        }
        let hashes = std::mem::take(&mut bucket_hashes[bucket]);
        let concatenated = concat_batches(left_schema, batches.iter())?;
        tables.push(Some(ProbeTable::build(concatenated, &hashes, left_keys)?));
    }

    Ok((tables, spilled))
}

/// Returns the index of the largest resident (non-empty, not-yet-spilled)
/// bucket, or `None` if every bucket is either spilled or empty.
fn largest_resident(resident_bytes: &[usize], spilled: &[bool]) -> Option<usize> {
    resident_bytes
        .iter()
        .enumerate()
        .filter(|&(b, &bytes)| !spilled[b] && bytes > 0)
        .max_by_key(|&(_, &bytes)| bytes)
        .map(|(b, _)| b)
}

/// Which phase `join_stream` is in.
enum Phase {
    /// Streaming the probe side against resident tables, routing spilled
    /// buckets' probe rows to disk.
    Probe,
    /// Draining spilled buckets one at a time.
    Drain,
}

/// The active spilled bucket being drained: its build-side `ProbeTable`, a
/// reader over its probe spill, and the pool bytes reserved for the table
/// (released when the bucket is exhausted).
struct DrainBucket {
    table: ProbeTable,
    probe_reader: JoinSpillReader,
    reserved: usize,
}

/// All state threaded through `join_stream`'s `stream::unfold`.
struct JoinState {
    phase: Phase,
    right: SendableRecordBatchStream,
    left_keys: Vec<PhysicalExprRef>,
    right_keys: Vec<PhysicalExprRef>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    output_schema: SchemaRef,
    /// Number of sub-partitions ("buckets"), reused as the fan-out for each
    /// drain-phase re-partition level.
    num_sub: usize,
    /// The memory pool's total capacity, or `None` for an unbounded pool.
    pool_limit: Option<usize>,
    /// Source of temp files for spilling re-partitioned buckets during drain.
    disk_manager: Arc<DiskManager>,
    /// Resident probe tables per bucket (`None` for empty or spilled buckets).
    /// Dropped when the drain phase begins.
    tables: Vec<Option<ProbeTable>>,
    spilled: Vec<bool>,
    /// Kept alive so the pool sees this join's resident memory until drain.
    reservation: MemoryReservation,
    build_spill: JoinSpillWriter,
    probe_spill: JoinSpillWriter,
    partitioner_left: RowPartitioner,
    partitioner_right: RowPartitioner,
    spill_metrics: SpillMetrics,
    /// Output batches assembled but not yet yielded.
    pending: VecDeque<RecordBatch>,
    /// Spilled bucket indices still to be drained (populated at drain start).
    drain_buckets: VecDeque<usize>,
    /// The spilled bucket currently being drained, if any.
    current: Option<DrainBucket>,
}

impl JoinState {
    /// Partitions one probe batch and, per resulting bucket, either probes the
    /// resident table (queuing output) or appends the sub-batch to the probe
    /// spill for that bucket.
    fn route_probe_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for pb in self.partitioner_right.partition(batch)? {
            if self.spilled[pb.bucket] {
                self.probe_spill.append(pb.bucket, &pb.batch)?;
                self.spill_metrics.record(&pb.batch);
                continue;
            }
            let Some(table) = &self.tables[pb.bucket] else {
                continue;
            };
            let (build_rows, probe_rows) =
                table.probe(&pb.batch, &pb.hashes, &self.right_keys)?;
            if build_rows.is_empty() {
                continue;
            }
            let out = assemble_output(
                &self.output_schema,
                table.build_batch(),
                &pb.batch,
                &build_rows,
                &probe_rows,
            )?;
            self.pending.push_back(out);
        }
        Ok(())
    }

    /// Ends the probe phase: finalizes both spill writers, releases the
    /// resident tables and their reservation, and lists the spilled buckets to
    /// drain.
    fn begin_drain(&mut self) -> Result<()> {
        self.build_spill.finish()?;
        self.probe_spill.finish()?;
        // Resident tables have been fully probed; drop them and free the pool
        // so only one spilled bucket's build side is resident during drain.
        self.tables = Vec::new();
        self.reservation.free();
        self.drain_buckets = (0..self.spilled.len())
            .filter(|&b| self.spilled[b])
            .collect();
        Ok(())
    }

    /// Advances the drain phase by one step. Queues output onto `pending` and
    /// returns `Ok(true)` while there is more work, or `Ok(false)` once every
    /// spilled bucket has been drained.
    fn drain_step(&mut self) -> Result<bool> {
        loop {
            if let Some(current) = &mut self.current {
                // Pull the next probe batch for the active bucket.
                match current.probe_reader.next() {
                    Some(Ok(probe_batch)) => {
                        // Recompute probe hashes from the same fixed seed so
                        // they match the build side's; all rows are in one
                        // bucket, so a single partitioned batch comes back.
                        for pb in self.partitioner_right.partition(&probe_batch)? {
                            let (build_rows, probe_rows) = current.table.probe(
                                &pb.batch,
                                &pb.hashes,
                                &self.right_keys,
                            )?;
                            if build_rows.is_empty() {
                                continue;
                            }
                            let out = assemble_output(
                                &self.output_schema,
                                current.table.build_batch(),
                                &pb.batch,
                                &build_rows,
                                &probe_rows,
                            )?;
                            self.pending.push_back(out);
                        }
                        if !self.pending.is_empty() {
                            return Ok(true);
                        }
                        // No output from this batch; keep pulling.
                    }
                    Some(Err(e)) => return Err(e),
                    None => {
                        // Bucket exhausted; free its build-table reservation and
                        // drop the table before loading the next one.
                        let reserved = current.reserved;
                        self.reservation.shrink(reserved);
                        self.current = None;
                    }
                }
                continue;
            }

            // Load the next spilled bucket.
            let Some(bucket) = self.drain_buckets.pop_front() else {
                return Ok(false);
            };

            // Read the bucket's build side back from disk and concatenate it.
            let build_batches: Vec<RecordBatch> = match self.build_spill.reader(bucket)? {
                Some(r) => r.collect::<Result<_>>()?,
                None => continue, // no build rows -> inner join yields nothing
            };
            if build_batches.is_empty() {
                continue;
            }
            let concatenated = concat_batches(&self.left_schema, build_batches.iter())?;
            let build_size = concatenated.get_array_memory_size();
            let probe_reader = self.probe_spill.reader(bucket)?;

            if self.reservation.try_grow(build_size).is_ok() {
                // The build side fits: build its `ProbeTable` and stream the
                // probe spill through it, one probe batch at a time. Re-partition
                // to recover per-row hashes consistent with build time; all rows
                // share the one bucket, so exactly one partitioned batch comes
                // back, carrying every row in concatenated order.
                let mut parts = self.partitioner_left.partition(&concatenated)?;
                let Some(part) = parts.pop() else {
                    self.reservation.shrink(build_size);
                    continue;
                };
                let table = ProbeTable::build(part.batch, &part.hashes, &self.left_keys)?;
                match probe_reader {
                    Some(probe_reader) => {
                        self.current = Some(DrainBucket {
                            table,
                            probe_reader,
                            reserved: build_size,
                        });
                    }
                    None => {
                        // Build rows but no probe rows -> no output.
                        self.reservation.shrink(build_size);
                    }
                }
            } else {
                // The build side does not fit right now: re-partition this
                // bucket into finer sub-buckets and drain them recursively (or
                // fail cleanly on irreducible single-join-key skew). Output is
                // materialized because this fallback is rare; boundedness is
                // preserved by keeping only one sub-bucket's table resident.
                let ctx = DrainCtx {
                    left_schema: &self.left_schema,
                    right_schema: &self.right_schema,
                    left_keys: &self.left_keys,
                    right_keys: &self.right_keys,
                    output_schema: &self.output_schema,
                    num_sub: self.num_sub,
                    pool_limit: self.pool_limit,
                    disk_manager: &self.disk_manager,
                    spill_metrics: &self.spill_metrics,
                };
                let out = drain_overflowed_bucket(
                    &ctx,
                    &mut self.reservation,
                    concatenated,
                    probe_reader,
                    0,
                )?;
                self.pending.extend(out);
                if !self.pending.is_empty() {
                    // Surface this bucket's output before loading the next one,
                    // otherwise `join_stream` would end the drain on the next
                    // `Ok(false)` and discard the queued rows.
                    return Ok(true);
                }
            }
        }
    }
}

/// Shared, read-only context threaded through the drain-phase recursion.
struct DrainCtx<'a> {
    left_schema: &'a SchemaRef,
    right_schema: &'a SchemaRef,
    left_keys: &'a [PhysicalExprRef],
    right_keys: &'a [PhysicalExprRef],
    output_schema: &'a SchemaRef,
    num_sub: usize,
    pool_limit: Option<usize>,
    disk_manager: &'a Arc<DiskManager>,
    spill_metrics: &'a SpillMetrics,
}

/// Drains one spilled (sub-)bucket whose rows were partitioned with
/// `seed_for_depth(depth)`. If the concatenated build side fits the pool it is
/// joined against its probe spill and the output returned; otherwise the bucket
/// is handled by [`drain_overflowed_bucket`] (re-partition, best-effort hold,
/// or clean error). Only one (sub-)bucket's build table is resident at any
/// instant, at any depth.
fn drain_bucket_recursive(
    ctx: &DrainCtx,
    reservation: &mut MemoryReservation,
    build: RecordBatch,
    probe_reader: Option<JoinSpillReader>,
    depth: usize,
) -> Result<Vec<RecordBatch>> {
    let build_size = build.get_array_memory_size();
    if reservation.try_grow(build_size).is_ok() {
        let out = join_resident_bucket(ctx, &build, probe_reader, depth);
        reservation.shrink(build_size);
        return out;
    }
    drain_overflowed_bucket(ctx, reservation, build, probe_reader, depth)
}

/// Handles a (sub-)bucket whose build side did not fit the pool via `try_grow`.
///
///   - More than one distinct join key (and depth budget left) → re-partition
///     into finer sub-buckets and drain each recursively.
///   - A single distinct key (or the depth cap) whose build side is larger than
///     the whole pool → irreducible skew, returned as a clean error.
///   - Otherwise the build side fits the pool and the `try_grow` only lost a
///     race with other resident buckets → hold this one bucket anyway with an
///     infallible reservation (still at most one table resident).
fn drain_overflowed_bucket(
    ctx: &DrainCtx,
    reservation: &mut MemoryReservation,
    build: RecordBatch,
    probe_reader: Option<JoinSpillReader>,
    depth: usize,
) -> Result<Vec<RecordBatch>> {
    if depth < MAX_DRAIN_DEPTH && has_multiple_distinct_keys(&build, ctx.left_keys)? {
        return repartition_and_recurse(ctx, reservation, build, probe_reader, depth);
    }

    let build_size = build.get_array_memory_size();
    if depth >= MAX_DRAIN_DEPTH || exceeds_pool(build_size, ctx.pool_limit) {
        return exec_err!(
            "SpillingHashJoinExec: build side for a single join key exceeds \
             memory; enable SMJ fallback for this query"
        );
    }

    reservation.grow(build_size);
    let out = join_resident_bucket(ctx, &build, probe_reader, depth);
    reservation.shrink(build_size);
    out
}

/// Returns `true` iff a bounded pool of total capacity `pool_limit` cannot hold
/// a build side of `size` even when otherwise empty (irreducible skew). An
/// unbounded pool (`None`) is never exceeded.
fn exceeds_pool(size: usize, pool_limit: Option<usize>) -> bool {
    matches!(pool_limit, Some(limit) if size > limit)
}

/// Joins one resident build bucket (already reserved by the caller) against its
/// probe spill, returning the assembled output batches. Hashes for both sides
/// are recomputed with `seed_for_depth(depth)` — the same seed the level that
/// produced this bucket used — so build and probe stay consistent.
fn join_resident_bucket(
    ctx: &DrainCtx,
    build: &RecordBatch,
    probe_reader: Option<JoinSpillReader>,
    depth: usize,
) -> Result<Vec<RecordBatch>> {
    let seed = seed_for_depth(depth);
    let partitioner_left =
        RowPartitioner::with_seed(ctx.left_keys.to_vec(), ctx.num_sub, seed);
    let partitioner_right =
        RowPartitioner::with_seed(ctx.right_keys.to_vec(), ctx.num_sub, seed);

    // All build rows share the one bucket at this seed, so a single partitioned
    // batch comes back carrying every row (with build-consistent hashes).
    let mut parts = partitioner_left.partition(build)?;
    let Some(part) = parts.pop() else {
        return Ok(Vec::new());
    };
    let table = ProbeTable::build(part.batch, &part.hashes, ctx.left_keys)?;

    let mut out = Vec::new();
    let Some(reader) = probe_reader else {
        return Ok(out); // build rows but no probe rows -> no output
    };
    for probe_batch in reader {
        let probe_batch = probe_batch?;
        for pb in partitioner_right.partition(&probe_batch)? {
            let (build_rows, probe_rows) =
                table.probe(&pb.batch, &pb.hashes, ctx.right_keys)?;
            if build_rows.is_empty() {
                continue;
            }
            out.push(assemble_output(
                ctx.output_schema,
                table.build_batch(),
                &pb.batch,
                &build_rows,
                &probe_rows,
            )?);
        }
    }
    Ok(out)
}

/// Re-partitions an oversized (sub-)bucket's build and probe spills into
/// `num_sub` finer sub-buckets using a deeper hash seed, then drains each
/// recursively. The caller (`drain_overflowed_bucket`) has already established
/// that the bucket has more than one distinct join key, so the split makes
/// progress.
fn repartition_and_recurse(
    ctx: &DrainCtx,
    reservation: &mut MemoryReservation,
    build: RecordBatch,
    probe_reader: Option<JoinSpillReader>,
    depth: usize,
) -> Result<Vec<RecordBatch>> {
    ctx.spill_metrics.record_repartition();

    let child_depth = depth + 1;
    let child_seed = seed_for_depth(child_depth);
    let child_left =
        RowPartitioner::with_seed(ctx.left_keys.to_vec(), ctx.num_sub, child_seed);
    let child_right =
        RowPartitioner::with_seed(ctx.right_keys.to_vec(), ctx.num_sub, child_seed);

    // Spill the build side into finer sub-buckets, then release it before
    // recursing so only one sub-bucket's build side is ever resident.
    let mut build_writer =
        JoinSpillWriter::new(Arc::clone(ctx.left_schema), Arc::clone(ctx.disk_manager));
    for pb in child_left.partition(&build)? {
        build_writer.append(pb.bucket, &pb.batch)?;
        ctx.spill_metrics.record(&pb.batch);
    }
    build_writer.finish()?;
    drop(build);

    // Spill the probe side into finer sub-buckets with the SAME seed, so rows
    // with equal keys co-locate with their build rows.
    let mut probe_writer =
        JoinSpillWriter::new(Arc::clone(ctx.right_schema), Arc::clone(ctx.disk_manager));
    if let Some(reader) = probe_reader {
        for probe_batch in reader {
            let probe_batch = probe_batch?;
            for pb in child_right.partition(&probe_batch)? {
                probe_writer.append(pb.bucket, &pb.batch)?;
                ctx.spill_metrics.record(&pb.batch);
            }
        }
    }
    probe_writer.finish()?;

    let mut out = Vec::new();
    for b in 0..ctx.num_sub {
        let build_batches: Vec<RecordBatch> = match build_writer.reader(b)? {
            Some(r) => r.collect::<Result<_>>()?,
            None => continue, // no build rows -> inner join yields nothing
        };
        if build_batches.is_empty() {
            continue;
        }
        let sub_build = concat_batches(ctx.left_schema, build_batches.iter())?;
        let sub_probe = probe_writer.reader(b)?;
        out.extend(drain_bucket_recursive(
            ctx,
            reservation,
            sub_build,
            sub_probe,
            child_depth,
        )?);
    }
    Ok(out)
}

/// Returns `true` iff `build` contains more than one distinct join-key value.
/// Keys are compared via Arrow's row encoding (the same equality basis the
/// probe table uses), comparing every row to row 0; a bucket with a single
/// distinct key is irreducible and cannot be re-partitioned further.
fn has_multiple_distinct_keys(
    build: &RecordBatch,
    keys: &[PhysicalExprRef],
) -> Result<bool> {
    let num_rows = build.num_rows();
    if num_rows <= 1 {
        return Ok(false);
    }
    let key_arrays: Vec<ArrayRef> = keys
        .iter()
        .map(|expr| expr.evaluate(build)?.into_array(num_rows))
        .collect::<Result<_>>()?;
    let fields: Vec<SortField> = key_arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields)?;
    let rows = converter.convert_columns(&key_arrays)?;
    let first = rows.row(0);
    Ok((1..num_rows).any(|i| rows.row(i) != first))
}

/// Turns `JoinState` into the output stream: emit any queued output, then in
/// the probe phase pull the probe side (routing spilled buckets to disk), and
/// finally drain spilled buckets one at a time.
fn join_stream(state: JoinState) -> impl Stream<Item = Result<RecordBatch>> + Send {
    stream::unfold(state, |mut state| async move {
        loop {
            if let Some(batch) = state.pending.pop_front() {
                return Some((Ok(batch), state));
            }

            match state.phase {
                Phase::Probe => match state.right.next().await {
                    Some(Ok(batch)) => {
                        if let Err(e) = state.route_probe_batch(&batch) {
                            return Some((Err(e), state));
                        }
                        // Loop back: this batch may have produced no output.
                    }
                    Some(Err(e)) => return Some((Err(e), state)),
                    None => {
                        if let Err(e) = state.begin_drain() {
                            return Some((Err(e), state));
                        }
                        state.phase = Phase::Drain;
                    }
                },
                Phase::Drain => match state.drain_step() {
                    Ok(true) => {
                        // Loop back to drain queued output.
                    }
                    Ok(false) => return None,
                    Err(e) => return Some((Err(e), state)),
                },
            }
        }
    })
}
