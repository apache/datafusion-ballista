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
//      its build spill is read back and concatenated, its hashes recomputed by
//      re-partitioning the concatenated build (the fixed seed guarantees all
//      rows land in the one bucket, so the recomputed hashes match build time),
//      a `ProbeTable` is built, and its probe spill is streamed through it.
//      The table is dropped before the next bucket, so only one spilled
//      bucket's build side is resident at a time.
//
// The stream is a single `futures::stream::once` future (do the async build
// phase) flattened via `try_flatten` into a `stream::unfold` that carries the
// probe and drain phases. Nothing polls the probe side until the build side is
// fully drained, which is exactly the ordering an inner hash join requires.

use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, TryStreamExt, stream};

use super::hash_table::{ProbeTable, assemble_output};
use super::partitioner::RowPartitioner;
use super::spill::{JoinSpillReader, JoinSpillWriter};

/// Spill counters for one `SpillingHashJoinExec` partition, published on the
/// operator's `ExecutionPlanMetricsSet`.
#[derive(Clone)]
struct SpillMetrics {
    /// Number of batches appended to a build- or probe-side spill file.
    spill_count: Count,
    /// Sum of `RecordBatch::get_array_memory_size` over all spilled batches.
    spilled_bytes: Count,
}

impl SpillMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            spill_count: MetricBuilder::new(metrics).counter("spill_count", partition),
            spilled_bytes: MetricBuilder::new(metrics)
                .counter("spilled_bytes", partition),
        }
    }

    /// Records one spilled batch.
    fn record(&self, batch: &RecordBatch) {
        self.spill_count.add(1);
        self.spilled_bytes.add(batch.get_array_memory_size());
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
        output_schema,
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

/// The active spilled bucket being drained: its build-side `ProbeTable` and a
/// reader over its probe spill.
struct DrainBucket {
    table: ProbeTable,
    probe_reader: JoinSpillReader,
}

/// All state threaded through `join_stream`'s `stream::unfold`.
struct JoinState {
    phase: Phase,
    right: SendableRecordBatchStream,
    left_keys: Vec<PhysicalExprRef>,
    right_keys: Vec<PhysicalExprRef>,
    left_schema: SchemaRef,
    output_schema: SchemaRef,
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
                        // Bucket exhausted; drop its table before the next one.
                        self.current = None;
                    }
                }
                continue;
            }

            // Load the next spilled bucket.
            let Some(bucket) = self.drain_buckets.pop_front() else {
                return Ok(false);
            };
            let Some(table) = self.build_drain_table(bucket)? else {
                // No build rows for this bucket: an inner join yields nothing,
                // so skip it (and its probe spill, if any).
                continue;
            };
            let probe_reader = match self.probe_spill.reader(bucket)? {
                Some(r) => r,
                None => continue, // build rows but no probe rows -> no output
            };
            self.current = Some(DrainBucket {
                table,
                probe_reader,
            });
        }
    }

    /// Reads a spilled bucket's build side back from disk, concatenates it, and
    /// builds its `ProbeTable`, recomputing hashes by re-partitioning the
    /// concatenated build. The fixed seed guarantees every row lands in the
    /// single original bucket, so the recomputed hashes match build time.
    /// Returns `None` if the bucket has no build rows.
    fn build_drain_table(&self, bucket: usize) -> Result<Option<ProbeTable>> {
        let Some(reader) = self.build_spill.reader(bucket)? else {
            return Ok(None);
        };
        let batches: Vec<RecordBatch> = reader.collect::<Result<_>>()?;
        if batches.is_empty() {
            return Ok(None);
        }
        let concatenated = concat_batches(&self.left_schema, batches.iter())?;

        // Re-partition to recover per-row hashes consistent with build time.
        // All rows share the one bucket, so exactly one partitioned batch is
        // returned, carrying every row in the concatenated order.
        let mut parts = self.partitioner_left.partition(&concatenated)?;
        let Some(part) = parts.pop() else {
            return Ok(None);
        };
        Ok(Some(ProbeTable::build(
            part.batch,
            &part.hashes,
            &self.left_keys,
        )?))
    }
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
