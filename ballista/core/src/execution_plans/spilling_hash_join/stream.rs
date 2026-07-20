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

// The in-memory (no-spill) execution stream for `SpillingHashJoinExec`. All
// build-side sub-partitions ("buckets") are kept resident for the lifetime of
// the stream — a later task adds spilling any of them to disk under memory
// pressure. Until then this is a plain, if internally sub-partitioned, hash
// join: build fully, then probe.
//
// The stream is built as a `futures::stream::once` future (drain the build
// side and construct one `ProbeTable` per bucket) that resolves to the probe
// stream, flattened via `TryStreamExt::try_flatten`. This mirrors the idiom
// `SortShuffleWriterExec::execute` already uses in this crate
// (`sort_shuffle/writer.rs`) for "do async setup, then hand back a stream" —
// it keeps the build phase as ordinary `async`/`await` code instead of a
// hand-rolled `Stream::poll_next` state machine, while still producing a
// single `SendableRecordBatchStream` that does no work until polled and
// never emits probe output before the build side is fully drained.

use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, TryStreamExt, stream};

use super::hash_table::{ProbeTable, assemble_output};
use super::partitioner::RowPartitioner;

/// Builds the `SendableRecordBatchStream` for `SpillingHashJoinExec::execute`:
/// drains `left` (the build side) fully into one resident `ProbeTable` per
/// sub-partition bucket, then probes `right` (the probe side) against those
/// tables, bucket-matched, as it arrives.
///
/// `output_schema` is the join's output schema (left columns ++ right
/// columns), independent of `left`/`right`'s own schemas — it is what the
/// returned stream's `schema()` reports.
pub fn execute_join(
    output_schema: SchemaRef,
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    left_keys: Vec<PhysicalExprRef>,
    right_keys: Vec<PhysicalExprRef>,
    num_sub_partitions: usize,
) -> SendableRecordBatchStream {
    let adapter_schema = Arc::clone(&output_schema);

    // A single future that drains `left`, builds the resident hash tables,
    // then resolves to the probe stream. `try_flatten` turns this
    // `Stream<Item = Result<ProbeStream>>` of one element into the actual
    // `Stream<Item = Result<RecordBatch>>` the adapter needs — nothing here
    // polls `right` until the future above has completed, which is exactly
    // the "fully build before any probe output" ordering the join requires.
    let joined = stream::once(build_then_probe(
        left,
        right,
        left_keys,
        right_keys,
        num_sub_partitions,
        output_schema,
    ))
    .try_flatten();

    Box::pin(RecordBatchStreamAdapter::new(adapter_schema, joined))
}

/// Drains `left` into resident hash tables, then returns a stream that
/// probes `right` against them. Split out of `execute_join` (rather than an
/// inline `async move` block) purely so its `Result<impl Stream<..>>` return
/// type is written down once, instead of needing an explicit turbofish at
/// every call site to disambiguate the error type.
async fn build_then_probe(
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    left_keys: Vec<PhysicalExprRef>,
    right_keys: Vec<PhysicalExprRef>,
    num_sub_partitions: usize,
    output_schema: SchemaRef,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send> {
    let tables = build_tables(left, &left_keys, num_sub_partitions).await?;
    Ok(probe_stream(right, right_keys, tables, output_schema))
}

/// Drains `left` fully, routing every row into one of `num_sub_partitions`
/// buckets by `left_keys` (via `RowPartitioner`), then builds one
/// `ProbeTable` per non-empty bucket. Buckets that never received a row are
/// `None`, so `probe_stream` can skip them (and later, spilling) without
/// treating "no rows" as an error.
async fn build_tables(
    mut left: SendableRecordBatchStream,
    left_keys: &[PhysicalExprRef],
    num_sub_partitions: usize,
) -> Result<Vec<Option<ProbeTable>>> {
    let left_schema = left.schema();
    let partitioner = RowPartitioner::new(left_keys.to_vec(), num_sub_partitions);

    let mut bucket_batches: Vec<Vec<RecordBatch>> = vec![Vec::new(); num_sub_partitions];
    let mut bucket_hashes: Vec<Vec<u64>> = vec![Vec::new(); num_sub_partitions];

    while let Some(batch) = left.next().await {
        let batch = batch?;
        for pb in partitioner.partition(&batch)? {
            bucket_batches[pb.bucket].push(pb.batch);
            bucket_hashes[pb.bucket].extend(pb.hashes);
        }
    }

    bucket_batches
        .into_iter()
        .zip(bucket_hashes)
        .map(|(batches, hashes)| {
            if batches.is_empty() {
                return Ok(None);
            }
            let concatenated = concat_batches(&left_schema, batches.iter())?;
            Ok(Some(ProbeTable::build(concatenated, &hashes, left_keys)?))
        })
        .collect()
}

/// Mutable state threaded through `probe_stream`'s `futures::stream::unfold`.
struct ProbeState {
    right: SendableRecordBatchStream,
    partitioner: RowPartitioner,
    right_keys: Vec<PhysicalExprRef>,
    tables: Vec<Option<ProbeTable>>,
    output_schema: SchemaRef,
    /// Output batches assembled from the most recent `right` batch but not
    /// yet yielded. A single `right` batch can fan out into multiple
    /// sub-partition buckets, each producing its own output batch, so these
    /// are queued and drained one at a time before pulling `right` again.
    pending: VecDeque<RecordBatch>,
}

/// Probes `right` against `tables` (one resident `ProbeTable` per bucket,
/// built from the fully-drained build side) as `right` batches arrive,
/// yielding one output batch per non-empty matched sub-partition. `right`
/// rows that hash to a bucket with no build-side table (`None`) have no
/// possible match and are dropped, matching inner-join semantics.
fn probe_stream(
    right: SendableRecordBatchStream,
    right_keys: Vec<PhysicalExprRef>,
    tables: Vec<Option<ProbeTable>>,
    output_schema: SchemaRef,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    let num_sub_partitions = tables.len();
    let state = ProbeState {
        right,
        partitioner: RowPartitioner::new(right_keys.clone(), num_sub_partitions),
        right_keys,
        tables,
        output_schema,
        pending: VecDeque::new(),
    };

    stream::unfold(state, |mut state| async move {
        loop {
            if let Some(batch) = state.pending.pop_front() {
                return Some((Ok(batch), state));
            }

            let next = state.right.next().await?;
            let batch = match next {
                Ok(batch) => batch,
                Err(e) => return Some((Err(e), state)),
            };

            if let Err(e) = probe_batch(&batch, &mut state) {
                return Some((Err(e), state));
            }
            // Loop back around: this `right` batch may have produced no
            // output (e.g. every bucket it touched was empty), in which case
            // `pending` is still empty and we must pull the next `right`
            // batch rather than returning `None` (which would end the
            // stream early).
        }
    })
}

/// Partitions one `right` batch by `state.right_keys` and probes each
/// resulting sub-batch against its bucket's `ProbeTable` (if any), pushing
/// every non-empty assembled output batch onto `state.pending`.
fn probe_batch(batch: &RecordBatch, state: &mut ProbeState) -> Result<()> {
    for pb in state.partitioner.partition(batch)? {
        let Some(table) = &state.tables[pb.bucket] else {
            continue;
        };
        let (build_rows, probe_rows) =
            table.probe(&pb.batch, &pb.hashes, &state.right_keys)?;
        if build_rows.is_empty() {
            continue;
        }
        let out = assemble_output(
            &state.output_schema,
            table.build_batch(),
            &pb.batch,
            &build_rows,
            &probe_rows,
        )?;
        state.pending.push_back(out);
    }
    Ok(())
}
