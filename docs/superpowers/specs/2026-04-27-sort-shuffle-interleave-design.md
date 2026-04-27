<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Sort-Shuffle: Buffer Whole Batches and Materialize via `interleave_record_batch`

Closes [#1432](https://github.com/apache/datafusion-ballista/issues/1432).

Branch: `feat/sort-shuffle-interleave` (based on `apache/main`).

## Problem

The current sort-based shuffle writer (`ballista/core/src/execution_plans/sort_shuffle/`) calls `BatchPartitioner::partition()` on every input batch, which internally `take_arrays()`-slices the batch into one sub-batch per output partition and stores those slices in per-partition `PartitionBuffer`s. With `N` output partitions, an 8192-row input batch becomes up to `N` sub-batches averaging `8192 / N` rows. At `N = 200`, that is ~41 rows per buffered batch.

These tiny batches are appended individually to `PartitionBuffer`, spilled individually to IPC, and written individually to the final output file. Each batch carries Arrow IPC framing overhead, hurts compression ratios, and inflates per-batch metadata cost.

## Goal

Adopt the approach used by Apache DataFusion Comet's sort-based shuffle: defer materialization. Hold whole input batches plus per-partition row-index lists, and only call `arrow::compute::interleave_record_batch` at spill or final-write time, producing output batches sized up to the configured `batch_size`.

In the same change, replace the per-task fixed-byte memory budget with `MemoryReservation` against the runtime memory pool, so the sort-shuffle writer participates in the same memory accounting as every other DataFusion operator.

## Non-Goals

- Changing the on-disk shuffle file format. The existing Arrow IPC `FileWriter` + `data.arrow.index` format is preserved. Reader code is untouched.
- Adding RangePartitioning or RoundRobin variants of the sort writer. Hash partitioning only, as today.
- Changing the hash-based shuffle writer. Untouched.
- TPC-H performance validation in this PR. The benchmark binary added here is the tool to measure that follow-on; the PR itself ships the change and verifies correctness.

## Architecture

### Data structures

Replace `PartitionBuffer` (`buffer.rs`) with a single `BufferedBatches` value owned by `execute_shuffle_write`:

```rust
pub struct BufferedBatches {
    schema: SchemaRef,
    /// Whole input batches, never sliced.
    batches: Vec<RecordBatch>,
    /// One entry per output partition. Each (u32, u32) is (batch_idx, row_idx)
    /// referring to a row inside `batches[batch_idx]`.
    indices: Vec<Vec<(u32, u32)>>,
    /// Total rows currently referenced by `indices`. Drives metrics only.
    num_buffered_rows: usize,
}
```

Add a `PartitionedBatchIterator` in a new sibling file `partitioned_batch_iterator.rs`. It borrows `&[RecordBatch]` and `&[(u32, u32)]`, holds a target `batch_size`, and yields `RecordBatch`es of up to `batch_size` rows by calling `arrow::compute::interleave_record_batch(batches, indices_chunk)`. This is the only path through which buffered rows are turned into output `RecordBatch`es.

**Invariant:** rows are copied exactly once, when materialized into a `batch_size`-shaped output batch. `take_arrays`-per-partition-per-input-batch goes away.

### Partitioning (inline `compute_partition_indices`)

Drop the `BatchPartitioner` dependency. Inline the same hashing logic so partition assignments stay byte-identical to DataFusion's `BatchPartitioner::Hash`:

```rust
fn compute_partition_indices(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    hash_buffer: &mut Vec<u64>, // reused across calls
) -> Result<Vec<Vec<u32>>>     // partition_id -> row indices
```

Implementation uses these public helpers:

- `datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays`
- `datafusion_common::hash_utils::create_hashes`
- `datafusion_physical_plan::repartition::REPARTITION_RANDOM_STATE` (`pub const`)

Caller path becomes:

```
compute_partition_indices -> push (batch_idx, row_idx) into BufferedBatches.indices[p]
                          -> push the input batch into BufferedBatches.batches
                          -> reservation.try_grow(growth); on Err, spill_all()
```

### Spill model: all-or-nothing

Because `BufferedBatches.batches` is shared across partitions, freeing memory requires draining _all_ partition indices and dropping the buffered batches. The current "spill the largest buffer" greedy logic goes away.

When `reservation.try_grow(growth)` returns `Err`:

1. For each partition `p` in `0..num_output_partitions`:
   - Walk `indices[p]` through `PartitionedBatchIterator`.
   - For each yielded `RecordBatch`, call `SpillManager::spill(p, &batch)` to append to that partition's spill file.
2. Drop `BufferedBatches.batches`, clear all `indices[p]`, reset `num_buffered_rows`.
3. `reservation.free()`.

Output batches in spill files are already `batch_size`-shaped at spill time; the spill IPC contains no tiny batches.

`SpillManager` keeps its current shape (one append-only IPC `StreamWriter` per partition, lazily created). Its `spill` signature changes from `(partition_id: usize, batches: Vec<RecordBatch>, schema: &SchemaRef) -> Result<()>` to `(partition_id: usize, batch: &RecordBatch) -> Result<()>` (called once per yielded batch). The internal `LimitedBatchCoalescer` usage in spill goes away — coalescing is now handled by `PartitionedBatchIterator` upstream.

### Memory pool integration

Replace the `SortShuffleConfig::spill_memory_threshold()` check with a `MemoryReservation`:

```rust
let reservation = MemoryConsumer::new(format!("SortShuffleWriter[{input_partition}]"))
    .with_can_spill(true)
    .register(&context.runtime_env().memory_pool);
```

After every input batch:

```rust
let mut growth = batch.get_array_memory_size();
for p in 0..num_output_partitions {
    let before = buffered.indices[p].allocated_size();
    buffered.indices[p].extend(rows_for_p.iter().map(|&r| (batch_idx, r)));
    growth += buffered.indices[p].allocated_size().saturating_sub(before);
}
buffered.batches.push(batch);

if reservation.try_grow(growth).is_err() {
    spill_all(&mut buffered, &mut spill_manager, batch_size, schema)?;
}
```

`reservation.free()` is called at the end of `spill_all`. The reservation is dropped naturally at the end of `execute_shuffle_write`.

### Final write (`finalize_output`)

Format unchanged: `data.arrow` (Arrow IPC `FileWriter`) + `data.arrow.index` (per-partition starting batch index, i64 little-endian). What changes is how each partition's batches are produced:

For each output partition `p`, in order:

1. Record `cumulative_batch_count` as that partition's starting batch index.
2. If `SpillManager` has a spill file for `p`, stream-read it (already `batch_size`-shaped) and write each batch into the `FileWriter`.
3. If `BufferedBatches.indices[p]` is non-empty, run it through `PartitionedBatchIterator` and write each yielded batch.
4. Update `cumulative_batch_count` by the number of batches written for `p`.

After the loop: `writer.finish()`, write `ShuffleIndex` to disk, return `ShuffleWritePartition` rows for partitions with `num_rows > 0`.

`finalize_output` shrinks: no more `take_batches_coalesced` (the iterator already produces well-sized batches), no more per-batch coalescer in the spill-read path.

## Configuration

`SortShuffleConfig` (`config.rs`) drops three fields that no longer make sense:

- `buffer_size` — was the per-partition byte cap on the old eager-buffer model. No equivalent.
- `memory_limit` — was the per-task fixed budget. The runtime memory pool replaces it.
- `spill_threshold` — was the fraction of `memory_limit` at which to start spilling. Pool decides; no fraction.

Also drops `spill_memory_threshold()`.

Keeps:

- `enabled`
- `batch_size`
- `compression`

The matching three keys in `ballista/core/src/config.rs` (`ballista.shuffle.sort_based.buffer_size`, `.memory_limit`, `.spill_threshold`) and their `SessionConfig` accessors are removed. Pre-1.0, no deprecation shim. Keys retained: `ballista.shuffle.sort_based.enabled`, `ballista.shuffle.sort_based.batch_size`.

## Files Changed

### Core writer

- `ballista/core/src/execution_plans/sort_shuffle/buffer.rs` — replace `PartitionBuffer` with `BufferedBatches`. Drop `LimitedBatchCoalescer` use, drop `coalesce_batches` helper, drop the existing unit tests (replaced; see Tests).
- `ballista/core/src/execution_plans/sort_shuffle/partitioned_batch_iterator.rs` — new file. `PartitionedBatchIterator` over `&[RecordBatch]` + `&[(u32, u32)]` calling `interleave_record_batch`.
- `ballista/core/src/execution_plans/sort_shuffle/writer.rs` — rewrite `execute_shuffle_write`: inline `compute_partition_indices`, hold a single `BufferedBatches`, register `MemoryReservation`, call `spill_all` on `try_grow` failure. Delete `spill_largest_buffers`. Simplify `finalize_output` to feed `PartitionedBatchIterator`.
- `ballista/core/src/execution_plans/sort_shuffle/spill.rs` — `SpillManager::spill` becomes `(partition_id, &RecordBatch)`. Drop the in-memory coalescing inside spill.
- `ballista/core/src/execution_plans/sort_shuffle/config.rs` — drop `buffer_size`, `memory_limit`, `spill_threshold`, `spill_memory_threshold()`. Drop the `new(...)` constructor's positional args for those fields. Keep `enabled`, `batch_size`, `compression`.
- `ballista/core/src/execution_plans/sort_shuffle/mod.rs` — re-export `partitioned_batch_iterator` if anything outside the module needs it (tests do).

### Configuration

- `ballista/core/src/config.rs` — remove the three obsolete config-key constants and their `SessionConfig` accessors. Keep `enabled` and `batch_size`.

### Tests

- `ballista/core/src/execution_plans/sort_shuffle/buffer.rs` — replace existing tests. New tests validate that `BufferedBatches` correctly tracks `(batch_idx, row_idx)` pairs as batches are added, and that `PartitionedBatchIterator` is row-equivalent to a manual gather (gold-standard: hand-build a 100-row input across 3 batches, partition into 4, verify each output partition's rows match `interleave` over the recorded indices and total exactly the input row count).
- `ballista/core/src/execution_plans/sort_shuffle/writer.rs` — extend the existing end-to-end `test_sort_shuffle_writer` to assert all output batches are at most `batch_size` rows (no tiny batches) and per-partition row counts are correct.
- New: memory-pressure spill test in `writer.rs`. Build a `RuntimeEnv` with `FairSpillPool` set to ~4 MiB, feed enough batches to force `try_grow` failure, assert (a) `spill_count > 0`, (b) round-trip read of the output file yields the original rows, (c) `reservation.size() == 0` after finalization.
- `ballista/client/tests/sort_shuffle.rs` — adjust any test that sets the removed `BALLISTA_SHUFFLE_SORT_BASED_*` keys (memory_limit / buffer_size / spill_threshold). Behavior under default settings should be unchanged.

### Benchmarks

- `benchmarks/src/bin/shuffle_bench.rs` — full rewrite, modeled on `datafusion-comet/native/shuffle/src/bin/shuffle_bench.rs`. Streams from real Parquet input via `read_parquet`, optional `CoalescePartitionsExec`, warmup + iteration loop with per-iter wall-clock + output bytes, aggregated input metrics, aggregated shuffle metrics (repart/encode/write time, spill counts/bytes, throughput), `--concurrent-tasks` for parallelism simulation, output cleanup. Adapted differences from Comet:
  - `--writer hash|sort` selects the Ballista writer.
  - `--partitioning hash|single|round-robin` controls the partitioning scheme. `--writer hash` only legally pairs with `hash`; `--writer sort` only legally pairs with `hash`. Other combinations are rejected at startup with a clear error.
  - `--memory-limit` wires through `RuntimeEnvBuilder::with_memory_limit(mem_limit, 1.0)`. With this PR's pool integration, the knob now actually drives sort-shuffle spilling.
- `benchmarks/Cargo.toml` — replace `structopt` dep with `clap` if not already a transitive dep at the right version. Keep the `mimalloc` feature gate.
- `benchmarks/benches/sort_shuffle.rs` — left untouched. Existing Criterion bench continues to cover the synthetic-data A/B case.

## Coordination

The in-flight docs PR (`30576dd7 docs: document hash-based and sort-based shuffle implementations`) adds a "Shuffle Implementation" section to `tuning-guide.md` and a "Shuffle Settings" table to `configs.md` that document the three keys this PR removes. Whichever PR lands first, the other is rebased to match. This PR's branch deliberately does not modify docs (none exist on `apache/main` for these keys yet).

## Risks and Trade-offs

- **All-or-nothing spill.** The greedy "spill the largest buffer" model could spill less data per event by targeting only the heaviest partition. The new model spills everything once. In practice the freed memory is the buffered input batches (the dominant cost), so partial spill provides little benefit and adds complexity. Comet uses all-or-nothing for the same reason.
- **Hash-semantics drift.** The inline `compute_partition_indices` will be byte-identical to `BatchPartitioner::Hash` _as long as DataFusion does not change `REPARTITION_RANDOM_STATE` or its hash routine_. If DataFusion changes either, the sort writer would diverge from any `RepartitionExec` upstream. Mitigation: a unit test in `writer.rs` that compares `compute_partition_indices` output to a `BatchPartitioner` reference run on the same input. Catches drift the next time `cargo update` rolls DataFusion.
- **Memory growth cost during high-fanout writes.** Each input batch grows `partition_indices` by `num_rows × 8` bytes total, distributed across `num_output_partitions` `Vec<(u32, u32)>`s. At 200 partitions with capacity-doubling growth, occasional reallocation cost is bounded; the `allocated_size().saturating_sub(before)` accounting captures actual heap growth, not capacity-doubled-then-shrunk.
- **`MemoryConsumer` registration pressure.** Each running shuffle task registers its own `MemoryConsumer`. Hundreds of concurrent shuffle tasks per executor would create hundreds of consumers — well within DataFusion's design tolerance, matching how `SortExec` and `HashJoinExec` already work.

## Verification

- `cargo build --workspace --all-targets --locked` clean.
- `./dev/rust_lint.sh` clean (fmt + clippy + toml fmt).
- `cargo test --workspace` passes (including integration tests in `ballista/client/tests/sort_shuffle.rs`).
- `cargo check -p ballista-scheduler -p ballista-executor -p ballista-core -p ballista --no-default-features --locked` clean (matches the CI feature-gating job).
- `cd ballista && cargo test --no-default-features --features standalone --locked` passes.
- New memory-pressure test in `writer.rs` runs green, confirming spill is triggered by pool pressure (not the deleted fixed-byte threshold).
- Manual benchmark run: `cargo run --release -p ballista-benchmarks --bin shuffle_bench -- --input <parquet> --writer sort --partitions 200 --iterations 3` produces non-zero metrics and a comparison run with `--writer hash` shows the sort writer produces consolidated output (one data + one index file per input partition) versus hash's `N × M` files.
