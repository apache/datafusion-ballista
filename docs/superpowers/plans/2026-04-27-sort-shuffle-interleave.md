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

# Sort-Shuffle Interleave Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the eager `take_arrays`-per-partition buffering in Ballista's sort-based shuffle writer with deferred materialization via `arrow::compute::interleave_record_batch`, and integrate the writer with DataFusion's runtime memory pool.

**Architecture:** The writer now stores whole input `RecordBatch`es in a single `Vec<RecordBatch>` plus per-partition `Vec<Vec<(u32, u32)>>` row-index lists. Materialization happens only at spill or final-write time, yielding `batch_size`-shaped output batches via `interleave_record_batch`. Spill is all-or-nothing, triggered by `MemoryReservation::try_grow` failure against the runtime pool. The Arrow IPC `FileWriter` output format is preserved (no reader change).

**Tech Stack:** Rust 2024, DataFusion 53, Arrow IPC, `tonic-prost-build` (protobuf), `clap` (CLI for the new bench binary).

**Spec:** [`docs/superpowers/specs/2026-04-27-sort-shuffle-interleave-design.md`](../specs/2026-04-27-sort-shuffle-interleave-design.md). Closes [#1432](https://github.com/apache/datafusion-ballista/issues/1432).

**Branch:** `feat/sort-shuffle-interleave` (already created from `apache/main`; spec already committed).

---

## File Structure

| File                                                                           | Role                                                                                          | Status                              |
| ------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- | ----------------------------------- |
| `ballista/core/src/execution_plans/sort_shuffle/buffer.rs`                     | Holds `BufferedBatches` (whole input batches + per-partition `(batch_idx, row_idx)` lists)    | Rewrite — replace `PartitionBuffer` |
| `ballista/core/src/execution_plans/sort_shuffle/partitioned_batch_iterator.rs` | `PartitionedBatchIterator` — yields `batch_size`-shaped batches via `interleave_record_batch` | New                                 |
| `ballista/core/src/execution_plans/sort_shuffle/writer.rs`                     | `SortShuffleWriterExec` + private `compute_partition_indices` + `spill_all_partitions`        | Rewrite                             |
| `ballista/core/src/execution_plans/sort_shuffle/spill.rs`                      | `SpillManager` — per-batch spill API                                                          | Modify (signature change)           |
| `ballista/core/src/execution_plans/sort_shuffle/config.rs`                     | `SortShuffleConfig` — slimmed to `enabled`, `batch_size`, `compression`                       | Modify (drop 3 fields)              |
| `ballista/core/src/execution_plans/sort_shuffle/mod.rs`                        | Re-exports                                                                                    | Modify (drop `PartitionBuffer`)     |
| `ballista/core/src/config.rs`                                                  | `BallistaConfig` — drop 3 obsolete sort-shuffle keys                                          | Modify                              |
| `ballista/core/proto/ballista.proto`                                           | `SortShuffleWriterExecNode` — drop 3 obsolete fields                                          | Modify                              |
| `ballista/core/src/serde/mod.rs`                                               | Encode/decode of `SortShuffleWriterExecNode`                                                  | Modify                              |
| `ballista/scheduler/src/planner.rs`                                            | `create_shuffle_writer_with_config` callsite                                                  | Modify                              |
| `ballista/client/tests/sort_shuffle.rs`                                        | Integration tests — drop removed config-key references                                        | Modify                              |
| `benchmarks/benches/sort_shuffle.rs`                                           | Criterion bench — `SortShuffleConfig::new` callsite                                           | Modify                              |
| `benchmarks/src/bin/shuffle_bench.rs`                                          | Standalone Parquet-driven bench, `--writer hash\|sort`                                        | Rewrite                             |
| `benchmarks/Cargo.toml`                                                        | Replace `structopt` with `clap`, add `parquet`, `arrow`                                       | Modify                              |

Each task lands as one commit. Use conventional commit format (`feat:`, `refactor:`, `chore:`, `test:`).

---

## Conventions for this plan

- **Run tests from repo root:** `/Users/andy/git/apache/datafusion-ballista`. Tests need submodule-tracked data; if missing, run `git submodule update --init` once.
- **Single-test command pattern:** `cargo test -p ballista-core <substring>` filters by name.
- **Edition 2024, MSRV 1.88.0** — `Self::` paths are pervasive; do not introduce 2018-style absolute paths.
- **Apache license header** required on every new source file. Copy from any existing file in the same directory; do not rewrite by hand.
- **`#![warn(missing_docs)]`** is on for `ballista-core`. New `pub` items need rustdoc, even one-liners.
- **No mock databases / fakes**: the integration tests use `SessionContext::standalone()` and a real Parquet file under `testdata/`.
- **Don't skip clippy** — CI runs `cargo clippy --all-targets --workspace --all-features -- -D warnings`. Each task's verification step runs it locally.

---

## Task 1: Add `compute_partition_indices` helper

**Files:**

- Modify: `ballista/core/src/execution_plans/sort_shuffle/writer.rs`

This task adds a private helper that, given an input batch and hash-partitioning expressions, returns row indices grouped by output partition. It does **not** wire the helper into `execute_shuffle_write` yet (Task 5 does that). Adding it standalone with tests gives us a known-good starting point byte-identical to DataFusion's `BatchPartitioner::Hash`.

- [ ] **Step 1: Write the failing test**

Add this test inside the existing `#[cfg(test)] mod tests { ... }` block in `ballista/core/src/execution_plans/sort_shuffle/writer.rs` (the block that already contains `test_sort_shuffle_writer`):

```rust
#[test]
fn compute_partition_indices_distributes_rows_by_hash() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..1000_i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..1000).map(|i| format!("v{i}")).collect::<Vec<String>>(),
            )),
        ],
    )
    .unwrap();

    let exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
        vec![Arc::new(datafusion::physical_expr::expressions::Column::new("k", 0))];

    let mut hash_buffer: Vec<u64> = Vec::new();
    let result =
        compute_partition_indices(&batch, &exprs, 8, &mut hash_buffer).unwrap();

    // 8 partition slots
    assert_eq!(result.len(), 8);
    // Total row count is preserved
    let total: usize = result.iter().map(|v| v.len()).sum();
    assert_eq!(total, 1000);
    // No row index appears in two partitions
    let mut seen = vec![false; 1000];
    for indices in &result {
        for &row in indices {
            assert!(!seen[row as usize], "row {row} appeared twice");
            seen[row as usize] = true;
        }
    }
    // Distribution is non-trivial — at least 2 distinct partitions used
    let used = result.iter().filter(|v| !v.is_empty()).count();
    assert!(used >= 2, "expected hash to use multiple partitions");
}
```

- [ ] **Step 2: Run test to verify it fails (helper doesn't exist yet)**

```bash
cargo test -p ballista-core --lib compute_partition_indices_distributes_rows_by_hash 2>&1 | tail -20
```

Expected: build fails with `error[E0425]: cannot find function 'compute_partition_indices' in this scope`.

- [ ] **Step 3: Implement the helper**

In `ballista/core/src/execution_plans/sort_shuffle/writer.rs`, add the imports near the existing `use` block (after the `BatchPartitioner` import — keep that for now; it goes in Task 5):

```rust
use datafusion::common::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
```

Add the helper function near the bottom of the file, just above `#[cfg(test)] mod tests`:

```rust
/// Computes per-row output partition assignments for hash partitioning.
///
/// Returns a `Vec` of length `num_partitions`, where entry `p` lists the
/// row indices in `batch` that hash to partition `p`. Hash semantics are
/// byte-identical to `datafusion::physical_plan::repartition::BatchPartitioner::Hash`.
///
/// `hash_buffer` is reused across calls to amortize allocations; it is
/// cleared and resized internally.
fn compute_partition_indices(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    hash_buffer: &mut Vec<u64>,
) -> Result<Vec<Vec<u32>>> {
    let arrays = evaluate_expressions_to_arrays(exprs, batch)?;
    hash_buffer.clear();
    hash_buffer.resize(batch.num_rows(), 0);
    create_hashes(&arrays, REPARTITION_RANDOM_STATE.random_state(), hash_buffer)?;

    let mut out: Vec<Vec<u32>> = (0..num_partitions).map(|_| Vec::new()).collect();
    for (row, &h) in hash_buffer.iter().enumerate() {
        out[(h % num_partitions as u64) as usize].push(row as u32);
    }
    Ok(out)
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p ballista-core --lib compute_partition_indices_distributes_rows_by_hash 2>&1 | tail -10
```

Expected: `test ... ok`.

- [ ] **Step 5: Verify clippy is clean for this file**

```bash
cargo clippy -p ballista-core --all-targets -- -D warnings 2>&1 | tail -5
```

Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add ballista/core/src/execution_plans/sort_shuffle/writer.rs
git commit -m "refactor: add compute_partition_indices helper for sort shuffle"
```

---

## Task 2: Add `PartitionedBatchIterator`

**Files:**

- Create: `ballista/core/src/execution_plans/sort_shuffle/partitioned_batch_iterator.rs`
- Modify: `ballista/core/src/execution_plans/sort_shuffle/mod.rs`

This task adds the iterator that materializes per-partition rows into `batch_size`-shaped output `RecordBatch`es by calling `arrow::compute::interleave_record_batch`. Self-contained, with unit tests. Not wired into the writer yet.

- [ ] **Step 1: Add module declaration**

In `ballista/core/src/execution_plans/sort_shuffle/mod.rs`, add `mod partitioned_batch_iterator;` to the existing `mod` declarations (near `mod buffer;`). Do NOT `pub use` anything yet — internal-only.

```rust
mod buffer;
mod config;
mod index;
mod partitioned_batch_iterator;
mod reader;
mod spill;
mod writer;
```

- [ ] **Step 2: Write the failing test (in a new file)**

Create `ballista/core/src/execution_plans/sort_shuffle/partitioned_batch_iterator.rs` with the Apache header (copy from `buffer.rs`) and start with the test module. Full file content for this step:

```rust
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

//! Iterator that materializes per-partition rows into well-sized
//! `RecordBatch`es using `arrow::compute::interleave_record_batch`.

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[test]
    fn yields_chunks_of_at_most_batch_size_rows() {
        use super::PartitionedBatchIterator;

        let batches = vec![batch(&[1, 2, 3, 4]), batch(&[5, 6, 7])];
        let indices: Vec<(u32, u32)> = vec![
            (0, 0), (0, 1), (0, 2), (0, 3),
            (1, 0), (1, 1), (1, 2),
        ];

        let mut iter = PartitionedBatchIterator::new(&batches, &indices, 4);

        let first = iter.next().unwrap().unwrap();
        assert_eq!(first.num_rows(), 4);
        let arr = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.values(), &[1, 2, 3, 4]);

        let second = iter.next().unwrap().unwrap();
        assert_eq!(second.num_rows(), 3);
        let arr = second
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.values(), &[5, 6, 7]);

        assert!(iter.next().is_none());
    }

    #[test]
    fn empty_indices_yields_nothing() {
        use super::PartitionedBatchIterator;

        let batches = vec![batch(&[1, 2, 3])];
        let indices: Vec<(u32, u32)> = vec![];
        let mut iter = PartitionedBatchIterator::new(&batches, &indices, 4);
        assert!(iter.next().is_none());
    }

    #[test]
    fn interleaves_rows_from_multiple_input_batches() {
        use super::PartitionedBatchIterator;

        let batches = vec![batch(&[10, 20, 30]), batch(&[40, 50, 60])];
        // Pull alternating rows from the two input batches
        let indices: Vec<(u32, u32)> =
            vec![(0, 0), (1, 0), (0, 1), (1, 1), (0, 2), (1, 2)];

        let mut iter = PartitionedBatchIterator::new(&batches, &indices, 8192);
        let only = iter.next().unwrap().unwrap();
        assert_eq!(only.num_rows(), 6);
        let arr = only
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(arr.values(), &[10, 40, 20, 50, 30, 60]);
        assert!(iter.next().is_none());
    }
}
```

- [ ] **Step 3: Run the test to verify it fails (struct doesn't exist)**

```bash
cargo test -p ballista-core --lib partitioned_batch_iterator 2>&1 | tail -20
```

Expected: build error `cannot find PartitionedBatchIterator in super`.

- [ ] **Step 4: Implement the iterator**

Above the `#[cfg(test)]` block in the same file, add:

```rust
use datafusion::arrow::compute::interleave_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::error::Result;

/// Iterator over per-partition output `RecordBatch`es.
///
/// Walks a slice of `(batch_idx, row_idx)` pairs in `indices`, and for each
/// chunk of up to `batch_size` pairs gathers the referenced rows from
/// `batches` via `interleave_record_batch`, yielding one `RecordBatch` per
/// chunk.
pub(crate) struct PartitionedBatchIterator<'a> {
    batches: &'a [RecordBatch],
    indices: &'a [(u32, u32)],
    batch_size: usize,
    pos: usize,
    /// `interleave_record_batch` takes `&[(usize, usize)]`, so we materialize
    /// a small reusable buffer per `next()` to avoid allocating on every call.
    scratch: Vec<(usize, usize)>,
}

impl<'a> PartitionedBatchIterator<'a> {
    pub(crate) fn new(
        batches: &'a [RecordBatch],
        indices: &'a [(u32, u32)],
        batch_size: usize,
    ) -> Self {
        Self {
            batches,
            indices,
            batch_size,
            pos: 0,
            scratch: Vec::with_capacity(batch_size.min(indices.len())),
        }
    }
}

impl<'a> Iterator for PartitionedBatchIterator<'a> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.indices.len() {
            return None;
        }
        let end = (self.pos + self.batch_size).min(self.indices.len());
        self.scratch.clear();
        self.scratch
            .extend(self.indices[self.pos..end].iter().map(|&(b, r)| (b as usize, r as usize)));
        self.pos = end;

        // `interleave_record_batch` expects a `Vec<&RecordBatch>` view.
        let batch_refs: Vec<&RecordBatch> = self.batches.iter().collect();
        match interleave_record_batch(&batch_refs, &self.scratch) {
            Ok(batch) => Some(Ok(batch)),
            Err(e) => Some(Err(DataFusionError::ArrowError(
                Box::new(e),
                Some(DataFusionError::get_back_trace()),
            ))),
        }
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cargo test -p ballista-core --lib partitioned_batch_iterator 2>&1 | tail -15
```

Expected: 3 tests pass.

- [ ] **Step 6: Verify clippy is clean**

```bash
cargo clippy -p ballista-core --all-targets -- -D warnings 2>&1 | tail -5
```

Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add ballista/core/src/execution_plans/sort_shuffle/mod.rs ballista/core/src/execution_plans/sort_shuffle/partitioned_batch_iterator.rs
git commit -m "feat: add PartitionedBatchIterator for sort shuffle"
```

---

## Task 3: Add `BufferedBatches`

**Files:**

- Modify: `ballista/core/src/execution_plans/sort_shuffle/buffer.rs`

Add `BufferedBatches` alongside the existing `PartitionBuffer` (which Task 5 will delete). New struct, new tests. Old struct and tests untouched.

- [ ] **Step 1: Write the failing test**

Append to the existing `#[cfg(test)] mod tests { ... }` block in `ballista/core/src/execution_plans/sort_shuffle/buffer.rs`:

```rust
#[test]
fn buffered_batches_pushes_and_partitions_indices() {
    use super::BufferedBatches;

    let schema = create_test_schema();
    let mut bb = BufferedBatches::new(3, schema.clone());
    assert!(bb.is_empty());
    assert_eq!(bb.num_partitions(), 3);

    let batch_a = create_test_batch(&schema, vec![10, 20, 30, 40]);
    let batch_b = create_test_batch(&schema, vec![50, 60]);

    // Partition 0 gets rows {0, 2} from batch 0; partition 1 gets {1, 3} from
    // batch 0; partition 2 gets {0, 1} from batch 1.
    let per_partition_a: Vec<Vec<u32>> = vec![vec![0, 2], vec![1, 3], vec![]];
    let per_partition_b: Vec<Vec<u32>> = vec![vec![], vec![], vec![0, 1]];

    bb.push_batch(batch_a, &per_partition_a);
    bb.push_batch(batch_b, &per_partition_b);

    assert!(!bb.is_empty());
    assert_eq!(bb.num_buffered_rows(), 4); // 2 + 2 + 2 - wait, total indices

    let p0 = bb.indices_for(0);
    assert_eq!(p0, &[(0u32, 0u32), (0, 2)]);
    let p1 = bb.indices_for(1);
    assert_eq!(p1, &[(0u32, 1u32), (0, 3)]);
    let p2 = bb.indices_for(2);
    assert_eq!(p2, &[(1u32, 0u32), (1, 1)]);
}

#[test]
fn buffered_batches_take_drains_state() {
    use super::BufferedBatches;

    let schema = create_test_schema();
    let mut bb = BufferedBatches::new(2, schema.clone());
    bb.push_batch(
        create_test_batch(&schema, vec![1, 2]),
        &[vec![0], vec![1]],
    );

    let (batches, indices) = bb.take();
    assert_eq!(batches.len(), 1);
    assert_eq!(indices.len(), 2);
    assert_eq!(indices[0], vec![(0, 0)]);
    assert_eq!(indices[1], vec![(0, 1)]);

    assert!(bb.is_empty());
    assert_eq!(bb.num_buffered_rows(), 0);
}
```

The first test has `assert_eq!(bb.num_buffered_rows(), 4)` deliberately — the count is the total rows referenced via indices (2 + 2 + 2 = 6). Fix the assertion to `6` before running:

```rust
    assert_eq!(bb.num_buffered_rows(), 6);
```

- [ ] **Step 2: Run the tests to verify they fail (struct doesn't exist)**

```bash
cargo test -p ballista-core --lib buffered_batches 2>&1 | tail -20
```

Expected: build error `cannot find BufferedBatches in super`.

- [ ] **Step 3: Implement `BufferedBatches`**

Add the new struct in `buffer.rs`, **above** the `#[cfg(test)]` block and **below** the existing `PartitionBuffer` impl. Add this code:

```rust
/// Holds whole input `RecordBatch`es and per-partition `(batch_idx, row_idx)`
/// index lists. Rows are not copied at insertion time — only the indices are
/// recorded. Materialization happens through `PartitionedBatchIterator` at
/// spill or final-write time, by way of `interleave_record_batch`.
#[derive(Debug)]
pub struct BufferedBatches {
    schema: SchemaRef,
    /// All input batches, in arrival order. Indexed by `batch_idx` in
    /// `indices`. Never sliced.
    batches: Vec<RecordBatch>,
    /// One entry per output partition. Each `(u32, u32)` is `(batch_idx,
    /// row_idx)`, referring to a row inside `batches[batch_idx]`.
    indices: Vec<Vec<(u32, u32)>>,
    /// Total rows currently referenced by `indices`. Drives metrics.
    num_buffered_rows: usize,
}

impl BufferedBatches {
    /// Creates a new buffer for the given partition count and schema.
    pub fn new(num_partitions: usize, schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            indices: (0..num_partitions).map(|_| Vec::new()).collect(),
            num_buffered_rows: 0,
        }
    }

    /// Returns the schema this buffer is bound to.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the configured number of output partitions.
    pub fn num_partitions(&self) -> usize {
        self.indices.len()
    }

    /// Returns true if no batches have been pushed yet.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Returns the total number of rows currently referenced by indices.
    pub fn num_buffered_rows(&self) -> usize {
        self.num_buffered_rows
    }

    /// Returns the buffered batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Returns the row indices for output partition `partition_id`.
    pub fn indices_for(&self, partition_id: usize) -> &[(u32, u32)] {
        &self.indices[partition_id]
    }

    /// Pushes a whole input `batch` and records, for each output partition
    /// `p`, the row indices from `per_partition_rows[p]` as `(batch_idx, r)`
    /// pairs in that partition's index list.
    ///
    /// `per_partition_rows.len()` must equal `num_partitions()`.
    pub fn push_batch(
        &mut self,
        batch: RecordBatch,
        per_partition_rows: &[Vec<u32>],
    ) {
        debug_assert_eq!(per_partition_rows.len(), self.indices.len());
        let batch_idx = self.batches.len() as u32;
        for (p, rows) in per_partition_rows.iter().enumerate() {
            let dst = &mut self.indices[p];
            dst.reserve(rows.len());
            for &r in rows {
                dst.push((batch_idx, r));
                self.num_buffered_rows += 1;
            }
        }
        self.batches.push(batch);
    }

    /// Drains all state, returning the buffered batches and per-partition
    /// index lists. After this call the buffer is empty.
    pub fn take(&mut self) -> (Vec<RecordBatch>, Vec<Vec<(u32, u32)>>) {
        self.num_buffered_rows = 0;
        let batches = std::mem::take(&mut self.batches);
        // Preserve the partition count by replacing each inner vec with empty
        let indices: Vec<Vec<(u32, u32)>> = self
            .indices
            .iter_mut()
            .map(std::mem::take)
            .collect();
        (batches, indices)
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p ballista-core --lib buffered_batches 2>&1 | tail -10
```

Expected: 2 new tests pass; old `PartitionBuffer` tests still pass.

- [ ] **Step 5: Verify clippy is clean**

```bash
cargo clippy -p ballista-core --all-targets -- -D warnings 2>&1 | tail -5
```

Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add ballista/core/src/execution_plans/sort_shuffle/buffer.rs
git commit -m "feat: add BufferedBatches type for deferred sort shuffle materialization"
```

---

## Task 4: Change `SpillManager::spill` to per-batch signature

**Files:**

- Modify: `ballista/core/src/execution_plans/sort_shuffle/spill.rs`
- Modify: `ballista/core/src/execution_plans/sort_shuffle/writer.rs` (callsite in `spill_largest_buffers`)

Move the schema parameter from `spill()` to `new()` and accept a single `&RecordBatch` per call. This is a prep step so Task 5 can call `spill` once per yielded batch from `PartitionedBatchIterator`.

- [ ] **Step 1: Update `SpillManager::new` to take schema**

In `ballista/core/src/execution_plans/sort_shuffle/spill.rs`, change `SpillManager` struct and `new`:

```rust
pub struct SpillManager {
    /// Base directory for spill files
    spill_dir: PathBuf,
    /// Schema shared by all spill writers
    schema: SchemaRef,
    /// Spill file path per output partition: partition_id -> spill_file_path
    spill_files: HashMap<usize, PathBuf>,
    /// Active writers per partition, kept open for appending
    active_writers: HashMap<usize, StreamWriter<BufWriter<File>>>,
    /// Compression codec for spill files
    compression: CompressionType,
    /// Total number of spills performed
    total_spills: usize,
    /// Total bytes spilled
    total_bytes_spilled: u64,
}

impl SpillManager {
    /// Creates a new spill manager.
    pub fn new(
        work_dir: &str,
        job_id: &str,
        stage_id: usize,
        input_partition: usize,
        schema: SchemaRef,
        compression: CompressionType,
    ) -> Result<Self> {
        let mut spill_dir = PathBuf::from(work_dir);
        spill_dir.push(job_id);
        spill_dir.push(format!("{stage_id}"));
        spill_dir.push(format!("{input_partition}"));
        spill_dir.push("spill");

        std::fs::create_dir_all(&spill_dir).map_err(BallistaError::IoError)?;

        Ok(Self {
            spill_dir,
            schema,
            spill_files: HashMap::new(),
            active_writers: HashMap::new(),
            compression,
            total_spills: 0,
            total_bytes_spilled: 0,
        })
    }
```

- [ ] **Step 2: Update `SpillManager::spill` to per-batch signature**

Replace the existing `spill` method body:

```rust
    /// Spills a single `batch` for `partition_id` to disk. The first call for
    /// a given `partition_id` creates the spill file; subsequent calls append.
    ///
    /// Returns the number of bytes written (estimated from the batch's array
    /// memory size).
    pub fn spill(
        &mut self,
        partition_id: usize,
        batch: &RecordBatch,
    ) -> Result<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        if !self.active_writers.contains_key(&partition_id) {
            let spill_path = self.spill_path(partition_id);
            debug!(
                "Creating spill file for partition {} at {:?}",
                partition_id, spill_path
            );
            let file = File::create(&spill_path).map_err(BallistaError::IoError)?;
            let buffered = BufWriter::new(file);

            let options = IpcWriteOptions::default()
                .try_with_compression(Some(self.compression))?;

            let writer =
                StreamWriter::try_new_with_options(buffered, &self.schema, options)?;

            self.active_writers.insert(partition_id, writer);
            self.spill_files.insert(partition_id, spill_path);
        }

        let writer = self.active_writers.get_mut(&partition_id).unwrap();
        let bytes_written = batch.get_array_memory_size() as u64;
        writer.write(batch)?;

        self.total_spills += 1;
        self.total_bytes_spilled += bytes_written;

        Ok(bytes_written)
    }
```

Note that this changes `total_spills` semantics from "spill events" to "batches spilled." Update the doc comment on `total_spills()` accessor accordingly:

```rust
    /// Returns the total number of batches spilled across all partitions.
    pub fn total_spills(&self) -> usize {
        self.total_spills
    }
```

- [ ] **Step 3: Update unit tests in `spill.rs`**

Replace the bodies of `test_spill_and_read`, `test_multiple_spills`, and `test_cleanup` to match the new API. Full replacement for the `tests` module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn create_test_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_spill_and_read() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            schema.clone(),
            CompressionType::LZ4_FRAME,
        )?;

        let b1 = create_test_batch(&schema, vec![1, 2, 3]);
        let b2 = create_test_batch(&schema, vec![4, 5]);
        assert!(manager.spill(0, &b1)? > 0);
        assert!(manager.spill(0, &b2)? > 0);

        assert!(manager.has_spill_files(0));
        assert!(!manager.has_spill_files(1));
        assert_eq!(manager.total_spills(), 2);

        manager.finish_writers()?;

        let reader = manager.open_spill_reader(0)?.unwrap();
        let read_batches: Vec<_> =
            reader.into_iter().collect::<std::result::Result<_, _>>()?;
        assert_eq!(read_batches.len(), 2);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(read_batches[1].num_rows(), 2);

        Ok(())
    }

    #[test]
    fn test_multiple_partitions() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            schema.clone(),
            CompressionType::LZ4_FRAME,
        )?;

        manager.spill(0, &create_test_batch(&schema, vec![1, 2]))?;
        manager.spill(1, &create_test_batch(&schema, vec![3, 4]))?;

        assert!(manager.has_spill_files(0));
        assert!(manager.has_spill_files(1));
        assert_eq!(manager.total_spills(), 2);

        manager.finish_writers()?;

        let r0 = manager
            .open_spill_reader(0)?
            .unwrap()
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let r1 = manager
            .open_spill_reader(1)?
            .unwrap()
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(r0.len(), 1);
        assert_eq!(r1.len(), 1);

        Ok(())
    }

    #[test]
    fn test_cleanup() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut manager = SpillManager::new(
            temp_dir.path().to_str().unwrap(),
            "job1",
            1,
            0,
            schema.clone(),
            CompressionType::LZ4_FRAME,
        )?;

        manager.spill(0, &create_test_batch(&schema, vec![1, 2]))?;

        let spill_dir = manager.spill_dir.clone();
        assert!(spill_dir.exists());

        manager.cleanup()?;
        assert!(!spill_dir.exists());

        Ok(())
    }
}
```

- [ ] **Step 4: Update `spill_largest_buffers` callsite in writer.rs**

In `ballista/core/src/execution_plans/sort_shuffle/writer.rs`, find the `spill_largest_buffers` function (currently at the bottom of the impl region, around line 332). Update the inner spill call:

Replace:

```rust
            Some(idx) if buffers[idx].memory_used() > 0 => {
                let partition_id = buffers[idx].partition_id();
                let batches = buffers[idx].drain_coalesced(batch_size);
                spill_manager
                    .spill(partition_id, batches, schema)
                    .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
            }
```

with:

```rust
            Some(idx) if buffers[idx].memory_used() > 0 => {
                let partition_id = buffers[idx].partition_id();
                let batches = buffers[idx].drain_coalesced(batch_size);
                for batch in &batches {
                    spill_manager
                        .spill(partition_id, batch)
                        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
                }
            }
```

The `schema: &SchemaRef` parameter on `spill_largest_buffers` is no longer used by the spill call. Inspect the function signature — if `schema` is now unused there, remove it from the signature and drop the corresponding argument in `execute_shuffle_write`. (Hint: it's used by nothing else inside the function.)

- [ ] **Step 5: Update `SpillManager::new` callsite in writer.rs**

In `execute_shuffle_write` (around line 221), find:

```rust
            let mut spill_manager = SpillManager::new(
                &work_dir,
                &job_id,
                stage_id,
                input_partition,
                config.compression,
            )
```

Update to pass schema:

```rust
            let mut spill_manager = SpillManager::new(
                &work_dir,
                &job_id,
                stage_id,
                input_partition,
                schema.clone(),
                config.compression,
            )
```

- [ ] **Step 6: Run all sort_shuffle tests to verify still passing**

```bash
cargo test -p ballista-core --lib sort_shuffle 2>&1 | tail -25
```

Expected: all existing tests (including `test_sort_shuffle_writer`) pass.

- [ ] **Step 7: Verify clippy is clean**

```bash
cargo clippy -p ballista-core --all-targets -- -D warnings 2>&1 | tail -5
```

- [ ] **Step 8: Commit**

```bash
git add ballista/core/src/execution_plans/sort_shuffle/spill.rs ballista/core/src/execution_plans/sort_shuffle/writer.rs
git commit -m "refactor: change SpillManager::spill to per-batch signature"
```

---

## Task 5: Rewrite `execute_shuffle_write` with `BufferedBatches`, memory pool, and all-or-nothing spill

**Files:**

- Modify: `ballista/core/src/execution_plans/sort_shuffle/writer.rs`
- Modify: `ballista/core/src/execution_plans/sort_shuffle/buffer.rs` (delete `PartitionBuffer`)
- Modify: `ballista/core/src/execution_plans/sort_shuffle/mod.rs` (drop `pub use buffer::PartitionBuffer`)

This is the core change. After this task:

- `BatchPartitioner` is no longer used.
- `PartitionBuffer` is gone.
- Spill is all-or-nothing, driven by `MemoryReservation::try_grow`.
- `finalize_output` materializes via `PartitionedBatchIterator`.

The existing `test_sort_shuffle_writer` still must pass at the end.

- [ ] **Step 1: Add `spill_all_partitions` helper in writer.rs**

In `ballista/core/src/execution_plans/sort_shuffle/writer.rs`, add this private free function (place it directly above the existing `spill_largest_buffers` function — you'll delete `spill_largest_buffers` in Step 4):

```rust
/// Spills *all* buffered partitions: for each partition, materializes its
/// indices through `PartitionedBatchIterator` and appends each yielded batch
/// to that partition's spill file. After this call, `buffered.is_empty()` is
/// true and `reservation.size() == 0`.
fn spill_all_partitions(
    buffered: &mut BufferedBatches,
    spill_manager: &mut SpillManager,
    reservation: &mut MemoryReservation,
    batch_size: usize,
) -> Result<()> {
    if buffered.is_empty() {
        return Ok(());
    }
    let (batches, indices) = buffered.take();
    for (partition_id, partition_indices) in indices.iter().enumerate() {
        if partition_indices.is_empty() {
            continue;
        }
        let iter = PartitionedBatchIterator::new(&batches, partition_indices, batch_size);
        for result in iter {
            let batch = result?;
            spill_manager
                .spill(partition_id, &batch)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
        }
    }
    reservation.free();
    Ok(())
}
```

Add the imports near the top of the file (next to existing `use super::buffer::PartitionBuffer`):

```rust
use super::buffer::BufferedBatches;
use super::partitioned_batch_iterator::PartitionedBatchIterator;
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
```

- [ ] **Step 2: Rewrite `execute_shuffle_write`**

Replace the entire body of `execute_shuffle_write` from `async move { ... }` onward. Find the existing async block (starting around line 204) and replace its contents with the following. Keep the function signature and the variable bindings before `async move` unchanged.

```rust
        async move {
            let now = Instant::now();
            let mut stream = plan.execute(input_partition, context.clone())?;
            let schema = stream.schema();

            let Partitioning::Hash(exprs, num_output_partitions) = partitioning else {
                return Err(DataFusionError::Internal(
                    "Expected hash partitioning".to_string(),
                ));
            };

            let mut spill_manager = SpillManager::new(
                &work_dir,
                &job_id,
                stage_id,
                input_partition,
                schema.clone(),
                config.compression,
            )
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            let mut buffered =
                BufferedBatches::new(num_output_partitions, schema.clone());

            let mut reservation = MemoryConsumer::new(format!(
                "SortShuffleWriter[{input_partition}]"
            ))
            .with_can_spill(true)
            .register(&context.runtime_env().memory_pool);

            let mut hash_buffer: Vec<u64> = Vec::new();

            while let Some(result) = stream.next().await {
                let input_batch = result?;
                metrics.input_rows.add(input_batch.num_rows());

                // Compute partition assignment for every row.
                let timer = metrics.repart_time.timer();
                let per_partition_rows = compute_partition_indices(
                    &input_batch,
                    &exprs,
                    num_output_partitions,
                    &mut hash_buffer,
                )?;
                timer.done();

                // Estimate memory growth: input batch + index Vec growth.
                let mut growth = input_batch.get_array_memory_size();
                // Pre-measure index allocation growth.
                // We snapshot allocated_size before extending, then after.
                // We extend by pushing in `push_batch` below; do the
                // allocation-size accounting around it.
                let before: usize = (0..num_output_partitions)
                    .map(|p| buffered.indices_for(p).allocated_size())
                    .sum();
                buffered.push_batch(input_batch, &per_partition_rows);
                let after: usize = (0..num_output_partitions)
                    .map(|p| buffered.indices_for(p).allocated_size())
                    .sum();
                growth += after.saturating_sub(before);

                if reservation.try_grow(growth).is_err() {
                    let spill_timer = metrics.spill_time.timer();
                    spill_all_partitions(
                        &mut buffered,
                        &mut spill_manager,
                        &mut reservation,
                        config.batch_size,
                    )?;
                    spill_timer.done();
                }
            }

            // Finish spill writers before reading them back during finalize.
            spill_manager
                .finish_writers()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            let timer = metrics.write_time.timer();
            let (data_path, index_path, partition_stats) = finalize_output(
                &work_dir,
                &job_id,
                stage_id,
                input_partition,
                &mut buffered,
                &mut spill_manager,
                &schema,
                &config,
            )?;
            timer.done();

            metrics.spill_count.add(spill_manager.total_spills());
            metrics
                .spill_bytes
                .add(spill_manager.total_bytes_spilled() as usize);

            let total_rows: u64 = partition_stats.iter().map(|(_, _, r, _)| *r).sum();
            metrics.output_rows.add(total_rows as usize);

            spill_manager
                .cleanup()
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            // Reservation drops naturally; nothing left to free.
            drop(reservation);

            info!(
                "Sort shuffle write for partition {} completed in {} seconds. \
                 Output: {:?}, Index: {:?}, Spill batches: {}, Spill bytes: {}",
                input_partition,
                now.elapsed().as_secs(),
                data_path,
                index_path,
                spill_manager.total_spills(),
                spill_manager.total_bytes_spilled()
            );

            let mut results = Vec::new();
            for (part_id, num_batches, num_rows, num_bytes) in partition_stats {
                if num_rows > 0 {
                    results.push(ShuffleWritePartition {
                        partition_id: part_id as u64,
                        num_batches,
                        num_rows,
                        num_bytes,
                        file_id: Some(input_partition as u64),
                        is_sort_shuffle: true,
                    });
                }
            }

            Ok(results)
        }
```

Note the second `info!` log references `spill_manager` after `cleanup()` — that's intentional and fine; `total_spills()` and `total_bytes_spilled()` read counters and don't touch the filesystem.

- [ ] **Step 3: Rewrite `finalize_output`**

Replace the entire `finalize_output` function. Its signature changes from `&mut [PartitionBuffer]` to `&mut BufferedBatches`. Replacement body:

```rust
/// Finalizes the output by writing the consolidated data file and index file.
///
/// Returns (data_path, index_path, partition_stats) where partition_stats is
/// a vector of (partition_id, num_batches, num_rows, num_bytes) tuples.
#[allow(clippy::too_many_arguments)]
fn finalize_output(
    work_dir: &str,
    job_id: &str,
    stage_id: usize,
    input_partition: usize,
    buffered: &mut BufferedBatches,
    spill_manager: &mut SpillManager,
    schema: &SchemaRef,
    config: &SortShuffleConfig,
) -> Result<FinalizeResult> {
    let num_partitions = buffered.num_partitions();
    let mut index = ShuffleIndex::new(num_partitions);
    let mut partition_stats = Vec::with_capacity(num_partitions);

    let mut output_dir = PathBuf::from(work_dir);
    output_dir.push(job_id);
    output_dir.push(format!("{stage_id}"));
    output_dir.push(format!("{input_partition}"));
    std::fs::create_dir_all(&output_dir)?;

    let data_path = output_dir.join("data.arrow");
    let index_path = output_dir.join("data.arrow.index");

    debug!("Writing consolidated shuffle output to {:?}", data_path);

    let file = File::create(&data_path)?;
    let mut buffered_writer = BufWriter::new(file);

    let options =
        IpcWriteOptions::default().try_with_compression(Some(config.compression))?;
    let mut writer =
        FileWriter::try_new_with_options(&mut buffered_writer, schema, options)?;

    let mut cumulative_batch_count: i64 = 0;

    // Take ownership of the in-memory state once. After this we can iterate
    // each partition's indices independently without borrow-checker grief.
    let (in_memory_batches, in_memory_indices) = buffered.take();

    for partition_id in 0..num_partitions {
        index.set_offset(partition_id, cumulative_batch_count);

        let mut partition_rows: u64 = 0;
        let mut partition_batches: u64 = 0;
        let mut partition_bytes: u64 = 0;

        // First, stream any spill file for this partition.
        if let Some(reader) = spill_manager
            .open_spill_reader(partition_id)
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
        {
            for batch_result in reader {
                let batch = batch_result?;
                partition_rows += batch.num_rows() as u64;
                partition_bytes += batch.get_array_memory_size() as u64;
                partition_batches += 1;
                writer.write(&batch)?;
            }
        }

        // Then write the in-memory remainder via interleave_record_batch.
        let partition_indices = &in_memory_indices[partition_id];
        if !partition_indices.is_empty() {
            let iter = PartitionedBatchIterator::new(
                &in_memory_batches,
                partition_indices,
                config.batch_size,
            );
            for result in iter {
                let batch = result?;
                partition_rows += batch.num_rows() as u64;
                partition_bytes += batch.get_array_memory_size() as u64;
                partition_batches += 1;
                writer.write(&batch)?;
            }
        }

        partition_stats.push((
            partition_id,
            partition_batches,
            partition_rows,
            partition_bytes,
        ));

        cumulative_batch_count += partition_batches as i64;
    }

    writer.finish()?;
    index.set_total_length(cumulative_batch_count);
    index
        .write_to_file(&index_path)
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    Ok((data_path, index_path, partition_stats))
}
```

- [ ] **Step 4: Delete `spill_largest_buffers`**

Delete the `spill_largest_buffers` function from `writer.rs` in its entirety.

- [ ] **Step 5: Delete `PartitionBuffer` and supporting code**

In `ballista/core/src/execution_plans/sort_shuffle/buffer.rs`, delete:

- The `PartitionBuffer` struct and its impl block.
- The `coalesce_batches` private helper.
- The `LimitedBatchCoalescer` import (and any other now-unused imports — clippy will tell you).
- The legacy `test_new_buffer`, `test_append`, `test_drain` tests (the new `BufferedBatches` tests stay).

- [ ] **Step 6: Update `mod.rs` re-exports**

In `ballista/core/src/execution_plans/sort_shuffle/mod.rs`, replace:

```rust
pub use buffer::PartitionBuffer;
```

with:

```rust
pub use buffer::BufferedBatches;
```

- [ ] **Step 7: Drop the `BatchPartitioner` import in writer.rs**

Remove the line:

```rust
use datafusion::physical_plan::repartition::BatchPartitioner;
```

(Already imported `REPARTITION_RANDOM_STATE` in Task 1; that stays.)

- [ ] **Step 8: Run sort_shuffle tests**

```bash
cargo test -p ballista-core --lib sort_shuffle 2>&1 | tail -25
```

Expected: all sort_shuffle tests pass. The existing `test_sort_shuffle_writer` should pass with no changes — it calls the writer end-to-end with default config and verifies the output files exist.

- [ ] **Step 9: Run all ballista-core tests to catch knock-on breakage**

```bash
cargo test -p ballista-core --lib 2>&1 | tail -10
```

Expected: all tests pass.

- [ ] **Step 10: Verify clippy is clean**

```bash
cargo clippy -p ballista-core --all-targets -- -D warnings 2>&1 | tail -10
```

- [ ] **Step 11: Commit**

```bash
git add ballista/core/src/execution_plans/sort_shuffle/
git commit -m "feat: defer sort-shuffle materialization with interleave_record_batch

Replace the eager take_arrays-per-partition buffering with whole-batch
storage plus per-partition (batch_idx, row_idx) index lists. Output
batches are produced via interleave_record_batch only at spill or final
write time. Spill is now all-or-nothing, driven by MemoryReservation
against the runtime memory pool.

Closes #1432."
```

---

## Task 6: Drop obsolete fields from `SortShuffleConfig`

**Files:**

- Modify: `ballista/core/src/execution_plans/sort_shuffle/config.rs`
- Modify: `ballista/core/src/serde/mod.rs`
- Modify: `ballista/scheduler/src/planner.rs`
- Modify: `benchmarks/benches/sort_shuffle.rs`
- Modify: `ballista/core/src/execution_plans/sort_shuffle/writer.rs` (test only)

The fields `buffer_size`, `memory_limit`, `spill_threshold` and the method `spill_memory_threshold()` are unused after Task 5. Delete them, then update every call site.

- [ ] **Step 1: Slim `SortShuffleConfig`**

Replace the contents of `ballista/core/src/execution_plans/sort_shuffle/config.rs` (preserve the Apache header):

```rust
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

//! Configuration for sort-based shuffle.

use datafusion::arrow::ipc::CompressionType;

/// Configuration for sort-based shuffle.
#[derive(Debug, Clone)]
pub struct SortShuffleConfig {
    /// Whether sort-based shuffle is enabled (default: false).
    pub enabled: bool,
    /// Compression codec for shuffle data (default: LZ4_FRAME).
    pub compression: CompressionType,
    /// Target batch size in rows when materializing buffered indices via
    /// `interleave_record_batch` (default: 8192).
    pub batch_size: usize,
}

impl Default for SortShuffleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            compression: CompressionType::LZ4_FRAME,
            batch_size: 8192,
        }
    }
}

impl SortShuffleConfig {
    /// Creates a new configuration.
    pub fn new(enabled: bool, compression: CompressionType, batch_size: usize) -> Self {
        Self {
            enabled,
            compression,
            batch_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SortShuffleConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.batch_size, 8192);
    }

    #[test]
    fn test_new() {
        let config = SortShuffleConfig::new(true, CompressionType::LZ4_FRAME, 4096);
        assert!(config.enabled);
        assert_eq!(config.batch_size, 4096);
    }
}
```

- [ ] **Step 2: Update the protobuf decode site**

In `ballista/core/src/serde/mod.rs`, find the `SortShuffleConfig::new(...)` call (around line 384). Replace:

```rust
                let batch_size = if sort_shuffle_writer.batch_size > 0 {
                    sort_shuffle_writer.batch_size as usize
                } else {
                    8192 // default for backwards compatibility
                };
                let config = SortShuffleConfig::new(
                    true,
                    sort_shuffle_writer.buffer_size as usize,
                    sort_shuffle_writer.memory_limit as usize,
                    sort_shuffle_writer.spill_threshold,
                    datafusion::arrow::ipc::CompressionType::LZ4_FRAME,
                    batch_size,
                );
```

with:

```rust
                let batch_size = if sort_shuffle_writer.batch_size > 0 {
                    sort_shuffle_writer.batch_size as usize
                } else {
                    8192
                };
                let config = SortShuffleConfig::new(
                    true,
                    datafusion::arrow::ipc::CompressionType::LZ4_FRAME,
                    batch_size,
                );
```

- [ ] **Step 3: Update the protobuf encode site**

In the same file (`ballista/core/src/serde/mod.rs`), around line 535. Replace:

```rust
                    protobuf::SortShuffleWriterExecNode {
                        job_id: exec.job_id().to_string(),
                        stage_id: exec.stage_id() as u32,
                        input: None,
                        output_partitioning,
                        buffer_size: config.buffer_size as u64,
                        memory_limit: config.memory_limit as u64,
                        spill_threshold: config.spill_threshold,
                        batch_size: config.batch_size as u64,
                    },
```

with:

```rust
                    protobuf::SortShuffleWriterExecNode {
                        job_id: exec.job_id().to_string(),
                        stage_id: exec.stage_id() as u32,
                        input: None,
                        output_partitioning,
                        batch_size: config.batch_size as u64,
                    },
```

The `buffer_size`/`memory_limit`/`spill_threshold` fields will be removed from the proto in Task 7; this serde change must land **with** the proto change. Keep both edits in this commit by combining Step 2/3 with Task 7's proto edit. **Defer this commit** — see Step 8.

- [ ] **Step 4: Update the scheduler planner callsite**

In `ballista/scheduler/src/planner.rs`, around line 344. Replace:

```rust
            let sort_config = SortShuffleConfig::new(
                true,
                ballista_config.shuffle_sort_based_buffer_size(),
                ballista_config.shuffle_sort_based_memory_limit(),
                ballista_config.shuffle_sort_based_spill_threshold(),
                datafusion::arrow::ipc::CompressionType::LZ4_FRAME,
                ballista_config.shuffle_sort_based_batch_size(),
            );
```

with:

```rust
            let sort_config = SortShuffleConfig::new(
                true,
                datafusion::arrow::ipc::CompressionType::LZ4_FRAME,
                ballista_config.shuffle_sort_based_batch_size(),
            );
```

The `shuffle_sort_based_buffer_size()` etc. accessors are still defined in `ballista/core/src/config.rs` for now; Task 8 removes them. This callsite change is safe because we stop calling the accessors here.

- [ ] **Step 5: Update the criterion bench callsite**

In `benchmarks/benches/sort_shuffle.rs`, around line 223 (search for `SortShuffleConfig::new`). Replace the multi-arg call with the new 3-arg form:

```rust
    let config = SortShuffleConfig::new(
        true,
        CompressionType::LZ4_FRAME,
        8192,
    );
```

(If the bench passes `8192` already as the batch size, keep that value. If it currently uses a different batch size, preserve whatever value the existing call passes.) Inspect the existing call in context and pick the right value.

- [ ] **Step 6: Verify build**

```bash
cargo build --workspace --all-targets 2>&1 | tail -10
```

Expected: build succeeds. The existing `benchmarks/src/bin/shuffle_bench.rs` will be replaced in Task 11; in the meantime its `SortShuffleConfig::new` call will also need to be updated. Patch that file too — search for `SortShuffleConfig::new` in `benchmarks/src/bin/shuffle_bench.rs` and update the call to match the new 3-arg form using the values that were previously passed for `compression` and `batch_size`. The other config params it used to pass become inert; the bench's `--memory-limit` flag still applies via `RuntimeEnvBuilder` in Task 11.

- [ ] **Step 7: Run sort_shuffle tests**

```bash
cargo test -p ballista-core --lib sort_shuffle 2>&1 | tail -10
```

Expected: tests pass.

- [ ] **Step 8: Verify clippy is clean**

```bash
cargo clippy -p ballista-core -p ballista-scheduler -p ballista-benchmarks --all-targets -- -D warnings 2>&1 | tail -10
```

- [ ] **Step 9: Defer commit**

Do **not** commit yet. Combine this commit with Task 7's proto edit, since the encode-side change (Step 3) only compiles after the proto fields are removed.

---

## Task 7: Drop obsolete proto fields and regenerate

**Files:**

- Modify: `ballista/core/proto/ballista.proto`
- Verify regeneration: `ballista/core/src/serde/generated/ballista.rs`

The proto fields `buffer_size`, `memory_limit`, `spill_threshold` on `SortShuffleWriterExecNode` are now unused. Drop them. The build script regenerates the Rust types via `tonic-prost-build`.

- [ ] **Step 1: Edit the proto**

In `ballista/core/proto/ballista.proto`, find `message SortShuffleWriterExecNode`:

```proto
message SortShuffleWriterExecNode {
  string job_id = 1;
  uint32 stage_id = 2;
  datafusion.PhysicalPlanNode input = 3;
  datafusion.PhysicalHashRepartition output_partitioning = 4;
  // Configuration for sort shuffle
  uint64 buffer_size = 5;
  uint64 memory_limit = 6;
  double spill_threshold = 7;
  uint64 batch_size = 8;
}
```

Replace with:

```proto
message SortShuffleWriterExecNode {
  string job_id = 1;
  uint32 stage_id = 2;
  datafusion.PhysicalPlanNode input = 3;
  datafusion.PhysicalHashRepartition output_partitioning = 4;
  // Target batch size in rows when materializing buffered shuffle data.
  uint64 batch_size = 8;
}
```

Note that we keep field number `8` for `batch_size` — protobuf field numbers are part of the wire format. Reusing them after removal is allowed but discouraged; in this case, since the codec is consumed only between matched-version executors and schedulers, gap-leaving on the wire is fine. (Ballista does not promise wire compatibility across non-matching versions.)

- [ ] **Step 2: Trigger codegen by building**

```bash
cargo build -p ballista-core 2>&1 | tail -10
```

`tonic-prost-build` regenerates `ballista/core/src/serde/generated/ballista.rs`. The build will succeed because Task 6 Step 3 already updated the encode site to no longer reference the removed fields.

- [ ] **Step 3: Verify generated file no longer references removed fields**

```bash
grep -nE "buffer_size|memory_limit|spill_threshold" /Users/andy/git/apache/datafusion-ballista/ballista/core/src/serde/generated/ballista.rs | head -10
```

Expected: no output (or only matches inside other unrelated messages).

- [ ] **Step 4: Run the full ballista-core test suite**

```bash
cargo test -p ballista-core 2>&1 | tail -15
```

Expected: all tests pass.

- [ ] **Step 5: Verify clippy is clean**

```bash
cargo clippy --all-targets --workspace -- -D warnings 2>&1 | tail -10
```

- [ ] **Step 6: Commit (this combines Task 6 + Task 7)**

```bash
git add ballista/core/src/execution_plans/sort_shuffle/config.rs \
        ballista/core/src/serde/mod.rs \
        ballista/scheduler/src/planner.rs \
        benchmarks/benches/sort_shuffle.rs \
        benchmarks/src/bin/shuffle_bench.rs \
        ballista/core/proto/ballista.proto \
        ballista/core/src/serde/generated/ballista.rs
git commit -m "refactor: drop unused buffer_size/memory_limit/spill_threshold from sort shuffle

The runtime MemoryReservation pool now governs spilling, replacing the
fixed-byte per-task memory budget. Drop the corresponding fields from
SortShuffleConfig, the SortShuffleWriterExecNode protobuf message, and
all callsites."
```

---

## Task 8: Drop obsolete `BALLISTA_SHUFFLE_SORT_BASED_*` config keys

**Files:**

- Modify: `ballista/core/src/config.rs`

Three config keys and their accessors are no longer referenced after Task 6: `BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE`, `BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT`, `BALLISTA_SHUFFLE_SORT_BASED_SPILL_THRESHOLD`. Drop them.

- [ ] **Step 1: Delete the const declarations**

In `ballista/core/src/config.rs`, delete these blocks (around lines 75-83):

```rust
/// Configuration key for sort shuffle per-partition buffer size in bytes.
pub const BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE: &str =
    "ballista.shuffle.sort_based.buffer_size";
/// Configuration key for sort shuffle total memory limit in bytes.
pub const BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT: &str =
    "ballista.shuffle.sort_based.memory_limit";
/// Configuration key for sort shuffle spill threshold (0.0-1.0).
pub const BALLISTA_SHUFFLE_SORT_BASED_SPILL_THRESHOLD: &str =
    "ballista.shuffle.sort_based.spill_threshold";
```

- [ ] **Step 2: Delete the `ConfigEntry` registrations**

In the same file, in the `LazyLock` block (around lines 148-159), delete these three entries:

```rust
        ConfigEntry::new(BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE.to_string(),
                         "Per-partition buffer size in bytes for sort shuffle".to_string(),
                         DataType::UInt64,
                         Some((1024 * 1024).to_string())),
        ConfigEntry::new(BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT.to_string(),
                         "Total memory limit in bytes for sort shuffle buffers".to_string(),
                         DataType::UInt64,
                         Some((256 * 1024 * 1024).to_string())),
        ConfigEntry::new(BALLISTA_SHUFFLE_SORT_BASED_SPILL_THRESHOLD.to_string(),
                         "Spill threshold as decimal fraction (0.0-1.0) of memory limit".to_string(),
                         DataType::Utf8,
                         Some("0.8".to_string())),
```

(Keep `BALLISTA_SHUFFLE_SORT_BASED_ENABLED` and `BALLISTA_SHUFFLE_SORT_BASED_BATCH_SIZE`.)

- [ ] **Step 3: Delete the accessor methods**

In the same file (around lines 365-379), delete:

```rust
    /// Returns the per-partition buffer size for sort-based shuffle in bytes.
    pub fn shuffle_sort_based_buffer_size(&self) -> usize {
        self.get_usize_setting(BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE)
    }

    /// Returns the total memory limit for sort-based shuffle buffers in bytes.
    pub fn shuffle_sort_based_memory_limit(&self) -> usize {
        self.get_usize_setting(BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT)
    }

    /// Returns the spill threshold for sort-based shuffle (0.0-1.0).
    pub fn shuffle_sort_based_spill_threshold(&self) -> f64 {
        self.get_f64_setting(BALLISTA_SHUFFLE_SORT_BASED_SPILL_THRESHOLD)
    }
```

(Keep `shuffle_sort_based_enabled()` and `shuffle_sort_based_batch_size()`.)

If `get_f64_setting` becomes unused after this deletion, leave it in place — other future callers may want it. (Quick sanity check: `grep -n get_f64_setting ballista/core/src/config.rs` — if zero hits, delete the helper too.)

- [ ] **Step 4: Search for stragglers**

```bash
grep -rn "BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE\|BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT\|BALLISTA_SHUFFLE_SORT_BASED_SPILL_THRESHOLD\|shuffle_sort_based_buffer_size\|shuffle_sort_based_memory_limit\|shuffle_sort_based_spill_threshold" /Users/andy/git/apache/datafusion-ballista --include="*.rs" 2>&1
```

Expected: no output (Task 9 handles the remaining client test).

If any hits remain in places not addressed by Task 9, update them. The remaining hit should only be in `ballista/client/tests/sort_shuffle.rs`.

- [ ] **Step 5: Build to surface remaining issues**

```bash
cargo build --workspace --all-targets 2>&1 | tail -10
```

Expected: build fails in `ballista/client/tests/sort_shuffle.rs` (the imports reference removed constants). That's fine — Task 9 fixes it. **Do not commit yet**; combine with Task 9.

---

## Task 9: Update integration test to drop removed key references

**Files:**

- Modify: `ballista/client/tests/sort_shuffle.rs`

- [ ] **Step 1: Update imports**

In `ballista/client/tests/sort_shuffle.rs` (around line 32-37), replace:

```rust
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ,
        BALLISTA_SHUFFLE_READER_REMOTE_PREFER_FLIGHT,
        BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE, BALLISTA_SHUFFLE_SORT_BASED_ENABLED,
        BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT,
    };
```

with:

```rust
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ,
        BALLISTA_SHUFFLE_READER_REMOTE_PREFER_FLIGHT, BALLISTA_SHUFFLE_SORT_BASED_ENABLED,
    };
```

- [ ] **Step 2: Drop the corresponding `set_str` calls**

In the same file (around lines 56-60), replace:

```rust
        let mut config = SessionConfig::new_with_ballista()
            .set_str(BALLISTA_SHUFFLE_SORT_BASED_ENABLED, "true")
            .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, false) // AQE does not support sort shuffle at the moment
            .set_str(BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE, "1048576") // 1MB
            .set_str(BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT, "268435456"); // 256MB
```

with:

```rust
        let mut config = SessionConfig::new_with_ballista()
            .set_str(BALLISTA_SHUFFLE_SORT_BASED_ENABLED, "true")
            .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, false); // AQE does not support sort shuffle at the moment
```

- [ ] **Step 3: Build and run the client tests**

```bash
cargo build --workspace --all-targets 2>&1 | tail -10
```

Expected: build succeeds.

```bash
cargo test -p ballista --tests sort_shuffle 2>&1 | tail -25
```

Expected: integration tests pass. (These tests boot a standalone in-process Ballista cluster and run real queries.)

- [ ] **Step 4: Verify clippy is clean**

```bash
cargo clippy --all-targets --workspace -- -D warnings 2>&1 | tail -10
```

- [ ] **Step 5: Commit (combines Task 8 + Task 9)**

```bash
git add ballista/core/src/config.rs ballista/client/tests/sort_shuffle.rs
git commit -m "refactor: remove obsolete sort-shuffle session config keys

ballista.shuffle.sort_based.{buffer_size, memory_limit, spill_threshold}
no longer have any effect now that spilling is governed by the runtime
memory pool. Drop the keys, their accessors, and all references."
```

---

## Task 10: Add memory-pressure spill integration test

**Files:**

- Modify: `ballista/core/src/execution_plans/sort_shuffle/writer.rs`

Add a test that verifies the new memory-pool-driven spill path actually triggers spilling under pressure, and that spilled data is correctly reassembled in the final output.

- [ ] **Step 1: Write the test**

In `ballista/core/src/execution_plans/sort_shuffle/writer.rs`, inside the existing `#[cfg(test)] mod tests { ... }` block, add:

```rust
#[tokio::test]
async fn spills_under_memory_pressure_and_round_trips() -> Result<()> {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::reader::FileReader;
    use datafusion::execution::memory_pool::FairSpillPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::{DisplayAs as _, ExecutionPlan as _};
    use std::collections::HashSet;
    use tempfile::TempDir;

    // Build an input plan with many wide rows so memory growth is noticeable.
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));

    // 10 batches of 8192 rows each = ~1.3 MiB raw. With a 512 KiB pool, this
    // forces at least one spill.
    let batches: Vec<RecordBatch> = (0..10)
        .map(|b| {
            let start = b * 8192_i64;
            let keys: Vec<i64> = (start..start + 8192).collect();
            let values: Vec<i64> = keys.iter().map(|k| k * 2).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .unwrap()
        })
        .collect();
    let partitions = vec![batches];
    let memory_data_source =
        Arc::new(MemorySourceConfig::try_new(&partitions, schema.clone(), None)?);
    let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(memory_data_source));

    // Tight memory pool to force spills.
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(512 * 1024)))
            .build()?,
    );
    let session_ctx = SessionContext::new_with_config_rt(
        datafusion::execution::config::SessionConfig::new(),
        runtime_env,
    );
    let task_ctx = session_ctx.task_ctx();

    let work_dir = TempDir::new()?;

    let writer = SortShuffleWriterExec::try_new(
        "job1".to_string(),
        1,
        input,
        work_dir.path().to_str().unwrap().to_string(),
        Partitioning::Hash(vec![Arc::new(Column::new("k", 0))], 4),
        SortShuffleConfig::default(),
    )?;

    let metric_handle = writer.metrics().expect("metrics");

    let mut stream = writer.execute(0, task_ctx)?;
    let result_batches: Vec<RecordBatch> = stream
        .by_ref()
        .try_collect()
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;
    assert_eq!(result_batches.len(), 1, "expected one summary batch");

    let metrics = writer.metrics().expect("metrics after execute");
    let spill_count = metrics
        .iter()
        .find(|m| m.value().name() == "spill_count")
        .map(|m| m.value().as_usize())
        .unwrap_or(0);
    assert!(
        spill_count > 0,
        "expected spilling under tight memory pool, got spill_count={spill_count}"
    );

    // Round-trip: read back the consolidated file via Arrow IPC FileReader and
    // assert all keys are present.
    let data_path = work_dir.path().join("job1").join("1").join("0").join("data.arrow");
    let file = std::fs::File::open(&data_path)?;
    let reader = FileReader::try_new(file, None)?;
    let mut seen: HashSet<i64> = HashSet::new();
    for batch_result in reader {
        let batch = batch_result?;
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for v in arr.values() {
            seen.insert(*v);
        }
    }
    assert_eq!(seen.len(), 10 * 8192);
    for k in 0..(10 * 8192_i64) {
        assert!(seen.contains(&k));
    }

    drop(metric_handle);
    Ok(())
}
```

If any of the imports above (`MemorySourceConfig`, `DataSourceExec`, `Column`, `try_collect`, `SessionContext`) are not already in scope inside the test module, add them via `use super::*` plus the additional imports the test needs. Inspect the existing `test_sort_shuffle_writer` in the same file to copy its import preamble.

- [ ] **Step 2: Run the new test**

```bash
cargo test -p ballista-core --lib spills_under_memory_pressure_and_round_trips 2>&1 | tail -20
```

Expected: test passes. If the test fails because spill is _not_ triggered, lower the memory pool to e.g. `256 * 1024` (256 KiB) — but it's preferable to size the input large enough to force spilling at 512 KiB so the test doesn't depend on exact heap behavior.

- [ ] **Step 3: Run all sort_shuffle tests**

```bash
cargo test -p ballista-core --lib sort_shuffle 2>&1 | tail -15
```

- [ ] **Step 4: Verify clippy is clean**

```bash
cargo clippy -p ballista-core --all-targets -- -D warnings 2>&1 | tail -5
```

- [ ] **Step 5: Commit**

```bash
git add ballista/core/src/execution_plans/sort_shuffle/writer.rs
git commit -m "test: add memory-pressure spill round-trip test for sort shuffle"
```

---

## Task 11: Replace standalone shuffle benchmark binary

**Files:**

- Replace: `benchmarks/src/bin/shuffle_bench.rs`
- Modify: `benchmarks/Cargo.toml`

Replace the synthetic-data, structopt-based bench with a Comet-style Parquet-driven runner. Adds `--writer hash|sort`, validates writer/partitioning combinations at startup, integrates with `RuntimeEnvBuilder::with_memory_limit`. Switches from `structopt` to `clap`.

- [ ] **Step 1: Update `benchmarks/Cargo.toml`**

Replace this block in `benchmarks/Cargo.toml`:

```toml
ballista = { path = "../ballista/client", version = "53.0.0" }
ballista-core = { path = "../ballista/core", version = "53.0.0" }
datafusion = { workspace = true }
datafusion-proto = { workspace = true }
env_logger = { workspace = true }
futures = { workspace = true }
mimalloc = { workspace = true, optional = true }
rand = { workspace = true }
serde = { workspace = true }
serde_json = "1.0.78"
structopt = { version = "0.3", default-features = false }
tempfile = { workspace = true }
```

with:

```toml
ballista = { path = "../ballista/client", version = "53.0.0" }
ballista-core = { path = "../ballista/core", version = "53.0.0" }
clap = { workspace = true }
datafusion = { workspace = true }
datafusion-proto = { workspace = true }
env_logger = { workspace = true }
futures = { workspace = true }
mimalloc = { workspace = true, optional = true }
parquet = { workspace = true }
rand = { workspace = true }
serde = { workspace = true }
serde_json = "1.0.78"
structopt = { version = "0.3", default-features = false }
tempfile = { workspace = true }
```

(`structopt` is kept because `tpch.rs` and `nyctaxi.rs` still use it.)

If `parquet` is not in the workspace deps, check `Cargo.toml` (workspace root) — DataFusion uses `parquet` so it must be available transitively. If you must add it explicitly:

```toml
parquet = "53"
```

at the workspace root's `[workspace.dependencies]` table. Inspect the root `Cargo.toml` first:

```bash
grep -n "^parquet\|workspace.dependencies" /Users/andy/git/apache/datafusion-ballista/Cargo.toml | head -10
```

If `parquet = "53"` (or similar) is already there, you can use `parquet = { workspace = true }` directly. If not, add it to the workspace block first, then reference it.

- [ ] **Step 2: Replace `benchmarks/src/bin/shuffle_bench.rs` with the new content**

Overwrite the file. Full content:

````rust
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

//! Standalone shuffle benchmark for profiling Ballista shuffle write
//! performance outside of a cluster. Streams input from Parquet files and
//! drives either the hash-based or sort-based shuffle writer end-to-end.
//!
//! # Usage
//!
//! ```sh
//! cargo run --release --bin shuffle_bench -- \
//!   --input /data/tpch-sf100/lineitem/ \
//!   --writer sort \
//!   --partitions 200 \
//!   --hash-columns 0,3
//! ```
//!
//! Profile with flamegraph:
//! ```sh
//! cargo flamegraph --release --bin shuffle_bench -- \
//!   --input /data/tpch-sf100/lineitem/ \
//!   --writer sort --partitions 200
//! ```

use ballista_core::execution_plans::sort_shuffle::{SortShuffleConfig, SortShuffleWriterExec};
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::utils;
use clap::Parser;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::ipc::CompressionType;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "shuffle_bench", about = "Standalone Ballista shuffle benchmark")]
struct Args {
    /// Path to input Parquet file or directory of Parquet files.
    #[arg(long)]
    input: PathBuf,

    /// Shuffle writer to drive: `hash` (default) or `sort`.
    #[arg(long, default_value = "hash")]
    writer: String,

    /// Partitioning scheme: `hash`, `single`, or `round-robin`. Currently
    /// both writers only support `hash`; other values are rejected.
    #[arg(long, default_value = "hash")]
    partitioning: String,

    /// Column indices to hash on (comma-separated, e.g. "0,3").
    #[arg(long, default_value = "0")]
    hash_columns: String,

    /// Number of output shuffle partitions.
    #[arg(long, default_value_t = 200)]
    partitions: usize,

    /// DataFusion target batch size (rows).
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Memory pool size in bytes (passed to RuntimeEnvBuilder::with_memory_limit).
    /// When set, the sort writer will spill once usage crosses this limit.
    #[arg(long)]
    memory_limit: Option<usize>,

    /// Limit rows read from Parquet (0 = no limit).
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Number of timed iterations.
    #[arg(long, default_value_t = 1)]
    iterations: usize,

    /// Number of warmup iterations before timing.
    #[arg(long, default_value_t = 0)]
    warmup: usize,

    /// Output work directory for shuffle data.
    #[arg(long, default_value = "/tmp/ballista_shuffle_bench")]
    output_dir: PathBuf,

    /// Concurrent shuffle tasks to simulate executor parallelism.
    #[arg(long, default_value_t = 1)]
    concurrent_tasks: usize,
}

#[derive(Clone, Copy, Debug)]
enum WriterKind {
    Hash,
    Sort,
}

#[derive(Clone, Copy, Debug)]
enum PartitioningKind {
    Hash,
}

fn parse_writer(s: &str) -> Result<WriterKind, String> {
    match s.to_lowercase().as_str() {
        "hash" => Ok(WriterKind::Hash),
        "sort" => Ok(WriterKind::Sort),
        other => Err(format!("unknown writer: {other} (expected 'hash' or 'sort')")),
    }
}

fn parse_partitioning(s: &str) -> Result<PartitioningKind, String> {
    match s.to_lowercase().as_str() {
        "hash" => Ok(PartitioningKind::Hash),
        "single" | "round-robin" => Err(format!(
            "partitioning '{s}' is not supported by Ballista shuffle writers; \
             only 'hash' is currently legal"
        )),
        other => Err(format!("unknown partitioning: {other}")),
    }
}

fn parse_hash_columns(s: &str) -> Vec<usize> {
    s.split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().parse::<usize>().expect("invalid column index"))
        .collect()
}

fn read_parquet_metadata(path: &Path, limit: usize) -> (SchemaRef, u64) {
    let paths = collect_parquet_paths(path);
    let mut schema = None;
    let mut total_rows = 0u64;

    for file_path in &paths {
        let file = fs::File::open(file_path)
            .unwrap_or_else(|e| panic!("Failed to open {}: {}", file_path.display(), e));
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap_or_else(|e| {
            panic!(
                "Failed to read Parquet metadata from {}: {}",
                file_path.display(),
                e
            )
        });
        if schema.is_none() {
            schema = Some(Arc::clone(builder.schema()));
        }
        total_rows += builder.metadata().file_metadata().num_rows() as u64;
        if limit > 0 && total_rows >= limit as u64 {
            total_rows = total_rows.min(limit as u64);
            break;
        }
    }

    (schema.expect("No parquet files found"), total_rows)
}

fn collect_parquet_paths(path: &Path) -> Vec<PathBuf> {
    if path.is_dir() {
        let mut files: Vec<PathBuf> = fs::read_dir(path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e))
            .filter_map(|entry| {
                let p = entry.ok()?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        files.sort();
        if files.is_empty() {
            panic!("No .parquet files in {}", path.display());
        }
        files
    } else {
        vec![path.to_path_buf()]
    }
}

fn build_partitioning(
    _kind: PartitioningKind,
    num_partitions: usize,
    hash_col_indices: &[usize],
    schema: &SchemaRef,
) -> Partitioning {
    let exprs = hash_col_indices
        .iter()
        .map(|&idx| {
            let field = schema.field(idx);
            Arc::new(Column::new(field.name(), idx))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>
        })
        .collect();
    Partitioning::Hash(exprs, num_partitions)
}

async fn execute_shuffle_write(
    args: &Args,
    writer_kind: WriterKind,
    partitioning_kind: PartitioningKind,
    hash_col_indices: &[usize],
    work_dir: PathBuf,
    task_id: usize,
) -> datafusion::error::Result<MetricsSet> {
    let mut runtime_builder = RuntimeEnvBuilder::new();
    if let Some(mem_limit) = args.memory_limit {
        runtime_builder = runtime_builder.with_memory_limit(mem_limit, 1.0);
    }
    let runtime_env = Arc::new(runtime_builder.build()?);
    let config = SessionConfig::new().with_batch_size(args.batch_size);
    let ctx = SessionContext::new_with_config_rt(config, runtime_env);

    let mut df = ctx
        .read_parquet(args.input.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    if args.limit > 0 {
        df = df.limit(0, Some(args.limit))?;
    }

    let parquet_plan = df.create_physical_plan().await?;
    let input: Arc<dyn ExecutionPlan> = if parquet_plan
        .properties()
        .output_partitioning()
        .partition_count()
        > 1
    {
        Arc::new(CoalescePartitionsExec::new(parquet_plan.clone()))
    } else {
        parquet_plan
    };
    let schema = input.schema();
    let partitioning = build_partitioning(
        partitioning_kind,
        args.partitions,
        hash_col_indices,
        &schema,
    );

    let work_dir_str = work_dir.to_str().unwrap().to_string();
    fs::create_dir_all(&work_dir).expect("create work dir");

    let metrics: MetricsSet = match writer_kind {
        WriterKind::Hash => {
            let exec = ShuffleWriterExec::try_new(
                format!("bench_job_{task_id}"),
                1,
                input,
                work_dir_str,
                Some(partitioning),
            )?;
            let task_ctx = ctx.task_ctx();
            let mut stream = exec.execute(0, task_ctx)?;
            let _ = utils::collect_stream(&mut stream).await;
            exec.metrics().unwrap_or_default()
        }
        WriterKind::Sort => {
            let cfg = SortShuffleConfig::new(true, CompressionType::LZ4_FRAME, args.batch_size);
            let exec = SortShuffleWriterExec::try_new(
                format!("bench_job_{task_id}"),
                1,
                input,
                work_dir_str,
                partitioning,
                cfg,
            )?;
            let task_ctx = ctx.task_ctx();
            let mut stream = exec.execute(0, task_ctx)?;
            let _ = utils::collect_stream(&mut stream).await;
            exec.metrics().unwrap_or_default()
        }
    };

    Ok(metrics)
}

fn run_iteration(
    args: &Args,
    writer_kind: WriterKind,
    partitioning_kind: PartitioningKind,
    hash_col_indices: &[usize],
) -> (f64, Option<MetricsSet>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let start = Instant::now();
        if args.concurrent_tasks <= 1 {
            let work_dir = args.output_dir.join("task_0");
            let metrics = execute_shuffle_write(
                args,
                writer_kind,
                partitioning_kind,
                hash_col_indices,
                work_dir.clone(),
                0,
            )
            .await
            .expect("shuffle write failed");
            let elapsed = start.elapsed().as_secs_f64();
            let _ = fs::remove_dir_all(&work_dir);
            (elapsed, Some(metrics))
        } else {
            let mut handles = Vec::with_capacity(args.concurrent_tasks);
            for task_id in 0..args.concurrent_tasks {
                let args = args.clone();
                let hash_col_indices = hash_col_indices.to_vec();
                let writer_kind = writer_kind;
                let partitioning_kind = partitioning_kind;
                let work_dir = args.output_dir.join(format!("task_{task_id}"));
                handles.push(tokio::spawn(async move {
                    let m = execute_shuffle_write(
                        &args,
                        writer_kind,
                        partitioning_kind,
                        &hash_col_indices,
                        work_dir.clone(),
                        task_id,
                    )
                    .await
                    .expect("shuffle write failed");
                    let _ = fs::remove_dir_all(&work_dir);
                    m
                }));
            }
            for h in handles {
                let _ = h.await.expect("task panicked");
            }
            (start.elapsed().as_secs_f64(), None)
        }
    })
}

fn print_shuffle_metrics(metrics: &MetricsSet, total_wall_time_secs: f64) {
    let total_ns = (total_wall_time_secs * 1e9) as u64;
    let fmt_time = |nanos: usize| -> String {
        let secs = nanos as f64 / 1e9;
        let pct = if total_ns > 0 {
            (nanos as f64 / total_ns as f64) * 100.0
        } else {
            0.0
        };
        format!("{:.3}s ({:.1}%)", secs, pct)
    };
    let aggregated = metrics.aggregate_by_name();
    for m in aggregated.iter() {
        let value = m.value();
        let name = value.name();
        let v = value.as_usize();
        if v == 0 {
            continue;
        }
        if matches!(
            value,
            MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_)
        ) {
            continue;
        }
        let is_time = matches!(
            value,
            MetricValue::ElapsedCompute(_) | MetricValue::Time { .. }
        );
        if is_time {
            println!("  {name}: {}", fmt_time(v));
        } else {
            println!("  {name}: {v}");
        }
    }
}

fn describe_schema(schema: &datafusion::arrow::datatypes::Schema) -> String {
    let mut counts: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    for field in schema.fields() {
        let type_name = match field.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "int",
            DataType::Float16 | DataType::Float32 | DataType::Float64 => "float",
            DataType::Utf8 | DataType::LargeUtf8 => "string",
            DataType::Boolean => "bool",
            DataType::Date32 | DataType::Date64 => "date",
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "decimal",
            DataType::Timestamp(_, _) => "timestamp",
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "binary",
            _ => "other",
        };
        *counts.entry(type_name).or_insert(0) += 1;
    }
    let mut parts: Vec<String> = counts.into_iter().map(|(k, v)| format!("{v}x{k}")).collect();
    parts.sort();
    parts.join(", ")
}

fn main() {
    let args = Args::parse();
    let writer_kind = parse_writer(&args.writer).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(2);
    });
    let partitioning_kind = parse_partitioning(&args.partitioning).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(2);
    });
    let hash_col_indices = parse_hash_columns(&args.hash_columns);

    fs::create_dir_all(&args.output_dir).expect("create output dir");

    let (schema, total_rows) = read_parquet_metadata(&args.input, args.limit);

    println!("=== Ballista Shuffle Benchmark ===");
    println!("Writer:         {:?}", writer_kind);
    println!("Partitioning:   {:?}", partitioning_kind);
    println!("Input:          {}", args.input.display());
    println!(
        "Schema:         {} cols ({})",
        schema.fields().len(),
        describe_schema(&schema)
    );
    println!("Total rows:     {}", total_rows);
    println!("Partitions:     {}", args.partitions);
    println!("Batch size:     {}", args.batch_size);
    if let Some(m) = args.memory_limit {
        println!("Memory limit:   {} bytes", m);
    }
    if args.concurrent_tasks > 1 {
        println!("Concurrent:     {} tasks", args.concurrent_tasks);
    }
    println!(
        "Iterations:     {} (warmup {})",
        args.iterations, args.warmup
    );
    println!();

    let total_iters = args.warmup + args.iterations;
    let mut times = Vec::with_capacity(args.iterations);
    let mut last_metrics: Option<MetricsSet> = None;

    for i in 0..total_iters {
        let is_warmup = i < args.warmup;
        let label = if is_warmup {
            format!("warmup {}/{}", i + 1, args.warmup)
        } else {
            format!("iter {}/{}", i - args.warmup + 1, args.iterations)
        };
        let (elapsed, metrics) =
            run_iteration(&args, writer_kind, partitioning_kind, &hash_col_indices);
        if !is_warmup {
            times.push(elapsed);
            if metrics.is_some() {
                last_metrics = metrics;
            }
        }
        println!("  [{label}] write: {elapsed:.3}s");
    }

    if !times.is_empty() {
        let avg = times.iter().sum::<f64>() / times.len() as f64;
        let total_writer_rows = total_rows * args.concurrent_tasks as u64;
        println!();
        println!("=== Results ===");
        println!("avg time: {avg:.3}s");
        if times.len() > 1 {
            let min = times.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            println!("min/max:  {min:.3}s / {max:.3}s");
        }
        println!(
            "throughput: {} rows/s (total across {} tasks)",
            (total_writer_rows as f64 / avg) as u64,
            args.concurrent_tasks
        );
        if let Some(metrics) = last_metrics {
            println!();
            println!("Shuffle metrics (last iteration):");
            print_shuffle_metrics(&metrics, avg);
        }
    }

    let _ = fs::remove_dir_all(&args.output_dir);
}
````

- [ ] **Step 3: Build the binary**

```bash
cargo build -p ballista-benchmarks --bin shuffle_bench 2>&1 | tail -10
```

Expected: build succeeds. If `parquet` isn't resolvable, add it to the workspace `[workspace.dependencies]` (root `Cargo.toml`). Use the same major version as DataFusion 53 expects — DataFusion's `Cargo.lock` will tell you the right version.

- [ ] **Step 4: Smoke-test with a small input**

Build a tiny temporary parquet input from the existing test data:

```bash
ls /Users/andy/git/apache/datafusion-ballista/ballista/client/testdata 2>&1
```

If `alltypes_plain.parquet` is there, use it:

```bash
cargo run --release --bin shuffle_bench -p ballista-benchmarks -- \
    --input ballista/client/testdata/alltypes_plain.parquet \
    --writer sort --partitions 4 --hash-columns 0 --iterations 1 2>&1 | tail -25
```

Expected: prints metrics, exits 0.

Then verify hash-writer also runs:

```bash
cargo run --release --bin shuffle_bench -p ballista-benchmarks -- \
    --input ballista/client/testdata/alltypes_plain.parquet \
    --writer hash --partitions 4 --hash-columns 0 --iterations 1 2>&1 | tail -25
```

Expected: prints metrics, exits 0.

Verify illegal partitioning is rejected:

```bash
cargo run --release --bin shuffle_bench -p ballista-benchmarks -- \
    --input ballista/client/testdata/alltypes_plain.parquet \
    --writer sort --partitioning round-robin --partitions 4 2>&1 | tail -5
```

Expected: prints `error: partitioning 'round-robin' is not supported...` and exits with code 2.

- [ ] **Step 5: Verify clippy is clean**

```bash
cargo clippy -p ballista-benchmarks --all-targets -- -D warnings 2>&1 | tail -10
```

- [ ] **Step 6: Commit**

```bash
git add benchmarks/Cargo.toml benchmarks/src/bin/shuffle_bench.rs Cargo.toml Cargo.lock
git commit -m "feat: rewrite standalone shuffle benchmark to drive Parquet input

The new shuffle_bench binary streams from real Parquet files and
exercises either the hash or sort shuffle writer end-to-end via
'--writer hash|sort'. Switches from structopt to clap and wires
'--memory-limit' through RuntimeEnvBuilder so it now drives sort-
shuffle spilling under the new MemoryReservation model."
```

(If the workspace `Cargo.toml` was not modified — `parquet` was already a workspace dep — drop it from the `git add` list.)

---

## Task 12: Final verification sweep

**Files:** none modified.

Run every verification gate the spec calls out, top to bottom.

- [ ] **Step 1: Format check**

```bash
./ci/scripts/rust_fmt.sh 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 2: Toml format check**

```bash
./ci/scripts/rust_toml_fmt.sh 2>&1 | tail -5
```

Expected: clean. If `cargo-tomlfmt` isn't installed, install with `cargo install cargo-tomlfmt`.

- [ ] **Step 3: Workspace clippy with all features**

```bash
./ci/scripts/rust_clippy.sh 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 4: Locked all-targets build**

```bash
cargo build --workspace --all-targets --locked 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 5: No-default-features build (CI feature gate)**

```bash
cargo check -p ballista-scheduler -p ballista-executor -p ballista-core -p ballista --no-default-features --locked 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 6: Standalone-only client test**

```bash
cd ballista && cargo test --no-default-features --features standalone --locked 2>&1 | tail -15
cd ..
```

Expected: passes.

- [ ] **Step 7: Full workspace tests**

Set up test data env if not already (one-time):

```bash
git submodule update --init
```

Then:

```bash
ARROW_TEST_DATA="$(pwd)/testing/data" \
PARQUET_TEST_DATA="$(pwd)/parquet-testing/data" \
cargo test --workspace 2>&1 | tail -15
```

Expected: all tests pass.

- [ ] **Step 8: Sanity check the integration-test path**

```bash
ARROW_TEST_DATA="$(pwd)/testing/data" \
PARQUET_TEST_DATA="$(pwd)/parquet-testing/data" \
cargo test -p ballista --tests sort_shuffle 2>&1 | tail -25
```

Expected: passes.

- [ ] **Step 9: Final commit-graph review**

```bash
git log --oneline apache/main..HEAD
```

Expected: a clean sequence of commits from the spec commit onward, each with a clear conventional-commit message, no fixups, no merge commits. If anything looks off, do not amend; instead document the issue in a follow-up commit.

- [ ] **Step 10: Push the branch**

Don't push without user confirmation — the user opens the PR. Stop here and report status.

---

## Self-Review Checklist (run after writing the plan)

This was performed before publishing. Issues found and fixed inline:

- **Spec coverage** — every section of the spec maps to a task: `BufferedBatches` (Task 3), `PartitionedBatchIterator` (Task 2), inline `compute_partition_indices` (Task 1), all-or-nothing spill via `MemoryReservation` (Tasks 4, 5), `finalize_output` rewrite (Task 5), config slimming (Task 6), proto changes (Task 7), `BallistaConfig` key removal (Task 8), integration tests update (Task 9), memory-pressure spill test (Task 10), Parquet bench binary (Task 11), final verification (Task 12). Coordination with the in-flight docs PR is called out in the spec — no doc edits in this PR.
- **Placeholder scan** — no "TBD" / "TODO" / "implement later" / "add appropriate error handling". Each step has either exact code or an exact command.
- **Type consistency** — `BufferedBatches::push_batch(batch, &per_partition_rows)`, `BufferedBatches::take() -> (Vec<RecordBatch>, Vec<Vec<(u32, u32)>>)`, `BufferedBatches::indices_for(p) -> &[(u32, u32)]`, `PartitionedBatchIterator::new(&[RecordBatch], &[(u32, u32)], usize)`, `SpillManager::spill(usize, &RecordBatch) -> Result<u64>` — used consistently across Tasks 3, 4, 5, 10, 11.
- **Order safety** — Tasks 1–4 are independent and each compiles standalone. Task 5 rewrites the writer once all the new pieces exist. Tasks 6–9 ripple the config slimming. Tasks 10–11 add tests + bench. Task 12 is verification only.
