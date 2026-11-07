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

//! In-memory buffering for sort-based shuffle.
//!
//! Rows are buffered in one of two layouts:
//!
//! * **Deferred.** Whole input batches plus a `(batch_idx, row_idx)` pair per
//!   row; output batches are gathered with `interleave_record_batch` at spill
//!   or write time. Memory does not grow with the partition count, but on
//!   large buffers the gather misses the cache on almost every row.
//! * **Split.** Each partition's rows are copied into its [`BatchCoalescer`]
//!   while the input batch is still in cache, and the batch is dropped.
//!   Cheaper per row, but every partition holds a partly filled
//!   `batch_size`-row batch, plus a string block per view column with long
//!   strings.
//!
//! Every buffer starts deferred. Once it has `num_partitions × batch_size`
//! rows, it switches to split if the partly filled batches would fit in
//! `split_budget_bytes` and the memory pool grants their memory, which stays
//! reserved while the buffer is split. A split buffer drained while mostly
//! partly filled, as happens under memory pressure, goes back to deferred for
//! good, so later spills do not each write a small batch per partition.

use super::partitioned_batch_iterator::PartitionedBatchIterator;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, OffsetSizeTrait, UInt32Array};
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::memory_pool::MemoryReservation;

/// Rows buffered for every output partition of one input partition.
#[derive(Debug)]
pub struct BufferedBatches {
    num_partitions: usize,
    batch_size: usize,
    split_budget_bytes: usize,
    mode: Mode,
}

#[derive(Debug)]
enum Mode {
    /// Deferred until enough rows have arrived to decide whether to split.
    Deciding(DeferredBuffer),
    Split(SplitBuffer),
    /// Deferred for good; `after_split` if it went back from split.
    Deferred {
        buffer: DeferredBuffer,
        after_split: bool,
    },
}

impl BufferedBatches {
    /// Creates a buffer for `num_partitions` output partitions that emits
    /// batches of at most `batch_size` rows. A `split_budget_bytes` of 0 keeps
    /// it deferred.
    pub fn new(
        num_partitions: usize,
        batch_size: usize,
        split_budget_bytes: usize,
    ) -> Self {
        let buffer = DeferredBuffer::new(num_partitions);
        let mode = if split_budget_bytes > 0 {
            Mode::Deciding(buffer)
        } else {
            Mode::Deferred {
                buffer,
                after_split: false,
            }
        };
        Self {
            num_partitions,
            batch_size,
            split_budget_bytes,
            mode,
        }
    }

    /// Returns the configured number of output partitions.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Returns true if rows are currently split into per-partition batches
    /// as they arrive, false if they are gathered at write time.
    #[cfg(test)]
    pub fn splits_on_arrival(&self) -> bool {
        matches!(self.mode, Mode::Split(_))
    }

    /// Returns true if the buffer has switched to the split layout at any
    /// point, even if it has since gone back to deferred.
    pub fn has_split(&self) -> bool {
        matches!(
            self.mode,
            Mode::Split(_)
                | Mode::Deferred {
                    after_split: true,
                    ..
                }
        )
    }

    /// Returns true if no rows are buffered.
    pub fn is_empty(&self) -> bool {
        match &self.mode {
            Mode::Deciding(b) | Mode::Deferred { buffer: b, .. } => b.batches.is_empty(),
            Mode::Split(b) => b.is_empty(),
        }
    }

    /// Returns the bytes held by the buffered rows, counting allocated
    /// capacity. While split, the partly filled batches count as at least the
    /// bytes reserved for them when the buffer switched.
    pub fn memory_size(&self) -> usize {
        match &self.mode {
            Mode::Deciding(b) | Mode::Deferred { buffer: b, .. } => b.memory_size(),
            Mode::Split(b) => b.memory_size(),
        }
    }

    /// Buffers the rows of `batch` listed in `per_partition_rows[p]` for
    /// output partition `p`, for every partition.
    ///
    /// `per_partition_rows.len()` must equal `num_partitions()`.
    ///
    /// Before switching to split, the buffer grows `reservation` to cover the
    /// switch, and stays deferred for good if the pool refuses.
    pub fn push_batch(
        &mut self,
        batch: RecordBatch,
        per_partition_rows: &[Vec<u32>],
        reservation: &MemoryReservation,
    ) -> Result<()> {
        debug_assert_eq!(per_partition_rows.len(), self.num_partitions);
        if batch.num_rows() == 0 {
            return Ok(());
        }
        match &mut self.mode {
            Mode::Split(b) => b.push_batch(&batch, per_partition_rows)?,
            Mode::Deferred { buffer, .. } => {
                buffer.push_batch(batch, per_partition_rows, false)
            }
            Mode::Deciding(b) => {
                b.push_batch(batch, per_partition_rows, true);
                self.maybe_switch_to_split(reservation)?;
            }
        }
        Ok(())
    }

    /// Once enough rows have arrived, switches to the split layout if the
    /// partly filled batches fit the split budget and the pool grants their
    /// memory, moving the buffered rows into the coalescers. Otherwise stays
    /// deferred for good.
    fn maybe_switch_to_split(&mut self, reservation: &MemoryReservation) -> Result<()> {
        let Mode::Deciding(deferred) = &mut self.mode else {
            return Ok(());
        };
        // Until a batch's worth of rows per partition has arrived, the
        // deferred buffer is the smaller of the two layouts.
        let enough_rows = self.num_partitions.saturating_mul(self.batch_size);
        let split_bytes = if deferred.rows < enough_rows {
            // Splitting would make room for more rows than these, which
            // already exceed the budget.
            if deferred.used_bytes <= self.split_budget_bytes {
                return Ok(());
            }
            None
        } else {
            let row_bytes = deferred.used_bytes.div_ceil(deferred.rows);
            let view_block_bytes = self
                .num_partitions
                .saturating_mul(view_columns_with_data(&deferred.batches))
                .saturating_mul(VIEW_BLOCK_BYTES);
            // Worst case, every partition's batch is partly filled:
            // partitions × batch_size × row_bytes + partitions × view_columns × 1 MiB.
            // `row_bytes` includes the strings the blocks hold, so long strings
            // are counted twice, erring towards staying deferred.
            let split_bytes = enough_rows
                .saturating_mul(row_bytes)
                .saturating_add(view_block_bytes);
            (split_bytes <= self.split_budget_bytes).then_some(split_bytes)
        };
        // Until the move finishes, the buffered rows are held twice.
        let split_bytes = split_bytes.filter(|split_bytes| {
            reservation
                .try_resize(deferred.memory_size().saturating_add(*split_bytes))
                .is_ok()
        });
        let Some(split_bytes) = split_bytes else {
            let buffer =
                std::mem::replace(deferred, DeferredBuffer::new(self.num_partitions));
            self.mode = Mode::Deferred {
                buffer,
                after_split: false,
            };
            return Ok(());
        };

        // The input batches' schema, as the deferred gather uses: the declared
        // one can differ in nullability or metadata.
        let schema = deferred.batches[0].schema();
        let mut split =
            SplitBuffer::new(self.num_partitions, schema, self.batch_size, split_bytes);
        deferred.drain(self.batch_size, |partition, batches| {
            for batch in batches {
                split.push_partition(partition, batch?)?;
            }
            Ok(())
        })?;
        self.mode = Mode::Split(split);
        Ok(())
    }

    /// Empties the buffer, calling `f` once per non-empty partition, in
    /// partition order, with an iterator over its output batches of at most
    /// `batch_size` rows, in arrival order. The buffer is emptied first, so if
    /// `f` or a gather fails, the remaining rows are dropped. A split buffer
    /// that was mostly partly filled goes back to the deferred layout for good.
    pub fn drain(
        &mut self,
        mut f: impl FnMut(usize, &mut dyn Iterator<Item = Result<RecordBatch>>) -> Result<()>,
    ) -> Result<()> {
        match &mut self.mode {
            Mode::Deciding(b) | Mode::Deferred { buffer: b, .. } => {
                b.drain(self.batch_size, f)
            }
            Mode::Split(b) => {
                // A coalescer reserves a whole batch of views and values on
                // its first row, so this roughly means fewer completed
                // batches than partitions since the last drain.
                let mostly_partial = b.partial_bytes() > b.completed_bytes;
                for (partition, batches) in b.take()?.into_iter().enumerate() {
                    if batches.is_empty() {
                        continue;
                    }
                    f(partition, &mut batches.into_iter().map(Ok))?;
                }
                if mostly_partial {
                    self.mode = Mode::Deferred {
                        buffer: DeferredBuffer::new(self.num_partitions),
                        after_split: true,
                    };
                }
                Ok(())
            }
        }
    }
}

/// Largest string block a [`BatchCoalescer`] allocates per view column
/// (arrow-select's private `MAX_BLOCK_SIZE`; a test checks it).
const VIEW_BLOCK_BYTES: usize = 1024 * 1024;

/// For a view array, the bytes of non-inline values its views reference.
fn view_data_bytes(array: &ArrayRef) -> Option<usize> {
    match array.data_type() {
        DataType::Utf8View => Some(array.as_string_view().total_buffer_bytes_used()),
        DataType::BinaryView => Some(array.as_binary_view().total_buffer_bytes_used()),
        _ => None,
    }
}

/// Number of view columns with non-inline values in any of `batches`.
fn view_columns_with_data(batches: &[RecordBatch]) -> usize {
    let num_columns = batches.first().map_or(0, RecordBatch::num_columns);
    (0..num_columns)
        .filter(|&col| {
            batches
                .iter()
                .any(|batch| view_data_bytes(batch.column(col)).is_some_and(|b| b > 0))
        })
        .count()
}

/// Bytes `array`'s values take in memory. Unused capacity does not count, nor,
/// at any depth, list values outside the list's offsets or view data no view
/// references. A dictionary counts its whole values array.
pub(crate) fn used_bytes(array: &ArrayRef) -> usize {
    let nulls = array.nulls().map_or(0, |n| n.len().div_ceil(8));
    if let Some(data) = view_data_bytes(array) {
        return array.len() * size_of::<u128>() + data + nulls;
    }
    match array.data_type() {
        DataType::Struct(_) => {
            // `StructArray` keeps its columns sliced to its own rows.
            let columns = array.as_struct().columns();
            nulls + columns.iter().map(used_bytes).sum::<usize>()
        }
        DataType::FixedSizeList(..) => {
            // `FixedSizeListArray` keeps its values sliced to its own rows.
            nulls + used_bytes(array.as_fixed_size_list().values())
        }
        DataType::List(_) => list_used_bytes(
            array.as_list::<i32>().value_offsets(),
            array.as_list::<i32>().values(),
            nulls,
        ),
        DataType::LargeList(_) => list_used_bytes(
            array.as_list::<i64>().value_offsets(),
            array.as_list::<i64>().values(),
            nulls,
        ),
        DataType::Map(..) => {
            let map = array.as_map();
            let entries: ArrayRef = std::sync::Arc::new(map.entries().clone());
            list_used_bytes(map.value_offsets(), &entries, nulls)
        }
        _ => array
            .to_data()
            .get_slice_memory_size()
            .unwrap_or_else(|_| array.get_array_memory_size()),
    }
}

/// [`used_bytes`] of a list: its `offsets`, its `nulls` bytes and the part of
/// `values` the offsets span.
fn list_used_bytes<O: OffsetSizeTrait>(
    offsets: &[O],
    values: &ArrayRef,
    nulls: usize,
) -> usize {
    let (start, end) = (offsets[0].as_usize(), offsets[offsets.len() - 1].as_usize());
    size_of_val(offsets) + nulls + used_bytes(&values.slice(start, end - start))
}

/// [`used_bytes`] summed over `batch`'s columns.
pub(crate) fn batch_used_bytes(batch: &RecordBatch) -> usize {
    batch.columns().iter().map(used_bytes).sum()
}

/// Rows split by output partition as they arrive.
#[derive(Debug)]
struct SplitBuffer {
    /// One per output partition, each holding a partly filled batch.
    coalescers: Vec<BatchCoalescer>,
    /// Each output partition's filled batches.
    completed: Vec<Vec<RecordBatch>>,
    /// Total `get_array_memory_size` of the batches in `completed`.
    completed_bytes: usize,
    /// The partly filled batches' worst-case size, reserved when the buffer
    /// switched.
    reserved_partial_bytes: usize,
}

impl SplitBuffer {
    fn new(
        num_partitions: usize,
        schema: SchemaRef,
        batch_size: usize,
        reserved_partial_bytes: usize,
    ) -> Self {
        Self {
            coalescers: (0..num_partitions)
                .map(|_| BatchCoalescer::new(schema.clone(), batch_size))
                .collect(),
            completed: vec![Vec::new(); num_partitions],
            completed_bytes: 0,
            reserved_partial_bytes,
        }
    }

    fn is_empty(&self) -> bool {
        self.completed.iter().all(Vec::is_empty)
            && self.coalescers.iter().all(BatchCoalescer::is_empty)
    }

    /// Bytes held by the coalescers' partly filled batches, counting the
    /// whole batch each reserves however few rows it holds.
    fn partial_bytes(&self) -> usize {
        self.coalescers.iter().map(BatchCoalescer::size).sum()
    }

    /// Completed batches plus the coalescers' partly filled batches, which
    /// count as at least the bytes reserved for them.
    fn memory_size(&self) -> usize {
        self.completed_bytes + self.partial_bytes().max(self.reserved_partial_bytes)
    }

    fn push_batch(
        &mut self,
        batch: &RecordBatch,
        per_partition_rows: &[Vec<u32>],
    ) -> std::result::Result<(), ArrowError> {
        for (partition, rows) in per_partition_rows.iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            if rows.len() == batch.num_rows() {
                // Every row goes to this partition, in order.
                self.coalescers[partition].push_batch(batch.clone())?;
            } else {
                let indices = UInt32Array::from_iter_values(rows.iter().copied());
                self.coalescers[partition]
                    .push_batch_with_indices(batch.clone(), &indices)?;
            }
            self.collect_completed(partition);
        }
        Ok(())
    }

    fn push_partition(
        &mut self,
        partition: usize,
        batch: RecordBatch,
    ) -> std::result::Result<(), ArrowError> {
        self.coalescers[partition].push_batch(batch)?;
        self.collect_completed(partition);
        Ok(())
    }

    fn collect_completed(&mut self, partition: usize) {
        while let Some(done) = self.coalescers[partition].next_completed_batch() {
            self.completed_bytes += done.get_array_memory_size();
            self.completed[partition].push(done);
        }
    }

    /// Empties the buffer, returning each partition's batches.
    fn take(&mut self) -> std::result::Result<Vec<Vec<RecordBatch>>, ArrowError> {
        self.completed_bytes = 0;
        self.coalescers
            .iter_mut()
            .zip(self.completed.iter_mut())
            .map(|(coalescer, completed)| {
                coalescer.finish_buffered_batch()?;
                let mut batches = std::mem::take(completed);
                batches.extend(std::iter::from_fn(|| coalescer.next_completed_batch()));
                Ok(batches)
            })
            .collect()
    }
}

/// Whole input batches plus each partition's `(batch_idx, row_idx)` pairs.
#[derive(Debug)]
struct DeferredBuffer {
    batches: Vec<RecordBatch>,
    /// Total `get_array_memory_size` of `batches`.
    batches_bytes: usize,
    /// One list per output partition.
    indices: Vec<Vec<(u32, u32)>>,
    /// Total allocated capacity of `indices`, in bytes.
    indices_bytes: usize,
    rows: usize,
    /// Total [`batch_used_bytes`] of `batches`, counted only while the buffer
    /// may still switch layout.
    used_bytes: usize,
}

impl DeferredBuffer {
    fn new(num_partitions: usize) -> Self {
        Self {
            batches: Vec::new(),
            batches_bytes: 0,
            indices: vec![Vec::new(); num_partitions],
            indices_bytes: 0,
            rows: 0,
            used_bytes: 0,
        }
    }

    /// Input batches plus the allocated capacity of the index lists.
    fn memory_size(&self) -> usize {
        self.batches_bytes + self.indices_bytes
    }

    /// Buffers `batch`, adding to `used_bytes` if `measure`.
    fn push_batch(
        &mut self,
        batch: RecordBatch,
        per_partition_rows: &[Vec<u32>],
        measure: bool,
    ) {
        let batch_idx = self.batches.len() as u32;
        for (partition_indices, rows) in self.indices.iter_mut().zip(per_partition_rows) {
            let capacity_before = partition_indices.capacity();
            partition_indices.extend(rows.iter().map(|&r| (batch_idx, r)));
            self.indices_bytes += (partition_indices.capacity() - capacity_before)
                * size_of::<(u32, u32)>();
        }
        self.batches_bytes += batch.get_array_memory_size();
        self.rows += batch.num_rows();
        if measure {
            self.used_bytes += batch_used_bytes(&batch);
        }
        self.batches.push(batch);
    }

    /// Empties the buffer, calling `f` once per non-empty partition with an
    /// iterator that gathers its rows into batches of at most `batch_size`
    /// rows, one at a time.
    fn drain(
        &mut self,
        batch_size: usize,
        mut f: impl FnMut(usize, &mut dyn Iterator<Item = Result<RecordBatch>>) -> Result<()>,
    ) -> Result<()> {
        self.batches_bytes = 0;
        self.indices_bytes = 0;
        self.rows = 0;
        self.used_bytes = 0;
        let batches = std::mem::take(&mut self.batches);
        let indices: Vec<_> = self.indices.iter_mut().map(std::mem::take).collect();
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        for (partition, partition_indices) in indices.iter().enumerate() {
            if partition_indices.is_empty() {
                continue;
            }
            let mut iter =
                PartitionedBatchIterator::new(&batch_refs, partition_indices, batch_size);
            f(partition, &mut iter)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Int32Array, ListBuilder, StringBuilder, StringViewArray, StructArray,
    };
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };
    use std::sync::Arc;

    /// A row of [`batch`]: a 4-byte int, a 16-byte view and a 32-byte string.
    const ROW_BYTES: usize = 4 + 16 + 32;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("s", DataType::Utf8View, false),
        ]))
    }

    fn batch_with(values: Vec<i32>, string: impl Fn(i32) -> String) -> RecordBatch {
        let strings =
            StringViewArray::from_iter_values(values.iter().copied().map(string));
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int32Array::from(values)), Arc::new(strings)],
        )
        .unwrap()
    }

    /// A batch of `values`, each with a 32-byte string.
    fn batch(values: Vec<i32>) -> RecordBatch {
        batch_with(values, |v| format!("a string longer than 12 bytes {v:<2}"))
    }

    /// A batch of `values`, each with a string short enough to inline.
    fn inline_batch(values: Vec<i32>) -> RecordBatch {
        batch_with(values, |v| format!("s{v}"))
    }

    /// Every batch `drain` emits, with its partition.
    fn drain(bb: &mut BufferedBatches) -> Vec<(usize, RecordBatch)> {
        let mut out = vec![];
        bb.drain(|p, batches| {
            for batch in batches {
                out.push((p, batch?));
            }
            Ok(())
        })
        .unwrap();
        assert!(bb.is_empty());
        out
    }

    fn reservation(pool: Arc<dyn MemoryPool>) -> MemoryReservation {
        MemoryConsumer::new("test").register(&pool)
    }

    impl BufferedBatches {
        /// [`BufferedBatches::push_batch`] against an unbounded pool.
        fn push(&mut self, batch: RecordBatch, rows: &[Vec<u32>]) -> Result<()> {
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            self.push_batch(batch, rows, &reservation(pool))
        }
    }

    /// A batch of `values` in one `Int32` column.
    fn ints(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn used_bytes_counts_referenced_view_bytes_only() {
        let batch = batch((0..10).collect());
        assert_eq!(batch_used_bytes(&batch), 10 * ROW_BYTES);
        // The slice shares the whole data buffer; only its strings count.
        assert_eq!(batch_used_bytes(&batch.slice(2, 3)), 3 * ROW_BYTES);
    }

    #[test]
    fn used_bytes_counts_only_the_rows_nested_columns_reach() {
        let structs: ArrayRef = Arc::new(StructArray::from(batch((0..10).collect())));
        assert_eq!(used_bytes(&structs), 10 * ROW_BYTES);
        assert_eq!(used_bytes(&structs.slice(2, 3)), 3 * ROW_BYTES);

        // Ten lists of two 2-byte strings each.
        let mut lists = ListBuilder::new(StringBuilder::new());
        for _ in 0..10 {
            lists.values().append_value("ab");
            lists.values().append_value("cd");
            lists.append(true);
        }
        let lists: ArrayRef = Arc::new(lists.finish());
        // 11 list offsets, 21 string offsets and 40 string bytes.
        assert_eq!(used_bytes(&lists), 11 * 4 + 21 * 4 + 40);
        // 4 list offsets, then only the 6 strings the slice reaches: 7
        // string offsets and 12 string bytes.
        assert_eq!(used_bytes(&lists.slice(2, 3)), 4 * 4 + 7 * 4 + 12);
    }

    #[test]
    fn split_keeps_the_input_batches_schema() {
        // A nullable column with metadata; the split output must keep both.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true)
                .with_metadata([("k".to_string(), "v".to_string())].into()),
        ]));
        let nullable = |values: Vec<Option<i32>>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])
                .unwrap()
        };
        let mut bb = BufferedBatches::new(2, 2, usize::MAX);
        bb.push(
            nullable(vec![Some(1), None, Some(3), None]),
            &[vec![0, 1], vec![2, 3]],
        )
        .unwrap();
        assert!(bb.splits_on_arrival());
        assert_eq!(
            drain(&mut bb),
            vec![
                (0, nullable(vec![Some(1), None])),
                (1, nullable(vec![Some(3), None])),
            ]
        );
    }

    #[test]
    fn switches_to_split_only_if_the_pool_grants_the_switch() {
        let rows = [vec![0, 2], vec![3, 1]];
        let input = || batch(vec![10, 20, 30, 40]);
        let mut deferred = BufferedBatches::new(2, 2, 0);
        deferred.push(input(), &rows).unwrap();
        // The deferred rows plus 2 partly filled 2-row batches with a string
        // block each.
        let switch_bytes =
            deferred.memory_size() + 2 * 2 * ROW_BYTES + 2 * VIEW_BLOCK_BYTES;

        for (pool_bytes, splits, reserved) in [
            (switch_bytes, true, switch_bytes),
            (switch_bytes - 1, false, 0),
        ] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
            let reservation = reservation(pool);
            let mut bb = BufferedBatches::new(2, 2, usize::MAX);
            bb.push_batch(input(), &rows, &reservation).unwrap();
            assert_eq!(
                (bb.splits_on_arrival(), bb.has_split(), reservation.size()),
                (splits, splits, reserved),
                "pool {pool_bytes}"
            );
            // A refused switch stays deferred for good.
            bb.push_batch(input(), &rows, &reservation).unwrap();
            assert_eq!(bb.splits_on_arrival(), splits, "pool {pool_bytes}");
        }
    }

    #[test]
    fn split_memory_size_keeps_the_partly_filled_batches_reserved() {
        let mut bb = BufferedBatches::new(1, 64, usize::MAX);
        bb.push(ints((0..64).collect()), &[(0..64).collect()])
            .unwrap();
        assert!(bb.splits_on_arrival());
        assert_eq!(drain(&mut bb), vec![(0, ints((0..64).collect()))]);
        // Still split with nothing buffered: 64 four-byte ints reserved.
        assert!(bb.splits_on_arrival());
        assert_eq!(bb.memory_size(), 64 * 4);
    }

    #[test]
    fn view_block_bytes_is_the_coalescers_largest_string_block() {
        // Slices share their batch's whole data buffer, so the coalescer
        // copies their strings, as it does the per-partition rows `take`
        // picks out. Copy enough for the blocks to grow as large as they get,
        // then check the block one more row is copied into.
        let rows = batch_with((0..1024).collect(), |v| format!("{v:<1024}"));
        let mut coalescer = BatchCoalescer::new(schema(), 1024);
        for offset in (0..1024).step_by(64).cycle().take(64) {
            coalescer.push_batch(rows.slice(offset, 64)).unwrap();
        }
        while coalescer.next_completed_batch().is_some() {}
        coalescer.push_batch(rows.slice(0, 1)).unwrap();
        coalescer.finish_buffered_batch().unwrap();

        let last = coalescer.next_completed_batch().unwrap();
        let blocks: Vec<usize> = last
            .column(1)
            .as_string_view()
            .data_buffers()
            .iter()
            .map(|b| b.capacity())
            .collect();
        assert_eq!(blocks, vec![VIEW_BLOCK_BYTES]);
    }

    #[test]
    fn view_columns_with_data_counts_columns_with_long_values() {
        let long = batch(vec![1, 2]);
        let short = inline_batch(vec![1, 2]);
        assert_eq!(view_columns_with_data(&[]), 0);
        assert_eq!(view_columns_with_data(std::slice::from_ref(&short)), 0);
        assert_eq!(view_columns_with_data(&[short, long]), 1);
    }

    #[test]
    fn switches_to_split_only_when_a_batch_per_partition_fits() {
        // 2 partitions of 2-row batches: 4 rows plus a string block per
        // partition.
        let fits = 2 * 2 * ROW_BYTES + 2 * VIEW_BLOCK_BYTES;
        for (budget, splits) in [(fits, true), (fits - 1, false), (0, false)] {
            let mut bb = BufferedBatches::new(2, 2, budget);
            bb.push(batch(vec![10, 20, 30, 40]), &[vec![0, 2], vec![3, 1]])
                .unwrap();
            assert_eq!(bb.splits_on_arrival(), splits, "budget {budget}");
            bb.push(batch(vec![50, 60, 70]), &[vec![0], vec![1, 2]])
                .unwrap();
            assert_eq!(bb.splits_on_arrival(), splits, "budget {budget}");

            // Either layout writes the same batches.
            assert_eq!(
                drain(&mut bb),
                vec![
                    (0, batch(vec![10, 30])),
                    (0, batch(vec![50])),
                    (1, batch(vec![40, 20])),
                    (1, batch(vec![60, 70])),
                ]
            );
        }
    }

    #[test]
    fn inline_strings_need_no_string_block() {
        // A 4-byte int and a 16-byte view: 2 partitions × 2 rows × 20 bytes.
        let mut bb = BufferedBatches::new(2, 2, 2 * 2 * 20);
        bb.push(inline_batch(vec![1, 2, 3, 4]), &[vec![0, 1], vec![2, 3]])
            .unwrap();
        assert!(bb.splits_on_arrival());
        assert_eq!(
            drain(&mut bb),
            vec![(0, inline_batch(vec![1, 2])), (1, inline_batch(vec![3, 4]))]
        );
    }

    #[test]
    fn stays_deferred_until_a_batch_per_partition_has_arrived() {
        let mut bb = BufferedBatches::new(2, 4, usize::MAX);
        // 7 of the 8 rows that would fill a batch per partition.
        bb.push(batch((0..7).collect()), &[vec![0, 1, 2], vec![3, 4, 5, 6]])
            .unwrap();
        assert!(!bb.splits_on_arrival());
        assert_eq!(
            drain(&mut bb),
            vec![(0, batch(vec![0, 1, 2])), (1, batch(vec![3, 4, 5, 6]))]
        );
    }

    #[test]
    fn decides_to_stay_deferred_once_the_buffered_rows_pass_the_budget() {
        // 3 rows fit a 3-row budget, 4 do not, both short of the 8 that
        // would fill a batch per partition.
        let mut bb = BufferedBatches::new(2, 4, 3 * ROW_BYTES);
        bb.push(batch(vec![0, 1, 2]), &[vec![0], vec![1, 2]])
            .unwrap();
        assert!(matches!(bb.mode, Mode::Deciding(_)));
        bb.push(batch(vec![3]), &[vec![0], vec![]]).unwrap();
        assert!(matches!(
            bb.mode,
            Mode::Deferred {
                after_split: false,
                ..
            }
        ));
        assert_eq!(
            drain(&mut bb),
            vec![(0, batch(vec![0, 3])), (1, batch(vec![1, 2]))]
        );
    }

    #[test]
    fn goes_back_to_deferred_after_draining_mostly_partial_batches() {
        let mut bb = BufferedBatches::new(2, 4, usize::MAX);
        // 8 rows switch to split, leaving 3 + 1 rows partly filled.
        bb.push(
            batch((0..8).collect()),
            &[vec![0, 1, 2], vec![3, 4, 5, 6, 7]],
        )
        .unwrap();
        assert!(bb.splits_on_arrival());
        assert_eq!(
            drain(&mut bb),
            vec![
                (0, batch(vec![0, 1, 2])),
                (1, batch(vec![3, 4, 5, 6])),
                (1, batch(vec![7])),
            ]
        );
        assert!(!bb.splits_on_arrival());

        // For good, however many rows arrive.
        bb.push(
            batch((0..16).collect()),
            &[(0..8).collect(), (8..16).collect()],
        )
        .unwrap();
        assert!(!bb.splits_on_arrival());
    }

    #[test]
    fn stays_split_after_draining_mostly_completed_batches() {
        let mut bb = BufferedBatches::new(1, 2, usize::MAX);
        bb.push(ints((0..9).collect()), &[(0..9).collect()])
            .unwrap();
        assert!(bb.splits_on_arrival());
        assert_eq!(
            drain(&mut bb),
            vec![
                (0, ints(vec![0, 1])),
                (0, ints(vec![2, 3])),
                (0, ints(vec![4, 5])),
                (0, ints(vec![6, 7])),
                (0, ints(vec![8])),
            ]
        );
        assert!(bb.splits_on_arrival());
    }

    #[test]
    fn both_layouts_compact_sparse_view_columns() {
        for budget in [usize::MAX, 0] {
            let mut bb = BufferedBatches::new(2, 8, budget);
            let rest: Vec<u32> = (0..50).filter(|&r| r != 7).collect();
            bb.push(batch((0..50).collect()), &[vec![7], rest]).unwrap();
            assert_eq!(bb.splits_on_arrival(), budget > 0);

            // Row 7's batch keeps only its own 32-byte string.
            let (partition, row_7) = drain(&mut bb).remove(0);
            assert_eq!((partition, &row_7), (0, &batch(vec![7])));
            let strings = row_7.column(1).as_string_view();
            let buffered: usize = strings.data_buffers().iter().map(|b| b.len()).sum();
            assert_eq!(buffered, 32, "budget {budget}");
        }
    }

    #[test]
    fn deferred_memory_size_counts_batches_and_index_capacity() {
        fn indices(bb: &BufferedBatches) -> &[Vec<(u32, u32)>] {
            match &bb.mode {
                Mode::Deciding(b) | Mode::Deferred { buffer: b, .. } => &b.indices,
                Mode::Split(_) => panic!("expected the deferred layout"),
            }
        }
        fn index_bytes(bb: &BufferedBatches) -> usize {
            indices(bb)
                .iter()
                .map(|v| v.capacity() * size_of::<(u32, u32)>())
                .sum()
        }

        // A zero budget keeps the buffer deferred.
        let mut bb = BufferedBatches::new(2, 8, 0);
        assert_eq!(bb.memory_size(), 0);

        let first = batch(vec![1, 2, 3]);
        let mut batch_bytes = first.get_array_memory_size();
        bb.push(first, &[vec![0, 2], vec![1]]).unwrap();
        assert_eq!(bb.memory_size(), batch_bytes + index_bytes(&bb));

        // More rows for partition 0 than its list has room for, so the
        // running total must follow a reallocation.
        let capacity_before = indices(&bb)[0].capacity();
        let second = batch((0..8).collect());
        batch_bytes += second.get_array_memory_size();
        bb.push(second, &[(0..8).collect(), vec![]]).unwrap();
        assert!(indices(&bb)[0].capacity() > capacity_before);
        assert_eq!(bb.memory_size(), batch_bytes + index_bytes(&bb));

        drain(&mut bb);
        assert_eq!(bb.memory_size(), 0);
    }
}
