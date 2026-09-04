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

//! The value index sitting beside a range shuffle file.
//!
//! One row per IPC message in the data file, in file order:
//!
//! ```text
//! <sort_expr_0..n>  key types, nullable   first row's key in this batch
//! is_dict           Boolean               this row describes a dictionary
//! byte_offset       UInt64                absolute, from the data file's footer
//! byte_len          UInt64
//! num_rows          UInt64, nullable      rows in the batch
//! ```
//!
//! A consumer assigned the value range `[lo, hi)` downloads this file — KB
//! against a data file's MB — binary-searches the key columns for the batches
//! covering its range, and fetches only those byte ranges.
//!
//! # Why the keys are typed columns
//!
//! One column per ORDER BY expression, in lexicographic order, carrying that
//! expression's own type. Multi-column keys, `DESC`, and mixed types all work
//! by construction rather than through a `ScalarValue` encoding, and the
//! index's schema names what it is indexed on.
//!
//! Nullable for two independent reasons: a dictionary row has no key, and a
//! record batch's first key can genuinely be NULL under `NULLS FIRST`. Since
//! those are indistinguishable from the key alone, `is_dict` carries the
//! distinction rather than nullability implying it.
//!
//! # Why dictionaries get rows
//!
//! A record batch referencing a dictionary cannot be decoded from its own
//! bytes alone — the `DictionaryBatch` lives at its own offset earlier in the
//! file. A consumer range-fetching record batches has to fetch those too, so
//! they are in the index rather than requiring a second trip to the data
//! file's footer.
//!
//! The IPC footer does not record which dictionary a block carries (the id is
//! inside the message header), so the index cannot say which record batch
//! needs which dictionary. The reader's rule is therefore to take every
//! dictionary row preceding its selected range — conservative, and correct
//! under both replacement and delta dictionaries.
//!
//! # Why `num_rows`
//!
//! A ROWS-frame window needs N rows of context before its range starts.
//! Summing `num_rows` across the batches below a boundary — over every file
//! covering that band — says how far to widen the value range to reach N rows,
//! computed from indexes alone before any data is fetched. Without it,
//! widening means fetching and decoding to find out, one round trip per guess.
//!
//! Summation assumes each row lives in exactly one file. See
//! `project_range_shuffle_index_open_questions` on the stage-0 write
//! amplification that has to be understood before a consumer relies on it.

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, RecordBatch, UInt64Array,
    UInt64Builder, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::ScalarValue;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt, ready};

use crate::error::{BallistaError, Result};

use super::ipc_file::FileLayout;

/// Column holding whether a row describes a dictionary rather than a batch.
pub const IS_DICT_COLUMN: &str = "is_dict";
/// Column holding a message's absolute byte offset in the data file.
pub const BYTE_OFFSET_COLUMN: &str = "byte_offset";
/// Column holding a message's length in bytes.
pub const BYTE_LEN_COLUMN: &str = "byte_len";
/// Column holding a record batch's row count.
pub const NUM_ROWS_COLUMN: &str = "num_rows";

/// How many fixed layout columns follow the key columns.
pub const FIXED_COLUMN_COUNT: usize = 4;

/// Offsets of the fixed columns past the last key column, in the order
/// [`fixed_fields`] emits them.
///
/// These columns are addressed by **position, never by name**. Key columns are
/// named for the user's sort expressions and sit ahead of these, Arrow permits
/// duplicate field names, and `column_by_name` resolves to the first match — so
/// a query ordering by a column called `byte_offset` would otherwise shadow the
/// real one and be read as whatever type the key happens to be.
const IS_DICT_POSITION: usize = 0;
const BYTE_OFFSET_POSITION: usize = 1;
const BYTE_LEN_POSITION: usize = 2;
const NUM_ROWS_POSITION: usize = 3;

/// The fixed layout columns, in the order they follow the keys.
///
/// Defined once so [`index_schema`] and the positional accessors cannot drift:
/// an entry's position here *is* its offset past the last key column.
pub(crate) fn fixed_fields() -> [Field; FIXED_COLUMN_COUNT] {
    [
        Field::new(IS_DICT_COLUMN, DataType::Boolean, false),
        Field::new(BYTE_OFFSET_COLUMN, DataType::UInt64, false),
        Field::new(BYTE_LEN_COLUMN, DataType::UInt64, false),
        Field::new(NUM_ROWS_COLUMN, DataType::UInt64, true),
    ]
}

/// How many leading columns of `index` carry sort-key values.
///
/// Derived from the column count, so a reader holding only the batch can still
/// address the fixed columns without knowing the ordering that produced it.
pub fn key_column_count(index: &RecordBatch) -> Result<usize> {
    index
        .num_columns()
        .checked_sub(FIXED_COLUMN_COUNT)
        .ok_or_else(|| {
            BallistaError::General(format!(
                "range shuffle index has {} columns, fewer than the \
                 {FIXED_COLUMN_COUNT} fixed layout columns",
                index.num_columns()
            ))
        })
}

/// Fixed column at `position` past the keys, downcast to the layout type
/// [`fixed_fields`] declares for it. `name` names it in the error only.
fn fixed_column<'a, T: Array + 'static>(
    index: &'a RecordBatch,
    position: usize,
    name: &str,
) -> Result<&'a T> {
    let column = key_column_count(index)? + position;
    index
        .column(column)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            BallistaError::General(format!(
                "range shuffle index column {column} (`{name}`) has type {}, not the \
             layout type",
                index.column(column).data_type()
            ))
        })
}

/// Whether each row describes a dictionary rather than a record batch.
pub fn is_dict_column(index: &RecordBatch) -> Result<&BooleanArray> {
    fixed_column(index, IS_DICT_POSITION, IS_DICT_COLUMN)
}

/// Each message's absolute byte offset in the data file.
pub fn byte_offset_column(index: &RecordBatch) -> Result<&UInt64Array> {
    fixed_column(index, BYTE_OFFSET_POSITION, BYTE_OFFSET_COLUMN)
}

/// Each message's length in bytes.
pub fn byte_len_column(index: &RecordBatch) -> Result<&UInt64Array> {
    fixed_column(index, BYTE_LEN_POSITION, BYTE_LEN_COLUMN)
}

/// Each record batch's row count; null on dictionary rows.
pub fn num_rows_column(index: &RecordBatch) -> Result<&UInt64Array> {
    fixed_column(index, NUM_ROWS_POSITION, NUM_ROWS_COLUMN)
}

/// Schema metadata key recording the sort options each key column was written
/// under, so a reader can check its own ordering agrees rather than assume it.
pub const SORT_OPTIONS_METADATA: &str = "ballista.range_shuffle.sort_options";

/// The sort key of one record batch's first row, one value per ORDER BY
/// expression.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchKey {
    /// Lexicographic key values, positionally matching the index's key columns.
    pub values: Vec<ScalarValue>,
    /// Rows the batch carries.
    pub num_rows: u64,
}

/// Path of the value index sitting beside a range shuffle data file:
/// `data-{file_id}.rangeidx.arrow` beside `data-{file_id}.arrow`.
///
/// The index is itself an Arrow IPC stream, so `.arrow` is its extension and
/// `rangeidx` says which role it plays — the sort shuffle's `.arrow.index`
/// names the two the other way round.
pub fn index_path(data_path: &Path) -> PathBuf {
    data_path.with_extension("rangeidx.arrow")
}

/// True when `data_path` has a value index beside it.
pub fn has_range_index(data_path: &Path) -> bool {
    index_path(data_path).exists()
}

/// Write the value index for one data file.
///
/// Arrow IPC stream format: the index is read whole, so it has no use for a
/// footer, and the stream framing is what every existing shuffle reader
/// already speaks.
pub fn write_index_file(
    path: &Path,
    schema: SchemaRef,
    layout: &FileLayout,
    keys: &[BatchKey],
) -> Result<()> {
    let batch = build_index_batch(schema.clone(), layout, keys)?;
    let file = File::create(path).map_err(|e| {
        BallistaError::General(format!(
            "range shuffle index: failed to create {path:?}: {e:?}"
        ))
    })?;
    let mut writer = StreamWriter::try_new(BufWriter::new(file), schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}

/// Read the value index beside a range shuffle data file.
pub fn read_index_file(path: &Path) -> Result<RecordBatch> {
    let file = File::open(path).map_err(|e| {
        BallistaError::General(format!(
            "range shuffle index: failed to open {path:?}: {e:?}"
        ))
    })?;
    let mut reader = StreamReader::try_new(BufReader::new(file), None)?;
    let batch = reader.next().transpose()?.ok_or_else(|| {
        BallistaError::General(format!("range shuffle index {path:?} holds no rows"))
    })?;
    if reader.next().is_some() {
        return Err(BallistaError::General(format!(
            "range shuffle index {path:?} holds more than one batch"
        )));
    }
    Ok(batch)
}

/// How many record batches an index describes, ignoring its dictionary rows.
pub fn count_record_batches(index: &RecordBatch) -> Result<usize> {
    let is_dict = is_dict_column(index)?;
    Ok((0..index.num_rows())
        .filter(|&row| !is_dict.value(row))
        .count())
}

/// Which record batches of an indexed file can hold rows in `[lo, hi)`.
///
/// Returns ordinals into the file's record batches — the order
/// `FileReader::set_index` addresses them by — or `None` when the index cannot
/// decide and the caller should read the whole file.
///
/// `None` on either bound means unbounded on that side. Selection is at batch
/// granularity and deliberately inclusive at the edges: a batch is taken when
/// it *may* hold a row in range, and the exact trim belongs to the
/// `RangeFilterExec` above the reader. Taking a batch too many costs bytes;
/// taking one too few loses rows.
///
/// # When it declines
///
/// A NULL key means the batch's first row has no value under the ordering, so
/// where it sits relative to `lo` and `hi` depends on null placement rather
/// than on comparison. Rather than encode that here, the index declines and
/// the caller reads everything — the answer stays right and only the saving is
/// lost.
pub fn select_record_batches(
    index: &RecordBatch,
    lo: Option<&ScalarValue>,
    hi: Option<&ScalarValue>,
    descending: bool,
) -> Result<Option<Vec<usize>>> {
    let is_dict = is_dict_column(index)?;
    let keys = index.column(0);

    // Record batches in file order, which is the order `set_index` counts in.
    let mut first_keys = Vec::with_capacity(index.num_rows());
    for row in 0..index.num_rows() {
        if is_dict.value(row) {
            continue;
        }
        if keys.is_null(row) {
            return Ok(None);
        }
        first_keys.push(ScalarValue::try_from_array(keys, row)?);
    }

    // `precedes(a, b)` is "a comes before b under this ordering", so the same
    // walk serves ASC and DESC.
    let precedes = |a: &ScalarValue, b: &ScalarValue| match a.partial_cmp(b) {
        Some(std::cmp::Ordering::Less) => !descending,
        Some(std::cmp::Ordering::Greater) => descending,
        _ => false,
    };

    // Batch `i` covers from its own first key up to the next batch's, so the
    // first batch that can hold `lo` is the last one starting at or before it.
    let start = match lo {
        None => 0,
        Some(lo) => first_keys
            .iter()
            .rposition(|key| !precedes(lo, key))
            .unwrap_or(0),
    };
    let end = match hi {
        None => first_keys.len(),
        Some(hi) => first_keys.iter().filter(|key| precedes(key, hi)).count(),
    };

    Ok(Some((start..end.max(start)).collect()))
}

/// The value a consumer has to read down to, below `lower_cut`, before it
/// holds `halo_rows` of the rows preceding that cut — at message granularity,
/// from `indexes` alone, fetching no data.
///
/// The scheduler answers the same question from the producer's runtime stats,
/// but only at file granularity: a file is one `[key_min, key_max]` with one
/// row count, so the furthest it can stop is a whole file below the cut. These
/// indexes carry a row per message, so the same walk stops a batch below it.
///
/// A message is counted only when its successor's first key proves it lies
/// wholly below the cut, since messages within a file are sorted. The last
/// message of a file has no successor and so always straddles: counting it
/// would need the file's own largest key, which the index does not carry.
/// Straddlers count zero — over-counting would stop the walk short of the rows
/// the frame asked for — and are read anyway, being inside the band.
///
/// `None` when the messages below the cut don't add up to `halo_rows`: every
/// row down there is needed, and the caller keeps whatever bound it had.
/// Consulting a subset of a consumer's sources is therefore safe, always
/// yielding a bound at or below the one every source would give.
///
/// Ascending keys only, matching the rules that plant a rank halo.
pub fn rank_widened_lower_bound(
    indexes: &[RecordBatch],
    lower_cut: &ScalarValue,
    halo_rows: u64,
) -> Result<Option<ScalarValue>> {
    if halo_rows == 0 {
        return Ok(Some(lower_cut.clone()));
    }
    // (first_key, rows) of every message the walk may count, flattened across
    // files and then ordered rather than merged with a cursor per file: an
    // index is KB against a data file's MB, and the whole set is already in
    // memory by the time this runs.
    let mut countable: Vec<(ScalarValue, u64)> = Vec::new();
    for index in indexes {
        let is_dict = is_dict_column(index)?;
        let num_rows = num_rows_column(index)?;
        let keys = index.column(0);
        // Dictionary rows describe bytes, not data, and carry no key.
        let messages: Vec<usize> = (0..index.num_rows())
            .filter(|row| !is_dict.value(*row))
            .collect();
        for (position, &row) in messages.iter().enumerate() {
            // A NULL first key is the NULL run, which sorts above every cut.
            if keys.is_null(row) {
                continue;
            }
            let first_key = ScalarValue::try_from_array(keys, row)?;
            if first_key.partial_cmp(lower_cut) != Some(std::cmp::Ordering::Less) {
                continue;
            }
            let Some(&successor) = messages.get(position + 1) else {
                continue;
            };
            if keys.is_null(successor) {
                continue;
            }
            // Strictly below: a successor starting *at* the cut leaves this
            // message holding rows at the cut, which are in range, not halo.
            let upper_bound = ScalarValue::try_from_array(keys, successor)?;
            if upper_bound.partial_cmp(lower_cut) != Some(std::cmp::Ordering::Less) {
                continue;
            }
            if num_rows.is_null(row) {
                continue;
            }
            countable.push((first_key, num_rows.value(row)));
        }
    }
    countable.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut rows_found = 0u64;
    let mut bound = None;
    for (first_key, rows) in countable {
        rows_found += rows;
        bound = Some(first_key);
        if rows_found >= halo_rows {
            break;
        }
    }
    Ok(bound.filter(|_| rows_found >= halo_rows))
}

/// Collect each batch's first-row sort key as batches pass through to the
/// writer.
///
/// A stream adapter rather than a hook inside the write path: the keys are the
/// index's business and the IPC framing is the writer's, and keeping them apart
/// means the write path never has to know what the data is sorted on.
///
/// Every batch yields an entry, including empty ones — the IPC writer emits a
/// block for an empty batch too, and the index pairs keys with blocks
/// positionally, so skipping one would shift every key onto the wrong byte
/// range.
pub struct KeyCollector {
    inner: SendableRecordBatchStream,
    ordering: LexOrdering,
    keys: Vec<BatchKey>,
}

impl KeyCollector {
    /// Wrap `inner`, evaluating `ordering` against each batch's first row.
    pub fn new(inner: SendableRecordBatchStream, ordering: LexOrdering) -> Self {
        Self {
            inner,
            ordering,
            keys: Vec::new(),
        }
    }

    /// The keys collected so far, in the order the batches were yielded.
    pub fn keys(&self) -> &[BatchKey] {
        &self.keys
    }

    /// Take ownership of the collected keys.
    pub fn into_keys(self) -> Vec<BatchKey> {
        self.keys
    }

    /// Evaluate the ordering against `batch`'s first row.
    ///
    /// An empty batch has no first row, so its key is all NULL — it selects
    /// nothing on a range search, which is the truth about a block holding no
    /// rows.
    fn key_for(&self, batch: &RecordBatch) -> Result<BatchKey> {
        let num_rows = batch.num_rows() as u64;
        let mut values = Vec::with_capacity(self.ordering.len());
        for sort_expr in self.ordering.iter() {
            if num_rows == 0 {
                let data_type =
                    sort_expr.expr.data_type(batch.schema_ref()).map_err(|e| {
                        BallistaError::General(format!(
                            "range shuffle index: sort expression `{}` has no type: {e}",
                            sort_expr.expr
                        ))
                    })?;
                values.push(ScalarValue::try_from(&data_type)?);
                continue;
            }
            let first_row = batch.slice(0, 1);
            let evaluated = sort_expr.expr.evaluate(&first_row).map_err(|e| {
                BallistaError::General(format!(
                    "range shuffle index: sort expression `{}` failed to evaluate: {e}",
                    sort_expr.expr
                ))
            })?;
            let array = evaluated.into_array(1)?;
            values.push(ScalarValue::try_from_array(&array, 0)?);
        }
        Ok(BatchKey { values, num_rows })
    }
}

impl Stream for KeyCollector {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let key = match self.key_for(&batch) {
                    Ok(key) => key,
                    Err(e) => {
                        return Poll::Ready(Some(Err(e.into_datafusion())));
                    }
                };
                self.keys.push(key);
                Poll::Ready(Some(Ok(batch)))
            }
            other => Poll::Ready(other),
        }
    }
}

impl RecordBatchStream for KeyCollector {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// Build the index's schema for a given ORDER BY.
///
/// Key columns come first, named for the expression they carry so the file
/// says what it is indexed on, followed by the fixed layout columns.
pub fn index_schema(ordering: &LexOrdering, data_schema: &Schema) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(ordering.len() + 4);
    for sort_expr in ordering.iter() {
        let data_type = sort_expr.expr.data_type(data_schema).map_err(|e| {
            BallistaError::General(format!(
                "range shuffle index: sort expression `{}` has no type against the \
                 shuffle schema: {e}",
                sort_expr.expr
            ))
        })?;
        fields.push(Field::new(key_column_name(sort_expr), data_type, true));
    }
    fields.extend(fixed_fields());

    let metadata = std::collections::HashMap::from([(
        SORT_OPTIONS_METADATA.to_string(),
        encode_sort_options(ordering),
    )]);
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

/// Name of the index column carrying `sort_expr`'s values.
fn key_column_name(sort_expr: &PhysicalSortExpr) -> String {
    sort_expr.expr.to_string()
}

/// Render each key's `ASC`/`DESC` and null placement, comma separated and in
/// lexicographic order.
fn encode_sort_options(ordering: &LexOrdering) -> String {
    ordering
        .iter()
        .map(|sort_expr| {
            let direction = if sort_expr.options.descending {
                "desc"
            } else {
                "asc"
            };
            let nulls = if sort_expr.options.nulls_first {
                "nulls_first"
            } else {
                "nulls_last"
            };
            format!("{direction} {nulls}")
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Build the index batch for one data file.
///
/// `keys` are the record batches' first-row keys in write order, which is the
/// order `layout.record_batches` is in — the data file's own footer is what
/// pairs a key with a byte range, so the two are positional and must be the
/// same length.
///
/// Rows come out sorted by byte offset, putting dictionaries ahead of the
/// batches referencing them, so a reader walking the index in order sees the
/// file's layout as it is.
pub fn build_index_batch(
    schema: SchemaRef,
    layout: &FileLayout,
    keys: &[BatchKey],
) -> Result<RecordBatch> {
    if layout.record_batches.len() != keys.len() {
        return Err(BallistaError::General(format!(
            "range shuffle index: {} record blocks against {} keys — the index \
             would pair keys with the wrong byte ranges",
            layout.record_batches.len(),
            keys.len(),
        )));
    }

    let key_count = schema.fields().len() - 4;

    // (offset, len, Some(key index)) for batches, None for dictionaries.
    let mut rows: Vec<(u64, u64, Option<usize>)> =
        Vec::with_capacity(layout.dictionaries.len() + layout.record_batches.len());
    rows.extend(
        layout
            .dictionaries
            .iter()
            .map(|block| (block.offset, block.len, None)),
    );
    rows.extend(
        layout
            .record_batches
            .iter()
            .enumerate()
            .map(|(idx, block)| (block.offset, block.len, Some(idx))),
    );
    rows.sort_by_key(|(offset, _, _)| *offset);

    let mut key_columns: Vec<Vec<ScalarValue>> =
        vec![Vec::with_capacity(rows.len()); key_count];
    let mut is_dict = BooleanBuilder::with_capacity(rows.len());
    let mut byte_offset = UInt64Builder::with_capacity(rows.len());
    let mut byte_len = UInt64Builder::with_capacity(rows.len());
    let mut num_rows = UInt64Builder::with_capacity(rows.len());

    for (offset, len, key_idx) in &rows {
        byte_offset.append_value(*offset);
        byte_len.append_value(*len);
        match key_idx {
            Some(idx) => {
                let key = &keys[*idx];
                if key.values.len() != key_count {
                    return Err(BallistaError::General(format!(
                        "range shuffle index: batch key has {} values against {} \
                         key columns",
                        key.values.len(),
                        key_count,
                    )));
                }
                for (column, value) in key_columns.iter_mut().zip(&key.values) {
                    column.push(value.clone());
                }
                is_dict.append_value(false);
                num_rows.append_value(key.num_rows);
            }
            None => {
                for (column, field) in key_columns.iter_mut().zip(schema.fields()) {
                    column.push(ScalarValue::try_from(field.data_type())?);
                }
                is_dict.append_value(true);
                num_rows.append_null();
            }
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (values, field) in key_columns.into_iter().zip(schema.fields()) {
        columns.push(if values.is_empty() {
            new_null_array(field.data_type(), 0)
        } else {
            ScalarValue::iter_to_array(values)?
        });
    }
    columns.push(Arc::new(is_dict.finish()));
    columns.push(Arc::new(byte_offset.finish()));
    columns.push(Arc::new(byte_len.finish()));
    columns.push(Arc::new(num_rows.finish()));

    RecordBatch::try_new(schema, columns)
        .map_err(|e| BallistaError::General(format!("range shuffle index: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::sort_shuffle::is_sort_shuffle_output;
    use datafusion::arrow::array::{Float64Array, UInt64Array};
    use datafusion::arrow::datatypes::Field;

    /// The positional accessors address the fixed columns by offset past the
    /// keys, so their constants have to agree with the order `fixed_fields`
    /// emits. Reordering one without the other would read the wrong column and
    /// only fail where the types happen to differ.
    #[test]
    fn fixed_column_positions_match_the_field_order() {
        let fields = fixed_fields();
        assert_eq!(fields[IS_DICT_POSITION].name(), IS_DICT_COLUMN);
        assert_eq!(fields[BYTE_OFFSET_POSITION].name(), BYTE_OFFSET_COLUMN);
        assert_eq!(fields[BYTE_LEN_POSITION].name(), BYTE_LEN_COLUMN);
        assert_eq!(fields[NUM_ROWS_POSITION].name(), NUM_ROWS_COLUMN);
    }

    /// A sort key named after a fixed column is legal — Arrow permits
    /// duplicate field names and resolves a lookup to the *first* match, and
    /// key columns sit first. Reading the fixed columns by name would return
    /// the key here; by position it stays correct.
    #[test]
    fn a_key_named_like_a_fixed_column_does_not_shadow_it() {
        let mut fields = vec![Field::new(BYTE_OFFSET_COLUMN, DataType::Float64, true)];
        fields.extend(fixed_fields());
        let index = RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])),
                Arc::new(BooleanArray::from(vec![false, false])),
                Arc::new(UInt64Array::from(vec![100u64, 200])),
                Arc::new(UInt64Array::from(vec![10u64, 20])),
                Arc::new(UInt64Array::from(vec![Some(4u64), Some(5)])),
            ],
        )
        .unwrap();

        assert_eq!(key_column_count(&index).unwrap(), 1);
        assert_eq!(byte_offset_column(&index).unwrap().values(), &[100, 200]);
        assert_eq!(byte_len_column(&index).unwrap().values(), &[10, 20]);
        assert_eq!(num_rows_column(&index).unwrap().values(), &[4, 5]);
        assert!(!is_dict_column(&index).unwrap().value(0));
        assert_eq!(count_record_batches(&index).unwrap(), 2);

        // The by-name lookup this replaced would have found the Float64 key.
        assert_eq!(
            index
                .column_by_name(BYTE_OFFSET_COLUMN)
                .unwrap()
                .data_type(),
            &DataType::Float64,
        );
    }

    /// An index over batches whose first keys are `keys`, laid out as the
    /// writer lays them out.
    fn index_over(keys: &[Option<f64>]) -> RecordBatch {
        let mut fields = vec![Field::new("v", DataType::Float64, true)];
        fields.extend(fixed_fields());
        let schema = Arc::new(Schema::new(fields));
        let rows = keys.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(keys.to_vec())),
                Arc::new(BooleanArray::from(vec![false; rows])),
                Arc::new(UInt64Array::from(
                    (0..rows).map(|r| r as u64 * 100).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(vec![100u64; rows])),
                Arc::new(UInt64Array::from(vec![10u64; rows])),
            ],
        )
        .unwrap()
    }

    fn select(
        keys: &[Option<f64>],
        lo: Option<f64>,
        hi: Option<f64>,
    ) -> Option<Vec<usize>> {
        select_record_batches(
            &index_over(keys),
            lo.map(ScalarValue::from).as_ref(),
            hi.map(ScalarValue::from).as_ref(),
            false,
        )
        .unwrap()
    }

    /// Batch `i` holds rows from its own first key up to the next batch's, so
    /// a range starting mid-batch has to take that batch, not the one after.
    /// Dropping it would silently lose the rows between `lo` and the next key.
    #[test]
    fn selects_every_batch_that_can_hold_a_row_in_range() {
        // First keys 0, 10, 20, 30 — batch 1 holds [10, 20).
        let keys = &[Some(0.0), Some(10.0), Some(20.0), Some(30.0)];

        assert_eq!(select(keys, Some(12.0), Some(19.0)), Some(vec![1]));
        assert_eq!(select(keys, Some(12.0), Some(21.0)), Some(vec![1, 2]));
        // A bound landing exactly on a boundary takes the batch starting there.
        assert_eq!(select(keys, Some(10.0), Some(20.0)), Some(vec![1]));
        // Unbounded below starts at the first batch, above runs to the last.
        assert_eq!(select(keys, None, Some(15.0)), Some(vec![0, 1]));
        assert_eq!(select(keys, Some(25.0), None), Some(vec![2, 3]));
        assert_eq!(select(keys, None, None), Some(vec![0, 1, 2, 3]));
    }

    /// A range below everything or above everything selects nothing rather
    /// than wrapping around or panicking on an empty walk.
    #[test]
    fn selects_nothing_outside_the_files_range() {
        let keys = &[Some(10.0), Some(20.0)];
        assert_eq!(select(keys, Some(0.0), Some(5.0)), Some(vec![]));
        assert_eq!(select(keys, Some(30.0), Some(40.0)), Some(vec![1]));
    }

    /// A NULL first key has no position under value comparison, so the index
    /// declines rather than guessing — the caller reads everything and the
    /// answer stays right.
    #[test]
    fn declines_when_a_key_is_null() {
        let keys = &[Some(10.0), None, Some(30.0)];
        assert_eq!(select(keys, Some(12.0), Some(19.0)), None);
    }

    /// Under DESC the keys walk downward, so "before" flips; selection has to
    /// follow the declared ordering rather than assume ascending.
    #[test]
    fn follows_a_descending_ordering() {
        let index = index_over(&[Some(30.0), Some(20.0), Some(10.0)]);
        let selected = select_record_batches(
            &index,
            Some(&ScalarValue::from(25.0)),
            Some(&ScalarValue::from(15.0)),
            true,
        )
        .unwrap();
        // Descending: `lo` is the high end. 25 sits inside batch 0's [30, 20).
        assert_eq!(selected, Some(vec![0, 1]));
    }

    /// The index sits in the same directory as the data files and ends in
    /// `.arrow` like they do, so its name has to be one no data file can take
    /// and one the sort shuffle's probe does not claim.
    #[test]
    fn index_path_cannot_collide_with_a_data_file() {
        let data = Path::new("/work/job/1/0/data-7.arrow");
        let index = index_path(data);

        assert_eq!(index, Path::new("/work/job/1/0/data-7.rangeidx.arrow"));
        assert_ne!(index, data.to_path_buf());
        assert!(
            !is_sort_shuffle_output(&index),
            "the sort shuffle probes for `.arrow.index`, which this must not be",
        );
    }
}

#[cfg(test)]
mod rank_walk_tests {
    //! `rank_widened_lower_bound` — how far below its cut a bounded-ROWS
    //! consumer has to start reading, resolved from index rows rather than
    //! from the file ranges the scheduler sees.

    use super::*;
    use datafusion::arrow::array::{Float64Array, UInt64Array};

    fn key(value: f64) -> ScalarValue {
        ScalarValue::Float64(Some(value))
    }

    /// An index in file order: `(first_key, num_rows, is_dict)` per message.
    fn index_of(rows: &[(Option<f64>, Option<u64>, bool)]) -> RecordBatch {
        let mut fields = vec![Field::new("v", DataType::Float64, true)];
        fields.extend(fixed_fields());
        let count = rows.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Float64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    (0..count).map(|row| row as u64 * 100).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(vec![100u64; count])),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// Messages, all record batches, none of them a dictionary.
    fn batches(messages: &[(f64, u64)]) -> RecordBatch {
        index_of(
            &messages
                .iter()
                .map(|&(first_key, rows)| (Some(first_key), Some(rows), false))
                .collect::<Vec<_>>(),
        )
    }

    fn walk(indexes: &[RecordBatch], lower_cut: f64, halo_rows: u64) -> Option<f64> {
        match rank_widened_lower_bound(indexes, &key(lower_cut), halo_rows).unwrap() {
            Some(ScalarValue::Float64(Some(bound))) => Some(bound),
            Some(other) => panic!("bound came back as {other}"),
            None => None,
        }
    }

    /// The highest message the walk can prove lies wholly below the cut, whose
    /// own rows already cover what the frame asked for.
    #[test]
    fn stops_at_the_highest_message_that_covers_the_halo() {
        let index = batches(&[(10.0, 500), (300.0, 500), (995.0, 500), (1200.0, 500)]);
        // 995 has a successor at 1200, so it straddles; 300's successor at 995
        // proves it wholly below, and its 500 rows cover the 100 wanted.
        assert_eq!(walk(&[index], 1000.0, 100), Some(300.0));
    }

    /// Draining one file before the next bounds correctly but hundreds of keys
    /// lower, so the walk is over every file's messages in key order.
    #[test]
    fn walks_every_files_messages_in_key_order() {
        let left = batches(&[(100.0, 60), (500.0, 60), (950.0, 60)]);
        let right = batches(&[(200.0, 60), (600.0, 60), (980.0, 60)]);
        // Descending over the countable messages: 600 (60 rows), then 500
        // (120, enough). Draining `left` first would take 500 then 100.
        assert_eq!(walk(&[left, right], 1000.0, 100), Some(500.0));
    }

    /// The last message of a file has no successor to bound it, and the index
    /// carries no file-level maximum, so it can hold rows above the cut.
    #[test]
    fn the_last_message_in_a_file_always_straddles() {
        let index = batches(&[(100.0, 500), (900.0, 500)]);
        // 900's rows are unprovable, so the walk takes 100 instead.
        assert_eq!(walk(&[index], 1000.0, 100), Some(100.0));
    }

    /// A message whose successor starts exactly at the cut holds rows *at* the
    /// cut, which are the consumer's own rows, not its halo.
    #[test]
    fn a_successor_at_the_cut_leaves_a_straddler() {
        let index = batches(&[(100.0, 500), (900.0, 500), (1000.0, 500)]);
        // 900 is bounded by a successor starting at the cut, so the walk falls
        // back to 100 rather than counting it.
        assert_eq!(walk(&[index], 1000.0, 100), Some(100.0));
    }

    /// A dictionary row describes bytes, carries no key, and must not be
    /// mistaken for the successor that bounds the message before it.
    #[test]
    fn a_dictionary_row_is_not_a_successor() {
        let index = index_of(&[
            (Some(100.0), Some(500), false),
            (None, None, true),
            (Some(900.0), Some(500), false),
            (Some(950.0), Some(500), false),
        ]);
        // 100 is bounded by 900, not by the dictionary between them, so it
        // counts; 900 is bounded by 950 and counts too, and being higher it
        // is what the walk stops on.
        assert_eq!(walk(&[index], 1000.0, 100), Some(900.0));
    }

    /// A NULL first key is the NULL run, which sorts above every cut.
    #[test]
    fn a_null_keyed_message_is_not_a_predecessor() {
        let index = index_of(&[
            (Some(100.0), Some(500), false),
            (None, Some(500), false),
            (Some(900.0), Some(500), false),
        ]);
        assert_eq!(walk(&[index], 1000.0, 100), None);
    }

    /// Not enough provable rows below the cut: the caller keeps the bound it
    /// already had rather than being handed a tighter one.
    #[test]
    fn too_few_rows_below_the_cut_gives_no_bound() {
        let index = batches(&[(100.0, 3), (200.0, 3), (900.0, 500)]);
        assert_eq!(walk(&[index], 1000.0, 100), None);
    }

    /// A frame wanting no preceding rows is not widened at all.
    #[test]
    fn no_preceding_rows_leaves_the_cut_alone() {
        let index = batches(&[(100.0, 500), (900.0, 500)]);
        assert_eq!(walk(&[index], 1000.0, 0), Some(1000.0));
    }
}
