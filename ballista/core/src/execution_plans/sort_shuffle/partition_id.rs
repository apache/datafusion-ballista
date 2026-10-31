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

//! Row-routing helpers for `ShuffleRouting::PartitionId`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int8Type, Int16Type, Int32Type,
    Int64Type, Schema, SchemaRef, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::common::{exec_err, internal_err, plan_err};
use datafusion::error::Result;

/// `schema` without the partition-id `column`.
///
/// Errors if `column` is out of range, is not an integer, or is the only
/// column: a shuffle has to carry something besides its routing.
pub(super) fn schema_without_partition_id(
    schema: &Schema,
    column: usize,
) -> Result<SchemaRef> {
    let Some(field) = schema.fields().get(column) else {
        return plan_err!(
            "partition id column {column} is out of range: the input has {} columns",
            schema.fields().len()
        );
    };
    if !field.data_type().is_integer() {
        return plan_err!(
            "partition id column `{}` must be an integer, found {}",
            field.name(),
            field.data_type()
        );
    }
    if schema.fields().len() == 1 {
        return plan_err!(
            "partition id column `{}` is the only column of the input: a shuffle \
             needs at least one other column to carry",
            field.name()
        );
    }
    let kept: Vec<usize> = (0..schema.fields().len())
        .filter(|i| *i != column)
        .collect();
    Ok(Arc::new(schema.project(&kept)?))
}

/// `batch` without `column`, rebuilt under `schema` (from
/// [`schema_without_partition_id`]) so every batch shares one schema allocation.
/// The other columns are moved, not copied. `column` must be in range.
pub(super) fn drop_partition_id(
    batch: RecordBatch,
    column: usize,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let (_, mut columns, _) = batch.into_parts();
    columns.remove(column);
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

/// Empties `out` and sizes it to `num_partitions` lists, keeping each list's
/// capacity so that the steady state allocates nothing.
pub(super) fn reset_buckets(out: &mut Vec<Vec<u32>>, num_partitions: usize) {
    out.resize_with(num_partitions, Vec::new);
    for rows in out.iter_mut() {
        rows.clear();
    }
}

/// Fills `out` with `num_partitions` lists, where list `p` holds the rows of
/// `batch` whose id is `p`, as `compute_partition_indices` does for hashes.
///
/// A null, negative or out-of-range id is an error: placement must be exact, so
/// a stray id is never wrapped or clamped into some partition.
pub(super) fn compute_partition_indices_from_ids(
    batch: &RecordBatch,
    column: usize,
    num_partitions: usize,
    out: &mut Vec<Vec<u32>>,
) -> Result<()> {
    let Some(ids) = batch.columns().get(column) else {
        return internal_err!(
            "partition id column {column} is missing from a batch of {} columns",
            batch.num_columns()
        );
    };

    reset_buckets(out, num_partitions);

    if ids.null_count() > 0 {
        let first_null = ids.nulls().and_then(|nulls| nulls.iter().position(|v| !v));
        return exec_err!("partition id is null at row {}", first_null.unwrap_or(0));
    }

    match ids.data_type() {
        DataType::Int8 => push_rows::<Int8Type>(ids, out),
        DataType::Int16 => push_rows::<Int16Type>(ids, out),
        DataType::Int32 => push_rows::<Int32Type>(ids, out),
        DataType::Int64 => push_rows::<Int64Type>(ids, out),
        DataType::UInt8 => push_rows::<UInt8Type>(ids, out),
        DataType::UInt16 => push_rows::<UInt16Type>(ids, out),
        DataType::UInt32 => push_rows::<UInt32Type>(ids, out),
        DataType::UInt64 => push_rows::<UInt64Type>(ids, out),
        other => internal_err!("partition id column has non-integer type {other}"),
    }
}

/// Pushes each row index onto the `out` entry its id names.
fn push_rows<T: ArrowPrimitiveType>(ids: &ArrayRef, out: &mut [Vec<u32>]) -> Result<()> {
    let num_partitions = out.len();
    for (row, id) in ids.as_primitive::<T>().values().iter().enumerate() {
        // `to_usize` is `None` for a negative id, so one bound covers both ends.
        match id.to_usize().and_then(|partition| out.get_mut(partition)) {
            Some(rows) => rows.push(row as u32),
            None => {
                return exec_err!(
                    "partition id {id:?} at row {row} is outside 0..{num_partitions}"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Int8Array, Int16Array, Int32Array, Int64Array, StringArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::datatypes::Field;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("pid", DataType::Int32, false),
            Field::new("w", DataType::Int64, true),
        ])
    }

    fn batch_of(ids: ArrayRef) -> RecordBatch {
        let rows = ids.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("pid", ids.data_type().clone(), true),
        ]));
        let values: Vec<String> = (0..rows).map(|i| format!("r{i}")).collect();
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values)), ids])
            .unwrap()
    }

    fn indices(batch: &RecordBatch, num_partitions: usize) -> Result<Vec<Vec<u32>>> {
        let mut out = Vec::new();
        compute_partition_indices_from_ids(batch, 1, num_partitions, &mut out)?;
        Ok(out)
    }

    #[test]
    fn schema_without_partition_id_drops_only_that_column() {
        let schema = schema_without_partition_id(&schema(), 1).unwrap();

        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["v", "w"]);
    }

    #[test]
    fn schema_without_partition_id_keeps_metadata() {
        let metadata = [("k".to_string(), "v".to_string())].into();
        let schema = schema().with_metadata(metadata);

        let projected = schema_without_partition_id(&schema, 1).unwrap();

        assert_eq!(projected.metadata().get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn schema_without_partition_id_rejects_a_missing_column() {
        let err = schema_without_partition_id(&schema(), 3).unwrap_err();

        assert!(err.to_string().contains("out of range"), "actual: {err}");
    }

    #[test]
    fn schema_without_partition_id_rejects_a_non_integer_column() {
        // Column 0 is a string, column 2 is an integer: only the string is refused.
        let err = schema_without_partition_id(&schema(), 0).unwrap_err();

        assert!(
            err.to_string().contains("must be an integer"),
            "actual: {err}"
        );
        assert!(schema_without_partition_id(&schema(), 2).is_ok());
    }

    #[test]
    fn drop_partition_id_keeps_the_other_columns_and_the_row_count() {
        let batch = batch_of(Arc::new(Int32Array::from(vec![2, 0, 1])));
        let schema = schema_without_partition_id(&batch.schema(), 1).unwrap();

        let dropped = drop_partition_id(batch.clone(), 1, &schema).unwrap();

        assert_eq!(dropped.schema(), schema);
        assert_eq!(dropped.num_columns(), 1);
        assert_eq!(dropped.num_rows(), 3);
        assert_eq!(dropped.column(0), batch.column(0));
    }

    #[test]
    fn schema_without_partition_id_rejects_an_input_with_only_the_id_column() {
        let schema = Schema::new(vec![Field::new("pid", DataType::Int32, false)]);

        let err = schema_without_partition_id(&schema, 0).unwrap_err();

        assert!(err.to_string().contains("only column"), "actual: {err}");
    }

    #[test]
    fn routes_each_row_to_the_partition_its_id_names() {
        let batch = batch_of(Arc::new(Int32Array::from(vec![2, 0, 2, 3, 0])));

        let out = indices(&batch, 4).unwrap();

        assert_eq!(out, vec![vec![1, 4], vec![], vec![0, 2], vec![3]]);
    }

    #[test]
    fn routes_every_integer_width() {
        let ids: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![1, 0])),
            Arc::new(Int16Array::from(vec![1, 0])),
            Arc::new(Int32Array::from(vec![1, 0])),
            Arc::new(Int64Array::from(vec![1, 0])),
            Arc::new(UInt8Array::from(vec![1, 0])),
            Arc::new(UInt16Array::from(vec![1, 0])),
            Arc::new(UInt32Array::from(vec![1, 0])),
            Arc::new(UInt64Array::from(vec![1, 0])),
        ];

        for ids in ids {
            let ty = ids.data_type().clone();
            let out = indices(&batch_of(ids), 2).unwrap();
            assert_eq!(out, vec![vec![1], vec![0]], "{ty}");
        }
    }

    /// `out` is reused across batches, so a second call must not see the first
    /// call's rows or partition count.
    #[test]
    fn reuses_the_output_buffer_between_batches() {
        let mut out = Vec::new();
        let first = batch_of(Arc::new(Int32Array::from(vec![0, 1, 2])));
        compute_partition_indices_from_ids(&first, 1, 3, &mut out).unwrap();

        let second = batch_of(Arc::new(Int32Array::from(vec![1, 1])));
        compute_partition_indices_from_ids(&second, 1, 2, &mut out).unwrap();

        assert_eq!(out, vec![vec![], vec![0, 1]]);
    }

    #[test]
    fn rejects_an_id_past_the_last_partition() {
        let batch = batch_of(Arc::new(Int32Array::from(vec![0, 4])));

        let err = indices(&batch, 4).unwrap_err().to_string();

        assert!(err.contains("partition id 4 at row 1"), "actual: {err}");
        assert!(err.contains("outside 0..4"), "actual: {err}");
    }

    #[test]
    fn rejects_a_negative_id() {
        let batch = batch_of(Arc::new(Int64Array::from(vec![0, -1])));

        let err = indices(&batch, 4).unwrap_err().to_string();

        assert!(err.contains("partition id -1 at row 1"), "actual: {err}");
    }

    #[test]
    fn rejects_a_null_id() {
        let batch = batch_of(Arc::new(Int32Array::from(vec![Some(0), None, Some(1)])));

        let err = indices(&batch, 4).unwrap_err().to_string();

        assert!(
            err.contains("partition id is null at row 1"),
            "actual: {err}"
        );
    }

    #[test]
    fn a_nullable_column_without_nulls_is_fine() {
        // `batch_of` marks the id nullable; having no nulls is what matters.
        let batch = batch_of(Arc::new(Int32Array::from(vec![Some(1), Some(0)])));

        assert_eq!(indices(&batch, 2).unwrap(), vec![vec![1], vec![0]]);
    }
}
