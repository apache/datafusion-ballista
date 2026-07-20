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

// ProbeTable is the in-memory correctness core of the spilling hash join: an
// inner-join hash table built over one resident build-side bucket, probed
// with a probe-side batch. It is deliberately agnostic to spilling — the
// stream (a later task) is responsible for choosing which build bucket is
// resident and feeding it, along with matching probe batches, to this table.
//
// Equality between build and probe keys is resolved via Arrow's row format
// (`arrow::row::RowConverter`) rather than a per-`DataType` match, so any key
// type combination Arrow can encode into row format works without dedicated
// dispatch code here. The one correctness wrinkle this creates: Arrow's row
// encoding treats two nulls as equal, which is wrong for
// `NullEquality::NullEqualsNothing` (the only null semantics this join
// supports; see `SpillingHashJoinExec`). This table therefore tracks, per
// side, which rows have a null in any key column, and excludes those rows
// from both hash-table insertion (build side) and matching (probe side).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExprRef;
use smallvec::SmallVec;

/// An in-memory hash table over one resident build-side bucket, used to
/// probe matching rows from a probe-side batch for an inner join with
/// `NullEquality::NullEqualsNothing` semantics.
pub struct ProbeTable {
    /// The build-side bucket this table was built from, kept so
    /// `assemble_output` can later `take` its columns by matched row index.
    build: RecordBatch,
    /// Row-encoded build key columns, indexed the same as `build`. Shares a
    /// `RowConverter` with the probe side (see `probe`) so encodings are
    /// directly comparable.
    build_rows: Rows,
    /// The row converter used to encode `build_rows`, reused to encode
    /// probe keys so both sides use identical row-format parameters.
    converter: RowConverter,
    /// `hash -> build row indices`, populated only for build rows with no
    /// null in any key column.
    map: HashMap<u64, SmallVec<[u32; 1]>>,
}

impl ProbeTable {
    /// Builds a table from one resident build bucket (already concatenated)
    /// and the per-row hash values computed for `keys` on that batch (e.g.
    /// by `RowPartitioner`).
    pub fn build(
        build: RecordBatch,
        build_hashes: &[u64],
        keys: &[PhysicalExprRef],
    ) -> Result<Self> {
        let key_arrays = evaluate_keys(keys, &build)?;
        let has_null_key = has_null_key_mask(&key_arrays);

        let converter = RowConverter::new(sort_fields(&key_arrays))?;
        let build_rows = converter.convert_columns(&key_arrays)?;

        let mut map: HashMap<u64, SmallVec<[u32; 1]>> = HashMap::new();
        for (i, &is_null_key) in has_null_key.iter().enumerate() {
            // A null key never matches anything under NullEquality::NullEqualsNothing,
            // so it must never become a matchable build-side entry.
            if is_null_key {
                continue;
            }
            map.entry(build_hashes[i]).or_default().push(i as u32);
        }

        Ok(Self {
            build,
            build_rows,
            converter,
            map,
        })
    }

    /// Probes one probe-side batch against this table, returning the
    /// matched `(build_row, probe_row)` index pairs as two parallel
    /// `UInt32Array`s. Order within a matched key group is unspecified.
    pub fn probe(
        &self,
        probe: &RecordBatch,
        probe_hashes: &[u64],
        keys: &[PhysicalExprRef],
    ) -> Result<(UInt32Array, UInt32Array)> {
        let probe_key_arrays = evaluate_keys(keys, probe)?;
        let has_null_key = has_null_key_mask(&probe_key_arrays);

        // Reuse the build side's converter so both sides use identical row
        // encoding parameters and their `Rows` are directly comparable.
        let probe_rows = self.converter.convert_columns(&probe_key_arrays)?;

        let mut build_out: Vec<u32> = Vec::new();
        let mut probe_out: Vec<u32> = Vec::new();

        for (j, &is_null_key) in has_null_key.iter().enumerate() {
            // A null key never matches anything under NullEquality::NullEqualsNothing.
            if is_null_key {
                continue;
            }
            let Some(candidates) = self.map.get(&probe_hashes[j]) else {
                continue;
            };
            let probe_row = probe_rows.row(j);
            for &i in candidates {
                // The hash lookup above is only a pre-filter: distinct keys can
                // share a hash value, so equality must still be resolved on the
                // actual encoded row values before counting a match.
                if self.build_rows.row(i as usize) == probe_row {
                    build_out.push(i);
                    probe_out.push(j as u32);
                }
            }
        }

        Ok((UInt32Array::from(build_out), UInt32Array::from(probe_out)))
    }

    /// Returns the resident build-side batch this table was built from, so
    /// callers (e.g. the join stream) can pass it to `assemble_output`
    /// without keeping a separate copy.
    pub fn build_batch(&self) -> &RecordBatch {
        &self.build
    }
}

/// Evaluates `keys` against `batch`, returning one array per key expression.
fn evaluate_keys(keys: &[PhysicalExprRef], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    let num_rows = batch.num_rows();
    keys.iter()
        .map(|expr| expr.evaluate(batch)?.into_array(num_rows))
        .collect()
}

/// Returns one `SortField` per key array, describing its data type to
/// `RowConverter`. Sort options are irrelevant here: rows are only ever
/// compared for equality, never ordered.
fn sort_fields(key_arrays: &[ArrayRef]) -> Vec<SortField> {
    key_arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect()
}

/// Returns a per-row mask that is `true` iff the row is null in at least one
/// of `key_arrays`. Used to exclude such rows from matching under
/// `NullEquality::NullEqualsNothing`, since Arrow's row encoding otherwise
/// treats two nulls as equal.
fn has_null_key_mask(key_arrays: &[ArrayRef]) -> Vec<bool> {
    let num_rows = key_arrays.first().map(|a| a.len()).unwrap_or(0);
    let mut mask = vec![false; num_rows];
    for arr in key_arrays {
        if arr.null_count() == 0 {
            continue;
        }
        for (i, is_null_key) in mask.iter_mut().enumerate() {
            *is_null_key = *is_null_key || arr.is_null(i);
        }
    }
    mask
}

/// Assembles a joined output batch from matched `(build_row, probe_row)`
/// index pairs: `take`s `build`'s columns by `build_rows`, `take`s `probe`'s
/// columns by `probe_rows`, and concatenates them left-then-right into
/// `schema`.
pub fn assemble_output(
    schema: &SchemaRef,
    build: &RecordBatch,
    probe: &RecordBatch,
    build_rows: &UInt32Array,
    probe_rows: &UInt32Array,
) -> Result<RecordBatch> {
    let bcols = build
        .columns()
        .iter()
        .map(|c| take(c, build_rows, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let pcols = probe
        .columns()
        .iter()
        .map(|c| take(c, probe_rows, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        [bcols, pcols].concat(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::hash_utils::{RandomState, create_hashes};
    use datafusion::physical_expr::expressions::Column;

    /// Same fixed seed `RowPartitioner` uses, so tests hash build/probe
    /// batches the way the real pipeline would. The exact value doesn't
    /// matter for these tests (only that build/probe use the same one) but
    /// reusing it documents the intended caller contract.
    const SEED: u64 = 0x5350_4a5f_484a_3121;

    fn batch_i32(name: &str, values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn batch_two_i32(
        names: (&str, &str),
        a: Vec<Option<i32>>,
        b: Vec<Option<i32>>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(names.0, DataType::Int32, true),
            Field::new(names.1, DataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
        )
        .unwrap()
    }

    fn batch_utf8(name: &str, values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    fn hashes(batch: &RecordBatch, keys: &[PhysicalExprRef]) -> Vec<u64> {
        let arrays = evaluate_keys(keys, batch).unwrap();
        let mut out = vec![0u64; batch.num_rows()];
        create_hashes(&arrays, &RandomState::with_seed(SEED), &mut out).unwrap();
        out
    }

    fn sorted_pairs(
        build_rows: &UInt32Array,
        probe_rows: &UInt32Array,
    ) -> Vec<(u32, u32)> {
        let mut pairs: Vec<(u32, u32)> = build_rows
            .values()
            .iter()
            .copied()
            .zip(probe_rows.values().iter().copied())
            .collect();
        pairs.sort_unstable();
        pairs
    }

    fn single_key(name: &str) -> Vec<PhysicalExprRef> {
        vec![Arc::new(Column::new(name, 0))]
    }

    #[test]
    fn inner_join_single_key_matches() {
        let keys = single_key("k");
        // build k=[1,2,2,3], probe k=[2,3,4]
        let build = batch_i32("k", vec![Some(1), Some(2), Some(2), Some(3)]);
        let probe = batch_i32("k", vec![Some(2), Some(3), Some(4)]);

        let build_hashes = hashes(&build, &keys);
        let probe_hashes = hashes(&probe, &keys);

        let table = ProbeTable::build(build, &build_hashes, &keys).unwrap();
        let (build_rows, probe_rows) = table.probe(&probe, &probe_hashes, &keys).unwrap();

        // expect pairs: probe 2 -> build rows {1,2}; probe 3 -> build row {3}; probe 4 -> none
        assert_eq!(
            sorted_pairs(&build_rows, &probe_rows),
            vec![(1, 0), (2, 0), (3, 1)]
        );
    }

    #[test]
    fn multi_key_and_nulls_never_match() {
        let keys = vec![
            Arc::new(Column::new("a", 0)) as PhysicalExprRef,
            Arc::new(Column::new("b", 1)) as PhysicalExprRef,
        ];

        // Build row 0: (1,10) - a valid, matchable row.
        // Build row 1: (NULL,20) - null in `a`.
        // Build row 2: (1,NULL) - null in `b`.
        let build = batch_two_i32(
            ("a", "b"),
            vec![Some(1), None, Some(1)],
            vec![Some(10), Some(20), None],
        );
        // Probe row 0: (1,10) - matches build row 0.
        // Probe row 1: (NULL,20) - null in `a`; must NOT match build row 1
        // even though both are (NULL,20).
        // Probe row 2: (1,NULL) - null in `b`; must NOT match build row 2
        // even though both are (1,NULL).
        let probe = batch_two_i32(
            ("a", "b"),
            vec![Some(1), None, Some(1)],
            vec![Some(10), Some(20), None],
        );

        let build_hashes = hashes(&build, &keys);
        let probe_hashes = hashes(&probe, &keys);

        let table = ProbeTable::build(build, &build_hashes, &keys).unwrap();
        let (build_rows, probe_rows) = table.probe(&probe, &probe_hashes, &keys).unwrap();

        assert_eq!(sorted_pairs(&build_rows, &probe_rows), vec![(0, 0)]);
    }

    #[test]
    fn hash_collision_resolved_by_key_equality() {
        // Two build rows with distinct keys but forced to share a hash
        // value; the probe key equal to only one of them must match just
        // that row, proving equality is resolved on actual key values, not
        // just the (colliding) hash.
        let keys = single_key("k");
        let build = batch_i32("k", vec![Some(1), Some(2)]);
        let probe = batch_i32("k", vec![Some(2)]);

        let build_hashes = vec![42u64, 42u64];
        let probe_hashes = vec![42u64];

        let table = ProbeTable::build(build, &build_hashes, &keys).unwrap();
        let (build_rows, probe_rows) = table.probe(&probe, &probe_hashes, &keys).unwrap();

        assert_eq!(sorted_pairs(&build_rows, &probe_rows), vec![(1, 0)]);
    }

    #[test]
    fn string_key_matches() {
        let keys = single_key("k");
        let build = batch_utf8("k", vec![Some("a"), Some("b"), Some("b")]);
        let probe = batch_utf8("k", vec![Some("b"), Some("c")]);

        let build_hashes = hashes(&build, &keys);
        let probe_hashes = hashes(&probe, &keys);

        let table = ProbeTable::build(build, &build_hashes, &keys).unwrap();
        let (build_rows, probe_rows) = table.probe(&probe, &probe_hashes, &keys).unwrap();

        assert_eq!(sorted_pairs(&build_rows, &probe_rows), vec![(1, 0), (2, 0)]);
    }

    #[test]
    fn assemble_output_concatenates_left_then_right() {
        let build_schema =
            Arc::new(Schema::new(vec![Field::new("bk", DataType::Int32, false)]));
        let build = RecordBatch::try_new(
            Arc::clone(&build_schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        let probe_schema =
            Arc::new(Schema::new(vec![Field::new("pk", DataType::Int32, false)]));
        let probe = RecordBatch::try_new(
            Arc::clone(&probe_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let out_schema = Arc::new(Schema::new(vec![
            Field::new("bk", DataType::Int32, false),
            Field::new("pk", DataType::Int32, false),
        ]));

        let build_rows = UInt32Array::from(vec![2, 0]);
        let probe_rows = UInt32Array::from(vec![1, 2]);

        let out = assemble_output(&out_schema, &build, &probe, &build_rows, &probe_rows)
            .unwrap();

        assert_eq!(out.num_rows(), 2);
        let bk = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let pk = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(bk.values(), &[30, 10]);
        assert_eq!(pk.values(), &[2, 3]);
    }
}
