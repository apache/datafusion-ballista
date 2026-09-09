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

//! Reading only the part of a range shuffle file a consumer's range covers.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use log::debug;

use crate::error::Result;

use super::index::{
    SORT_OPTIONS_METADATA, has_range_index, index_path, read_index_file,
    select_record_batches,
};
use super::ipc_file::open_ipc_file;

/// Yields the record batches at `ordinals`, seeking to each.
///
/// The seek is `FileReader::set_index`, which resolves an ordinal against the
/// file's own footer — read once when the file was opened. The index's byte
/// offsets are what a remote reader needs, where paying for a footer read
/// defeats the point; a local reader already has it.
pub struct SelectedBatches {
    reader: FileReader<BufReader<File>>,
    ordinals: std::vec::IntoIter<usize>,
}

impl Iterator for SelectedBatches {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let ordinal = self.ordinals.next()?;
        if let Err(e) = self.reader.set_index(ordinal) {
            return Some(Err(e));
        }
        self.reader.next()
    }
}

/// What a data file's index says about the batches covering `[lo, hi)`.
///
/// `None` means read the whole file: either there is no index beside it, or
/// the index declined to narrow the range. Both are correctness-preserving —
/// only the saving is lost.
pub fn select_local_batches(
    data_path: &Path,
    lo: Option<&ScalarValue>,
    hi: Option<&ScalarValue>,
) -> Result<Option<Vec<usize>>> {
    if !has_range_index(data_path) {
        return Ok(None);
    }
    let index = read_index_file(&index_path(data_path))?;
    let descending = index
        .schema()
        .metadata()
        .get(SORT_OPTIONS_METADATA)
        .and_then(|options| options.split(',').next().map(str::to_owned))
        .is_some_and(|options| options.starts_with("desc"));

    select_record_batches(&index, lo, hi, descending)
}

/// Open a range shuffle file, reading only the batches covering `[lo, hi)`.
///
/// Selection is at batch granularity, so the stream still carries rows outside
/// the range at either edge — the `RangeFilterExec` above the reader does the
/// exact trim, as it does for a whole-file read.
pub fn open_ipc_file_range(
    data_path: &Path,
    lo: Option<&ScalarValue>,
    hi: Option<&ScalarValue>,
) -> Result<SelectedBatches> {
    let reader = open_ipc_file(data_path)?;
    let ordinals = match select_local_batches(data_path, lo, hi)? {
        Some(ordinals) => {
            debug!(
                "range shuffle reading {} of {} batches from {data_path:?}",
                ordinals.len(),
                reader.num_batches(),
            );
            ordinals
        }
        None => (0..reader.num_batches()).collect(),
    };
    Ok(SelectedBatches {
        reader,
        ordinals: ordinals.into_iter(),
    })
}
