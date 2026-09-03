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

//! Range shuffle: a passthrough shuffle whose output can be seeked into.
//!
//! A stage that routes rows by value range (`OrderedRangeRepartitionExec`)
//! hands each consumer a set of producer files, of which the consumer wants
//! only the rows inside its own cut range. It reads them whole and drops the
//! rest — measured on h2o Q8 at 1e8 rows, the consumer fetches 8.2 GB to feed
//! a window that needs 3.6 GB.
//!
//! Skipping the rest means seeking, and seeking means the data file has to
//! carry a chunk index. The passthrough shuffle's Arrow IPC **stream** format
//! has none. This module writes the IPC **file** format instead, whose footer
//! lists a `Block { offset, metadata_length, body_length }` per record batch.
//!
//! Reads are still whole-file: the point of this layer is the substrate, not
//! the saving. What it establishes is a footer to seek against, read back once
//! per file so the byte range of every record batch — and of every dictionary
//! those batches reference — is known exactly. That layout is what the value
//! index turns a consumer's cut range into.

mod index;
mod ipc_file;
mod reader;
mod remote;
mod writer;

// The fixed layout columns are reached through the positional accessors, not
// by name: key columns are named for the user's sort expressions and sit ahead
// of them, and Arrow resolves a duplicate name to the first match. The name
// constants stay internal so no caller outside can reintroduce that lookup.
pub use index::{
    BatchKey, KeyCollector, SORT_OPTIONS_METADATA, build_index_batch, byte_len_column,
    byte_offset_column, count_record_batches, has_range_index, index_path, index_schema,
    is_dict_column, key_column_count, num_rows_column, read_index_file,
    select_record_batches, write_index_file,
};
pub use ipc_file::{
    FileLayout, MessageBlock, is_ipc_file, open_ipc_file, read_file_layout,
    write_stream_to_ipc_file,
};
pub use reader::{SelectedBatches, open_ipc_file_range, select_local_batches};
pub use remote::{byte_ranges_for, covers, schema_message};
pub use writer::RangeShuffleWriterExec;
