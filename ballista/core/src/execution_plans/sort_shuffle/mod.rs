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

//! Sort-based shuffle implementation for Ballista.
//!
//! This module provides an alternative to the hash-based shuffle that writes
//! a single consolidated file per input partition (sorted by output partition ID)
//! along with an index file mapping partition IDs to byte offsets.
//!
//! This approach reduces file count from `N × M` (N input partitions × M output partitions)
//! to `2 × N` files (one data + one index per input partition).

mod buffer;
mod config;
mod index;
mod reader;
mod spill;
mod writer;

pub use buffer::PartitionBuffer;
pub use config::SortShuffleConfig;
pub use index::ShuffleIndex;
pub use reader::{
    get_index_path, is_sort_shuffle_output, read_all_batches, read_sort_shuffle_partition,
};
pub use spill::SpillManager;
pub use writer::SortShuffleWriterExec;
