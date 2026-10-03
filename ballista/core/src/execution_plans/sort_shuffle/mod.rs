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
//! Every hash-repartitioning stage writes its output through this module.
//! Each task writes a single consolidated `data.arrow` file holding all K
//! output partitions back-to-back (partition-major, so each partition's rows
//! from every input partition the task drained are contiguous), along with a
//! `data.arrow.index` file mapping each output partition ID to its byte range.
//!
//! This keeps the file count at `2 × T` (one data + one index per task)
//! rather than `T × K` (T tasks × K output partitions) for a
//! one-file-per-partition layout.
//!
//! The algorithm follows the approach used by Apache Spark: each task buffers
//! its rows by target partition in memory, spilling to disk when they don't
//! fit, and at end of input writes every partition, buffered and spilled, into
//! the single file in partition order. On the reduce side, the task for output
//! partition k reads the `index[k]` byte range from each upstream task's file.

mod buffer;
mod config;
mod index;
mod multi_stream_reader;
mod partitioned_batch_iterator;
mod reader;
mod spill;
mod writer;

pub use buffer::BufferedBatches;
pub use config::SortShuffleConfig;
pub use index::ShuffleIndex;
pub use reader::{get_index_path, is_sort_shuffle_output, stream_sort_shuffle_partition};
pub use spill::SpillManager;
pub use writer::SortShuffleWriterExec;
