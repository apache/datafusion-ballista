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

//! This module contains execution plans that are needed to distribute DataFusion's execution plans into
//! several Ballista executors.

mod distributed_query;
mod shuffle_reader;
mod shuffle_writer;
mod shuffle_writer_trait;
pub mod sort_shuffle;
mod unresolved_shuffle;

use std::path::{Path, PathBuf};

use datafusion::common::exec_err;
pub use distributed_query::DistributedQueryExec;
pub use shuffle_reader::ShuffleReaderExec;
pub use shuffle_reader::{stats_for_partition, stats_for_partitions};
pub use shuffle_writer::ShuffleWriterExec;
pub use shuffle_writer_trait::ShuffleWriter;
pub use sort_shuffle::SortShuffleWriterExec;
pub use unresolved_shuffle::UnresolvedShuffleExec;

/// Creates the file path for a shuffle output partition.
///
/// The path structure depends on the shuffle type:
///
/// - **Hash shuffle** (`is_sort_shuffle = false`): produces one directory per output
///   partition; `partition_id` is always part of the path. `file_id` is an optional
///   sequence number used when a single partition is written in multiple files:
///   - With `file_id`: `{work_dir}/{job_id}/{stage_id}/{partition_id}/data-{file_id}.arrow`
///   - Without `file_id`: `{work_dir}/{job_id}/{stage_id}/{partition_id}/data.arrow`
///
/// - **Sort shuffle** (`is_sort_shuffle = true`): produces a single output partition,
///   so `partition_id` is **ignored** and not included in the path. `file_id` acts as a
///   file sequence counter and is **required**:
///   - With `file_id`: `{work_dir}/{job_id}/{stage_id}/{file_id}/data.arrow`
///   - Without `file_id`: returns an error
///
/// # Arguments
///
/// - `work_dir` — base directory where shuffle files are written
/// - `job_id` — unique identifier for the job
/// - `stage_id` — stage within the job that produced this shuffle output
/// - `partition_id` — output partition index; used by hash shuffle only, ignored for sort shuffle
/// - `file_id` — file sequence number; optional for hash shuffle, required for sort shuffle
/// - `is_sort_shuffle` — selects between sort-shuffle and hash-shuffle path layout
pub fn create_shuffle_path<P: AsRef<Path>>(
    work_dir: P,
    job_id: &str,
    stage_id: usize,
    partition_id: usize,
    file_id: Option<u64>,
    is_sort_shuffle: bool,
) -> datafusion::error::Result<PathBuf> {
    let mut path = PathBuf::new();

    path.push(work_dir);
    path.push(job_id);
    path.push(stage_id.to_string());

    match (file_id, is_sort_shuffle) {
        (Some(file_id), false) => {
            path.push(partition_id.to_string());
            path.push(format!("data-{}.arrow", file_id));
        }
        (Some(file_id), true) => {
            path.push(file_id.to_string());
            path.push("data.arrow");
        }
        (None, false) => {
            path.push(partition_id.to_string());
            path.push("data.arrow");
        }
        (None, true) => {
            exec_err!("cant create path for sort shuffle without file_id provided")?
        }
    }

    Ok(path)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_regular_shuffle_with_file_id() {
        let path = create_shuffle_path("/work", "job1", 2, 3, Some(42), false).unwrap();
        assert_eq!(path, PathBuf::from("/work/job1/2/3/data-42.arrow"));
    }

    #[test]
    fn test_regular_shuffle_without_file_id() {
        let path = create_shuffle_path("/work", "job1", 2, 3, None, false).unwrap();
        assert_eq!(path, PathBuf::from("/work/job1/2/3/data.arrow"));
    }

    #[test]
    fn test_sort_shuffle_with_file_id() {
        let path = create_shuffle_path("/work", "job1", 2, 3, Some(42), true).unwrap();
        assert_eq!(path, PathBuf::from("/work/job1/2/42/data.arrow"));
    }

    #[test]
    fn test_sort_shuffle_without_file_id_returns_error() {
        let result = create_shuffle_path("/work", "job1", 2, 3, None, true);
        assert!(result.is_err());
    }

    /// Verifies that even when the root directory `/` is used as `work_dir`, the
    /// returned path always has a parent (i.e. the file is never at the filesystem root).
    #[test]
    fn test_root_work_dir_path_has_parent() {
        for (file_id, is_sort_shuffle) in
            [(Some(1), false), (None, false), (Some(1), true)]
        {
            let path =
                create_shuffle_path("/", "job1", 2, 3, file_id, is_sort_shuffle).unwrap();
            assert!(
                path.parent().is_some(),
                "path {path:?} (file_id={file_id:?}, is_sort_shuffle={is_sort_shuffle}) has no parent"
            );
            assert_ne!(
                path.parent().unwrap(),
                std::path::Path::new("/"),
                "path {path:?} parent is root — expected deeper nesting"
            );
        }
    }
}
