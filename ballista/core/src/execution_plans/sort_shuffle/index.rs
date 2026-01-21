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

//! Shuffle index file for sort-based shuffle.
//!
//! The index file stores byte offsets for each partition in the consolidated
//! shuffle data file. Format:
//!
//! ```text
//! [i64: offset_0][i64: offset_1]...[i64: offset_n][i64: total_length]
//! ```
//!
//! - All values are little-endian i64
//! - `offset_i` = byte offset where partition `i` starts
//! - Last entry is total file length
//! - Partition `i` data spans `[offset_i, offset_{i+1})`

use crate::error::{BallistaError, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Shuffle index that maps partition IDs to byte offsets in the data file.
#[derive(Debug, Clone)]
pub struct ShuffleIndex {
    /// Byte offsets for each partition. Length is partition_count + 1,
    /// where the last entry is the total file length.
    offsets: Vec<i64>,
}

impl ShuffleIndex {
    /// Creates a new shuffle index for the given number of partitions.
    ///
    /// All offsets are initialized to 0.
    pub fn new(partition_count: usize) -> Self {
        Self {
            offsets: vec![0i64; partition_count + 1],
        }
    }

    /// Returns the number of partitions in this index.
    pub fn partition_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Sets the byte offset for a partition.
    ///
    /// # Panics
    /// Panics if `partition_id >= partition_count`.
    pub fn set_offset(&mut self, partition_id: usize, offset: i64) {
        self.offsets[partition_id] = offset;
    }

    /// Sets the total file length (stored as the last entry).
    pub fn set_total_length(&mut self, length: i64) {
        if let Some(last) = self.offsets.last_mut() {
            *last = length;
        }
    }

    /// Returns the byte range `(start, end)` for the given partition.
    ///
    /// The partition data spans `[start, end)` bytes in the data file.
    ///
    /// # Panics
    /// Panics if `partition_id >= partition_count`.
    pub fn get_partition_range(&self, partition_id: usize) -> (i64, i64) {
        (self.offsets[partition_id], self.offsets[partition_id + 1])
    }

    /// Returns the size in bytes for the given partition.
    pub fn get_partition_size(&self, partition_id: usize) -> i64 {
        let (start, end) = self.get_partition_range(partition_id);
        end - start
    }

    /// Returns true if the partition has data (size > 0).
    pub fn partition_has_data(&self, partition_id: usize) -> bool {
        self.get_partition_size(partition_id) > 0
    }

    /// Writes the index to a file.
    pub fn write_to_file(&self, path: &Path) -> Result<()> {
        let file = File::create(path).map_err(BallistaError::IoError)?;
        let mut writer = BufWriter::new(file);

        for &offset in &self.offsets {
            writer
                .write_all(&offset.to_le_bytes())
                .map_err(BallistaError::IoError)?;
        }

        writer.flush().map_err(BallistaError::IoError)?;
        Ok(())
    }

    /// Reads an index from a file.
    pub fn read_from_file(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(BallistaError::IoError)?;
        let metadata = file.metadata().map_err(BallistaError::IoError)?;
        let file_size = metadata.len() as usize;

        // Each offset is 8 bytes (i64)
        if !file_size.is_multiple_of(8) {
            return Err(BallistaError::General(format!(
                "Invalid index file size: {file_size} (must be multiple of 8)"
            )));
        }

        let entry_count = file_size / 8;
        if entry_count < 2 {
            return Err(BallistaError::General(format!(
                "Index file too small: {entry_count} entries (need at least 2)"
            )));
        }

        let mut reader = BufReader::new(file);
        let mut offsets = Vec::with_capacity(entry_count);
        let mut buf = [0u8; 8];

        for _ in 0..entry_count {
            reader
                .read_exact(&mut buf)
                .map_err(BallistaError::IoError)?;
            offsets.push(i64::from_le_bytes(buf));
        }

        Ok(Self { offsets })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_new_index() {
        let index = ShuffleIndex::new(4);
        assert_eq!(index.partition_count(), 4);
        assert_eq!(index.offsets.len(), 5);
    }

    #[test]
    fn test_set_offsets() {
        let mut index = ShuffleIndex::new(3);
        index.set_offset(0, 0);
        index.set_offset(1, 100);
        index.set_offset(2, 250);
        index.set_total_length(500);

        assert_eq!(index.get_partition_range(0), (0, 100));
        assert_eq!(index.get_partition_range(1), (100, 250));
        assert_eq!(index.get_partition_range(2), (250, 500));

        assert_eq!(index.get_partition_size(0), 100);
        assert_eq!(index.get_partition_size(1), 150);
        assert_eq!(index.get_partition_size(2), 250);
    }

    #[test]
    fn test_partition_has_data() {
        let mut index = ShuffleIndex::new(3);
        index.set_offset(0, 0);
        index.set_offset(1, 0); // Empty partition
        index.set_offset(2, 100);
        index.set_total_length(200);

        assert!(!index.partition_has_data(0));
        assert!(index.partition_has_data(1));
        assert!(index.partition_has_data(2));
    }

    #[test]
    fn test_write_and_read() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let index_path = temp_dir.path().join("test.index");

        // Create and write index
        let mut index = ShuffleIndex::new(3);
        index.set_offset(0, 0);
        index.set_offset(1, 100);
        index.set_offset(2, 300);
        index.set_total_length(500);
        index.write_to_file(&index_path)?;

        // Read it back
        let loaded = ShuffleIndex::read_from_file(&index_path)?;

        assert_eq!(loaded.partition_count(), 3);
        assert_eq!(loaded.get_partition_range(0), (0, 100));
        assert_eq!(loaded.get_partition_range(1), (100, 300));
        assert_eq!(loaded.get_partition_range(2), (300, 500));

        Ok(())
    }
}
