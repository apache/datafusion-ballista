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

//! Configuration for sort-based shuffle.

use datafusion::arrow::ipc::CompressionType;

/// Configuration for sort-based shuffle.
#[derive(Debug, Clone)]
pub struct SortShuffleConfig {
    /// Whether sort-based shuffle is enabled (default: false).
    pub enabled: bool,
    /// Compression codec for shuffle data (default: LZ4_FRAME).
    pub compression: CompressionType,
    /// Target batch size in rows when materializing buffered indices via
    /// `interleave_record_batch` (default: 8192).
    pub batch_size: usize,
}

impl Default for SortShuffleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            compression: CompressionType::LZ4_FRAME,
            batch_size: 8192,
        }
    }
}

impl SortShuffleConfig {
    /// Creates a new configuration.
    pub fn new(enabled: bool, compression: CompressionType, batch_size: usize) -> Self {
        Self {
            enabled,
            compression,
            batch_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SortShuffleConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.batch_size, 8192);
    }

    #[test]
    fn test_new() {
        let config = SortShuffleConfig::new(true, CompressionType::LZ4_FRAME, 4096);
        assert!(config.enabled);
        assert_eq!(config.batch_size, 4096);
    }
}
