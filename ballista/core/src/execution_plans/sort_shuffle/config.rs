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
///
/// Controls memory buffering, spilling behavior, and compression settings
/// for the sort-based shuffle writer.
#[derive(Debug, Clone)]
pub struct SortShuffleConfig {
    /// Whether sort-based shuffle is enabled (default: false)
    pub enabled: bool,
    /// Per-partition buffer size in bytes before considering spill (default: 1MB)
    pub buffer_size: usize,
    /// Total memory limit for all shuffle buffers combined (default: 256MB)
    pub memory_limit: usize,
    /// Spill threshold as fraction of memory limit (default: 0.8)
    /// When total memory usage exceeds `memory_limit * spill_threshold`,
    /// the largest buffers are spilled to disk.
    pub spill_threshold: f64,
    /// Compression codec for shuffle data (default: LZ4_FRAME)
    pub compression: CompressionType,
}

impl Default for SortShuffleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            buffer_size: 1024 * 1024,        // 1 MB
            memory_limit: 256 * 1024 * 1024, // 256 MB
            spill_threshold: 0.8,
            compression: CompressionType::LZ4_FRAME,
        }
    }
}

impl SortShuffleConfig {
    /// Creates a new configuration with the specified settings.
    pub fn new(
        enabled: bool,
        buffer_size: usize,
        memory_limit: usize,
        spill_threshold: f64,
        compression: CompressionType,
    ) -> Self {
        Self {
            enabled,
            buffer_size,
            memory_limit,
            spill_threshold,
            compression,
        }
    }

    /// Returns the memory threshold at which spilling should occur.
    pub fn spill_memory_threshold(&self) -> usize {
        (self.memory_limit as f64 * self.spill_threshold) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SortShuffleConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.buffer_size, 1024 * 1024);
        assert_eq!(config.memory_limit, 256 * 1024 * 1024);
        assert!((config.spill_threshold - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_spill_memory_threshold() {
        let config = SortShuffleConfig {
            memory_limit: 100,
            spill_threshold: 0.8,
            ..Default::default()
        };
        assert_eq!(config.spill_memory_threshold(), 80);
    }
}
