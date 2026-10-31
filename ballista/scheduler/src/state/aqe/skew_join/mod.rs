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

//! Helpers for splitting skewed join inputs.

mod algorithm;
pub(crate) use algorithm::*;

use ballista_core::serde::scheduler::PartitionLocation;

/// Shard windows use mapper order, not the order task completions arrived.
/// Apply the same ordering when planning ranges and materializing readers.
pub(crate) fn sort_mapper_outputs(partitions: &mut [Vec<PartitionLocation>]) {
    for locations in partitions {
        locations.sort_unstable_by_key(|location| location.map_partition_id);
    }
}
