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

use ballista_core::config::BallistaConfig;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::config::ConfigOptions;

/// Reports an explicitly enabled skew-join optimization blocked by configuration.
pub(crate) fn skew_join_config_warning(options: &ConfigOptions) -> Option<&'static str> {
    if options
        .extensions
        .get::<BallistaConfig>()
        .is_some_and(|config| config.skew_join_enabled())
        && options.optimizer.enable_join_dynamic_filter_pushdown
    {
        Some(
            "skew-join optimization skipped: join dynamic-filter pushdown is enabled; \
             set datafusion.optimizer.enable_dynamic_filter_pushdown=false to allow skew splitting",
        )
    } else {
        None
    }
}

/// Shard windows use mapper order, not the order task completions arrived.
/// Apply the same ordering when planning ranges and materializing readers.
pub(crate) fn sort_mapper_outputs(partitions: &mut [Vec<PartitionLocation>]) {
    for locations in partitions {
        locations.sort_unstable_by_key(|location| location.map_partition_id);
    }
}
