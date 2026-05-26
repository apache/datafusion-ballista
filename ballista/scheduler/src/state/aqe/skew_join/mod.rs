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

//! AQE optimize-skewed-join helpers (pure-CPU).
//!
//! This submodule packages the per-upstream skew detection and shard pairing
//! helpers `OptimizeSkewedJoinRule` consumes:
//!
//! - [`is_skewed`] — per-side detection (factor × median AND absolute threshold).
//! - [`map_ranges_for_upstream`] — bin-pack one upstream partition's per-mapper
//!   byte sizes into `(start_map_idx, end_map_idx)` ranges via the shared
//!   `split_size_list_by_target_size` helper from the coalesce module.
//! - [`pair_shards`] — cartesian-product the per-upstream left/right ranges
//!   into the matched K' shard lists Spark's `OptimizeSkewedJoin` produces.

pub(crate) mod algorithm;
pub(crate) use algorithm::*;
