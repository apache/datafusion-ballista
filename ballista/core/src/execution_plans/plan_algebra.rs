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

//! Algebraic properties of physical plan nodes — do they preserve
//! partitioning, do they preserve the distribution of row values, etc.
//!
//! DataFusion doesn't expose these as trait methods on `ExecutionPlan`
//! (nice-to-haves like `ExecutionPlan::affects_partitioning()` /
//! `ExecutionPlan::affects_distribution()` that would hopefully land one
//! day), so we downcast against a hand-maintained whitelist. Being
//! conservative is the safety net: unrecognized node → property assumed
//! false → caller falls back to the safer path.

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};

use crate::execution_plans::{
    BufferExec, RuntimeStatsExec, ShuffleWriterExec, SortShuffleWriterExec,
};

/// Whitelisted ops preserve the routing key's row set, values, and
/// partitioning — an upstream sketch remains valid after the operator.
pub fn preserves_distribution(plan: &dyn ExecutionPlan) -> bool {
    // Buffered batches replayed verbatim.
    plan.downcast_ref::<BufferExec>().is_some()
        // Per-partition sort: rows reorder within a partition, row set
        // and counts unchanged. `preserve_partitioning=false` collapses
        // N→1 (like SortPreservingMergeExec), so gate on the flag.
        || plan
            .downcast_ref::<SortExec>()
            .is_some_and(|sort| sort.preserve_partitioning())
        // Stage-boundary writers: batches to disk unchanged.
        || plan.downcast_ref::<ShuffleWriterExec>().is_some()
        || plan.downcast_ref::<SortShuffleWriterExec>().is_some()
        // Pure row-annotation: one input row → one output row with an
        // added column (window fn result); values, partitioning, count preserved.
        || plan.downcast_ref::<BoundedWindowAggExec>().is_some()
        || plan.downcast_ref::<WindowAggExec>().is_some()
}

/// Looser sibling of [`preserves_distribution`]: partitioning survives,
/// but rows and values within a partition are fair game.
pub fn preserves_partitioning(plan: &dyn ExecutionPlan) -> bool {
    // Distribution-preserving is strictly stronger; compose to keep the
    // whitelist deduplicated.
    preserves_distribution(plan)
        // Drops rows, but per-partition — no rows migrate.
        || plan.downcast_ref::<FilterExec>().is_some()
        // Rewrites columns; partition boundaries untouched.
        || plan.downcast_ref::<ProjectionExec>().is_some()
        // Stats tap; no data mutation.
        || plan.downcast_ref::<RuntimeStatsExec>().is_some()
}
