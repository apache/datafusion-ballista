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

use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};

use crate::execution_plans::{
    BufferExec, PrefixMergeExec, RangeFilterExec, RuntimeStatsExec, ShuffleWriterExec,
    SortShuffleWriterExec,
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

/// Operators carrying data indexed by *global* input partition, which the
/// scheduler must slice when it restricts a stage plan to one task's
/// partition subset.
///
/// The executor still never learns its task's global identity — slicing is
/// how that stays true. After restriction an operator holds a vec covering
/// exactly the partitions its task will run, in local order, so `execute(k)`
/// indexes it directly.
///
/// Implement this next to the fields being sliced. The alternative — the
/// scheduler's task builder knowing each operator's internals — means adding
/// a partition-indexed field silently leaves a stale slicer two crates away,
/// discovered at runtime if some length check happens to catch it.
///
/// Like the whitelists above, this exists because `ExecutionPlan` has no
/// "restrict yourself to a subset of input partitions" method. A defaulted
/// upstream one would let each operator answer for itself and retire
/// [`as_partition_sliceable`].
pub trait PartitionSliceable: ExecutionPlan {
    /// Rebuild over `child` — already restricted to `partitions` — carrying
    /// only the entries for `partitions`, in that order.
    ///
    /// # Arguments
    ///
    /// * `child` - this operator's single input, already restricted
    /// * `partitions` - global input partition indices this task will run
    fn slice_to_partitions(
        &self,
        child: Arc<dyn ExecutionPlan>,
        partitions: &[usize],
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Hand-maintained whitelist of [`PartitionSliceable`] operators, for the
/// same reason as the property whitelists above: `dyn ExecutionPlan` can't
/// be downcast to an arbitrary trait.
pub fn as_partition_sliceable(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<&dyn PartitionSliceable> {
    if let Some(op) = plan.downcast_ref::<RangeFilterExec>() {
        return Some(op);
    }
    if let Some(op) = plan.downcast_ref::<PrefixMergeExec>() {
        return Some(op);
    }
    None
}

/// Take `values[global]` for each global partition index, in task-local
/// order. Errors rather than dropping, so a slice that outruns its operator's
/// data surfaces as a named failure instead of a silently short vec.
///
/// # Arguments
///
/// * `values` - the operator's per-global-partition entries
/// * `partitions` - global indices to keep, in task-local order
/// * `owner` - operator name, for the error message
/// * `what` - what the entries are, for the error message
pub fn slice_by_global_partition<T: Clone>(
    values: &[T],
    partitions: &[usize],
    owner: &str,
    what: &str,
) -> Result<Vec<T>> {
    partitions
        .iter()
        .map(|&global| {
            values.get(global).cloned().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{owner}: partition index {global} out of bounds ({} {what})",
                    values.len()
                ))
            })
        })
        .collect()
}
