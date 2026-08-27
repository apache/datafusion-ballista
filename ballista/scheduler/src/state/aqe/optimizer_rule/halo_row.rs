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

//! Rewrite bounded-`ROWS`-frame windows into a distributed range-shuffle with
//! a rank-derived halo, so `BoundedWindowAggExec`'s single-partition
//! constraint isn't a serial bottleneck.
//!
//! Third sibling of the two window rewrites, split by frame shape:
//! [`super::parallel_window`] takes bounded `RANGE`, [`super::prefix_window`]
//! takes `UNBOUNDED PRECEDING`, and this one takes bounded `ROWS`.
//!
//! # The shape it gates on
//!
//! No PARTITION BY, a single ascending ORDER BY column whose type the routing
//! sketch's `SortKeyCodec` encodes, and a `ROWS` frame with finite
//! `PRECEDING` / `FOLLOWING` / `CurrentRow` bounds. Everything except the
//! frame units matches [`super::parallel_window`]'s gate.
//!
//! h2o window q6 is the smallest query in that shape:
//!
//! ```sql
//! SELECT id1, id2, id3, v2,
//!        avg(v2) OVER (ORDER BY id3 ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
//! FROM large;
//! ```
//!
//! # Actual rule input
//!
//! Captured by logging `optimize`'s argument on q6. The rule sits after
//! DataFusion's optimizer chain and before
//! [`DistributedExchangeRule`](super::DistributedExchangeRule), so the first
//! pass sees no exchange or shuffle reader — just the DataFusion plan with
//! `EnforceSorting`'s `SortExec` placement already materialized. AQE re-plans
//! for each stage, so `optimize` is called three times for q6; the later
//! passes wrap this in `AdaptiveDatafusionExec` and swap the `SortExec`
//! subtree for a resolved `ExchangeExec`.
//!
//! ```text
//! ProjectionExec: expr=[id1@0 as id1, id2@1 as id2, id3@2 as id3, v2@3 as v2,
//!                       avg(large.v2) ORDER BY [large.id3 ASC NULLS LAST]
//!                       ROWS BETWEEN 100 PRECEDING AND CURRENT ROW@4
//!                       as my_moving_average]
//!   BoundedWindowAggExec: wdw=[avg(large.v2) ORDER BY [large.id3 ASC NULLS LAST]
//!                              ROWS BETWEEN 100 PRECEDING AND CURRENT ROW:
//!                              Field { nullable Float64 }],
//!                         frame: ROWS BETWEEN 100 PRECEDING AND CURRENT ROW,
//!                         mode=[Sorted]
//!     SortPreservingMergeExec: [id3@2 ASC NULLS LAST]
//!       SortExec: expr=[id3@2 ASC NULLS LAST], preserve_partitioning=[true]
//!         DataSourceExec: file_groups={8 groups}, projection=[id1, id2, id3, v2],
//!                         file_type=parquet,
//!                         sort_order_for_reorder=[id3@2 ASC NULLS LAST]
//! ```
//!
//! Identical to the input [`super::prefix_window`] documents for q7 except
//! for the frame, which is why all three rules can share a chain position and
//! gate purely on frame shape.
//!
//! # The serial shape it replaces
//!
//! q6 against a 2-executor cluster at h2o 1e7, `target_partitions=8`,
//! `max_partitions_per_task=4`, read back from
//! `/api/job/{job_id}/stages?plan_format=metrics`:
//!
//! ```text
//! === stage 0  tasks=2 ===
//! ShuffleWriterExec: partitioning: UnknownPartitioning(8)
//!   SortExec: expr=[id3@2 ASC NULLS LAST], preserve_partitioning=[true]
//!     DataSourceExec: file_groups={8 groups}, projection=[id1, id2, id3, v2],
//!                     file_type=parquet,
//!                     sort_order_for_reorder=[id3@2 ASC NULLS LAST]
//!
//! === stage 1  tasks=1 ===
//! ShuffleWriterExec: partitioning: UnknownPartitioning(1)
//!   ProjectionExec: expr=[id1@0, id2@1, id3@2, v2@3,
//!                         avg(large.v2) ORDER BY [large.id3 ASC NULLS LAST]
//!                         ROWS BETWEEN 100 PRECEDING AND CURRENT ROW@4
//!                         as my_moving_average]
//!     BoundedWindowAggExec: frame: ROWS BETWEEN 100 PRECEDING AND CURRENT ROW,
//!                           mode=[Sorted]              <- elapsed_compute=10.03s
//!       SortPreservingMergeExec: [id3@2 ASC NULLS LAST]
//!         RangeShuffleReaderExec: upstream_stage: 0, partitions: 8,
//!                                 ordering: id3@2 ASC NULLS LAST, bounds: none
//! ```
//!
//! Stage 0's writer declares `UnknownPartitioning(8)`, so its partitions are
//! each locally sorted but every one spans the whole `id3` range. That leaves
//! stage 1 a single task: the SPM collapses 8 → 1 and BWAG runs the whole
//! window serially, 10.03s of a 19.33s wall clock. The reader is the ordered
//! one, but with `bounds: none` it merges rather than bounds — no cuts exist
//! because nothing upstream sketched the key.
//!
//! # Why the `RANGE` halo doesn't extend to `ROWS`
//!
//! [`super::parallel_window`]'s halo is a **value** delta end to end:
//! `cut_partitions` routes whole files by sketched `[min,max]` overlap against
//! `cuts[k] ± halo`, and `RangeFilterExec` keeps
//! `cuts[k-1] - halo_lo <= v < cuts[k] + halo_hi`. A `ROWS` halo is a **rank**
//! delta, and nothing on that path expresses rank. Two consequences:
//!
//! - Widening a boundary by value arithmetic can land in a gap holding no rows
//!   at all, so `n PRECEDING` rows is not reachable by any choice of value
//!   width. It also needs `add`/`sub` on the key type, the same wall
//!   `halo_from_bound` type-probes for.
//! - There is no "bucket k-1" to walk back into. `k` indexes a value range,
//!   not a physical object; stage 0 emits sorted runs that may overlap
//!   arbitrarily in value.
//!
//! # Where the rank bound comes from
//!
//! The range-shuffle sidecar index (`range_shuffle::index`) carries one row
//! per IPC message, holding that message's first key and its `num_rows`. To
//! find the `n` rows preceding a cut `c`, merge every file's index rows
//! descending by first key — comparison only, so strings, timestamps,
//! multi-column and DESC keys all work — and accumulate `num_rows` until the
//! total reaches `n`. The first key of the last message consumed is the
//! widened lower bound: reading `[v, c)` from every file with rows below `c`
//! yields a superset of the true `n` predecessors, trimmed after the merge.
//!
//! Per file exactly one message straddles `c`, and its `num_rows` is only an
//! upper bound on how much of it falls below `c`. It counts as zero in the
//! accumulation — otherwise the sum overestimates and the walk stops short of
//! `n` — and is still read, being inside the band.
//!
//! Cuts stay approximate KLL value cuts. Halo exactness rests on the index
//! walk, not on cut exactness. `cut_partitions`' overlap test is the wrong
//! routing test here: a file that doesn't overlap `[cuts[k-1], cuts[k])` can
//! still hold predecessors.
//!
//! # Target shape
//!
//! Same preamble [`super::parallel_window`] builds, with the halo bounds
//! coming from the index walk above instead of from value arithmetic on the
//! frame bounds:
//!
//! ```text
//! RangeFilterExec (narrow, halo_lo=0, halo_hi=0, cuts=pending)
//!   PartitionedBoundedWindowAggExec [wraps BWAG; UnspecifiedDistribution]
//!     RangeFilterExec (wide, rank-derived bounds, cuts=pending)
//!       RuntimeStatsExec #2
//!         OrderedRangeRepartitionExec [K range-disjoint outputs]
//!           SortExec (preserve_partitioning=true)
//!             RuntimeStatsExec #1 [local sketch -> cuts]
//!               <source>
//! ```

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use datafusion::common::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;

/// Physical optimizer pass for bounded-`ROWS` windows. Returns the plan
/// unchanged; the rewrite to the module docs' target shape goes in
/// `maybe_rewrite_bwag`, alongside the rank-bound index walk.
#[derive(Default, Debug)]
pub struct HaloRowRule;

impl PhysicalOptimizerRule for HaloRowRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        if !bc.parallel_window_enabled() {
            return Ok(plan);
        }
        // The module docs' "Actual rule input" section is a capture of this.
        // Re-run with `RUST_LOG=ballista_scheduler=debug` to refresh it after
        // anything upstream in the optimizer chain changes shape.
        debug!(
            "HaloRowRule input:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        Ok(plan)
    }

    fn name(&self) -> &str {
        "HaloRow"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
