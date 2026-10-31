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

//! Per-task plan rewriter.
//!
//! Before shipping a stage's plan to an executor, the scheduler restricts the
//! plan's leaves (Scan file groups, ShuffleReader partition locations) to the
//! task's assigned partition slice. The executor then just runs whatever it's
//! given — no operator needs to know its task's global identity, and any
//! within-stage operator's `output_partitioning()` naturally reflects the
//! restricted count.
//!
//! Restriction is *scoped*: a leaf below a collapse (`CoalescePartitionsExec`,
//! `SortPreservingMergeExec`, or the `SinglePartition`-requiring build side
//! of a join) must read the entire upstream, not the task's slice — otherwise
//! each of N sibling tasks would collapse only 1/N of the input, producing
//! partial results downstream tries to merge (e.g. the wrong scalar threshold
//! from a HAVING subquery).
//!
//! Scoping is expressed as an `under_collect: bool` parameter threaded down
//! the recursion. The call stack is the tree walk's natural stack; scope
//! flows from parent to descendants via function arguments, so sibling
//! subtrees never share state and there's no traversal-order dependency.

use ballista_core::execution_plans::plan_algebra::{
    PartitionSliceableResolver, as_partition_sliceable,
};
use ballista_core::execution_plans::{RangeShuffleReaderExec, ShuffleReaderExec};
use datafusion::common::{internal_err, plan_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::physical_expr::{Distribution, RangePartitioning};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::replace_children_if_necessary;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use std::any::Any;
use std::sync::Arc;

/// Restrict `plan` so that its leaves only see the given `partitions`, unless
/// a leaf sits below a collapse operator (`CoalescePartitionsExec`,
/// `SortPreservingMergeExec`, or a `SinglePartition`-requiring join build
/// side) — those leaves keep the full upstream so the collapse sees every
/// partition.
///
/// Nodes Ballista does not list are restricted through `resolver`. A leaf that
/// nothing can restrict is an error unless `partitions` is all of its
/// partitions, in order.
pub fn restrict_plan_to_partitions(
    plan: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    resolver: Option<&PartitionSliceableResolver>,
) -> Result<Arc<dyn ExecutionPlan>> {
    restrict(plan, partitions, /* under_collect */ false, resolver)
}

/// Recursive worker. `under_collect` is the scope inherited from ancestors:
/// once set, every descendant leaf reads the full upstream. Scope is passed
/// by value, so sibling subtrees never leak state to each other.
fn restrict(
    plan: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    under_collect: bool,
    resolver: Option<&PartitionSliceableResolver>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Leaves: apply the algebraic partition-projection when we're not
    // under a collapse. Under collapse, leaves keep their full upstream —
    // the generic recursion below leaves them unchanged.
    if !under_collect
        && let Some(rewritten) = select_output_partitions(&plan, partitions)?
    {
        return Ok(rewritten);
    }

    // Operators carrying data indexed by global partition slice it parallel to
    // the restriction. Each one implements the slicing beside its own fields;
    // this walker only supplies the restricted children.
    if !under_collect
        && let Some(sliceable) = as_partition_sliceable(&plan)
            .or_else(|| resolver.and_then(|resolve| resolve(&plan)))
    {
        let children = restrict_children(&plan, partitions, false, resolver)?;
        let restricted = sliceable.restrict_to_partitions(children, partitions)?;
        return checked_restriction(plan.name(), restricted, partitions);
    }

    // UnionExec: parent partition `p` maps to exactly one child's local
    // partition (`p` minus the sum of preceding children's counts). Split
    // `partitions` into disjoint per-child sub-slices and recurse; a child
    // that gets `[]` becomes a 0-partition subplan, and UnionExec's
    // partition-index math routes `execute(i)` past it correctly. Under
    // `under_collect`, every descendant reads the full upstream anyway,
    // so the generic recursion below applies instead.
    if !under_collect && plan.is::<UnionExec>() {
        let per_child = union_child_partitions(&plan, partitions);
        let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
            .children()
            .into_iter()
            .cloned()
            .zip(per_child)
            .map(|(child, sub)| restrict(child, &sub, false, resolver))
            .collect::<Result<Vec<_>>>()?;
        return replace_children_if_necessary(plan, new_children);
    }

    // A leaf nothing above could restrict: read whole below a collapse, and
    // otherwise only right if the task runs all of it.
    if plan.children().is_empty() {
        if !under_collect {
            ensure_leaf_runs_whole(&plan, partitions)?;
        }
        return Ok(plan);
    }

    // Interior: compute per-child scope, recurse, rebuild the node.
    let new_children = restrict_children(&plan, partitions, under_collect, resolver)?;
    replace_children_if_necessary(plan, new_children)
}

/// Restrict each child of `plan`, in the scope `child_scopes` gives it.
fn restrict_children(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    under_collect: bool,
    resolver: Option<&PartitionSliceableResolver>,
) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    let per_child = child_scopes(plan, under_collect);
    plan.children()
        .into_iter()
        .cloned()
        .zip(per_child)
        .map(|(child, collect)| restrict(child, partitions, collect, resolver))
        .collect()
}

/// Hold a `PartitionSliceable` to its contract: one partition for each
/// partition the task runs. Another count would run the wrong partitions.
fn checked_restriction(
    name: &str,
    restricted: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Result<Arc<dyn ExecutionPlan>> {
    let count = restricted
        .properties()
        .output_partitioning()
        .partition_count();
    if count != partitions.len() {
        return plan_err!(
            "{name}::restrict_to_partitions returned {count} partitions for \
             partitions {partitions:?}; it has to return exactly one for each"
        );
    }
    Ok(restricted)
}

/// Accept a leaf that nothing could restrict only if the task runs all of it,
/// in order. Left as it is, it numbers its partitions from zero, so any other
/// slice would run the wrong ones. A one-partition leaf therefore passes in the
/// task that owns it, and fails in a `UnionExec` branch that belongs to another.
fn ensure_leaf_runs_whole(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Result<()> {
    let count = plan.properties().output_partitioning().partition_count();
    if partitions.iter().copied().eq(0..count) {
        return Ok(());
    }
    let name = plan.name();
    let described = displayable(plan.as_ref()).one_line().to_string();
    let described = described.trim_end();
    plan_err!(
        "cannot restrict `{described}` to partitions {partitions:?} of its {count}: \
         the scheduler does not know how to restrict a `{name}`, and left alone it \
         would number its partitions from zero in every task. A node of your own \
         can implement `PartitionSliceable` and be returned from \
         `SchedulerConfig::partition_sliceable_resolver`."
    )
}

/// Bucket each parent partition of a `UnionExec` into its owning child's
/// local index. Iterates over `partitions` and, for each, subtracts each
/// child's count until the remainder lands inside a child. Returns one
/// sub-slice per child (empty for children this task never polls).
fn union_child_partitions(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Vec<Vec<usize>> {
    let child_counts: Vec<usize> = plan
        .children()
        .iter()
        .map(|c| c.properties().output_partitioning().partition_count())
        .collect();
    let mut per_child: Vec<Vec<usize>> = vec![vec![]; child_counts.len()];
    for &p in partitions {
        let mut remaining = p;
        for (i, &count) in child_counts.iter().enumerate() {
            if remaining < count {
                per_child[i].push(remaining);
                break;
            }
            remaining -= count;
        }
    }
    per_child
}

/// The `under_collect` value to pass to each child of `plan`.
///
/// - Once `under_collect` is set on the parent, it propagates unchanged to
///   every child (it's sticky — descendants of a collapse all read
///   everything).
/// - `CoalescePartitionsExec` / `SortPreservingMergeExec`: set for all
///   children. These cannot be derived from the rule below — both declare
///   `UnspecifiedDistribution`, because they consume every input partition
///   without constraining how the input is partitioned. Keep the explicit
///   case.
/// - Anything else: set per child from `input_distribution_requirements()`. An
///   input declared `SinglePartition` is one the operator consumes whole —
///   a join or cross-join build side, for instance — so restricting it to
///   the task's slice would hand each sibling task a different fraction of
///   it. Typically only the build side is `SinglePartition`, so the probe
///   side inherits `false` and remains partition-aligned.
///
///   This is asked of every operator rather than a list of join types.
///   `CrossJoinExec` also collects its left input but was not on that list,
///   so each task of a multi-partition stage rebuilt the cross-join's left
///   side from its own slice and the result came back inflated (seen on
///   TPC-DS q77, whose catalog branch is `from cs, cr`). It also brings in
///   a non-preserving `SortExec` and `GlobalLimitExec`, which declare
///   `SinglePartition` for the same reason.
///
/// `UnionExec` is handled by the caller before reaching this function —
/// its children need per-child *partition* sub-slices, not just per-child
/// scope, so a `Vec<bool>` can't express what it needs.
fn child_scopes(plan: &Arc<dyn ExecutionPlan>, under_collect: bool) -> Vec<bool> {
    let children = plan.children();
    if under_collect {
        return vec![true; children.len()];
    }
    if plan.is::<CoalescePartitionsExec>() || plan.is::<SortPreservingMergeExec>() {
        return vec![true; children.len()];
    }
    let required = plan.input_distribution_requirements();
    let per_child = required.per_child_distributions();
    if per_child.len() != children.len() {
        // The trait contract is one entry per child, so a mismatch means a
        // broken operator. Stay partition-aligned rather than reading whole:
        // `true` here is not the safe direction, it makes every task of the
        // stage read the entire input, which duplicates the data instead of
        // protecting it. `false` is also what every operator outside the
        // cases above got before this rule generalized.
        return vec![false; children.len()];
    }
    per_child
        .map(|d| matches!(d, Distribution::SinglePartition))
        .collect()
}

/// Restrict a plan node to a subset of its output partitions, re-indexed
/// so position `i` of the result corresponds to input `indices[i]`.
///
/// Contract when `Some(new)` is returned:
///
/// - `new.output_partitioning().partition_count() == indices.len()`
/// - `new.execute(j)` ≡ `plan.execute(indices[j])`
/// - `Partitioning` kind is preserved (`Hash(exprs, N) → Hash(exprs, |S|)`,
///   `RoundRobinBatch(N) → RoundRobinBatch(|S|)`, `Unknown(N) → Unknown(|S|)`).
///   A `Range` slices its split points along with the partitions; see
///   [`restrict_range_partitioning`] for the one case, a slice that does not
///   ascend, where it reports `Unknown(|S|)` instead.
/// - `new.schema() == plan.schema()`
///
/// `None` means "unrecognised plan type — no idea how." The caller decides
/// whether to fall through to a generic children walk, bail, or panic.
/// `None` is NOT "no restriction needed."
///
/// # Why a helper and not a trait method
///
/// The operation is algebraically a projection on the partition-index
/// axis and belongs on `ExecutionPlan` as `select_partitions(&[usize])`
/// with default impls for partition-preserving operators. DataFusion's
/// trait doesn't provide it — the closest is `repartitioned(N)`, which
/// reshapes count but doesn't preserve partition identity, and isn't
/// implemented on the self-generating leaves anyway. So we downcast here.
///
/// **When a new leaf type surfaces, add a branch below.** Currently
/// covered: `ShuffleReaderExec`, `DataSourceExec` (files, in-memory),
/// `PlaceholderRowExec`, `EmptyExec`. `ValuesExec` and other future
/// self-generating leaves fall through as `None`; if one sits under a
/// partition-splitting fan-in, the walker refuses the task rather than let the
/// executor duplicate output (see the PlaceholderRow-under-UnionExec bug for
/// the failure mode).
///
/// # Scope
///
/// This helper is scope-agnostic. The `under_collect` decision (whether
/// a leaf below a `CoalescePartitionsExec` / `SortPreservingMergeExec` /
/// join build side must read its full upstream) lives in the walker —
/// the walker simply doesn't call this function under collapse.
fn select_output_partitions(
    plan: &Arc<dyn ExecutionPlan>,
    indices: &[usize],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // ShuffleReaderExec: cross-stage inputs.
    if let Some(reader) = plan.downcast_ref::<ShuffleReaderExec>() {
        // Broadcast readers serve everything from partition[0] for every
        // consumer — slicing would drop the payload. Leave intact.
        if reader.broadcast {
            return Ok(Some(plan.clone()));
        }
        // Keep the indices alongside the locations: a range partitioning
        // slices its split points by them. An index past the end is dropped,
        // as it always was.
        let kept_indices: Vec<usize> = indices
            .iter()
            .copied()
            .filter(|&p| p < reader.partition.len())
            .collect();
        let kept: Vec<Vec<_>> = kept_indices
            .iter()
            .map(|&p| reader.partition[p].clone())
            .collect();
        let partitioning = match reader.properties().output_partitioning() {
            Partitioning::Hash(exprs, _) => Partitioning::Hash(exprs.clone(), kept.len()),
            Partitioning::RoundRobinBatch(_) => Partitioning::RoundRobinBatch(kept.len()),
            Partitioning::UnknownPartitioning(_) => {
                Partitioning::UnknownPartitioning(kept.len())
            }
            Partitioning::Range(range) => {
                restrict_range_partitioning(range, &kept_indices)
            }
        };
        let restricted = ShuffleReaderExec::try_new(
            reader.stage_id,
            kept,
            reader.schema(),
            partitioning,
        )?;
        return Ok(Some(Arc::new(restricted)));
    }

    // RangeShuffleReaderExec: cross-stage inputs, ordering-preserving. Same
    // partition-slice restriction as ShuffleReaderExec — carry the merge
    // ordering unchanged.
    if let Some(reader) = plan.downcast_ref::<RangeShuffleReaderExec>() {
        let kept: Vec<Vec<_>> = indices
            .iter()
            .filter_map(|&p| reader.partition.get(p).cloned())
            .collect();
        let restricted = RangeShuffleReaderExec::try_new(
            reader.stage_id,
            kept,
            reader.schema(),
            reader.merge_ordering().clone(),
        )?;
        let restricted = restricted.with_fetch_limit(reader.fetch());
        // Bounds are per output partition, so they follow the same slice the
        // locations did. Dropping them here would leave the task reading its
        // sources whole — right answer, none of the saving.
        let restricted = match reader.bounds() {
            Some(bounds) => {
                let kept = indices
                    .iter()
                    .filter_map(|&p| bounds.get(p).cloned())
                    .collect();
                restricted.with_bounds(kept)?
            }
            None => restricted,
        };
        return Ok(Some(Arc::new(restricted)));
    }

    // DataSourceExec: file-backed or in-memory scans.
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>() {
        let source: &dyn Any = exec.data_source().as_ref();
        if let Some(config) = source.downcast_ref::<FileScanConfig>() {
            let file_groups: Vec<FileGroup> = indices
                .iter()
                .filter_map(|&i| config.file_groups.get(i).cloned())
                .collect();
            let restricted = FileScanConfigBuilder::from(config.clone())
                .with_file_groups(file_groups)
                .build();
            return Ok(Some(DataSourceExec::from_data_source(restricted)));
        }
        if let Some(config) = source.downcast_ref::<MemorySourceConfig>() {
            let kept: Vec<Vec<_>> = indices
                .iter()
                .filter_map(|&i| config.partitions().get(i).cloned())
                .collect();
            let restricted = MemorySourceConfig::try_new(
                &kept,
                config.original_schema(),
                config.projection().clone(),
            )?;
            return Ok(Some(DataSourceExec::from_data_source(restricted)));
        }
        // Any other source is left to the walker, which refuses a task that runs
        // only some of its partitions.
        return Ok(None);
    }

    // Self-generating leaves: PlaceholderRow (1 all-null row per partition)
    // and EmptyExec (0 rows per partition). Every partition of one of these
    // is semantically identical, so `with_partitions(indices.len())` is the
    // algebraically correct restriction.
    //
    // Caveat: DataFusion's proto codec for these two doesn't roundtrip the
    // `partitions` field — encode preserves only schema; decode always
    // reconstructs with `::new(schema)` (partitions = 1). So a plan we
    // restrict to `with_partitions(0)` here would arrive at the executor as
    // a 1-partition leaf, undoing the restriction and reproducing the
    // duplicated-output bug this whole function is meant to prevent.
    //
    // Workaround for the empty-slice case: swap in a 0-partition
    // `MemorySourceConfig` (which DOES roundtrip its partition list).
    // For non-empty slices we return the leaf as it is — currently safe because
    // the only shape emitted is `partitions == 1` and UnionExec routing gives
    // sub-slices of `[0]` (identity, no restriction needed) or `[]` (handled by
    // this branch). Returned rather than `None` so the walker's leaf check
    // accepts it: an `EmptyExec` under the round-robin `RepartitionExec` that
    // `make_empty_exec_serde_safe` adds sees that repartition's partition
    // numbers, not its own.
    if plan.is::<PlaceholderRowExec>() || plan.is::<EmptyExec>() {
        if indices.is_empty() {
            let stub = MemorySourceConfig::try_new(&[], plan.schema(), None)?;
            return Ok(Some(DataSourceExec::from_data_source(stub)));
        }
        return Ok(Some(plan.clone()));
    }

    Ok(None)
}

/// `range` narrowed to the partitions `kept` selects, in `kept`'s order, so the
/// restricted reader keeps reporting a range partitioning instead of
/// downgrading to `UnknownPartitioning`.
///
/// Position `j` of the result is old partition `kept[j]`, so the boundary after
/// it is that partition's upper split point. Everything at `j` is below it, and
/// everything at `j + 1` is at or above it, because old partition `kept[j + 1]`
/// starts no earlier than `kept[j]` ends. The last position has no boundary
/// after it, which leaves one fewer split point than partitions, as
/// `RangePartitioning` requires.
///
/// That only holds while `kept` ascends. A task's slice can run out of order
/// (a retried partition is queued ahead of a later one that was never lost),
/// and then positions no longer follow the key order, which no set of split
/// points describes. An empty selection has no range partitioning either: a
/// `RangePartitioning` always has at least one partition. Both fall back to
/// `UnknownPartitioning`, which says less but nothing untrue. That is safe
/// because a restricted plan is only executed from here on, never re-optimized
/// against what its readers report, and failing the task instead would turn a
/// harmless retry ordering into a failed job.
fn restrict_range_partitioning(
    range: &RangePartitioning,
    kept: &[usize],
) -> Partitioning {
    let unknown = || Partitioning::UnknownPartitioning(kept.len());
    let ascends = kept.windows(2).all(|pair| pair[0] < pair[1]);
    let Some((_, leading)) = kept.split_last().filter(|_| ascends) else {
        return unknown();
    };
    // A split point is missing only if the reader has more partitions than its
    // own partitioning, which no planner produces; treat it as unknown rather
    // than guess.
    let Some(split_points) = leading
        .iter()
        .map(|&p| range.split_points().get(p).cloned())
        .collect::<Option<Vec<_>>>()
    else {
        return unknown();
    };
    RangePartitioning::try_new(range.ordering().clone(), split_points)
        .map_or_else(|_| unknown(), Partitioning::Range)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::JobId;
    use ballista_core::execution_plans::{
        CoalescePlan, PartitionGroup, RangeBound, ShuffleReaderExec,
    };
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, PartitionId, PartitionLocation,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::Partitioning;

    fn create_partition(partition: usize) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: 0,
            partition_id: PartitionId {
                job_id: JobId::new("demo".to_string()),
                stage_id: 0,
                partition_id: partition,
            },
            executor_meta: ExecutorMetadata {
                id: "1".to_string(),
                host: "1.1.1.1".to_string(),
                port: 0,
                grpc_port: 0,
                specification: Default::default(),
                os_info: Default::default(),
            },
            partition_stats: Default::default(),
            file_id: None,
            is_sort_shuffle: false,
        }
    }

    /// A `ShuffleReaderExec` over `n` single-location partitions.
    fn shuffle_reader(n: usize) -> Arc<dyn ExecutionPlan> {
        let partitions = (0..n).map(|i| vec![create_partition(i)]).collect();
        Arc::new(
            ShuffleReaderExec::try_new(
                1,
                partitions,
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                Partitioning::UnknownPartitioning(n),
            )
            .unwrap(),
        )
    }

    #[test]
    fn keeps_only_the_wanted_partition() {
        let plan = shuffle_reader(4);
        let pruned = restrict_plan_to_partitions(plan, &[1], None).unwrap();
        let r = pruned
            .downcast_ref::<ShuffleReaderExec>()
            .expect("expected a ShuffleReaderExec");
        // Kept partitions get positions 0..kept.len(); position 0 now holds
        // what used to live at upstream partition 1.
        assert_eq!(r.partition.len(), 1);
        assert_eq!(r.partition[0].len(), 1);
        assert_eq!(r.partition[0][0].partition_id.partition_id, 1);
    }

    #[test]
    fn keeps_multiple_wanted_partitions_in_request_order() {
        let plan = shuffle_reader(4);
        let pruned = restrict_plan_to_partitions(plan, &[0, 3], None).unwrap();
        let r = pruned
            .downcast_ref::<ShuffleReaderExec>()
            .expect("expected a ShuffleReaderExec");
        assert_eq!(r.partition.len(), 2);
        assert_eq!(r.partition[0][0].partition_id.partition_id, 0);
        assert_eq!(r.partition[1][0].partition_id.partition_id, 3);
    }

    #[test]
    fn broadcast_reader_is_not_pruned() {
        // Broadcast readers serve partition[0] for every consumer index, so
        // restriction must be a no-op regardless of the assigned slice.
        let reader = ShuffleReaderExec::try_new_broadcast(
            1,
            (0..4).map(create_partition).collect(),
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            4,
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(reader);
        let pruned = restrict_plan_to_partitions(plan, &[1], None).unwrap();
        let r = pruned
            .downcast_ref::<ShuffleReaderExec>()
            .expect("expected a ShuffleReaderExec");
        assert!(r.broadcast);
        assert_eq!(r.partition.len(), 1);
        assert_eq!(r.partition[0].len(), 4);
    }

    // TODO: rewrite_shuffle_reader currently drops the CoalescePlan when
    // restricting a coalesced reader (it goes through ShuffleReaderExec::try_new,
    // not try_new_coalesced). Coalesced readers ARE constructed at runtime in
    // aqe/adapter.rs, so a slice-restricted coalesced reader would silently
    // become non-coalesced. Once that's fixed, this test should assert the
    // coalesce metadata survives and the reindexed partition[0] holds the
    // second coalesce group's 2 locations.
    #[test]
    #[ignore = "coalesce metadata is stripped during restriction — see fn comment"]
    fn coalesced_reader_stays_coalesced_when_restricted() {
        let coalesce = CoalescePlan {
            upstream_partition_count: 4,
            groups: vec![
                PartitionGroup {
                    upstream_indices: vec![0, 1],
                },
                PartitionGroup {
                    upstream_indices: vec![2, 3],
                },
            ],
        };
        let reader = ShuffleReaderExec::try_new_coalesced(
            1,
            vec![
                vec![create_partition(0), create_partition(1)],
                vec![create_partition(2), create_partition(3)],
            ],
            coalesce,
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            Partitioning::UnknownPartitioning(2),
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(reader);
        let pruned = restrict_plan_to_partitions(plan, &[1], None).unwrap();
        let r = pruned
            .downcast_ref::<ShuffleReaderExec>()
            .expect("expected a ShuffleReaderExec");
        assert!(r.coalesce.is_some());
        assert_eq!(r.partition.len(), 1);
        assert_eq!(r.partition[0].len(), 2);
    }

    #[test]
    fn restricting_to_every_partition_is_a_no_op() {
        let plan = shuffle_reader(3);
        let pruned = restrict_plan_to_partitions(plan, &[0, 1, 2], None).unwrap();
        let r = pruned
            .downcast_ref::<ShuffleReaderExec>()
            .expect("expected a ShuffleReaderExec");
        assert_eq!(r.partition.len(), 3);
        for (i, loc) in r.partition.iter().enumerate() {
            assert_eq!(loc.len(), 1);
            assert_eq!(loc[0].partition_id.partition_id, i);
        }
    }

    // --- UnionExec routing ---
    //
    // These tests exercise the walker's UnionExec branch. Under a union, the
    // parent's partition indices are disjointly allocated across children by
    // subtracting each preceding child's partition count. A child that owns
    // none of the requested partitions is restricted to `[]` and produces a
    // 0-partition subplan; UnionExec's `execute(i)` math routes past it. The
    // upstream fix used a per-single-partition helper on the executor side;
    // this suite ports the same invariants to the scheduler-side rewriter.

    use crate::test_utils::scan_with_file_groups;
    use datafusion::physical_plan::union::UnionExec;

    /// The file groups of a `DataSourceExec` scan.
    fn file_groups(plan: &Arc<dyn ExecutionPlan>) -> &[FileGroup] {
        let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
        let source: &dyn Any = exec.data_source().as_ref();
        &source.downcast_ref::<FileScanConfig>().unwrap().file_groups
    }

    /// File counts per file group of a `DataSourceExec` — the shape a union
    /// test cares about after restriction.
    fn group_file_counts(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
        file_groups(plan).iter().map(|g| g.len()).collect()
    }

    fn union_child_file_counts(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<usize>> {
        plan.children().into_iter().map(group_file_counts).collect()
    }

    /// A `UnionExec` concatenates its children's partitions, so stage
    /// partition `left_count + k` is the right child's local partition
    /// `k`. Applying the parent's index directly to every child (the pre-
    /// port behaviour) would route every partition into every child and
    /// most would land out of range, emptying every scan.
    #[test]
    fn union_partition_maps_to_the_right_childs_local_group() {
        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![scan_with_file_groups(4), scan_with_file_groups(4)])
                .unwrap();
        assert_eq!(
            union.properties().output_partitioning().partition_count(),
            8
        );

        // Parent partition 1 is served by the left child's local partition 1.
        let restricted = restrict_plan_to_partitions(union.clone(), &[1], None).unwrap();
        assert_eq!(
            union_child_file_counts(&restricted),
            vec![vec![1], vec![]],
            "left scan keeps only its local group 1; right scan is not polled \
             and becomes 0-partition"
        );

        // Parent partition 6 is served by the right child's local partition 2
        // (6 - 4). Before the mapping existed this would have addressed no
        // group at all in either child.
        let restricted = restrict_plan_to_partitions(union, &[6], None).unwrap();
        assert_eq!(
            union_child_file_counts(&restricted),
            vec![vec![], vec![1]],
            "right scan keeps only its local group 2; left scan is not polled"
        );
    }

    /// Every partition of a union must pin exactly one group in exactly one
    /// child, so across the whole stage each file group is read exactly once.
    #[test]
    fn every_union_partition_pins_exactly_one_group() {
        for partition in 0..6 {
            let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![
                scan_with_file_groups(3),
                scan_with_file_groups(3),
            ])
            .unwrap();
            let restricted =
                restrict_plan_to_partitions(union, &[partition], None).unwrap();
            let counts = union_child_file_counts(&restricted);
            let (serving, idle) = if partition < 3 { (0, 1) } else { (1, 0) };
            assert_eq!(
                counts[serving],
                vec![1],
                "partition {partition}: serving child must keep exactly one group"
            );
            assert!(
                counts[idle].is_empty(),
                "partition {partition}: idle child must be a 0-partition subplan, \
                 got {:?}",
                counts[idle]
            );
        }
    }

    /// Without a union a partition passes straight through, so the behaviour
    /// is unchanged from restricting the scan directly.
    #[test]
    fn scan_without_union_is_pinned_to_its_own_partition() {
        let plan = scan_with_file_groups(4);
        let restricted = restrict_plan_to_partitions(plan, &[2], None).unwrap();
        assert_eq!(group_file_counts(&restricted), vec![1]);
    }

    /// A `CrossJoinExec` collects its left input, so that side must be read
    /// whole. Scoping is taken from `required_input_distribution()` rather
    /// than a list of join types, which is what brings cross joins in.
    ///
    /// Restricting the collected side would give each sibling task a
    /// different fraction of it, and the join would emit a different subset
    /// of the cartesian product per task — TPC-DS q77's `from cs, cr` branch
    /// came back inflated exactly this way.
    #[test]
    fn cross_join_left_side_is_read_whole() {
        use datafusion::physical_plan::joins::CrossJoinExec;

        let join: Arc<dyn ExecutionPlan> = Arc::new(CrossJoinExec::new(
            scan_with_file_groups(4),
            scan_with_file_groups(4),
        ));

        // Sanity: the operator really does declare its left input collected.
        assert!(
            matches!(
                join.input_distribution_requirements().child_distribution(0),
                Some(Distribution::SinglePartition)
            ),
            "CrossJoinExec must declare SinglePartition on its left input"
        );

        let restricted = restrict_plan_to_partitions(join, &[1], None).unwrap();
        let children = restricted.children();
        assert_eq!(
            group_file_counts(children[0]),
            vec![1, 1, 1, 1],
            "collected left side keeps every group"
        );
        assert_eq!(
            group_file_counts(children[1]),
            vec![1],
            "streaming right side is pinned to this task's partition"
        );
    }

    /// A `RangeFilterExec` over a four-partition `ShuffleReaderExec`, with the
    /// raw bounds it was built from. Partition `i` of the stage owns bound `i`.
    fn range_filter_over_reader() -> (Arc<dyn ExecutionPlan>, Vec<RangeBound>) {
        use ballista_core::execution_plans::RangeFilterExec;
        use datafusion::physical_expr::Partitioning;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::scalar::ScalarValue;

        // 4 upstream partitions with a global 4-way range partition.
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let partitions_locs: Vec<Vec<PartitionLocation>> =
            (0..4).map(|i| vec![create_partition(i)]).collect();
        let reader = ShuffleReaderExec::try_new(
            1,
            partitions_locs,
            schema.clone(),
            Partitioning::UnknownPartitioning(4),
        )
        .unwrap();
        let filter_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("v", 0));
        // K=4 raw bounds derived from cuts [100, 200, 300].
        let sv = |v: f64| ScalarValue::Float64(Some(v));
        let raw_bounds: Vec<RangeBound> = vec![
            (None, Some(sv(100.0))),
            (Some(sv(100.0)), Some(sv(200.0))),
            (Some(sv(200.0)), Some(sv(300.0))),
            (Some(sv(300.0)), None),
        ];
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            RangeFilterExec::try_new_resolved(
                Arc::new(reader) as Arc<dyn ExecutionPlan>,
                filter_expr,
                ScalarValue::Float64(Some(0.0)),
                ScalarValue::Float64(Some(0.0)),
                None,
                raw_bounds.clone(),
            )
            .unwrap(),
        );
        (plan, raw_bounds)
    }

    /// A `RangeFilterExec` restricted to a subset of partitions must slice
    /// its `raw_bounds` by the same indices in the same order as its child.
    /// Halos + routing_expr carry over unchanged.
    #[test]
    fn range_filter_bounds_are_sliced_with_partitions() {
        use ballista_core::execution_plans::RangeFilterExec;

        let (plan, raw_bounds) = range_filter_over_reader();

        let restricted = restrict_plan_to_partitions(plan, &[1, 3], None).unwrap();
        let rf = restricted
            .downcast_ref::<RangeFilterExec>()
            .expect("top must remain RangeFilterExec");
        let restricted_bounds = rf.raw_bounds().unwrap();
        assert_eq!(restricted_bounds.len(), 2);
        assert_eq!(restricted_bounds[0], raw_bounds[1]);
        assert_eq!(restricted_bounds[1], raw_bounds[3]);

        // Reader below must have been restricted in the same order.
        let child = rf.children()[0].clone();
        let reader = child
            .downcast_ref::<ShuffleReaderExec>()
            .expect("child must be a ShuffleReaderExec");
        assert_eq!(reader.partition.len(), 2);
        assert_eq!(reader.partition[0][0].partition_id.partition_id, 1);
        assert_eq!(reader.partition[1][0].partition_id.partition_id, 3);
    }

    // --- Nodes the scheduler does not know: `PayloadExec` stands in for one ---

    use crate::test_utils::payload::{PayloadExec, payload_resolver, payloads_of};
    use ballista_core::execution_plans::plan_algebra::PartitionSliceableResolver;
    use std::sync::Mutex;

    fn partition_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        plan.properties().output_partitioning().partition_count()
    }

    /// The file of each group of a scan (`group_file_counts` only counts them).
    fn group_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        file_groups(plan)
            .iter()
            .map(|group| group.files()[0].object_meta.location.to_string())
            .collect()
    }

    #[test]
    fn resolver_restricts_a_node_the_scheduler_does_not_know() {
        let resolver = payload_resolver();
        let leaf = PayloadExec::leaf(&["p0", "p1", "p2", "p3"]);

        let restricted =
            restrict_plan_to_partitions(leaf, &[1, 3], Some(&resolver)).unwrap();

        assert_eq!(payloads_of(&restricted), ["p1", "p3"]);
        assert_eq!(partition_count(&restricted), 2);
    }

    #[test]
    fn a_leaf_nobody_can_restrict_is_refused_unless_the_task_runs_all_of_it() {
        let leaf = PayloadExec::leaf(&["p0", "p1"]);

        let kept = restrict_plan_to_partitions(leaf.clone(), &[0, 1], None).unwrap();
        assert!(
            Arc::ptr_eq(&kept, &leaf),
            "an unrestricted leaf is left alone"
        );

        // A one-partition leaf is right for the task that owns it.
        let single = PayloadExec::leaf(&["p0"]);
        assert!(restrict_plan_to_partitions(single, &[0], None).is_ok());

        for partitions in [&[0][..], &[1], &[1, 0], &[]] {
            assert!(
                restrict_plan_to_partitions(leaf.clone(), partitions, None).is_err(),
                "{partitions:?} is not all of the leaf, in order"
            );
        }

        let error = restrict_plan_to_partitions(leaf, &[1], None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("PayloadExec"), "{error}");
        assert!(error.contains("[1]"), "{error}");
        assert!(error.contains("PartitionSliceable"), "{error}");
    }

    #[test]
    fn the_resolver_is_asked_only_after_the_built_in_list() {
        let asked = Arc::new(Mutex::new(vec![]));
        let record = asked.clone();
        let resolver: PartitionSliceableResolver = Arc::new(move |plan| {
            record.lock().unwrap().push(plan.name().to_string());
            None
        });
        // A `RangeFilterExec` is on the `PartitionSliceable` list, the
        // `ShuffleReaderExec` below it is a leaf the scheduler restricts itself.
        let (plan, _) = range_filter_over_reader();

        restrict_plan_to_partitions(plan, &[1, 3], Some(&resolver)).unwrap();
        assert!(asked.lock().unwrap().is_empty());

        // A node it does not know is put to the resolver.
        let leaf = PayloadExec::leaf(&["p0"]);
        restrict_plan_to_partitions(leaf, &[0], Some(&resolver)).unwrap();
        assert_eq!(*asked.lock().unwrap(), ["PayloadExec"]);
    }

    #[test]
    fn inputs_are_restricted_except_one_the_node_reads_whole() {
        let resolver = payload_resolver();
        let node = PayloadExec::over(
            &["p0", "p1", "p2", "p3"],
            vec![scan_with_file_groups(4), scan_with_file_groups(4)],
            &[1],
        );

        let restricted =
            restrict_plan_to_partitions(node, &[1, 3], Some(&resolver)).unwrap();

        assert_eq!(payloads_of(&restricted), ["p1", "p3"]);
        let inputs = restricted.children();
        assert_eq!(group_files(inputs[0]), ["file1.parquet", "file3.parquet"]);
        assert_eq!(group_file_counts(inputs[1]), vec![1, 1, 1, 1]);
    }

    #[test]
    fn a_node_below_a_collapse_is_read_whole() {
        let resolver = payload_resolver();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(PayloadExec::leaf(&[
                "p0", "p1", "p2", "p3",
            ])));

        let restricted =
            restrict_plan_to_partitions(plan, &[1], Some(&resolver)).unwrap();

        assert_eq!(
            payloads_of(restricted.children()[0]),
            ["p0", "p1", "p2", "p3"]
        );
    }

    /// A union branch that belongs to another task must restrict to nothing,
    /// which needs the resolver for a node Ballista does not know.
    #[test]
    fn a_union_branch_owned_by_another_task_restricts_to_nothing() {
        let resolver = payload_resolver();
        let union = || -> Arc<dyn ExecutionPlan> {
            UnionExec::try_new(vec![
                scan_with_file_groups(2),
                PayloadExec::leaf(&["p0", "p1"]),
            ])
            .unwrap()
        };

        // Partition 2 is the second branch's partition 0.
        let restricted =
            restrict_plan_to_partitions(union(), &[2], Some(&resolver)).unwrap();
        let branches = restricted.children();
        assert_eq!(partition_count(branches[0]), 0);
        assert_eq!(payloads_of(branches[1]), ["p0"]);

        // And partition 0 is the first branch's, which leaves the second none.
        let restricted =
            restrict_plan_to_partitions(union(), &[0], Some(&resolver)).unwrap();
        let branches = restricted.children();
        assert_eq!(group_files(branches[0]), ["file0.parquet"]);
        assert_eq!(partition_count(branches[1]), 0);

        assert!(restrict_plan_to_partitions(union(), &[0], None).is_err());
        assert!(restrict_plan_to_partitions(union(), &[2], None).is_err());
    }

    #[test]
    fn a_restriction_that_keeps_the_wrong_partition_count_is_rejected() {
        let resolver = payload_resolver();
        let leaf = PayloadExec::forgets_to_restrict(&["p0", "p1", "p2", "p3"]);

        let error = restrict_plan_to_partitions(leaf, &[1, 3], Some(&resolver))
            .unwrap_err()
            .to_string();

        assert!(error.contains("PayloadExec"), "{error}");
        assert!(error.contains("returned 4 partitions"), "{error}");
    }

    /// An `EmptyExec` sees the partition numbers of the repartition above it,
    /// not its own, so these two leaves are exempt from the leaf check.
    #[test]
    fn self_generating_leaves_are_exempt_from_the_leaf_check() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        assert!(restrict_plan_to_partitions(empty, &[1], None).is_ok());

        let placeholder: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(schema));
        assert!(restrict_plan_to_partitions(placeholder.clone(), &[0], None).is_ok());

        // A task that polls none of it gets a source with no partitions.
        let nothing = restrict_plan_to_partitions(placeholder, &[], None).unwrap();
        assert_eq!(partition_count(&nothing), 0);
    }

    // --- Range-partitioned readers ---
    //
    // A reader fed by a range shuffle reports `Partitioning::Range`, and
    // restricting it must slice the split points the same way it slices the
    // locations, or the restricted reader would claim ranges its partitions do
    // not hold.

    use datafusion::arrow::compute::SortOptions;
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, SplitPoint};

    /// The key `a`, ascending.
    fn range_ordering() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
        .unwrap()
    }

    /// A reader over `n` partitions where partition `i` holds the keys in
    /// `[10 * i, 10 * (i + 1))`: split points 10, 20, ... `10 * (n - 1)`.
    fn range_shuffle_reader(n: usize) -> Arc<dyn ExecutionPlan> {
        let split_points = (1..n)
            .map(|i| SplitPoint::new(vec![ScalarValue::Int32(Some(10 * i as i32))]))
            .collect();
        let partitions = (0..n).map(|i| vec![create_partition(i)]).collect();
        Arc::new(
            ShuffleReaderExec::try_new(
                1,
                partitions,
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                Partitioning::Range(
                    RangePartitioning::try_new(range_ordering(), split_points).unwrap(),
                ),
            )
            .unwrap(),
        )
    }

    /// The upper bounds of the key ranges a restricted reader reports, in
    /// partition order; the last partition is unbounded above.
    fn reported_split_points(plan: &Arc<dyn ExecutionPlan>) -> Vec<i32> {
        let Partitioning::Range(range) = plan.properties().output_partitioning() else {
            panic!(
                "expected a range partitioning, got {}",
                plan.properties().output_partitioning()
            );
        };
        range
            .split_points()
            .iter()
            .map(|split| match split.values() {
                [ScalarValue::Int32(Some(v))] => *v,
                other => panic!("unexpected split point {other:?}"),
            })
            .collect()
    }

    fn location_ids(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
        plan.downcast_ref::<ShuffleReaderExec>()
            .expect("expected a ShuffleReaderExec")
            .partition
            .iter()
            .map(|locations| locations[0].partition_id.partition_id)
            .collect()
    }

    #[test]
    fn contiguous_slice_of_a_range_reader_keeps_the_ranges_between_its_split_points() {
        // Partitions 1 and 2 of [..10) [10..20) [20..30) [30..40) [40..): they
        // meet at 20, where partition 1 ends.
        let restricted =
            restrict_plan_to_partitions(range_shuffle_reader(5), &[1, 2], None).unwrap();

        assert_eq!(location_ids(&restricted), vec![1, 2]);
        assert_eq!(reported_split_points(&restricted), vec![20]);
        assert_eq!(
            restricted
                .properties()
                .output_partitioning()
                .partition_count(),
            2
        );
    }

    #[test]
    fn gapped_slice_of_a_range_reader_splits_at_the_end_of_each_kept_partition() {
        // Partitions 0 and 3 are not neighbours. The boundary between them can
        // sit anywhere in the gap; 10, where partition 0 ends, is as good as any
        // because nothing in the gap is read.
        let restricted =
            restrict_plan_to_partitions(range_shuffle_reader(5), &[0, 3], None).unwrap();

        assert_eq!(location_ids(&restricted), vec![0, 3]);
        assert_eq!(reported_split_points(&restricted), vec![10]);
    }

    #[test]
    fn single_partition_of_a_range_reader_has_no_split_points() {
        let restricted =
            restrict_plan_to_partitions(range_shuffle_reader(5), &[3], None).unwrap();

        assert_eq!(location_ids(&restricted), vec![3]);
        assert_eq!(reported_split_points(&restricted), Vec::<i32>::new());
        assert_eq!(
            restricted
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
    }

    #[test]
    fn restricting_a_range_reader_to_every_partition_changes_nothing() {
        let reader = range_shuffle_reader(4);
        let restricted =
            restrict_plan_to_partitions(reader.clone(), &[0, 1, 2, 3], None).unwrap();

        assert_eq!(
            restricted.properties().output_partitioning(),
            reader.properties().output_partitioning()
        );
    }

    /// The ordering, sort options included, is what the split points are
    /// relative to, so it must come through restriction untouched.
    #[test]
    fn restricting_a_range_reader_keeps_its_ordering() {
        let restricted =
            restrict_plan_to_partitions(range_shuffle_reader(5), &[1, 2, 4], None)
                .unwrap();

        let Partitioning::Range(range) = restricted.properties().output_partitioning()
        else {
            panic!("expected a range partitioning");
        };
        assert_eq!(range.ordering(), &range_ordering());
        assert_eq!(reported_split_points(&restricted), vec![20, 30]);
    }

    /// A task's slice need not ascend: a retried partition is queued ahead of
    /// later ones. No split points describe partitions that are out of key
    /// order, so the reader reports an unknown partitioning of the right width
    /// rather than failing the task or claiming ranges it does not hold.
    #[test]
    fn range_reader_with_a_slice_out_of_key_order_reports_unknown_partitioning() {
        for slice in [vec![3, 1], vec![2, 2], vec![4, 0, 1]] {
            let restricted =
                restrict_plan_to_partitions(range_shuffle_reader(5), &slice, None)
                    .unwrap();

            assert_eq!(location_ids(&restricted), slice);
            assert!(
                matches!(
                    restricted.properties().output_partitioning(),
                    Partitioning::UnknownPartitioning(n) if *n == slice.len()
                ),
                "slice {slice:?} reported {}",
                restricted.properties().output_partitioning()
            );
        }
    }

    /// A union branch the task does not own is restricted to nothing, and a
    /// range partitioning cannot have zero partitions.
    #[test]
    fn range_reader_restricted_to_nothing_has_no_partitions() {
        let restricted =
            restrict_plan_to_partitions(range_shuffle_reader(5), &[], None).unwrap();

        assert!(location_ids(&restricted).is_empty());
        assert!(matches!(
            restricted.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(0)
        ));
    }

    /// The walker rebuilds the operators above the reader around the restricted
    /// one, so the reader's narrower partitioning has to carry up.
    #[test]
    fn operators_above_a_restricted_range_reader_see_its_partition_count() {
        use datafusion::physical_plan::sorts::sort::SortExec;

        let sorted: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(range_ordering(), range_shuffle_reader(5))
                .with_preserve_partitioning(true),
        );

        let restricted = restrict_plan_to_partitions(sorted, &[2, 3], None).unwrap();

        assert_eq!(
            restricted
                .properties()
                .output_partitioning()
                .partition_count(),
            2
        );
        let reader = restricted.children()[0].clone();
        assert_eq!(location_ids(&reader), vec![2, 3]);
        assert_eq!(reported_split_points(&reader), vec![30]);
    }
}
