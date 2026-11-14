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
    PartitionSliceableResolver, as_partition_sliceable, slice_by_global_partition,
};
use ballista_core::execution_plans::{
    PipelinedShuffleReaderExec, RangeShuffleReaderExec, ShuffleReaderExec,
    UnresolvedShuffleExec,
};
use datafusion::common::plan_err;
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
/// A leaf Ballista does not list becomes an empty leaf when the task runs none
/// of it. Other nodes Ballista does not list are restricted by `resolver`, and
/// a leaf that nothing can restrict is an error unless `partitions` is all of
/// its partitions, in order. Each branch of a `UnionExec` must end up with one
/// partition for each partition the task runs of it.
pub fn restrict_plan_to_partitions(
    plan: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    resolver: Option<&dyn PartitionSliceableResolver>,
) -> Result<Arc<dyn ExecutionPlan>> {
    restrict(plan, partitions, /* under_collect */ false, resolver)
}

/// The stages whose output a leaf of the stage plan `plan` reads in full,
/// in every task of the stage, because the leaf sits below a collapse: the
/// leaves [`restrict_plan_to_partitions`] leaves unrestricted.
///
/// A pipelined producer serves each output partition to a single reader, so
/// one read in full by every task of a stage with several tasks has to keep
/// its output for each of them.
pub(crate) fn stages_read_whole(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    fn walk(plan: &Arc<dyn ExecutionPlan>, under_collect: bool, found: &mut Vec<usize>) {
        if let Some(reader) = plan.downcast_ref::<UnresolvedShuffleExec>() {
            if under_collect {
                found.push(reader.stage_id);
            }
            return;
        }
        // The same scopes `restrict` gives each child.
        let scopes = if !under_collect && plan.is::<UnionExec>() {
            vec![false; plan.children().len()]
        } else {
            child_scopes(plan, under_collect)
        };
        for (child, collect) in plan.children().into_iter().zip(scopes) {
            walk(child, collect, found);
        }
    }
    let mut found = vec![];
    walk(plan, false, &mut found);
    found
}

/// Recursive worker. `under_collect` is the scope inherited from ancestors:
/// once set, every descendant leaf reads the full upstream. Scope is passed
/// by value, so sibling subtrees never leak state to each other.
fn restrict(
    plan: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    under_collect: bool,
    resolver: Option<&dyn PartitionSliceableResolver>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Leaves: apply the algebraic partition-projection when we're not
    // under a collapse. Under collapse, leaves keep their full upstream —
    // the generic recursion below leaves them unchanged.
    if !under_collect
        && let Some(rewritten) = select_output_partitions(&plan, partitions)?
    {
        return Ok(rewritten);
    }

    // Any other leaf the task runs none of, such as one in a `UnionExec`
    // branch another task owns, is never executed.
    if !under_collect && partitions.is_empty() && plan.children().is_empty() {
        return no_partitions(&plan);
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
            .map(|(child, sub)| {
                let restricted = restrict(child, &sub, false, resolver)?;
                ensure_branch_restricted(&restricted, &sub)?;
                Ok(restricted)
            })
            .collect::<Result<Vec<_>>>()?;
        return replace_children_if_necessary(plan, new_children);
    }

    let children = restrict_children(&plan, partitions, under_collect, resolver)?;
    if under_collect {
        return replace_children_if_necessary(plan, children);
    }

    // Operators carrying data indexed by global partition slice it parallel to
    // the restriction. Each one implements the slicing beside its own fields;
    // this walker only supplies the restricted children. A node Ballista does
    // not list may be restricted by the resolver.
    let restricted = match as_partition_sliceable(&plan) {
        Some(sliceable) => {
            Some(sliceable.restrict_to_partitions(children.clone(), partitions))
        }
        None => resolver.and_then(|resolver| {
            resolver.restrict_to_partitions(&plan, children.clone(), partitions)
        }),
    };
    if let Some(restricted) = restricted {
        return checked_restriction(plan.name(), restricted?, partitions);
    }

    // A leaf nothing above could restrict is only right if the task runs all
    // of it.
    if children.is_empty() {
        ensure_leaf_runs_whole(&plan, partitions)?;
        return Ok(plan);
    }
    replace_children_if_necessary(plan, children)
}

/// Restrict each child of `plan`, in the scope `child_scopes` gives it.
fn restrict_children(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    under_collect: bool,
    resolver: Option<&dyn PartitionSliceableResolver>,
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

/// A leaf with `plan`'s schema and no partitions, for a leaf the task runs
/// none of.
///
/// Not an `EmptyExec`: DataFusion's proto codec decodes one with zero
/// partitions as one partition, so it would reach the executor with a
/// partition the task doesn't own, which the executor would run. A
/// `MemorySourceConfig` keeps its empty partition list.
fn no_partitions(plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let source = MemorySourceConfig::try_new(&[], plan.schema(), None)?;
    Ok(DataSourceExec::from_data_source(source))
}

/// Accept a leaf that nothing could restrict only if the task runs all of it,
/// in order. Left as it is, it numbers its partitions from zero, so any other
/// slice would run the wrong ones. A one-partition leaf therefore passes in the
/// task that owns it.
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
         can be restricted by `SchedulerConfig::partition_sliceable_resolver`, for instance \
         by implementing `PartitionSliceable` and calling it from there."
    )
}

/// Check that a restricted `UnionExec` branch has one partition for each of
/// `partitions`. The executor runs every partition of the task's plan, so a
/// branch that keeps partitions the task wasn't given runs them in this task
/// too, and the union's later partitions no longer line up with the task's.
/// When those partitions read their input whole, as below a `SortExec`, a
/// `LIMIT` or an aggregate without `GROUP BY` that merges the branch into one
/// partition, the task returns rows that another task returns as well. A node
/// the scheduler doesn't know that keeps its own count does the same.
fn ensure_branch_restricted(
    branch: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Result<()> {
    let count = branch.properties().output_partitioning().partition_count();
    if count == partitions.len() {
        return Ok(());
    }
    let name = branch.name();
    plan_err!(
        "cannot restrict a `UnionExec` branch to partitions {partitions:?}: its \
         `{name}` keeps a partition count of {count}, so the task would also run \
         partitions another task owns. The adaptive planner can leave a branch \
         that merges its input into one partition, such as through an `ORDER BY`, \
         a `LIMIT` or an aggregate without `GROUP BY`, in the union's stage; the \
         static planner (`ballista.planner.adaptive.enabled = false`) gives it a \
         stage of its own. A node of your own can be restricted by \
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
///   A `Range` slices its split points too: [`narrow_range`].
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
    // PipelinedShuffleReaderExec: cross-stage inputs of a pipelined job.
    if let Some(reader) = plan.downcast_ref::<PipelinedShuffleReaderExec>() {
        // A broadcast reader streams everything into its one partition for
        // every consumer task; slicing would drop the payload.
        if reader.broadcast {
            return Ok(Some(plan.clone()));
        }
        return Ok(Some(Arc::new(reader.with_partitions(indices)?)));
    }
    // ShuffleReaderExec: cross-stage inputs.
    if let Some(reader) = plan.downcast_ref::<ShuffleReaderExec>() {
        // Broadcast readers serve everything from partition[0] for every
        // consumer — slicing would drop the payload. Leave intact.
        if reader.broadcast {
            return Ok(Some(plan.clone()));
        }
        // Indices past the end are dropped; a range partitioning slices its
        // split points by the rest.
        let (kept_indices, kept): (Vec<_>, Vec<_>) = indices
            .iter()
            .filter_map(|&p| {
                reader
                    .partition
                    .get(p)
                    .map(|locations| (p, locations.clone()))
            })
            .unzip();
        let partitioning = match reader.properties().output_partitioning() {
            Partitioning::Hash(exprs, _) => Partitioning::Hash(exprs.clone(), kept.len()),
            Partitioning::RoundRobinBatch(_) => Partitioning::RoundRobinBatch(kept.len()),
            Partitioning::UnknownPartitioning(_) => {
                Partitioning::UnknownPartitioning(kept.len())
            }
            Partitioning::Range(range) => narrow_range(range, &kept_indices).map_or(
                Partitioning::UnknownPartitioning(kept.len()),
                Partitioning::Range,
            ),
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

    // Self-generating leaves: PlaceholderRow (1 row per partition) and
    // EmptyExec (0 rows per partition). Every partition is the same, so a
    // slice is the leaf with one partition for each index it has. As for the
    // sources above, an index past its own count is dropped: under a
    // round-robin `RepartitionExec` the leaf sees the repartition's numbers,
    // and only the task that owns partition 0 may run a one-partition leaf.
    if plan.is::<PlaceholderRowExec>() || plan.is::<EmptyExec>() {
        let count = plan.properties().output_partitioning().partition_count();
        let kept = indices.iter().filter(|&&i| i < count).count();
        if kept == 0 {
            return no_partitions(plan).map(Some);
        }
        let restricted: Arc<dyn ExecutionPlan> = if plan.is::<EmptyExec>() {
            Arc::new(EmptyExec::new(plan.schema()).with_partitions(kept))
        } else {
            Arc::new(PlaceholderRowExec::new(plan.schema()).with_partitions(kept))
        };
        return Ok(Some(restricted));
    }

    Ok(None)
}

/// `range` narrowed to the partitions in `kept`: new partition `j` ends at old
/// partition `kept[j]`'s upper split point. `None` if `kept` is empty or not
/// ascending (a retried partition can queue ahead of a later one), since no
/// range describes it then; the reader reports `UnknownPartitioning` instead.
fn narrow_range(range: &RangePartitioning, kept: &[usize]) -> Option<RangePartitioning> {
    let (_, leading) = kept.split_last()?;
    if !kept.windows(2).all(|pair| pair[0] < pair[1]) {
        return None;
    }
    let boundaries = slice_by_global_partition(
        range.split_points(),
        leading,
        "ShuffleReaderExec",
        "split points",
    )
    .ok()?;
    RangePartitioning::try_new(range.ordering().clone(), boundaries).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::JobId;
    use ballista_core::execution_plans::{
        CoalescePlan, PartitionGroup, RangeBound, ShuffleReaderExec,
    };
    use ballista_core::serde::scheduler::{
        ExecutorEndpoint, PartitionId, PartitionLocation,
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
            executor_meta: Arc::new(ExecutorEndpoint {
                id: "1".to_string(),
                host: "1.1.1.1".to_string(),
                port: 0,
            }),
            partition_stats: Default::default(),
            file_id: None,
            is_sort_shuffle: false,
        }
    }

    /// A `ShuffleReaderExec` over `n` single-location partitions.
    fn shuffle_reader(n: usize) -> Arc<dyn ExecutionPlan> {
        shuffle_reader_with(n, Partitioning::UnknownPartitioning(n))
    }

    fn shuffle_reader_with(
        n: usize,
        partitioning: Partitioning,
    ) -> Arc<dyn ExecutionPlan> {
        let partitions = (0..n).map(|i| vec![create_partition(i)]).collect();
        Arc::new(
            ShuffleReaderExec::try_new(
                1,
                partitions,
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                partitioning,
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

    // --- Range-partitioned readers ---

    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, SplitPoint};

    fn range_ordering() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
            "a", 0,
        )))])
        .unwrap()
    }

    /// Restricts a reader over five partitions, where partition `i` holds the
    /// keys in `[10 * i, 10 * (i + 1))`, to `slice`. Returns the partition ids
    /// the result reads and the partitioning it reports.
    fn restricted_range_reader(slice: &[usize]) -> (Vec<usize>, Partitioning) {
        let split_points = (1..5)
            .map(|i| SplitPoint::new(vec![ScalarValue::Int32(Some(10 * i))]))
            .collect();
        let range = RangePartitioning::try_new(range_ordering(), split_points).unwrap();

        let reader = shuffle_reader_with(5, Partitioning::Range(range));
        let restricted = restrict_plan_to_partitions(reader, slice, None).unwrap();

        let ids = restricted
            .downcast_ref::<ShuffleReaderExec>()
            .expect("expected a ShuffleReaderExec")
            .partition
            .iter()
            .map(|locations| locations[0].partition_id.partition_id)
            .collect();
        (ids, restricted.properties().output_partitioning().clone())
    }

    /// Each kept partition but the last ends at its own upper split point.
    #[test]
    fn restricting_a_range_reader_slices_its_split_points() {
        for (slice, upper_bounds) in [
            (vec![1, 2], vec![20]),
            // Not neighbours: split where partition 0 ends.
            (vec![0, 3], vec![10]),
            (vec![3], vec![]),
            (vec![1, 2, 4], vec![20, 30]),
            (vec![0, 1, 2, 3, 4], vec![10, 20, 30, 40]),
        ] {
            let (ids, partitioning) = restricted_range_reader(&slice);

            assert_eq!(ids, slice);
            let Partitioning::Range(range) = &partitioning else {
                panic!("{slice:?} reported {partitioning}");
            };
            assert_eq!(range.ordering(), &range_ordering());
            let expected: Vec<_> = upper_bounds
                .iter()
                .map(|&v| SplitPoint::new(vec![ScalarValue::Int32(Some(v))]))
                .collect();
            assert_eq!(range.split_points(), expected.as_slice(), "{slice:?}");
        }
    }

    /// A slice out of key order, or an empty one, reports an unknown
    /// partitioning of the same width instead of failing the task.
    #[test]
    fn range_reader_without_split_points_for_its_slice_reports_unknown_partitioning() {
        for slice in [vec![3, 1], vec![2, 2], vec![4, 0, 1], vec![]] {
            let (ids, partitioning) = restricted_range_reader(&slice);

            assert_eq!(ids, slice);
            assert!(
                matches!(partitioning, Partitioning::UnknownPartitioning(n) if n == slice.len()),
                "{slice:?} reported {partitioning}"
            );
        }
    }

    // --- Nodes the scheduler does not know: `PayloadExec` stands in for one ---

    use crate::test_utils::payload::{
        PayloadExec, fn_resolver, payload_resolver, payloads_of,
    };
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
            restrict_plan_to_partitions(leaf, &[1, 3], Some(resolver.as_ref())).unwrap();

        assert_eq!(payloads_of(&restricted), ["p1", "p3"]);
        assert_eq!(partition_count(&restricted), 2);
    }

    /// A node that cannot implement `PartitionSliceable`, such as a
    /// `DataSourceExec` over a source of your own, is restricted by the
    /// resolver building a replacement.
    #[test]
    fn resolver_restricts_a_node_without_partition_sliceable() {
        let resolver: Arc<dyn PartitionSliceableResolver> =
            fn_resolver(|plan, children, partitions| {
                assert!(children.is_empty());
                plan.is::<PayloadExec>().then(|| {
                    Ok(Arc::new(
                        EmptyExec::new(plan.schema()).with_partitions(partitions.len()),
                    ) as Arc<dyn ExecutionPlan>)
                })
            });
        let leaf = PayloadExec::leaf(&["p0", "p1", "p2", "p3"]);

        let restricted =
            restrict_plan_to_partitions(leaf, &[1, 3], Some(resolver.as_ref())).unwrap();

        assert!(restricted.is::<EmptyExec>());
        assert_eq!(partition_count(&restricted), 2);
    }

    #[test]
    fn a_leaf_nobody_can_restrict_is_refused_unless_the_task_runs_all_or_none_of_it() {
        let leaf = PayloadExec::leaf(&["p0", "p1"]);

        let none = restrict_plan_to_partitions(leaf.clone(), &[], None).unwrap();
        assert_no_partitions(&none, &leaf);

        let kept = restrict_plan_to_partitions(leaf.clone(), &[0, 1], None).unwrap();
        assert!(
            Arc::ptr_eq(&kept, &leaf),
            "an unrestricted leaf is left alone"
        );

        // A one-partition leaf is right for the task that owns it.
        let single = PayloadExec::leaf(&["p0"]);
        assert!(restrict_plan_to_partitions(single, &[0], None).is_ok());

        for partitions in [&[0][..], &[1], &[1, 0]] {
            assert!(
                restrict_plan_to_partitions(leaf.clone(), partitions, None).is_err(),
                "{partitions:?} is not all of the leaf, in order"
            );
        }

        let error = restrict_plan_to_partitions(leaf, &[1], None)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: cannot restrict \
             `PayloadExec: payloads=[\"p0\", \"p1\"]` to partitions [1] of its 2: \
             the scheduler does not know how to restrict a `PayloadExec`, and left \
             alone it would number its partitions from zero in every task. A node \
             of your own can be restricted by \
             `SchedulerConfig::partition_sliceable_resolver`, for instance by \
             implementing `PartitionSliceable` and calling it from there."
        );
    }

    #[test]
    fn the_resolver_is_asked_only_after_the_built_in_list() {
        let asked = Arc::new(Mutex::new(vec![]));
        let record = asked.clone();
        let resolver: Arc<dyn PartitionSliceableResolver> =
            fn_resolver(move |plan, _children, _partitions| {
                record.lock().unwrap().push(plan.name().to_string());
                None
            });
        // A `RangeFilterExec` is on the `PartitionSliceable` list, the
        // `ShuffleReaderExec` below it is a leaf the scheduler restricts itself.
        let (plan, _) = range_filter_over_reader();

        restrict_plan_to_partitions(plan, &[1, 3], Some(resolver.as_ref())).unwrap();
        assert!(asked.lock().unwrap().is_empty());

        // A node it does not know is put to the resolver.
        let leaf = PayloadExec::leaf(&["p0"]);
        restrict_plan_to_partitions(leaf, &[0], Some(resolver.as_ref())).unwrap();
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
            restrict_plan_to_partitions(node, &[1, 3], Some(resolver.as_ref())).unwrap();

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
            restrict_plan_to_partitions(plan, &[1], Some(resolver.as_ref())).unwrap();

        assert_eq!(
            payloads_of(restricted.children()[0]),
            ["p0", "p1", "p2", "p3"]
        );
    }

    /// A union branch that belongs to another task restricts to nothing, even
    /// a node Ballista does not know. Restricting one to some of its
    /// partitions needs the resolver.
    #[test]
    fn a_union_branch_owned_by_another_task_restricts_to_nothing() {
        let resolver = payload_resolver();
        let unknown = PayloadExec::leaf(&["p0", "p1"]);
        let union = || -> Arc<dyn ExecutionPlan> {
            UnionExec::try_new(vec![scan_with_file_groups(2), unknown.clone()]).unwrap()
        };

        // Partition 0 is the scan's, which leaves the unknown leaf none.
        let restricted = restrict_plan_to_partitions(union(), &[0], None).unwrap();
        let branches = restricted.children();
        assert_eq!(group_files(branches[0]), ["file0.parquet"]);
        assert_no_partitions(branches[1], &unknown);

        // Partition 2 is the unknown leaf's partition 0, which only the
        // resolver can cut down.
        let restricted =
            restrict_plan_to_partitions(union(), &[2], Some(resolver.as_ref())).unwrap();
        let branches = restricted.children();
        assert_eq!(group_files(branches[0]), Vec::<String>::new());
        assert_eq!(payloads_of(branches[1]), ["p0"]);

        let in_union = restrict_plan_to_partitions(union(), &[2], None).unwrap_err();
        let alone = restrict_plan_to_partitions(unknown.clone(), &[0], None).unwrap_err();
        assert_eq!(in_union.to_string(), alone.to_string());
    }

    /// The task plan is serialized on its way to the executor, and each union
    /// branch must keep its restricted partition count there, or the task would
    /// run partitions it doesn't own: none for a branch the task runs none of,
    /// and its share for a multi-partition `EmptyExec`.
    #[test]
    fn restricted_union_branches_keep_their_partition_counts_through_serialization() {
        use ballista_core::serde::BallistaPhysicalExtensionCodec;
        use datafusion::prelude::SessionContext;
        use datafusion_proto::bytes::{
            physical_plan_from_bytes_with_extension_codec,
            physical_plan_to_bytes_with_extension_codec,
        };

        let schema = scan_with_file_groups(2).schema();
        // Partitions 0-1 are the scan's, 2 the unknown leaf's, 3-6 the empty one's.
        let union = UnionExec::try_new(vec![
            scan_with_file_groups(2),
            PayloadExec::leaf(&["p0"]),
            Arc::new(EmptyExec::new(schema).with_partitions(4)),
        ])
        .unwrap();
        let restricted = restrict_plan_to_partitions(union, &[0, 4, 6], None).unwrap();

        let codec = BallistaPhysicalExtensionCodec::default();
        let bytes =
            physical_plan_to_bytes_with_extension_codec(restricted, &codec).unwrap();
        let decoded = physical_plan_from_bytes_with_extension_codec(
            &bytes,
            &SessionContext::new().task_ctx(),
            &codec,
        )
        .unwrap();

        let branches = decoded.children();
        assert_eq!(
            branches.iter().map(|b| describe(b)).collect::<Vec<_>>(),
            ["DataSourceExec(1)", "DataSourceExec(0)", "EmptyExec(2)"]
        );
        assert_eq!(group_files(branches[0]), ["file0.parquet"]);
    }

    /// A `UnionExec` branch that keeps another partition count after
    /// restriction would make the task run partitions it doesn't own, so it is
    /// refused.
    #[test]
    fn a_union_branch_with_the_wrong_partition_count_is_refused() {
        use datafusion::physical_plan::sorts::sort::SortExec;
        // A sort that merges its branch into one partition, as the adaptive
        // planner leaves under `UNION ALL ... ORDER BY`. It reads its input
        // whole, so it keeps that partition when the task runs none of it.
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("a", 0),
        ))])
        .unwrap();
        let sorted = Arc::new(SortExec::new(ordering, scan_with_file_groups(2)));
        // Partition 0 is the sort's, 1-2 the scan's.
        let union = UnionExec::try_new(vec![sorted, scan_with_file_groups(2)]).unwrap();

        let error = restrict_plan_to_partitions(union, &[1], None)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: cannot restrict a `UnionExec` branch to \
             partitions []: its `SortExec` keeps a partition count of 1, so the \
             task would also run partitions another task owns. The adaptive \
             planner can leave a branch that merges its input into one partition, \
             such as through an `ORDER BY`, a `LIMIT` or an aggregate without \
             `GROUP BY`, in the union's stage; the static planner \
             (`ballista.planner.adaptive.enabled = false`) gives it a stage of its \
             own. A node of your own can be restricted by \
             `SchedulerConfig::partition_sliceable_resolver`."
        );
    }

    /// `plan` is a leaf with `like`'s schema and no partitions, as the
    /// executor will decode it.
    fn assert_no_partitions(
        plan: &Arc<dyn ExecutionPlan>,
        like: &Arc<dyn ExecutionPlan>,
    ) {
        let memory = plan
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<MemorySourceConfig>());
        assert!(
            memory.is_some(),
            "an empty in-memory source, not {}",
            plan.name()
        );
        assert_eq!(partition_count(plan), 0);
        assert_eq!(plan.schema(), like.schema());
    }

    #[test]
    fn a_restriction_that_keeps_the_wrong_partition_count_is_rejected() {
        let resolver = payload_resolver();
        let leaf = PayloadExec::forgets_to_restrict(&["p0", "p1", "p2", "p3"]);

        let error = restrict_plan_to_partitions(leaf, &[1, 3], Some(resolver.as_ref()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("PayloadExec"), "{error}");
        assert!(error.contains("returned 4 partitions"), "{error}");
    }

    /// A node's name and partition count, such as `EmptyExec(2)`.
    fn describe(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{}({})", plan.name(), partition_count(plan))
    }

    /// `leaf` restricted to each of `slices`, described.
    fn restricted_shapes(
        leaf: &Arc<dyn ExecutionPlan>,
        slices: &[&[usize]],
    ) -> Vec<String> {
        slices
            .iter()
            .map(|slice| {
                let restricted =
                    restrict_plan_to_partitions(leaf.clone(), slice, None).unwrap();
                assert_eq!(restricted.schema(), leaf.schema());
                describe(&restricted)
            })
            .collect()
    }

    /// Every partition of these leaves is the same, so a task gets one for
    /// each of its partitions. The `AdaptivePlanner` replaces a stage that
    /// turns out empty with an `EmptyExec` of that stage's partition count.
    #[test]
    fn self_generating_leaves_keep_one_partition_for_each_the_task_runs() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let empty: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(schema.clone()).with_partitions(4));
        let placeholder: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(schema).with_partitions(4));
        let slices: &[&[usize]] = &[&[0, 1, 2, 3], &[1, 3], &[2], &[]];

        assert_eq!(
            restricted_shapes(&empty, slices),
            [
                "EmptyExec(4)",
                "EmptyExec(2)",
                "EmptyExec(1)",
                "DataSourceExec(0)"
            ]
        );
        assert_eq!(
            restricted_shapes(&placeholder, slices),
            [
                "PlaceholderRowExec(4)",
                "PlaceholderRowExec(2)",
                "PlaceholderRowExec(1)",
                "DataSourceExec(0)",
            ]
        );
    }

    /// Under a round-robin `RepartitionExec`, which the `AdaptivePlanner`
    /// keeps inside a stage, a leaf sees the repartition's partition numbers.
    /// Only the task that owns partition 0 runs a one-partition leaf, so a
    /// `PlaceholderRowExec` there gives its row once, not once per task.
    #[test]
    fn self_generating_leaves_drop_partitions_past_their_own() {
        use datafusion::physical_plan::repartition::RepartitionExec;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let placeholder: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(schema));
        let leaf_in_task = |partitions: &[usize]| {
            let repartition = RepartitionExec::try_new(
                placeholder.clone(),
                Partitioning::RoundRobinBatch(4),
            )
            .unwrap();
            restrict_plan_to_partitions(Arc::new(repartition), partitions, None)
                .unwrap()
                .children()[0]
                .clone()
        };

        assert_eq!(describe(&leaf_in_task(&[0, 1])), "PlaceholderRowExec(1)");
        assert_no_partitions(&leaf_in_task(&[2, 3]), &placeholder);
    }

    /// A union branch the `AdaptivePlanner` replaced with a multi-partition
    /// `EmptyExec`, because its stage gave no rows, is cut down like any other,
    /// so the union routes the task's partitions to the right branch.
    #[test]
    fn a_union_branch_replaced_by_an_empty_exec_is_restricted() {
        let empty: Arc<dyn ExecutionPlan> = Arc::new(
            EmptyExec::new(scan_with_file_groups(4).schema()).with_partitions(4),
        );
        let union = || -> Arc<dyn ExecutionPlan> {
            UnionExec::try_new(vec![empty.clone(), scan_with_file_groups(4)]).unwrap()
        };

        // Partition 0 is the empty branch's.
        let restricted = restrict_plan_to_partitions(union(), &[0], None).unwrap();
        let branches = restricted.children();
        assert_eq!(describe(branches[0]), "EmptyExec(1)");
        assert_eq!(group_files(branches[1]), Vec::<String>::new());

        // Partitions 3 and 6 are the last of the empty branch's and the third
        // of the scan's.
        let restricted = restrict_plan_to_partitions(union(), &[3, 6], None).unwrap();
        let branches = restricted.children();
        assert_eq!(describe(branches[0]), "EmptyExec(1)");
        assert_eq!(group_files(branches[1]), ["file2.parquet"]);
        assert_eq!(describe(&restricted), "UnionExec(2)");
    }
}
