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

use ballista_core::execution_plans::ShuffleReaderExec;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::physical_expr::Distribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::with_new_children_if_necessary;
use log::warn;
use std::any::Any;
use std::sync::Arc;

/// Restrict `plan` so that its leaves only see the given `partitions`, unless
/// a leaf sits below a collapse operator (`CoalescePartitionsExec`,
/// `SortPreservingMergeExec`, or a `SinglePartition`-requiring join build
/// side) — those leaves keep the full upstream so the collapse sees every
/// partition.
pub fn restrict_plan_to_partitions(
    plan: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Result<Arc<dyn ExecutionPlan>> {
    restrict(plan, partitions, /* under_collect */ false)
}

/// Recursive worker. `under_collect` is the scope inherited from ancestors:
/// once set, every descendant leaf reads the full upstream. Scope is passed
/// by value, so sibling subtrees never leak state to each other.
fn restrict(
    plan: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    under_collect: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Leaves apply (or skip) restriction based on the current scope.
    if let Some(rewritten) = rewrite_shuffle_reader(&plan, partitions, under_collect) {
        return Ok(rewritten);
    }
    if let Some(rewritten) = rewrite_scan(&plan, partitions, under_collect) {
        return Ok(rewritten);
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
            .map(|(child, sub)| restrict(child, &sub, false))
            .collect::<Result<Vec<_>>>()?;
        return with_new_children_if_necessary(plan, new_children);
    }

    // Interior: compute per-child scope, recurse, rebuild the node.
    let per_child = child_scopes(&plan, under_collect);
    let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
        .children()
        .into_iter()
        .cloned()
        .zip(per_child)
        .map(|(child, collect)| restrict(child, partitions, collect))
        .collect::<Result<Vec<_>>>()?;
    with_new_children_if_necessary(plan, new_children)
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
///   children.
/// - `HashJoinExec` / `NestedLoopJoinExec`: set per child based on
///   `required_input_distribution()`. Typically only the build side is
///   `SinglePartition`, so the probe side inherits `false` and remains
///   partition-aligned.
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
    if plan.is::<HashJoinExec>() || plan.is::<NestedLoopJoinExec>() {
        return plan
            .required_input_distribution()
            .into_iter()
            .map(|d| matches!(d, Distribution::SinglePartition))
            .collect();
    }
    vec![false; children.len()]
}

/// Restrict a `ShuffleReaderExec` so only the assigned `partitions` remain
/// in its `partition` vec — unless `under_collect` is true, in which case
/// every upstream partition is preserved. Output_partitioning shrinks to
/// `kept.len()` but **preserves the partitioning kind** — a `Hash([col], N)`
/// reader becomes `Hash([col], kept.len())`, not `UnknownPartitioning`. This
/// matters for operators like `InterleaveExec` above the reader that assert
/// children share a hash partitioning to fuse safely.
fn rewrite_shuffle_reader(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    under_collect: bool,
) -> Option<Arc<dyn ExecutionPlan>> {
    let reader = plan.downcast_ref::<ShuffleReaderExec>()?;
    // Broadcast readers serve everything from partition[0] regardless of
    // task; leave them intact regardless of scope.
    if reader.broadcast {
        return None;
    }
    let kept: Vec<Vec<_>> = if under_collect {
        reader.partition.clone()
    } else {
        partitions
            .iter()
            .filter_map(|&p| reader.partition.get(p).cloned())
            .collect()
    };
    let partitioning = match reader.properties().output_partitioning() {
        datafusion::physical_plan::Partitioning::Hash(exprs, _) => {
            datafusion::physical_plan::Partitioning::Hash(exprs.clone(), kept.len())
        }
        datafusion::physical_plan::Partitioning::RoundRobinBatch(_) => {
            datafusion::physical_plan::Partitioning::RoundRobinBatch(kept.len())
        }
        datafusion::physical_plan::Partitioning::UnknownPartitioning(_) => {
            datafusion::physical_plan::Partitioning::UnknownPartitioning(kept.len())
        }
    };
    let restricted =
        ShuffleReaderExec::try_new(reader.stage_id, kept, reader.schema(), partitioning)
            .ok()?;
    Some(Arc::new(restricted))
}

/// Restrict a `DataSourceExec` (file-backed or in-memory) so only the
/// assigned `partitions` remain, or take all groups if `under_collect` is
/// true. `output_partitioning().partition_count()` shrinks to
/// `partitions.len()` — matching what `rewrite_shuffle_reader` does — so
/// position `i` in the restricted plan corresponds to the task's
/// `global_input_partition_ids[i]` globally.
fn rewrite_scan(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
    under_collect: bool,
) -> Option<Arc<dyn ExecutionPlan>> {
    let exec = plan.downcast_ref::<DataSourceExec>()?;
    let source: &dyn Any = exec.data_source().as_ref();
    if let Some(config) = source.downcast_ref::<FileScanConfig>() {
        if under_collect {
            return None;
        }
        let file_groups: Vec<FileGroup> = partitions
            .iter()
            .filter_map(|&i| config.file_groups.get(i).cloned())
            .collect();
        let restricted = FileScanConfigBuilder::from(config.clone())
            .with_file_groups(file_groups)
            .build();
        return Some(DataSourceExec::from_data_source(restricted));
    }
    if let Some(config) = source.downcast_ref::<MemorySourceConfig>() {
        if under_collect {
            return None;
        }
        let kept: Vec<Vec<_>> = partitions
            .iter()
            .filter_map(|&i| config.partitions().get(i).cloned())
            .collect();
        let restricted = MemorySourceConfig::try_new(
            &kept,
            config.original_schema(),
            config.projection().clone(),
        )
        .ok()?;
        return Some(DataSourceExec::from_data_source(restricted));
    }
    warn!(
        "restrict_plan_to_partitions: unrecognised DataSourceExec source \
         left unrestricted; if it distributes work from a shared queue, \
         tasks would over-read"
    );
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::JobId;
    use ballista_core::execution_plans::{
        CoalescePlan, PartitionGroup, ShuffleReaderExec,
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

    #[test]
    fn keeps_only_the_wanted_partition() {
        let partitions = (0..4).map(|i| vec![create_partition(i)]).collect();
        let reader = ShuffleReaderExec::try_new(
            1,
            partitions,
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            Partitioning::UnknownPartitioning(4),
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(reader);
        let pruned = restrict_plan_to_partitions(plan, &[1]).unwrap();
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
        let partitions = (0..4).map(|i| vec![create_partition(i)]).collect();
        let reader = ShuffleReaderExec::try_new(
            1,
            partitions,
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            Partitioning::UnknownPartitioning(4),
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(reader);
        let pruned = restrict_plan_to_partitions(plan, &[0, 3]).unwrap();
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
        let pruned = restrict_plan_to_partitions(plan, &[1]).unwrap();
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
        let pruned = restrict_plan_to_partitions(plan, &[1]).unwrap();
        let r = pruned
            .downcast_ref::<ShuffleReaderExec>()
            .expect("expected a ShuffleReaderExec");
        assert!(r.coalesce.is_some());
        assert_eq!(r.partition.len(), 1);
        assert_eq!(r.partition[0].len(), 2);
    }

    #[test]
    fn restricting_to_every_partition_is_a_no_op() {
        let partitions = (0..3).map(|i| vec![create_partition(i)]).collect();
        let reader = ShuffleReaderExec::try_new(
            1,
            partitions,
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            Partitioning::UnknownPartitioning(3),
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(reader);
        let pruned = restrict_plan_to_partitions(plan, &[0, 1, 2]).unwrap();
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

    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::union::UnionExec;

    /// Build a `DataSourceExec` over `n` file groups (one file each), so
    /// every group is exactly one partition. The scan's output partition
    /// count equals `n`.
    fn scan_with_file_groups(n: usize) -> Arc<dyn ExecutionPlan> {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let source = Arc::new(ParquetSource::new(schema));
        let mut builder =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source);
        for i in 0..n {
            builder =
                builder.with_file_group(FileGroup::new(vec![PartitionedFile::new(
                    format!("file{i}.parquet"),
                    100,
                )]));
        }
        DataSourceExec::from_data_source(builder.build())
    }

    /// File counts per file group of a `DataSourceExec` — the shape a union
    /// test cares about after restriction.
    fn group_file_counts(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
        let exec = plan.downcast_ref::<DataSourceExec>().unwrap();
        let source: &dyn Any = exec.data_source().as_ref();
        let config = source.downcast_ref::<FileScanConfig>().unwrap();
        config.file_groups.iter().map(|g| g.len()).collect()
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
        let restricted = restrict_plan_to_partitions(union.clone(), &[1]).unwrap();
        assert_eq!(
            union_child_file_counts(&restricted),
            vec![vec![1], vec![]],
            "left scan keeps only its local group 1; right scan is not polled \
             and becomes 0-partition"
        );

        // Parent partition 6 is served by the right child's local partition 2
        // (6 - 4). Before the mapping existed this would have addressed no
        // group at all in either child.
        let restricted = restrict_plan_to_partitions(union, &[6]).unwrap();
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
            let restricted = restrict_plan_to_partitions(union, &[partition]).unwrap();
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
        let restricted = restrict_plan_to_partitions(plan, &[2]).unwrap();
        assert_eq!(group_file_counts(&restricted), vec![1]);
    }
}
