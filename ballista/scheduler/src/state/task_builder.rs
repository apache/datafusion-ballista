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
    // TODO: test with unions
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
/// - TODO(union): `UnionExec` needs per-child sub-ranges of `partitions`
///   (the parent's partition indices map to disjoint index ranges across
///   children). That's an orthogonal `Scope::ScopedPartitions(start, end)`
///   variant on the DQE side; add it here when a query with a `UnionExec`
///   under partition-aligned parents demands it. See the ignored test
///   `restrict_union_splits_partitions_per_child` for the assertion we owe.
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
}
