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

//! Pinot-style colocated join rule.
//!
//! When both inputs of a [`HashJoinExec`] are already hash-partitioned by the
//! join keys on the same number of buckets with the same hash function, the
//! [`RepartitionExec`] that DataFusion's `EnforceDistribution` rule would
//! otherwise insert is redundant. This rule strips it, eliminating one shuffle
//! per pair of co-located inputs.
//!
//! The rule is conservative: it only removes an existing `RepartitionExec`
//! when both sides' source plans expose matching [`BallistaPartitionMetadata`]
//! and the join keys align positionally with the declared distribution keys.

use std::sync::Arc;

use ballista_core::partitioning::{
    BallistaPartitionMetadata, BucketSubPartitionExec, HashDistribution,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::repartition::RepartitionExec;

/// Optimizer rule that elides redundant [`RepartitionExec`] above the inputs
/// of a [`HashJoinExec`] when both inputs are co-located.
#[derive(Debug, Default)]
pub struct ColocatedJoinRule {}

impl ColocatedJoinRule {
    /// Construct the rule.
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for ColocatedJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(try_elide_join_repartitions).data()
    }

    fn name(&self) -> &str {
        "ColocatedJoinRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn try_elide_join_repartitions(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
        return Ok(Transformed::no(plan));
    };

    let on = join.on();
    let (left_keys, right_keys): (Vec<_>, Vec<_>) =
        on.iter().cloned().unzip();

    let left_dist = source_distribution(join.left());
    let right_dist = source_distribution(join.right());
    let (Some(left_dist), Some(right_dist)) = (left_dist, right_dist) else {
        return Ok(Transformed::no(plan));
    };

    if !same_keying(&left_dist, &right_dist, &left_keys, &right_keys) {
        return Ok(Transformed::no(plan));
    }

    let target_buckets = match colocation_target(&left_dist, &right_dist) {
        Some(n) => n,
        None => return Ok(Transformed::no(plan)),
    };

    let new_left = rewrite_side(join.left(), &left_dist, target_buckets)?;
    let new_right = rewrite_side(join.right(), &right_dist, target_buckets)?;
    if Arc::ptr_eq(&new_left, join.left()) && Arc::ptr_eq(&new_right, join.right()) {
        return Ok(Transformed::no(plan));
    }

    let new_join = Arc::clone(&plan).with_new_children(vec![new_left, new_right])?;
    Ok(Transformed::yes(new_join))
}

/// Decide the bucket count to colocate at: either the matching count (full
/// colocation) or the smaller of the two when one divides the other
/// (sub-partition colocation). Returns `None` when no rewrite is possible.
fn colocation_target(left: &HashDistribution, right: &HashDistribution) -> Option<usize> {
    if left.num_buckets == right.num_buckets {
        return Some(left.num_buckets);
    }
    let (small, large) = if left.num_buckets < right.num_buckets {
        (left.num_buckets, right.num_buckets)
    } else {
        (right.num_buckets, left.num_buckets)
    };
    if small > 0 && large % small == 0 {
        Some(small)
    } else {
        None
    }
}

/// Walk down through a single [`RepartitionExec`] (if present) to find the
/// [`BallistaPartitionMetadata`] declared by the underlying source.
fn source_distribution(plan: &Arc<dyn ExecutionPlan>) -> Option<HashDistribution> {
    let candidate = plan
        .as_any()
        .downcast_ref::<RepartitionExec>()
        .map(|r| r.input())
        .unwrap_or(plan);
    metadata_of(candidate)
}

fn metadata_of(plan: &Arc<dyn ExecutionPlan>) -> Option<HashDistribution> {
    let any = plan.as_any();
    if let Some(meta) =
        any.downcast_ref::<ballista_core::partitioning::HashDistributedScanExec>()
    {
        return meta.hash_distribution();
    }
    if let Some(meta) = any.downcast_ref::<BucketSubPartitionExec>() {
        return meta.hash_distribution();
    }
    None
}

fn same_keying(
    left: &HashDistribution,
    right: &HashDistribution,
    left_keys: &[PhysicalExprRef],
    right_keys: &[PhysicalExprRef],
) -> bool {
    left.hash_fn == right.hash_fn
        && keys_match_columns(left_keys, &left.keys)
        && keys_match_columns(right_keys, &right.keys)
}

fn keys_match_columns(exprs: &[PhysicalExprRef], names: &[String]) -> bool {
    if exprs.len() != names.len() {
        return false;
    }
    exprs.iter().zip(names.iter()).all(|(expr, name)| {
        expr.as_any()
            .downcast_ref::<Column>()
            .is_some_and(|c| c.name() == name)
    })
}

/// Rebuild one side of the join so its output partitioning is `Hash(keys,
/// target_buckets)` without going through a network shuffle.
///
/// Walks past any single intervening `RepartitionExec` to find the source
/// plan that exposes a `BallistaPartitionMetadata`. If the source already has
/// `target_buckets` partitions, returns it directly (eliding the repartition).
/// Otherwise wraps it in a [`BucketSubPartitionExec`] that locally chains the
/// relevant input partitions.
fn rewrite_side(
    plan: &Arc<dyn ExecutionPlan>,
    source_dist: &HashDistribution,
    target_buckets: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let source = repartition_input(plan)
        .map(Arc::clone)
        .unwrap_or_else(|| Arc::clone(plan));

    if source_dist.num_buckets == target_buckets {
        return Ok(source);
    }

    let target_dist = HashDistribution::new(
        source_dist.keys.clone(),
        source_dist.hash_fn,
        target_buckets,
    );
    Ok(Arc::new(BucketSubPartitionExec::try_new(source, target_dist)?))
}

/// Returns the input of `plan` if `plan` is a `RepartitionExec`, else `None`.
fn repartition_input(plan: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    plan.as_any()
        .downcast_ref::<RepartitionExec>()
        .map(|r| r.input())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::partitioning::{
        HashDistributedScanExec, HashDistribution, HashFn,
    };
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::JoinType;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::joins::{HashJoinExecBuilder, PartitionMode};
    use datafusion::prelude::SessionContext;

    fn build_scan(
        keys: &[&str],
        hash_fn: HashFn,
        buckets: usize,
        col_names: &[&str],
    ) -> Arc<dyn ExecutionPlan> {
        let fields: Vec<_> = col_names
            .iter()
            .map(|n| Field::new(*n, DataType::Int32, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let partitions: Vec<Vec<RecordBatch>> = (0..buckets)
            .map(|b| {
                let arrays: Vec<_> = (0..col_names.len())
                    .map(|_| {
                        Arc::new(Int32Array::from(vec![b as i32, b as i32 + 1])) as _
                    })
                    .collect();
                vec![RecordBatch::try_new(schema.clone(), arrays).unwrap()]
            })
            .collect();
        let provider = Arc::new(MemTable::try_new(schema, partitions).unwrap());
        let dist = HashDistribution::new(
            keys.iter().map(|s| s.to_string()).collect(),
            hash_fn,
            buckets,
        );
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let ctx = SessionContext::new();
        let inner = rt
            .block_on(provider.scan(&ctx.state(), None, &[], None))
            .unwrap();
        Arc::new(HashDistributedScanExec::try_new(inner, dist).unwrap())
    }

    fn col(name: &str, idx: usize) -> PhysicalExprRef {
        Arc::new(Column::new(name, idx))
    }

    fn join_with_repartitions(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        buckets: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let (left_keys, right_keys): (Vec<_>, Vec<_>) = on.iter().cloned().unzip();
        let left_repart = Arc::new(
            RepartitionExec::try_new(left, Partitioning::Hash(left_keys, buckets))
                .unwrap(),
        );
        let right_repart = Arc::new(
            RepartitionExec::try_new(right, Partitioning::Hash(right_keys, buckets))
                .unwrap(),
        );
        Arc::new(
            HashJoinExecBuilder::new(left_repart, right_repart, on, JoinType::Inner)
                .with_partition_mode(PartitionMode::Partitioned)
                .build()
                .unwrap(),
        )
    }

    fn count_repartitions(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut count = 0;
        let _ = plan.clone().transform_up(|p| {
            if p.as_any().downcast_ref::<RepartitionExec>().is_some() {
                count += 1;
            }
            Ok(Transformed::no(p))
        });
        count
    }

    fn count_sub_partitions(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut count = 0;
        let _ = plan.clone().transform_up(|p| {
            if p.as_any()
                .downcast_ref::<ballista_core::partitioning::BucketSubPartitionExec>()
                .is_some()
            {
                count += 1;
            }
            Ok(Transformed::no(p))
        });
        count
    }

    #[test]
    fn elides_repartition_when_inputs_are_colocated() {
        let left = build_scan(&["id"], HashFn::Murmur3, 4, &["id", "lval"]);
        let right = build_scan(&["id"], HashFn::Murmur3, 4, &["id", "rval"]);
        let join = join_with_repartitions(
            left,
            right,
            vec![(col("id", 0), col("id", 0))],
            4,
        );
        assert_eq!(count_repartitions(&join), 2, "setup precondition");

        let optimized = ColocatedJoinRule::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            count_repartitions(&optimized),
            0,
            "expected both repartitions to be elided\n{}",
            displayable(optimized.as_ref()).indent(false),
        );
    }

    #[test]
    fn elides_when_left_and_right_use_different_column_names() {
        // Left scan bucketed by `id`, right scan bucketed by `other`. Each
        // side's declared bucketing key still matches its join key, so the
        // join is colocated despite the cross-side name difference.
        let left = build_scan(&["id"], HashFn::Murmur3, 4, &["id", "lval"]);
        let right = build_scan(&["other"], HashFn::Murmur3, 4, &["other", "rval"]);
        let join = join_with_repartitions(
            left,
            right,
            vec![(col("id", 0), col("other", 0))],
            4,
        );
        let optimized = ColocatedJoinRule::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(count_repartitions(&optimized), 0);
    }

    #[test]
    fn preserves_repartition_when_bucket_count_mismatch() {
        // 4 vs 5: not divisible. (For divisor case see
        // `sub_partitions_when_bucket_count_divides`.)
        let left = build_scan(&["id"], HashFn::Murmur3, 4, &["id", "lval"]);
        let right = build_scan(&["id"], HashFn::Murmur3, 5, &["id", "rval"]);
        let join = join_with_repartitions(
            left,
            right,
            vec![(col("id", 0), col("id", 0))],
            4,
        );
        let optimized = ColocatedJoinRule::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(count_repartitions(&optimized), 2);
    }

    #[test]
    fn sub_partitions_when_bucket_count_divides() {
        // Left bucketed 8 ways, right 4 ways → divisor relationship.
        // Larger side is locally coalesced; both repartitions are removed.
        let left = build_scan(&["id"], HashFn::Murmur3, 8, &["id", "lval"]);
        let right = build_scan(&["id"], HashFn::Murmur3, 4, &["id", "rval"]);
        let join = join_with_repartitions(
            left,
            right,
            vec![(col("id", 0), col("id", 0))],
            4,
        );
        let optimized = ColocatedJoinRule::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            count_repartitions(&optimized),
            0,
            "expected both repartitions removed via sub-partitioning\n{}",
            displayable(optimized.as_ref()).indent(false),
        );
        assert_eq!(
            count_sub_partitions(&optimized),
            1,
            "expected one BucketSubPartitionExec on the larger side\n{}",
            displayable(optimized.as_ref()).indent(false),
        );
    }

    #[test]
    fn preserves_repartition_when_bucket_count_indivisible() {
        // 6 vs 4: neither divides the other → not colocated.
        let left = build_scan(&["id"], HashFn::Murmur3, 6, &["id", "lval"]);
        let right = build_scan(&["id"], HashFn::Murmur3, 4, &["id", "rval"]);
        let join = join_with_repartitions(
            left,
            right,
            vec![(col("id", 0), col("id", 0))],
            4,
        );
        let optimized = ColocatedJoinRule::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(count_repartitions(&optimized), 2);
        assert_eq!(count_sub_partitions(&optimized), 0);
    }

    #[test]
    fn preserves_repartition_when_hash_fn_mismatch() {
        let left = build_scan(&["id"], HashFn::Murmur3, 4, &["id", "lval"]);
        let right = build_scan(&["id"], HashFn::XxHash64, 4, &["id", "rval"]);
        let join = join_with_repartitions(
            left,
            right,
            vec![(col("id", 0), col("id", 0))],
            4,
        );
        let optimized = ColocatedJoinRule::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(count_repartitions(&optimized), 2);
    }

    #[test]
    fn preserves_repartition_when_join_key_misaligned() {
        // Left scan declares bucketing on "lval", but join is on "id".
        let left = build_scan(&["lval"], HashFn::Murmur3, 4, &["id", "lval"]);
        let right = build_scan(&["id"], HashFn::Murmur3, 4, &["id", "rval"]);
        let join = join_with_repartitions(
            left,
            right,
            vec![(col("id", 0), col("id", 0))],
            4,
        );
        let optimized = ColocatedJoinRule::new()
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        assert_eq!(count_repartitions(&optimized), 2);
    }
}
