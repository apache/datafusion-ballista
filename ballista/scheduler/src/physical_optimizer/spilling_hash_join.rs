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

//! [`SpillingHashJoinRule`] substitutes eligible `HashJoinExec` nodes with
//! [`SpillingHashJoinExec`], a hash join whose build side spills sub-partitions
//! to disk under memory pressure instead of requiring the whole build side to
//! fit in memory at once.
//!
//! # Eligibility (v1, strict)
//!
//! A `HashJoinExec` is substituted only when all of the following hold:
//!   - `join_type() == JoinType::Inner`
//!   - `*partition_mode() == PartitionMode::Partitioned`
//!   - `projection` is `None` (no output projection folded into the join)
//!   - `filter()` is `None` (no residual `JoinFilter`)
//!
//! `SpillingHashJoinExec` itself only supports this exact shape (see its
//! constructor), so the predicate here is not just a performance heuristic —
//! it is the full set of plans the replacement operator can represent. Any
//! join that does not match is left untouched.
//!
//! # Gating
//!
//! The rule is a no-op unless `ballista.execution.spilling_hash_join.enabled`
//! is set on the `BallistaConfig` extension carried by the `ConfigOptions`
//! passed to `optimize`. This mirrors how `CoalescePartitionsRule` (see
//! `crate::state::aqe::optimizer_rule::coalesce_partitions`) reads Ballista
//! options: `config.extensions.get::<BallistaConfig>()`, defaulting to
//! `BallistaConfig::default()` (which has the flag off) when the extension is
//! absent.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::SpillingHashJoinExec;
use datafusion::common::JoinType;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

/// Physical optimizer rule that substitutes eligible `HashJoinExec` nodes with
/// `SpillingHashJoinExec`.
///
/// See module docs for the eligibility predicate and the config-gating
/// approach.
#[derive(Debug, Default)]
pub struct SpillingHashJoinRule {}

impl PhysicalOptimizerRule for SpillingHashJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ballista_config = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();

        if !ballista_config.spilling_hash_join_enabled() {
            return Ok(plan);
        }

        plan.transform_up(|node| {
            let substituted =
                maybe_substitute_spilling_hash_join(Arc::clone(&node), config)?;
            if Arc::ptr_eq(&substituted, &node) {
                Ok(Transformed::no(node))
            } else {
                Ok(Transformed::yes(substituted))
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "SpillingHashJoinRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Substitutes a single `HashJoinExec` node with a `SpillingHashJoinExec` when
/// it is eligible and the feature flag is on. Operates on the given node only
/// (non-recursive), returning `plan` unchanged when the flag is off, the node
/// is not a `HashJoinExec`, or the join does not match the eligibility
/// predicate.
///
/// This is the single source of truth for spilling-join eligibility and
/// substitution. It is shared by [`SpillingHashJoinRule`] (which applies it
/// bottom-up over a whole tree) and the distributed planner (which applies it
/// per node, immediately after broadcast promotion).
pub(crate) fn maybe_substitute_spilling_hash_join(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ballista_config = config
        .extensions
        .get::<BallistaConfig>()
        .cloned()
        .unwrap_or_default();

    if !ballista_config.spilling_hash_join_enabled() {
        return Ok(plan);
    }

    let Some(hj) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };

    if !is_eligible(hj) {
        return Ok(plan);
    }

    let spilling = SpillingHashJoinExec::try_new(
        Arc::clone(hj.left()),
        Arc::clone(hj.right()),
        hj.on().to_vec(),
        PartitionMode::Partitioned,
        ballista_config.spilling_hash_join_partitions(),
    )?;

    Ok(Arc::new(spilling) as Arc<dyn ExecutionPlan>)
}

/// Whether `hj` matches the strict v1 shape `SpillingHashJoinExec` can
/// represent: inner, partitioned, no projection, no residual filter.
fn is_eligible(hj: &HashJoinExec) -> bool {
    *hj.join_type() == JoinType::Inner
        && *hj.partition_mode() == PartitionMode::Partitioned
        && hj.projection.is_none()
        && hj.filter().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::config::{ConfigOptions, ExtensionOptions};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};

    /// Two 4-partition `DataSourceExec` inputs with schema `(k Int32, v
    /// Int32)`, plus the equijoin pair `on = [(k_left, k_right)]`.
    #[allow(clippy::type_complexity)]
    fn two_inputs() -> (
        Arc<dyn ExecutionPlan>,
        Arc<dyn ExecutionPlan>,
        Vec<(PhysicalExprRef, PhysicalExprRef)>,
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let partitions: Vec<Vec<_>> = (0..4).map(|_| vec![]).collect();

        let left_source =
            MemorySourceConfig::try_new(&partitions, Arc::clone(&schema), None)
                .expect("left MemorySourceConfig");
        let right_source =
            MemorySourceConfig::try_new(&partitions, Arc::clone(&schema), None)
                .expect("right MemorySourceConfig");

        let left: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(left_source)));
        let right: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(right_source)));

        let on: Vec<(PhysicalExprRef, PhysicalExprRef)> =
            vec![(Arc::new(Column::new("k", 0)), Arc::new(Column::new("k", 0)))];

        (left, right, on)
    }

    fn hash_join(
        join_type: JoinType,
        partition_mode: PartitionMode,
        filter: Option<JoinFilter>,
    ) -> Arc<dyn ExecutionPlan> {
        let (left, right, on) = two_inputs();
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                filter,
                &join_type,
                None,
                partition_mode,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    /// A trivial `JoinFilter` over `k_left > v_right`, just enough to make
    /// `filter()` return `Some(..)`.
    fn some_filter() -> JoinFilter {
        let intermediate_schema = Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]);
        let expr = Arc::new(Column::new_with_schema("k", &intermediate_schema).unwrap())
            as PhysicalExprRef;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: datafusion::common::JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: datafusion::common::JoinSide::Right,
            },
        ];
        JoinFilter::new(expr, column_indices, Arc::new(intermediate_schema))
    }

    fn config_with_spilling(enabled: bool) -> ConfigOptions {
        let mut bc = BallistaConfig::default();
        bc.set(
            "execution.spilling_hash_join.enabled",
            if enabled { "true" } else { "false" },
        )
        .unwrap();
        let mut config = ConfigOptions::default();
        config.extensions.insert(bc);
        config
    }

    fn plan_string(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{}", displayable(plan.as_ref()).indent(false))
    }

    /// Whether the rendered plan contains a bare `HashJoinExec` node (as
    /// opposed to a `SpillingHashJoinExec` node, whose name has
    /// `HashJoinExec` as a trailing substring and would otherwise produce a
    /// false-positive `contains("HashJoinExec")` match).
    fn has_bare_hash_join(s: &str) -> bool {
        s.lines()
            .any(|line| line.trim_start().starts_with("HashJoinExec"))
    }

    #[test]
    fn substitutes_eligible_inner_partitioned() {
        let plan = hash_join(JoinType::Inner, PartitionMode::Partitioned, None);
        let cfg = config_with_spilling(true);

        let out = SpillingHashJoinRule::default()
            .optimize(plan, &cfg)
            .unwrap();
        let s = plan_string(&out);

        assert!(s.contains("SpillingHashJoinExec"), "{s}");
        assert!(!has_bare_hash_join(&s), "{s}");
    }

    #[test]
    fn leaves_left_outer_untouched() {
        let plan = hash_join(JoinType::Left, PartitionMode::Partitioned, None);
        let cfg = config_with_spilling(true);

        let out = SpillingHashJoinRule::default()
            .optimize(plan, &cfg)
            .unwrap();
        let s = plan_string(&out);

        assert!(has_bare_hash_join(&s), "{s}");
        assert!(!s.contains("SpillingHashJoinExec"), "{s}");
    }

    #[test]
    fn no_substitution_when_flag_off() {
        let plan = hash_join(JoinType::Inner, PartitionMode::Partitioned, None);
        let cfg = config_with_spilling(false);

        let out = SpillingHashJoinRule::default()
            .optimize(plan, &cfg)
            .unwrap();
        let s = plan_string(&out);

        assert!(has_bare_hash_join(&s), "{s}");
        assert!(!s.contains("SpillingHashJoinExec"), "{s}");
    }

    #[test]
    fn leaves_filter_present_untouched() {
        let plan = hash_join(
            JoinType::Inner,
            PartitionMode::Partitioned,
            Some(some_filter()),
        );
        let cfg = config_with_spilling(true);

        let out = SpillingHashJoinRule::default()
            .optimize(plan, &cfg)
            .unwrap();
        let s = plan_string(&out);

        assert!(has_bare_hash_join(&s), "{s}");
        assert!(!s.contains("SpillingHashJoinExec"), "{s}");
    }

    #[test]
    fn leaves_collect_left_untouched() {
        let plan = hash_join(JoinType::Inner, PartitionMode::CollectLeft, None);
        let cfg = config_with_spilling(true);

        let out = SpillingHashJoinRule::default()
            .optimize(plan, &cfg)
            .unwrap();
        let s = plan_string(&out);

        assert!(has_bare_hash_join(&s), "{s}");
        assert!(!s.contains("SpillingHashJoinExec"), "{s}");
    }

    #[test]
    fn config_extension_absent_defaults_to_off() {
        // No `BallistaConfig` extension inserted at all: the rule must fall
        // back to `BallistaConfig::default()`, whose flag is off, rather than
        // panicking or substituting unconditionally.
        let plan = hash_join(JoinType::Inner, PartitionMode::Partitioned, None);
        let cfg = ConfigOptions::default();

        let out = SpillingHashJoinRule::default()
            .optimize(plan, &cfg)
            .unwrap();
        let s = plan_string(&out);

        assert!(has_bare_hash_join(&s), "{s}");
        assert!(!s.contains("SpillingHashJoinExec"), "{s}");
    }
}
