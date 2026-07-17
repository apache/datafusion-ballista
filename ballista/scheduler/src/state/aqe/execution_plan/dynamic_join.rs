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

use ballista_core::config::BallistaConfig;
use datafusion::{
    arrow::compute::SortOptions,
    common::{JoinType, NullEquality, Result, exec_err, internal_err},
    config::ConfigOptions,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr_common::physical_expr::fmt_sql,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
        joins::{
            HashJoinExec, HashJoinExecBuilder, JoinOn, PartitionMode, SortMergeJoinExec,
            utils::JoinFilter,
        },
    },
};
use log::debug;
use std::sync::Arc;

use crate::physical_optimizer::join_selection::collect_left_broadcast_safe;
use crate::state::aqe::execution_plan::ExchangeExec;
use crate::state::aqe::optimizer_rule::join_selection::SelectJoinRule;

/// has children of this join been
/// repartitioned
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinInputState {
    /// All inputs has been repartitioned
    /// which means this join can be resolved
    Repartitioned,
    /// State on join inputs is unknown
    Unknown,
}

#[derive(Debug)]
pub struct DynamicJoinSelectionExec {
    pub left: Arc<dyn ExecutionPlan>,
    pub right: Arc<dyn ExecutionPlan>,
    pub on: JoinOn,
    pub filter: Option<JoinFilter>,
    pub join_type: JoinType,
    pub projection: Option<Vec<usize>>,
    pub null_equality: NullEquality,
    pub properties: Arc<PlanProperties>,
    pub selection_state: JoinInputState,
    pub null_aware: bool,
    pub(crate) plan_id: usize,
}

impl ExecutionPlan for DynamicJoinSelectionExec {
    fn name(&self) -> &str {
        "DynamicJoinSelectionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!(
                "DynamicJoinSelectionExec expects 2 children, got {}",
                children.len()
            );
        }
        Ok(Arc::new(DynamicJoinSelectionExec {
            left: Arc::clone(&children[0]),
            right: Arc::clone(&children[1]),
            on: self.on.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            projection: self.projection.clone(),
            null_equality: self.null_equality,
            properties: Arc::clone(&self.properties),
            selection_state: self.selection_state.clone(),
            null_aware: self.null_aware,
            plan_id: self.plan_id,
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        exec_err!(
            "Operator should not be executed; it should have been replaced by an optimizer rule."
        )
    }
}

impl DisplayAs for DynamicJoinSelectionExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({c1}, {c2})"))
                    .collect::<Vec<String>>()
                    .join(", ");
                let display_null_equality =
                    if self.null_equality == NullEquality::NullEqualsNull {
                        ", NullsEqual: true"
                    } else {
                        ""
                    };
                write!(
                    f,
                    "{}: plan_id={}, join_type={:?}, on=[{}]{}{}",
                    Self::static_name(),
                    self.plan_id,
                    self.join_type,
                    on,
                    self.filter.as_ref().map_or_else(
                        || "".to_string(),
                        |f| format!(", filter={}", f.expression())
                    ),
                    display_null_equality,
                )?;

                write!(
                    f,
                    " repartitioned={}",
                    matches!(self.selection_state, JoinInputState::Repartitioned)
                )?;

                Ok(())
            }
            DisplayFormatType::TreeRender => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| {
                        format!("({} = {})", fmt_sql(c1.as_ref()), fmt_sql(c2.as_ref()))
                    })
                    .collect::<Vec<String>>()
                    .join(", ");

                writeln!(f, "plan_id={}", self.plan_id)?;
                if self.join_type != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)?;
                }
                writeln!(f, "on={on}")?;

                if self.null_equality == NullEquality::NullEqualsNull {
                    writeln!(f, "NullsEqual: true")?;
                }

                writeln!(
                    f,
                    " repartitioned={}",
                    matches!(self.selection_state, JoinInputState::Repartitioned)
                )?;

                Ok(())
            }
        }
    }
}

pub enum JoinSelectionAction {
    Repartition(Arc<DynamicJoinSelectionExec>),
    CollectLeft(Arc<HashJoinExec>),
    LateCollectLeft(Arc<HashJoinExec>),
    Hash(Arc<HashJoinExec>),
    Sort(Arc<SortMergeJoinExec>),
}

impl DynamicJoinSelectionExec {
    // this is required, as we do not want to implement
    // ExecutionPlan::required_input_distribution as other
    // datafusion planners are going to add repartition
    pub(crate) fn _required_input_distribution(&self) -> Vec<Distribution> {
        let (left_expr, right_expr) = self
            .on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();
        vec![
            Distribution::HashPartitioned(left_expr),
            Distribution::HashPartitioned(right_expr),
        ]
    }

    pub(crate) fn to_actual_join(
        &self,
        config: &ConfigOptions,
    ) -> Result<JoinSelectionAction> {
        let prefer_hash_join = config.optimizer.prefer_hash_join;
        // The byte threshold comes from Ballista's own broadcast config so that a
        // single key governs broadcast selection under both the static planner
        // (`maybe_promote_to_broadcast`) and AQE. A value of 0 disables broadcast
        // promotion entirely, matching the static planner. The row threshold is
        // only a fallback used when byte statistics are absent, so it stays on
        // DataFusion's setting.
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        let threshold_collect_left_join_bytes = bc.broadcast_join_threshold_bytes();
        let threshold_collect_left_join_rows =
            config.optimizer.hash_join_single_partition_threshold_rows;

        // The `!= 0` guard sits here rather than inside
        // `supports_collect_by_thresholds`: that helper falls back to the row
        // check when the byte threshold is 0/absent, so gating at this level is
        // what makes a 0 threshold disable both the byte and row broadcast paths.
        let under_threshold = threshold_collect_left_join_bytes != 0
            && (Self::supports_collect_by_thresholds(
                self.left.as_ref(),
                threshold_collect_left_join_bytes,
                threshold_collect_left_join_rows,
            ) || Self::supports_collect_by_thresholds(
                self.right.as_ref(),
                threshold_collect_left_join_bytes,
                threshold_collect_left_join_rows,
            ));

        // The resolver collects the smaller side onto the build (left) input,
        // swapping the join type when the right side is the smaller one (the
        // `swap_inputs` calls in `SelectJoinRule`). A `CollectLeft` join then
        // broadcasts that build side to every probe task, which is only correct
        // for join types that never emit rows on behalf of the build side. Use
        // the same swap decision as the resolver to determine the resulting join
        // type and skip the broadcast when it would be unsafe.
        let build_side_join_type = if SelectJoinRule::supports_swap_join_order(
            self.left.as_ref(),
            self.right.as_ref(),
        )? {
            self.join_type.swap()
        } else {
            self.join_type
        };

        let partition_mode =
            if under_threshold && collect_left_broadcast_safe(build_side_join_type) {
                PartitionMode::CollectLeft
            } else {
                PartitionMode::Partitioned
            };

        let stats_left = self.left.partition_statistics(None)?;
        let stats_right = self.right.partition_statistics(None)?;

        debug!(
            "to_actual_join - plan_id: {}, decision: {:?} left: ({:?} | {:?}), right: ({:?} | {:?})",
            self.plan_id,
            partition_mode,
            stats_left.num_rows,
            stats_left.total_byte_size,
            stats_right.num_rows,
            stats_right.total_byte_size
        );

        match (&self.selection_state, partition_mode) {
            (JoinInputState::Unknown, PartitionMode::CollectLeft) => self
                .to_hash_join(PartitionMode::CollectLeft)
                .map(JoinSelectionAction::CollectLeft),
            (JoinInputState::Repartitioned, PartitionMode::Partitioned)
                if prefer_hash_join =>
            {
                self.to_hash_join(PartitionMode::Partitioned)
                    .map(JoinSelectionAction::Hash)
            }
            (JoinInputState::Repartitioned, PartitionMode::Partitioned) => {
                self.to_sort_merge_join().map(JoinSelectionAction::Sort)
            }
            // TODO: not sure about this point
            // at this point, both inputs has been repartitioned
            // making it collect left may not make sense, perhaps only
            // valid strategy at this point is to check if both sides
            // are small enough to make it single partitioned join.
            // if single partition join is possibility should we leave it
            // to coalesce shuffle to make this decision? (if thats the
            // case we should remove LateCollectLeft )
            //
            // would it be better if we try to shuffle build side first
            // and then if build side is small enough make decision if
            // this is CollectLeft or Partitioned join. The issue is
            // we have flip coin chances to pick side which to run first
            (JoinInputState::Repartitioned, PartitionMode::CollectLeft) => self
                .to_hash_join(PartitionMode::CollectLeft)
                .map(JoinSelectionAction::LateCollectLeft),
            (JoinInputState::Unknown, PartitionMode::Partitioned) => Ok(
                JoinSelectionAction::Repartition(Arc::new(self.to_partitioned())),
            ),
            // this method calculates partition mode, and at the moment it
            // can't calculate it as PartitionMode::Auto
            (_, PartitionMode::Auto) => internal_err!("this case should not be possible"),
        }
    }

    pub(crate) fn to_hash_join(
        &self,
        partition_mode: PartitionMode,
    ) -> Result<Arc<HashJoinExec>> {
        let hash_join_exec = HashJoinExecBuilder::new(
            self.left.clone(),
            self.right.clone(),
            self.on.clone(),
            self.join_type,
        )
        .with_filter(self.filter.clone())
        .with_projection(self.projection.clone())
        .with_partition_mode(partition_mode)
        .with_null_equality(self.null_equality)
        .with_null_aware(self.null_aware)
        .build()?;

        Ok(Arc::new(hash_join_exec))
    }

    pub fn to_sort_merge_join(&self) -> Result<Arc<SortMergeJoinExec>> {
        //let null_equals_null = self.null_equality == NullEquality::NullEqualsNull;
        let sort_options = vec![SortOptions::default(); self.on.len()];
        Ok(Arc::new(SortMergeJoinExec::try_new(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            self.on.clone(),
            self.filter.clone(),
            self.join_type,
            sort_options,
            self.null_equality,
        )?))
    }
    // this method has been taken from datafusion
    // join selection optimizer rule
    fn supports_collect_by_thresholds(
        plan: &dyn ExecutionPlan,
        threshold_byte_size: usize,
        threshold_num_rows: usize,
    ) -> bool {
        // Currently we do not trust the 0 value from stats, due to stats collection might have bug
        // TODO check the logic in datasource::get_statistics_with_limit()
        let Ok(stats) = plan.partition_statistics(None) else {
            return false;
        };

        if let Some(byte_size) = stats.total_byte_size.get_value() {
            *byte_size != 0 && *byte_size < threshold_byte_size
        } else if let Some(num_rows) = stats.num_rows.get_value() {
            *num_rows != 0 && *num_rows < threshold_num_rows
        } else {
            false
        }
    }

    pub(crate) fn to_partitioned(&self) -> Self {
        Self {
            left: self.left.clone(),
            right: self.right.clone(),
            on: self.on.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            projection: self.projection.clone(),
            null_equality: self.null_equality,
            properties: self.properties.clone(),
            selection_state: JoinInputState::Repartitioned,
            null_aware: self.null_aware,
            plan_id: self.plan_id,
        }
    }

    pub(crate) fn from_hash_join(
        hash_join: &HashJoinExec,
        plan_id: usize,
    ) -> Result<Arc<DynamicJoinSelectionExec>> {
        Ok(Arc::new(DynamicJoinSelectionExec {
            left: Arc::clone(hash_join.left()),
            right: Arc::clone(hash_join.right()),
            on: hash_join.on().to_vec(),
            filter: hash_join.filter().cloned(),
            join_type: *hash_join.join_type(),
            projection: hash_join.projection.as_deref().map(|p| p.to_vec()),
            null_equality: hash_join.null_equality,
            properties: Arc::clone(hash_join.properties()),
            selection_state: JoinInputState::Unknown,
            null_aware: hash_join.null_aware,
            plan_id,
        }))
    }
    /// will return true if all upstream [ExchangeExec] has been resolved
    /// and all other [DynamicJoinSelectionExec].
    ///
    /// Method will short circuit on first resolved [ExchangeExec],
    /// as there must not be any unresolved [ExchangeExec], in any if
    /// its children.
    pub(crate) fn upstream_resolved(&self) -> bool {
        fn has_join_or_unresolved_exchange(plan: &Arc<dyn ExecutionPlan>) -> bool {
            if let Some(exchange) = plan.downcast_ref::<ExchangeExec>() {
                // this should be fine, once we find first resolved
                // exchange it should not have any unresolved shuffles later
                // nor dynamic joins
                return !exchange.shuffle_created();
            };

            plan.downcast_ref::<DynamicJoinSelectionExec>().is_some()
                || plan
                    .children()
                    .iter()
                    .any(|c| has_join_or_unresolved_exchange(c))
        }

        let left_has_join = has_join_or_unresolved_exchange(&self.left);
        let right_has_join = has_join_or_unresolved_exchange(&self.right);

        !(left_has_join || right_has_join)
    }

    pub(crate) fn from_sort_join(
        merge_join: &SortMergeJoinExec,
        plan_id: usize,
    ) -> Result<Arc<DynamicJoinSelectionExec>> {
        Ok(Arc::new(DynamicJoinSelectionExec {
            left: Arc::clone(merge_join.left()),
            right: Arc::clone(merge_join.right()),
            on: merge_join.on().to_vec(),
            filter: merge_join.filter().clone(),
            join_type: merge_join.join_type(),
            projection: None,
            null_equality: merge_join.null_equality,
            properties: Arc::clone(merge_join.properties()),
            selection_state: JoinInputState::Unknown,
            null_aware: false,
            plan_id,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::execution_plan::ExchangeExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::config::ExtensionOptions;
    use datafusion::common::{ColumnStatistics, Statistics, stats::Precision};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::test::exec::StatisticsExec;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
    }

    fn stats_exec(num_rows: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(num_rows),
                total_byte_size: Precision::Inexact(num_rows * 16),
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            Schema::new(vec![Field::new("k", DataType::Int32, false)]),
        ))
    }

    /// Run the resolver's join-strategy decision for `join_type` with `left_rows`
    /// and `right_rows` on the two sides, both small enough to be collectable.
    fn actual_join_for(
        join_type: JoinType,
        left_rows: usize,
        right_rows: usize,
        prefer_hash_join: bool,
    ) -> JoinSelectionAction {
        // 10 MiB is large enough for the tiny StatisticsExec sides below to be
        // collectable, matching the default broadcast threshold.
        actual_join_with_threshold(
            join_type,
            left_rows,
            right_rows,
            prefer_hash_join,
            10 * 1024 * 1024,
        )
    }

    /// Same as [`actual_join_for`] but with an explicit
    /// `broadcast_join_threshold_bytes`, so tests can prove the Ballista config
    /// drives the byte cutoff (0 disables broadcast promotion).
    fn actual_join_with_threshold(
        join_type: JoinType,
        left_rows: usize,
        right_rows: usize,
        prefer_hash_join: bool,
        broadcast_threshold_bytes: usize,
    ) -> JoinSelectionAction {
        let left = stats_exec(left_rows);
        let right = stats_exec(right_rows);
        let on: JoinOn =
            vec![(Arc::new(Column::new("k", 0)), Arc::new(Column::new("k", 0)))];
        let dj = DynamicJoinSelectionExec {
            properties: Arc::clone(left.properties()),
            left,
            right,
            on,
            filter: None,
            join_type,
            projection: None,
            null_equality: NullEquality::NullEqualsNothing,
            selection_state: JoinInputState::Unknown,
            null_aware: false,
            plan_id: 0,
        };
        let mut config = ConfigOptions::new();
        config.optimizer.prefer_hash_join = prefer_hash_join;
        config.optimizer.hash_join_single_partition_threshold_rows = 1_000_000;
        let mut bc = BallistaConfig::default();
        bc.set(
            "optimizer.broadcast_join_threshold_bytes",
            &broadcast_threshold_bytes.to_string(),
        )
        .unwrap();
        config.extensions.insert(bc);
        dj.to_actual_join(&config).unwrap()
    }

    fn is_collected(action: &JoinSelectionAction) -> bool {
        matches!(
            action,
            JoinSelectionAction::CollectLeft(_) | JoinSelectionAction::LateCollectLeft(_)
        )
    }

    // With equal-sized sides (no swap) the resolver must collect a small build
    // side into a CollectLeft broadcast only for join types whose output is
    // driven by the probe side; everything that emits build-side rows must be
    // repartitioned instead. `prefer_hash_join` only selects the repartitioned
    // fallback (hash vs sort-merge), so the broadcast-safety decision must be
    // identical under both settings.
    #[test]
    fn to_actual_join_collects_only_broadcast_safe_join_types() {
        for prefer_hash_join in [true, false] {
            for join_type in [
                JoinType::Inner,
                JoinType::Left,
                JoinType::Right,
                JoinType::Full,
                JoinType::LeftSemi,
                JoinType::RightSemi,
                JoinType::LeftAnti,
                JoinType::RightAnti,
                JoinType::LeftMark,
                JoinType::RightMark,
            ] {
                let action = actual_join_for(join_type, 100, 100, prefer_hash_join);
                assert_eq!(
                    is_collected(&action),
                    collect_left_broadcast_safe(join_type),
                    "join_type {join_type:?} (prefer_hash_join={prefer_hash_join}): collected={}, expected safe={}",
                    is_collected(&action),
                    collect_left_broadcast_safe(join_type),
                );
            }
        }
    }

    // Safety is evaluated on the join type *after* the build-side swap. A LEFT
    // join with the smaller side on the right swaps to a (safe) RIGHT join and is
    // collected; a RIGHT join with the smaller side on the right swaps to an
    // (unsafe) LEFT join and must be repartitioned.
    #[test]
    fn to_actual_join_uses_post_swap_join_type() {
        assert!(
            is_collected(&actual_join_for(JoinType::Left, 1000, 10, true)),
            "LEFT with small right swaps to a safe RIGHT join and should collect"
        );
        assert!(
            !is_collected(&actual_join_for(JoinType::Right, 1000, 10, true)),
            "RIGHT with small right swaps to an unsafe LEFT join and must repartition"
        );
    }

    // The byte cutoff comes from `ballista.optimizer.broadcast_join_threshold_bytes`,
    // not DataFusion's `hash_join_single_partition_threshold`. `stats_exec(n)`
    // reports `n * 16` bytes, so the smaller side here is 1600 bytes. A threshold
    // above it collects; a threshold below it repartitions.
    #[test]
    fn broadcast_threshold_bytes_drives_collect_decision() {
        // smaller (left) side = 100 * 16 = 1600 bytes, larger = 16000 bytes
        assert!(
            is_collected(&actual_join_with_threshold(
                JoinType::Inner,
                100,
                1000,
                true,
                2000,
            )),
            "smaller side (1600 B) is under a 2000 B threshold and must collect"
        );
        assert!(
            !is_collected(&actual_join_with_threshold(
                JoinType::Inner,
                100,
                1000,
                true,
                1000,
            )),
            "neither side is under a 1000 B threshold, so the join must repartition"
        );
    }

    // A threshold of 0 disables broadcast promotion entirely, matching the static
    // planner, even for a side small enough to collect under any positive cutoff.
    #[test]
    fn zero_broadcast_threshold_disables_collect() {
        assert!(
            !is_collected(&actual_join_with_threshold(JoinType::Inner, 1, 1, true, 0,)),
            "broadcast_join_threshold_bytes=0 must disable CollectLeft promotion"
        );
    }

    /// Constructs a minimal `DynamicJoinSelectionExec` around the given children.
    /// Properties are borrowed from the left child — correct enough for unit tests
    /// that only exercise `children_resolved`.
    fn make_dynamic_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> DynamicJoinSelectionExec {
        DynamicJoinSelectionExec {
            properties: Arc::clone(left.properties()),
            left,
            right,
            on: vec![],
            filter: None,
            join_type: JoinType::Inner,
            projection: None,
            null_equality: NullEquality::NullEqualsNothing,
            selection_state: JoinInputState::Unknown,
            null_aware: false,
            plan_id: 0,
        }
    }

    #[test]
    fn children_resolved_returns_true_for_simple_leaves() {
        let schema = test_schema();
        let left = Arc::new(EmptyExec::new(schema.clone())) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;

        let dj = make_dynamic_join(left, right);

        assert!(
            dj.upstream_resolved(),
            "plain EmptyExec children carry no exchange or nested join — must be resolved"
        );
    }

    #[test]
    fn children_resolved_returns_false_for_unresolved_left_exchange() {
        let schema = test_schema();
        let leaf = Arc::new(EmptyExec::new(schema.clone())) as Arc<dyn ExecutionPlan>;
        // ExchangeExec::new starts with shuffle_partitions = None → not resolved
        let unresolved_exchange =
            Arc::new(ExchangeExec::new(leaf.clone(), None, 0)) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;

        let dj = make_dynamic_join(unresolved_exchange, right);

        assert!(
            !dj.upstream_resolved(),
            "an unresolved ExchangeExec on the left must block resolution"
        );
    }

    #[test]
    fn children_resolved_returns_false_for_unresolved_right_exchange() {
        let schema = test_schema();
        let leaf = Arc::new(EmptyExec::new(schema.clone())) as Arc<dyn ExecutionPlan>;
        let left = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;
        let unresolved_exchange =
            Arc::new(ExchangeExec::new(leaf, None, 0)) as Arc<dyn ExecutionPlan>;

        let dj = make_dynamic_join(left, unresolved_exchange);

        assert!(
            !dj.upstream_resolved(),
            "an unresolved ExchangeExec on the right must block resolution"
        );
    }

    #[test]
    fn children_resolved_returns_true_when_both_exchanges_resolved() {
        let schema = test_schema();
        let leaf = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;

        let left_exchange = ExchangeExec::new(leaf.clone(), None, 0);
        left_exchange.resolve_shuffle_partitions(vec![]);

        let right_exchange = ExchangeExec::new(leaf, None, 1);
        right_exchange.resolve_shuffle_partitions(vec![]);

        let dj = make_dynamic_join(
            Arc::new(left_exchange) as Arc<dyn ExecutionPlan>,
            Arc::new(right_exchange) as Arc<dyn ExecutionPlan>,
        );

        assert!(
            dj.upstream_resolved(),
            "exchanges with resolved shuffle partitions must not block resolution"
        );
    }

    #[test]
    fn children_resolved_returns_false_when_left_child_is_dynamic_join() {
        let schema = test_schema();
        let leaf = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;

        // Inner join whose children are plain leaves — still blocks the outer one
        let inner = Arc::new(make_dynamic_join(leaf.clone(), leaf.clone()))
            as Arc<dyn ExecutionPlan>;

        let outer = make_dynamic_join(inner, leaf);

        assert!(
            !outer.upstream_resolved(),
            "a DynamicJoinSelectionExec nested in the left child must block resolution"
        );
    }

    #[test]
    fn children_resolved_returns_false_when_right_child_is_dynamic_join() {
        let schema = test_schema();
        let leaf = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;

        let inner = Arc::new(make_dynamic_join(leaf.clone(), leaf.clone()))
            as Arc<dyn ExecutionPlan>;

        let outer = make_dynamic_join(leaf, inner);

        assert!(
            !outer.upstream_resolved(),
            "a DynamicJoinSelectionExec nested in the right child must block resolution"
        );
    }
}
