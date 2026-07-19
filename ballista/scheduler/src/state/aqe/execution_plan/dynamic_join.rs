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
    arrow::datatypes::{DataType, Schema},
    common::{ColumnStatistics, JoinType, NullEquality, Result, exec_err, internal_err},
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
use log::info;
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
        // Both broadcast thresholds come from Ballista's own config so that a
        // single set of keys governs broadcast selection under both the static
        // planner (`maybe_promote_to_broadcast`) and AQE. A byte threshold of 0
        // disables broadcast promotion entirely, matching the static planner.
        // The row threshold is only consulted as a fallback when byte-size
        // statistics are absent.
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        let threshold_collect_left_join_bytes = bc.broadcast_join_threshold_bytes();
        let threshold_collect_left_join_rows = bc.broadcast_join_threshold_rows();
        let max_build_bytes = bc.hash_join_max_build_partition_bytes();
        let build_max_partition_bytes = max_per_partition_build_bytes(&self.left);

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

        let action = match (&self.selection_state, partition_mode) {
            (JoinInputState::Unknown, PartitionMode::CollectLeft) => self
                .to_hash_join(PartitionMode::CollectLeft)
                .map(JoinSelectionAction::CollectLeft),
            (JoinInputState::Repartitioned, PartitionMode::Partitioned)
                if prefer_hash_join
                    && hash_build_fits(max_build_bytes, build_max_partition_bytes) =>
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
        }?;

        let action_label = match &action {
            JoinSelectionAction::Repartition(_) => "Repartition",
            JoinSelectionAction::CollectLeft(_) => "CollectLeft(broadcast)",
            JoinSelectionAction::LateCollectLeft(_) => "LateCollectLeft(broadcast)",
            JoinSelectionAction::Hash(_) => "Hash(Partitioned)",
            JoinSelectionAction::Sort(_) => "SortMerge(Partitioned)",
        };

        info!(
            "AQE join decision plan_id={} action={} partition_mode={:?} under_threshold={} \
             build_max_partition_bytes={:?} hash_join_max_build_partition_bytes={} \
             left=(rows={:?}, bytes={:?}) right=(rows={:?}, bytes={:?}) \
             byte_threshold={} row_threshold={}",
            self.plan_id,
            action_label,
            partition_mode,
            under_threshold,
            build_max_partition_bytes,
            max_build_bytes,
            stats_left.num_rows,
            stats_left.total_byte_size,
            stats_right.num_rows,
            stats_right.total_byte_size,
            threshold_collect_left_join_bytes,
            threshold_collect_left_join_rows,
        );

        Ok(action)
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
            return *byte_size != 0 && *byte_size < threshold_byte_size;
        }

        // `total_byte_size` is unknown, which is the common case rather than the
        // exception: DataFusion discards it on every join, and rebuilding it in
        // `Statistics::calculate_total_byte_size` only works when every column has
        // a fixed width, so one `Utf8` column loses it for good.
        //
        // A row count on its own says nothing about how much data a broadcast
        // would replicate to every probe task, so estimate the size and hold it
        // to the same byte threshold. The row threshold is kept as an additional
        // ceiling, so this can only ever reject a broadcast the row rule would
        // have allowed, never introduce a new one.
        let Some(num_rows) = stats.num_rows.get_value().copied() else {
            return false;
        };

        if num_rows == 0 || num_rows >= threshold_num_rows {
            return false;
        }

        estimate_output_byte_size(&plan.schema(), num_rows, &stats.column_statistics)
            .is_some_and(|estimated| estimated < threshold_byte_size)
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

/// Whether the build (left) side of a `Partitioned` hash join fits the
/// per-slot memory budget. A `max_build_bytes` of 0 disables the check (the
/// join always uses hash join); an unknown build size (`build_max_partition_bytes
/// == None`) is treated as fitting too, since there is no evidence to force a
/// fallback.
fn hash_build_fits(
    max_build_bytes: usize,
    build_max_partition_bytes: Option<usize>,
) -> bool {
    if max_build_bytes == 0 {
        return true;
    }
    match build_max_partition_bytes {
        Some(bytes) => bytes <= max_build_bytes,
        None => true, // unknown size → don't force a fallback
    }
}

/// Skew-aware fit check for the build side of a `Partitioned` hash join: the
/// MAX (not average) per-partition materialized byte size. A single
/// oversized partition is enough to blow the per-slot memory pool even when
/// every other partition — and thus the average — is small; this is exactly
/// the shape of the Q18 OOM, so an average would have hidden the failure
/// mode this check exists to catch.
///
/// Reachability: at the one call site this feeds — the
/// `(JoinInputState::Repartitioned, PartitionMode::Partitioned)` arm of
/// `to_actual_join` — `self.left` is always the `ExchangeExec` that
/// `SelectJoinRule` inserted while resolving the prior `Repartition` action
/// (see `JoinSelectionAction::Repartition` in `join_selection.rs`), and
/// `upstream_resolved()` guarantees that exchange's shuffle has already
/// finished. So the actual, materialized per-partition byte sizes that
/// `CoalescePartitionsRule` reads (`ExchangeExec::shuffle_partitions()` ->
/// `PartitionLocation::partition_stats::num_bytes()`) are reachable here too,
/// and are used directly rather than falling back to an average.
///
/// Returns `None` (callers treat this as "fits", not as a forced fallback)
/// when `build` isn't an `ExchangeExec`, its shuffle hasn't resolved yet, or
/// the resolved shuffle has no partitions at all. A resolved shuffle whose
/// partitions are missing byte-size stats is not `None`: each such partition
/// contributes 0 bytes, so the result is `Some(0)` (or higher, if other
/// partitions do report sizes) — still small enough to be treated as fitting.
fn max_per_partition_build_bytes(build: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let exchange = build.downcast_ref::<ExchangeExec>()?;
    let partitions = exchange.shuffle_partitions()?;
    partitions
        .iter()
        .map(|locations| {
            locations
                .iter()
                .filter_map(|location| location.partition_stats.num_bytes())
                .sum::<u64>()
        })
        .max()
        .map(|bytes| bytes as usize)
}

/// Assumed width of a variable-width value when statistics carry no size for it.
/// Mirrors Spark's `StringType.defaultSize`, which serves the same purpose in
/// `EstimationUtils.getSizePerRow`.
const DEFAULT_VARIABLE_WIDTH_BYTES: usize = 20;

/// Assumed width of a binary value with no size in statistics. Mirrors Spark's
/// `BinaryType.defaultSize`.
const DEFAULT_BINARY_WIDTH_BYTES: usize = 100;

/// Estimates the size in bytes of `num_rows` rows of `schema`.
///
/// Used only when `Statistics::total_byte_size` is `Absent`. Each column
/// contributes the best figure available: its own `byte_size` statistic (which
/// is a total for the column's output, already scaled for filters and limits),
/// otherwise its fixed width times the row count, otherwise a default width.
///
/// Returns `None` if the estimate overflows, so the caller declines to broadcast
/// rather than wrapping around to a small number.
fn estimate_output_byte_size(
    schema: &Schema,
    num_rows: usize,
    column_statistics: &[ColumnStatistics],
) -> Option<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .try_fold(0usize, |total, (idx, field)| {
            let column_bytes = match column_statistics
                .get(idx)
                .and_then(|c| c.byte_size.get_value().copied())
            {
                Some(byte_size) => byte_size,
                None => num_rows.checked_mul(estimated_value_width(field.data_type()))?,
            };
            total.checked_add(column_bytes)
        })
}

/// Estimated width of a single value of `data_type`.
///
/// `DataType::primitive_width` covers the fixed-width types. It returns `None`
/// for `Boolean` and for the variable-width types, which are the cases handled
/// here.
fn estimated_value_width(data_type: &DataType) -> usize {
    if let Some(width) = data_type.primitive_width() {
        return width;
    }

    match data_type {
        DataType::Boolean => 1,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => DEFAULT_BINARY_WIDTH_BYTES,
        _ => DEFAULT_VARIABLE_WIDTH_BYTES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::execution_plan::ExchangeExec;
    use ballista_core::serde::scheduler::{
        ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
        PartitionId, PartitionLocation, PartitionStats,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{ColumnStatistics, Statistics, stats::Precision};
    use datafusion::config::ExtensionOptions;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::test::exec::StatisticsExec;

    const BYTE_THRESHOLD: usize = 10 * 1024 * 1024;
    const ROW_THRESHOLD: usize = 1_000_000;
    const MB: usize = 1024 * 1024;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
    }

    /// A plan reporting `num_rows` with no `total_byte_size` and no per-column
    /// size, which is what a join output looks like.
    fn sizeless_stats_exec(
        num_rows: usize,
        fields: Vec<Field>,
    ) -> Arc<dyn ExecutionPlan> {
        let column_statistics = vec![ColumnStatistics::new_unknown(); fields.len()];
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(num_rows),
                total_byte_size: Precision::Absent,
                column_statistics,
            },
            Schema::new(fields),
        ))
    }

    fn supports_collect(plan: &Arc<dyn ExecutionPlan>) -> bool {
        DynamicJoinSelectionExec::supports_collect_by_thresholds(
            plan.as_ref(),
            BYTE_THRESHOLD,
            ROW_THRESHOLD,
        )
    }

    // A build side under the row threshold but far over the byte threshold must
    // not be broadcast. 900k rows of a string column is roughly 18 MB by the
    // default width, which every probe task would otherwise have to hold.
    #[test]
    fn does_not_collect_wide_rows_when_byte_size_is_unknown() {
        let plan = sizeless_stats_exec(
            900_000,
            vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("address", DataType::Utf8, false),
            ],
        );

        assert!(!supports_collect(&plan));
    }

    // The estimate must not reject a build side that really is small: the same
    // row count over narrow fixed-width columns stays well inside the threshold.
    #[test]
    fn collects_narrow_rows_when_byte_size_is_unknown() {
        let plan = sizeless_stats_exec(
            100_000,
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ],
        );

        assert!(supports_collect(&plan));
    }

    // A per-column `byte_size` is a measured total for that column, so it should
    // be preferred over the default width. Here it proves the column is small
    // even though the type is variable-width.
    #[test]
    fn prefers_column_byte_size_over_default_width() {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(900_000),
                total_byte_size: Precision::Absent,
                column_statistics: vec![ColumnStatistics {
                    byte_size: Precision::Exact(1024),
                    ..ColumnStatistics::new_unknown()
                }],
            },
            Schema::new(vec![Field::new("name", DataType::Utf8, false)]),
        ));

        assert!(supports_collect(&plan));
    }

    // A known `total_byte_size` is authoritative and must still short-circuit the
    // estimate, in both directions.
    #[test]
    fn known_total_byte_size_still_decides() {
        let over: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(10),
                total_byte_size: Precision::Exact(BYTE_THRESHOLD + 1),
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            Schema::new(vec![Field::new("x", DataType::Int32, false)]),
        ));
        assert!(!supports_collect(&over));

        let under: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(10),
                total_byte_size: Precision::Exact(64),
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            Schema::new(vec![Field::new("x", DataType::Int32, false)]),
        ));
        assert!(supports_collect(&under));
    }

    // The row threshold is retained as a ceiling, so a row count at or above it
    // is rejected without regard to how narrow the rows are.
    #[test]
    fn row_threshold_remains_a_ceiling() {
        let plan = sizeless_stats_exec(
            ROW_THRESHOLD,
            vec![Field::new("a", DataType::Int8, false)],
        );

        assert!(!supports_collect(&plan));
    }

    // Statistics with neither a size nor a row count carry no evidence that the
    // side is small.
    #[test]
    fn does_not_collect_without_any_statistics() {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Absent,
                total_byte_size: Precision::Absent,
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            Schema::new(vec![Field::new("x", DataType::Int32, false)]),
        ));

        assert!(!supports_collect(&plan));
    }

    // Zero rows is treated as absent rather than as a very small side, matching
    // the existing distrust of a zero in statistics.
    #[test]
    fn does_not_collect_on_zero_rows() {
        let plan = sizeless_stats_exec(0, vec![Field::new("x", DataType::Int32, false)]);

        assert!(!supports_collect(&plan));
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

    /// A source that reports a row count but no byte-size statistic, so the
    /// resolver must fall back to the row-count broadcast threshold.
    fn stats_exec_rows_only(num_rows: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Inexact(num_rows),
                total_byte_size: Precision::Absent,
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            Schema::new(vec![Field::new("k", DataType::Int32, false)]),
        ))
    }

    /// Core driver: runs the join-strategy decision for the given children with
    /// explicit Ballista broadcast thresholds.
    fn run_to_actual_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
        prefer_hash_join: bool,
        broadcast_threshold_bytes: usize,
        broadcast_threshold_rows: usize,
    ) -> JoinSelectionAction {
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
        let mut bc = BallistaConfig::default();
        bc.set(
            "optimizer.broadcast_join_threshold_bytes",
            &broadcast_threshold_bytes.to_string(),
        )
        .unwrap();
        bc.set(
            "optimizer.broadcast_join_threshold_rows",
            &broadcast_threshold_rows.to_string(),
        )
        .unwrap();
        config.extensions.insert(bc);
        dj.to_actual_join(&config).unwrap()
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
        run_to_actual_join(
            stats_exec(left_rows),
            stats_exec(right_rows),
            join_type,
            prefer_hash_join,
            broadcast_threshold_bytes,
            // Large row threshold: byte stats are present here, so the row path
            // is never consulted; keep it out of the way of the byte decision.
            1_000_000,
        )
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

    // When byte-size statistics are absent, the decision falls back to the
    // Ballista row-count threshold (`broadcast_join_threshold_rows`), not
    // DataFusion's `hash_join_single_partition_threshold_rows`.
    #[test]
    fn broadcast_threshold_rows_drives_fallback_decision() {
        // Byte-size is Absent on both sides, so only the row threshold applies.
        // Smaller side = 100 rows. A generous byte threshold keeps the byte
        // guard open so the row fallback is what decides.
        assert!(
            is_collected(&run_to_actual_join(
                stats_exec_rows_only(100),
                stats_exec_rows_only(1000),
                JoinType::Inner,
                true,
                10 * 1024 * 1024,
                200,
            )),
            "smaller side (100 rows) is under a 200-row threshold and must collect"
        );
        assert!(
            !is_collected(&run_to_actual_join(
                stats_exec_rows_only(100),
                stats_exec_rows_only(1000),
                JoinType::Inner,
                true,
                10 * 1024 * 1024,
                50,
            )),
            "neither side is under a 50-row threshold, so the join must repartition"
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

    /// Builds a resolved `ExchangeExec` with one shuffle partition per entry in
    /// `per_partition_bytes`, each reporting that many bytes via a single
    /// `PartitionLocation`. Mirrors `test/coalesce_rule.rs`'s
    /// `partitions_with_byte_sizes` helper (same synthetic-stats pattern),
    /// duplicated here since that helper lives in a sibling private test
    /// module this file can't reach.
    fn test_exchange_with_partition_bytes(
        per_partition_bytes: &[usize],
    ) -> Arc<dyn ExecutionPlan> {
        let schema = test_schema();
        let input = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;
        let n = per_partition_bytes.len();
        let exchange =
            ExchangeExec::new(input, Some(Partitioning::UnknownPartitioning(n)), 0);

        let partitions = per_partition_bytes
            .iter()
            .enumerate()
            .map(|(idx, &bytes)| {
                vec![PartitionLocation {
                    map_partition_id: 0,
                    partition_id: PartitionId {
                        job_id: "".into(),
                        stage_id: 0,
                        partition_id: idx,
                    },
                    executor_meta: ExecutorMetadata {
                        id: "".to_string(),
                        host: "".to_string(),
                        port: 0,
                        grpc_port: 0,
                        specification: ExecutorSpecification::default().with_vcores(0),
                        os_info: ExecutorOperatingSystemSpecification::default(),
                    },
                    partition_stats: PartitionStats::new(
                        Some(1),
                        None,
                        Some(bytes as u64),
                    ),
                    file_id: None,
                    is_sort_shuffle: false,
                }]
            })
            .collect();
        exchange.resolve_shuffle_partitions(partitions);

        Arc::new(exchange) as Arc<dyn ExecutionPlan>
    }

    // Q18 OOMed on one fat partition while the average partition was small, so
    // the fit check must key off the MAX per-partition build size, not the
    // average — an average would have hidden exactly the skew that caused the
    // OOM.
    #[test]
    fn max_per_partition_build_bytes_takes_the_max() {
        let build = test_exchange_with_partition_bytes(&[100 * MB, 400 * MB]);
        assert_eq!(max_per_partition_build_bytes(&build), Some(400 * MB));
    }

    /// Builds a `ConfigOptions` with `prefer_hash_join = true` and a Ballista
    /// `hash_join_max_build_partition_bytes` set to `max_build_bytes`, via the
    /// string-keyed `BallistaConfig::set` path (the same path
    /// `run_to_actual_join` above uses for other Ballista settings) since
    /// there is no fluent setter for this key yet.
    fn config_with_max_build(max_build_bytes: usize) -> ConfigOptions {
        let mut config = ConfigOptions::new();
        config.optimizer.prefer_hash_join = true;
        let mut bc = BallistaConfig::default();
        bc.set(
            "optimizer.hash_join_max_build_partition_bytes",
            &max_build_bytes.to_string(),
        )
        .unwrap();
        config.extensions.insert(bc);
        config
    }

    /// Builds a `DynamicJoinSelectionExec` already in `Repartitioned` state
    /// whose build side (`left`) is a resolved `ExchangeExec` reporting the
    /// given per-partition byte sizes, so `to_actual_join`'s
    /// `(Repartitioned, Partitioned)` arm is reached and
    /// `max_per_partition_build_bytes` sees known sizes.
    fn repartitioned_join_with_build_partition_bytes(
        per_partition_bytes: &[usize],
    ) -> DynamicJoinSelectionExec {
        // test_exchange_with_partition_bytes wraps test_schema(), whose sole
        // field is named "x" (not "k" like stats_exec's schema), so the join
        // key below must match that name.
        let left = test_exchange_with_partition_bytes(per_partition_bytes);
        let right = stats_exec(1_000_000); // large enough to never be broadcast
        let on: JoinOn =
            vec![(Arc::new(Column::new("x", 0)), Arc::new(Column::new("k", 0)))];
        DynamicJoinSelectionExec {
            properties: Arc::clone(left.properties()),
            left,
            right,
            on,
            filter: None,
            join_type: JoinType::Inner,
            projection: None,
            null_equality: NullEquality::NullEqualsNothing,
            selection_state: JoinInputState::Repartitioned,
            null_aware: false,
            plan_id: 0,
        }
    }

    /// Renders the `ExecutionPlan` a `JoinSelectionAction` resolves to, so
    /// tests can assert on `displayable(..).indent(false).to_string()` rather
    /// than matching on the action's structure.
    fn resolved_plan(action: &JoinSelectionAction) -> Arc<dyn ExecutionPlan> {
        match action {
            JoinSelectionAction::Repartition(exec) => {
                Arc::clone(exec) as Arc<dyn ExecutionPlan>
            }
            JoinSelectionAction::CollectLeft(exec) => {
                Arc::clone(exec) as Arc<dyn ExecutionPlan>
            }
            JoinSelectionAction::LateCollectLeft(exec) => {
                Arc::clone(exec) as Arc<dyn ExecutionPlan>
            }
            JoinSelectionAction::Hash(exec) => Arc::clone(exec) as Arc<dyn ExecutionPlan>,
            JoinSelectionAction::Sort(exec) => Arc::clone(exec) as Arc<dyn ExecutionPlan>,
        }
    }

    // A build partition over the configured max falls back to SortMergeJoin,
    // even though `prefer_hash_join` is true — the fit check overrides the
    // preference rather than the other way around.
    #[test]
    fn build_over_threshold_falls_back_to_sort_merge_join() {
        let exec = repartitioned_join_with_build_partition_bytes(&[500 * MB]); // > 200 MB
        let action = exec
            .to_actual_join(&config_with_max_build(200 * MB))
            .unwrap();
        let rendered = displayable(resolved_plan(&action).as_ref())
            .indent(false)
            .to_string();
        assert!(rendered.contains("SortMergeJoinExec"), "{rendered}");
    }

    // A build partition within the configured max keeps the Partitioned hash
    // join `prefer_hash_join` selected.
    #[test]
    fn build_within_threshold_uses_partitioned_hash_join() {
        let exec = repartitioned_join_with_build_partition_bytes(&[50 * MB]); // < 200 MB
        let action = exec
            .to_actual_join(&config_with_max_build(200 * MB))
            .unwrap();
        let rendered = displayable(resolved_plan(&action).as_ref())
            .indent(false)
            .to_string();
        assert!(rendered.contains("HashJoinExec"), "{rendered}");
        assert!(rendered.contains("mode=Partitioned"), "{rendered}");
    }

    // `hash_join_max_build_partition_bytes = 0` disables the check entirely,
    // matching the design contract: 0 always fits regardless of build size.
    #[test]
    fn zero_threshold_disables_the_check() {
        let exec = repartitioned_join_with_build_partition_bytes(&[500 * MB]);
        let action = exec.to_actual_join(&config_with_max_build(0)).unwrap();
        let rendered = displayable(resolved_plan(&action).as_ref())
            .indent(false)
            .to_string();
        assert!(rendered.contains("HashJoinExec"), "{rendered}");
    }
}
