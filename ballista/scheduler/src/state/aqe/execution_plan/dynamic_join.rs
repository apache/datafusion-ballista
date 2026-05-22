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

//! Functional tests for [`CoalescePartitionsRule`]: drive a query through
//! `AdaptivePlanner`, finalize the upstream stage with synthetic per-partition
//! byte stats, and snapshot the displayed plan tree so the rule's effect on
//! the leaf `ExchangeExec` is visible at the `coalesce=K of M` field.
//!
//! Each test uses small synthetic byte sizes paired with a small
//! `coalesce_target_partition_bytes` so the bin-pack outcome is hand-traceable
//! against `split_size_list_by_target_size`.

use datafusion::{
    arrow::compute::SortOptions,
    common::{JoinType, NullEquality, Result, internal_err},
    physical_expr_common::physical_expr::fmt_sql,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
        joins::{
            HashJoinExec, HashJoinExecBuilder, JoinOn, PartitionMode, SortMergeJoinExec,
            utils::JoinFilter,
        },
    },
};
use std::sync::Arc;

use crate::state::aqe::execution_plan::ExchangeExec;

/// has children of this join been
/// repartitioned
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildrenState {
    Repartitioned,
    Unknown,
}

// SortMergeJoinExec::try_new(left, right, on, filter, join_type, sort_options,               null_equality )
// HashJoinExec::try_new     (left, right, on, filter, join_type, projection, partition_mode, null_equality, null_aware )
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
    pub selection_state: ChildrenState,
}

impl ExecutionPlan for DynamicJoinSelectionExec {
    fn name(&self) -> &str {
        "DynamicJoinSelectionExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &std::sync::Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
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
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        todo!(
            "this should not be executed, it should have been replaced with optimizer rule"
        )
    }
}

impl DisplayAs for DynamicJoinSelectionExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
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
                    "{}: join_type={:?}, on=[{}]{}{}",
                    Self::static_name(),
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
                    matches!(self.selection_state, ChildrenState::Repartitioned)
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
                    matches!(self.selection_state, ChildrenState::Repartitioned)
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
        config: &datafusion::config::ConfigOptions,
    ) -> Result<JoinSelectionAction> {
        let prefer_hash_join = config.optimizer.prefer_hash_join;
        let threshold_collect_left_join_bytes =
            config.optimizer.hash_join_single_partition_threshold;
        let threshold_collect_left_join_rows =
            config.optimizer.hash_join_single_partition_threshold_rows;

        let partition_mode = if Self::supports_collect_by_thresholds(
            self.left.as_ref(),
            threshold_collect_left_join_bytes,
            threshold_collect_left_join_rows,
        ) || Self::supports_collect_by_thresholds(
            self.right.as_ref(),
            threshold_collect_left_join_rows,
            threshold_collect_left_join_rows,
        ) {
            PartitionMode::CollectLeft
        } else {
            PartitionMode::Partitioned
        };
        match (&self.selection_state, partition_mode) {
            (ChildrenState::Unknown, PartitionMode::CollectLeft) => self
                .to_hash_join(PartitionMode::CollectLeft)
                .map(JoinSelectionAction::CollectLeft),
            //
            //
            (ChildrenState::Repartitioned, PartitionMode::Partitioned)
                if prefer_hash_join =>
            {
                self.to_hash_join(PartitionMode::Partitioned)
                    .map(JoinSelectionAction::Hash)
            }
            //
            //
            (ChildrenState::Repartitioned, PartitionMode::Partitioned) => {
                self.to_sort_merge_join().map(JoinSelectionAction::Sort)
            }
            //
            //
            (ChildrenState::Repartitioned, PartitionMode::CollectLeft) => self
                .to_hash_join(PartitionMode::CollectLeft)
                .map(JoinSelectionAction::LateCollectLeft),
            //(ChildrenState::Repartitioned, PartitionMode::CollectLeft) =>
            // self
            //     .to_hash_join(PartitionMode::Partitioned)
            //     .map(JoinSelectionAction::Hash),
            (ChildrenState::Unknown, PartitionMode::Partitioned) => Ok(
                JoinSelectionAction::Repartition(Arc::new(self.to_partitioned())),
            ),
            //
            //
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
        .with_null_aware(false)
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
            selection_state: ChildrenState::Repartitioned,
        }
    }

    pub(crate) fn from_hash_join(
        hash_join: &HashJoinExec,
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
            selection_state: ChildrenState::Unknown,
        }))
    }

    pub(crate) fn children_are_ready(&self) -> bool {
        fn has_dynamic_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
            plan.as_any()
                .downcast_ref::<DynamicJoinSelectionExec>()
                .is_some()
                || plan.children().iter().any(|c| has_dynamic_join(c))
        }

        fn has_unresolved_exchange(plan: &Arc<dyn ExecutionPlan>) -> bool {
            if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>()
                && !exchange.shuffle_created()
            {
                true
            } else {
                plan.children().iter().any(|c| has_unresolved_exchange(c))
            }
        }
        let left_has_join = has_dynamic_join(&self.left);
        let right_has_join = has_dynamic_join(&self.right);
        let left_has_exchange = has_unresolved_exchange(&self.left);
        let right_has_exchange = has_unresolved_exchange(&self.right);

        !(left_has_join || left_has_exchange || right_has_join || right_has_exchange)
    }

    pub(crate) fn from_sort_join(
        merge_join: &SortMergeJoinExec,
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
            selection_state: ChildrenState::Unknown,
        }))
    }
}
