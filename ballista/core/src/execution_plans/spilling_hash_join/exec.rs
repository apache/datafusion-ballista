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

// SpillingHashJoinExec is a physical execution plan node for a hash join
// whose build side spills to disk instead of requiring the entire hash
// table to fit in memory. This is currently a scaffold only: the struct,
// its constructor, and its accessors exist so later work can wire up
// planning/serde/substitution, but `execute` is not yet implemented.
//
// v1 invariants (enforced by the constructor, which always builds an
// inner join with no residual filter and no output projection):
//   - join_type is always JoinType::Inner
//   - filter is always None
//   - projection is always None

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{JoinType, NullEquality, Result, internal_err, not_impl_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

/// Physical execution plan node for a hash join whose build side can spill
/// sub-partitions to disk rather than requiring the whole build side to fit
/// in memory at once.
///
/// This is currently a scaffold: `execute` is not yet implemented (see
/// `SpillingHashJoinExec::execute` below).
#[derive(Debug)]
pub struct SpillingHashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    partition_mode: PartitionMode,
    num_sub_partitions: usize,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl SpillingHashJoinExec {
    /// Creates a new `SpillingHashJoinExec` joining `left` and `right` on the
    /// equijoin pairs in `on`.
    ///
    /// This is a v1 scaffold: the join is always `JoinType::Inner`, with no
    /// residual filter and no output projection. A throwaway `HashJoinExec`
    /// is constructed purely to borrow its output `schema()` and
    /// `properties()` (partitioning/equivalence info) — it is never
    /// executed.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        partition_mode: PartitionMode,
        num_sub_partitions: usize,
    ) -> Result<Self> {
        let hj = HashJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            None,
            &JoinType::Inner,
            None,
            partition_mode,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let schema = hj.schema();
        let properties = hj.properties().clone();

        Ok(Self {
            left,
            right,
            on,
            partition_mode,
            num_sub_partitions,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Returns the equijoin column pairs `(left_expr, right_expr)`.
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    /// Returns the configured partition mode.
    pub fn partition_mode(&self) -> PartitionMode {
        self.partition_mode
    }

    /// Returns the number of sub-partitions the build side is split into
    /// for spilling.
    pub fn num_sub_partitions(&self) -> usize {
        self.num_sub_partitions
    }

    /// Returns the left (build) child.
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// Returns the right (probe) child.
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
}

impl ExecutionPlan for SpillingHashJoinExec {
    fn name(&self) -> &str {
        "SpillingHashJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!(
                "SpillingHashJoinExec expected two children, got {}",
                children.len()
            );
        }
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        Ok(Arc::new(Self::try_new(
            left,
            right,
            self.on.clone(),
            self.partition_mode,
            self.num_sub_partitions,
        )?))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        not_impl_err!("SpillingHashJoinExec::execute")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for SpillingHashJoinExec {
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
                    .map(|(l, r)| format!("({l}, {r})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "SpillingHashJoinExec: on=[{on}], mode={:?}",
                    self.partition_mode
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "mode={:?}", self.partition_mode)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::expressions::Column;

    /// Returns `(left, right, on)`: two single-partition-schema `DataSourceExec`
    /// plans with schema `(k Int32, v Int32)`, each split into 4 partitions,
    /// joined on `on = [(k_left, k_right)]`.
    #[allow(clippy::type_complexity)]
    fn two_col_inputs() -> (
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

    #[test]
    fn scaffold_schema_and_name() {
        let (left, right, on) = two_col_inputs();
        let exec = SpillingHashJoinExec::try_new(
            left,
            right,
            on,
            PartitionMode::Partitioned,
            16,
        )
        .unwrap();
        assert_eq!(exec.name(), "SpillingHashJoinExec");
        // Output schema is left ++ right columns.
        assert_eq!(exec.schema().fields().len(), 4);
        let rendered =
            format!("{}", displayable(&exec as &dyn ExecutionPlan).indent(false));
        assert!(rendered.contains("SpillingHashJoinExec"), "{rendered}");
    }
}
