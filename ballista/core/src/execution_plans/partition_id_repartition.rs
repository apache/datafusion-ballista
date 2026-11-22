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

//! Exchange marker for shuffles whose partition ids the plan computes.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, plan_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

use crate::execution_plans::sort_shuffle::ShuffleRouting;

/// A shuffle boundary for clients that place rows with a hash of their own, such
/// as Spark's `pmod(murmur3(keys), n)`: the counterpart of `RepartitionExec(Hash)`.
///
/// The plan below computes each row's output partition into the integer column
/// `partition_id_column`. Output partition `i` holds exactly the rows whose id is
/// `i`, so the result lines up with data already partitioned that way, such as a
/// Spark-bucketed table read without a shuffle. The id column is not part of the
/// output, and a null, negative or out-of-range id fails the task.
///
/// The node only marks the boundary and can't be executed. The static planner
/// lowers it to a [`SortShuffleWriterExec`] with [`ShuffleRouting::PartitionId`];
/// the adaptive planner rejects it, since coalescing or re-planning the
/// partitions would break the placement.
///
/// The output is [`Partitioning::UnknownPartitioning`] whatever function produced
/// the ids, so operators that require hash-partitioned input won't see their
/// requirement met: put the node under operators that declare no distribution
/// requirement, such as a client's own fragment node.
///
/// [`SortShuffleWriterExec`]: super::SortShuffleWriterExec
#[derive(Debug)]
pub struct PartitionIdRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    partition_id_column: usize,
    properties: Arc<PlanProperties>,
}

impl PartitionIdRepartitionExec {
    /// Wrap `input`, routing each row to one of `partition_count` partitions by
    /// its integer `partition_id_column`, an index into `input`'s schema.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_id_column: usize,
        partition_count: usize,
    ) -> Result<Self> {
        if partition_count == 0 {
            return plan_err!(
                "PartitionIdRepartitionExec requires at least one output partition"
            );
        }
        let schema = ShuffleRouting::PartitionId {
            column: partition_id_column,
            partition_count,
        }
        .output_schema(&input.schema())?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            partition_id_column,
            properties,
        })
    }

    /// Index of the partition-id column in the input's schema.
    pub fn partition_id_column(&self) -> usize {
        self.partition_id_column
    }

    /// Number of output partitions. Ids must lie in `0..partition_count`.
    pub fn partition_count(&self) -> usize {
        self.properties.output_partitioning().partition_count()
    }
}

impl DisplayAs for PartitionIdRepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let schema = self.input.schema();
        let column = Column::new(
            schema.field(self.partition_id_column).name(),
            self.partition_id_column,
        );
        let partitions = self.partition_count();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "PartitionIdRepartitionExec: partition_id_column={column}, partitions={partitions}"
            ),
            DisplayFormatType::TreeRender => {
                write!(f, "partition_id_column={column}\npartitions={partitions}")
            }
        }
    }
}

impl ExecutionPlan for PartitionIdRepartitionExec {
    fn name(&self) -> &str {
        "PartitionIdRepartitionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// The routing is a column index, so there are no expressions to report.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!(
                "PartitionIdRepartitionExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            self.partition_id_column,
            self.partition_count(),
        )?))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Plan(
            "PartitionIdRepartitionExec only marks a shuffle boundary and cannot be \
             executed: the distributed planner replaces it with a shuffle writer"
                .to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
    use datafusion::prelude::SessionContext;

    /// Two empty input partitions of `(v: Utf8, pid: Int32, w: Int64)`.
    fn input() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("pid", DataType::Int32, false),
            Field::new("w", DataType::Int64, true),
        ]));
        MemorySourceConfig::try_new_exec(&[vec![], vec![]], schema, None).unwrap()
    }

    fn marker() -> PartitionIdRepartitionExec {
        PartitionIdRepartitionExec::try_new(input(), 1, 8).unwrap()
    }

    #[test]
    fn schema_is_the_input_without_the_id_column() {
        let schema = marker().schema();

        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["v", "w"]);
    }

    #[test]
    fn output_is_unknown_partitioning_of_the_requested_count() {
        let marker = marker();

        // Not hash: nothing may assume DataFusion's placement. The input has two
        // partitions, so the output's eight come from the node.
        assert_eq!(
            marker.children()[0].output_partitioning().partition_count(),
            2
        );
        assert!(matches!(
            marker.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(8)
        ));
    }

    #[test]
    fn rejects_zero_partitions() {
        let err = PartitionIdRepartitionExec::try_new(input(), 1, 0)
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("requires at least one output partition"),
            "actual: {err}"
        );
    }

    /// The id column's own checks are tested in `partition_id.rs`; this shows
    /// they reach the marker.
    #[test]
    fn rejects_an_id_column_that_is_not_an_integer() {
        let err = PartitionIdRepartitionExec::try_new(input(), 0, 4)
            .unwrap_err()
            .to_string();

        assert!(err.contains("must be an integer"), "actual: {err}");
    }

    #[test]
    fn with_new_children_keeps_the_routing() {
        let rebuilt = Arc::new(marker())
            .replace_children(
                vec![input()],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();

        let rebuilt = rebuilt
            .downcast_ref::<PartitionIdRepartitionExec>()
            .expect("with_new_children must return a PartitionIdRepartitionExec");
        assert_eq!(rebuilt.partition_id_column(), 1);
        assert_eq!(rebuilt.partition_count(), 8);
        assert_eq!(rebuilt.schema(), marker().schema());
    }

    #[test]
    fn with_new_children_rejects_the_wrong_number_of_children() {
        let err = Arc::new(marker())
            .replace_children(
                vec![input(), input()],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap_err()
            .to_string();

        assert!(err.contains("exactly one child"), "actual: {err}");
    }

    #[test]
    fn execution_is_refused() {
        let err = marker()
            .execute(0, SessionContext::new().task_ctx())
            .err()
            .unwrap()
            .to_string();

        assert!(err.contains("cannot be executed"), "actual: {err}");
    }

    #[test]
    fn display_names_the_id_column_and_partition_count() {
        let marker = marker();

        let default = datafusion::physical_plan::displayable(&marker)
            .indent(false)
            .to_string();
        assert_eq!(
            default.lines().next().unwrap(),
            "PartitionIdRepartitionExec: partition_id_column=pid@1, partitions=8"
        );

        // The tree renderer lays `key=value` lines out as `key:` over `value`.
        let tree = datafusion::physical_plan::displayable(&marker)
            .tree_render()
            .to_string();
        assert!(tree.contains("partition_id_column:"), "actual: {tree}");
        assert!(tree.contains("pid@1"), "actual: {tree}");
        assert!(tree.contains("partitions: 8"), "actual: {tree}");
    }
}
