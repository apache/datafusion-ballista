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

//! A table whose statistics are declared rather than measured.
//!
//! Planner decisions such as broadcast-vs-partitioned are a function of
//! statistics, not of data, so testing them by materialising rows means a test
//! can only describe table sizes it is willing to build. That rules out the
//! cases those decisions actually turn on: a build side of millions of rows, or
//! one whose `total_byte_size` is unknown.
//!
//! Unknown sizes are the norm rather than an edge case. DataFusion discards
//! `total_byte_size` on every join, and rebuilds it in
//! `Statistics::calculate_total_byte_size` only when every column has a fixed
//! width, so one `Utf8` column loses it permanently.
//!
//! [`StatsTable`] reports whatever statistics a test asks for and holds no data.
//! It supports planning only; executing its scan fails.

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::physical_plan::{Partitioning, project_schema};

use std::fmt::Formatter;
use std::sync::Arc;

/// A table that declares `statistics` over `schema` and holds no rows.
#[derive(Debug)]
pub(crate) struct StatsTable {
    schema: SchemaRef,
    statistics: Statistics,
    partitions: usize,
}

impl StatsTable {
    /// `statistics` must carry one entry per field in `schema`, matching the
    /// contract [`Statistics`] is read under everywhere else.
    pub(crate) fn new(
        schema: SchemaRef,
        statistics: Statistics,
        partitions: usize,
    ) -> Self {
        assert_eq!(
            statistics.column_statistics.len(),
            schema.fields().len(),
            "column statistics must have one entry per field"
        );
        Self {
            schema,
            statistics,
            partitions,
        }
    }
}

#[async_trait]
impl TableProvider for StatsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StatsExec::try_new(
            Arc::clone(&self.schema),
            self.statistics.clone(),
            self.partitions,
            projection,
        )?))
    }
}

/// The scan [`StatsTable`] produces: reports statistics, yields no rows.
#[derive(Debug)]
pub(crate) struct StatsExec {
    statistics: Statistics,
    properties: Arc<PlanProperties>,
}

impl StatsExec {
    fn try_new(
        schema: SchemaRef,
        statistics: Statistics,
        partitions: usize,
        projection: Option<&Vec<usize>>,
    ) -> Result<Self> {
        let schema = project_schema(&schema, projection)?;

        // Project the column statistics alongside the schema, but leave
        // `total_byte_size` exactly as the test declared it. Recomputing it here
        // would silently discard the case these fixtures exist to express: a
        // side whose size is unknown.
        let statistics = match projection {
            Some(indices) => Statistics {
                column_statistics: indices
                    .iter()
                    .map(|i| statistics.column_statistics[*i].clone())
                    .collect(),
                ..statistics
            },
            None => statistics,
        };

        Ok(Self {
            statistics,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }
}

impl DisplayAs for StatsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "StatsExec: partitions={}, rows={:?}, bytes={:?}",
                    self.properties.partitioning.partition_count(),
                    self.statistics.num_rows,
                    self.statistics.total_byte_size,
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "rows={:?}", self.statistics.num_rows)?;
                writeln!(f, "bytes={:?}", self.statistics.total_byte_size)
            }
        }
    }
}

impl ExecutionPlan for StatsExec {
    fn name(&self) -> &str {
        "StatsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!("StatsExec holds no data and cannot be executed")
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        // The declared statistics describe the table as a whole. A caller asking
        // per-partition gets the same figures rather than a share of them: these
        // fixtures exist to pin what the planner sees, and dividing by the
        // partition count would put a second, unasked-for rule in the way.
        let _ = partition;
        Ok(Arc::new(self.statistics.clone()))
    }
}

/// Statistics for a table of `num_rows` whose size is unknown, as a join output
/// or anything carrying a variable-width column would report it.
pub(crate) fn sizeless_statistics(schema: &Schema, num_rows: usize) -> Statistics {
    use datafusion::common::{ColumnStatistics, stats::Precision};

    Statistics {
        num_rows: Precision::Inexact(num_rows),
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
    }
}

/// Statistics for a table of `num_rows` occupying a known `total_byte_size`.
pub(crate) fn sized_statistics(
    schema: &Schema,
    num_rows: usize,
    total_byte_size: usize,
) -> Statistics {
    use datafusion::common::{ColumnStatistics, stats::Precision};

    Statistics {
        num_rows: Precision::Exact(num_rows),
        total_byte_size: Precision::Exact(total_byte_size),
        column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
    }
}
