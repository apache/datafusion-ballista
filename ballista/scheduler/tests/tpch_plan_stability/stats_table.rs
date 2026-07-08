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

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType,
};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

use async_trait::async_trait;

/// A leaf execution plan that produces no rows but reports a fixed row-count
/// statistic, so the physical optimizer makes realistic join/broadcast choices
/// without any data on disk.
#[derive(Debug)]
pub struct StatsExec {
    schema: SchemaRef,
    num_rows: usize,
    cache: Arc<PlanProperties>,
}

impl StatsExec {
    pub fn new(schema: SchemaRef, num_rows: usize) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        Self {
            schema,
            num_rows,
            cache: Arc::new(cache),
        }
    }
}

impl DisplayAs for StatsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "StatsExec: rows={}", self.num_rows)
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for StatsExec {
    fn name(&self) -> &'static str {
        "StatsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            vec![],
            Arc::clone(&self.schema),
            None,
        )?))
    }
    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(
            Statistics::new_unknown(&self.schema)
                .with_num_rows(Precision::Inexact(self.num_rows)),
        ))
    }
}

/// A dataless `TableProvider` with a real schema and a fixed row-count statistic.
#[derive(Debug)]
pub struct TpchStatsTable {
    schema: SchemaRef,
    num_rows: usize,
}

impl TpchStatsTable {
    pub fn new(schema: SchemaRef, num_rows: usize) -> Self {
        Self { schema, num_rows }
    }
}

#[async_trait]
impl TableProvider for TpchStatsTable {
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
        let schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => Arc::clone(&self.schema),
        };
        Ok(Arc::new(StatsExec::new(schema, self.num_rows)))
    }
    fn statistics(&self) -> Option<Statistics> {
        Some(
            Statistics::new_unknown(&self.schema)
                .with_num_rows(Precision::Inexact(self.num_rows)),
        )
    }
}
