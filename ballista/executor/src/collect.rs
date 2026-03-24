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

//! The CollectExec operator retrieves results from the cluster and returns them as a single
//! vector of [`RecordBatch`](datafusion::arrow::record_batch::RecordBatch).

use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, pin::Pin};

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::{error::Result, physical_plan::RecordBatchStream};
use futures::Stream;
use futures::stream::SelectAll;

/// Execution plan that collects results from multiple partitions into a single stream.
///
/// The CollectExec operator merges all partitions of its input plan into a single
/// output partition, combining results from distributed query execution into one
/// unified result set.
#[derive(Debug, Clone)]
pub struct CollectExec {
    /// The underlying execution plan whose partitions will be merged.
    plan: Arc<dyn ExecutionPlan>,
    /// Properties describing this plan's partitioning and ordering.
    properties: Arc<PlanProperties>,
}

impl CollectExec {
    /// Creates a new CollectExec that merges all partitions of the given plan.
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(plan.schema()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self { plan, properties }
    }
}

impl DisplayAs for CollectExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CollectExec")
            }
            DisplayFormatType::TreeRender => write!(f, "CollectExec"),
        }
    }
}

impl ExecutionPlan for CollectExec {
    fn name(&self) -> &str {
        "CollectExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);
        let num_partitions = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();

        let streams = (0..num_partitions)
            .map(|i| self.plan.execute(i, context.clone()))
            .collect::<Result<Vec<_>>>()
            .map_err(|e| DataFusionError::Execution(format!("BallistaError: {e:?}")))?;

        Ok(Box::pin(MergedRecordBatchStream {
            schema: self.schema(),
            select_all: Box::pin(futures::stream::select_all(streams)),
        }))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.plan.partition_statistics(partition)
    }
}

struct MergedRecordBatchStream {
    schema: SchemaRef,
    select_all: Pin<Box<SelectAll<SendableRecordBatchStream>>>,
}

impl Stream for MergedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.select_all.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for MergedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
