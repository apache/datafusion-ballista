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

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

/// UnresolvedShuffleExec represents a dependency on the results of a ShuffleWriterExec node which hasn't computed yet.
///
/// An ExecutionPlan that contains an UnresolvedShuffleExec isn't ready for execution. The presence of this ExecutionPlan
/// is used as a signal so the scheduler knows it can't start computation until the dependent shuffle has completed.
#[derive(Debug, Clone)]
pub struct UnresolvedShuffleExec {
    /// The query stage ID which needs to be computed.
    pub stage_id: usize,

    /// The schema this node will have once it is replaced with a ShuffleReaderExec.
    pub schema: SchemaRef,

    /// The partition count this node will have once it is replaced with a ShuffleReaderExec.
    pub output_partition_count: usize,

    /// The number of shuffle output partitions on the upstream stage. For
    /// non-broadcast readers this equals `output_partition_count`. For
    /// broadcast readers this is M (one logical output partition fans in
    /// all M upstream partition files).
    pub upstream_partition_count: usize,

    /// When true, the resolved `ShuffleReaderExec` reads *all* upstream
    /// partition files into its single output partition (broadcast pattern).
    pub broadcast: bool,

    properties: Arc<PlanProperties>,
}

impl UnresolvedShuffleExec {
    /// Create a new UnresolvedShuffleExec for a standard one-to-one
    /// per-partition read.
    pub fn new(stage_id: usize, schema: SchemaRef, partitioning: Partitioning) -> Self {
        let partition_count = partitioning.partition_count();
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            stage_id,
            schema,
            output_partition_count: partition_count,
            upstream_partition_count: partition_count,
            broadcast: false,
            properties,
        }
    }

    /// Create a broadcast UnresolvedShuffleExec. The resolved reader has
    /// one logical output partition that fans in all `upstream_partition_count`
    /// shuffle files from the upstream stage.
    pub fn new_broadcast(
        stage_id: usize,
        schema: SchemaRef,
        upstream_partition_count: usize,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            stage_id,
            schema,
            output_partition_count: 1,
            upstream_partition_count,
            broadcast: true,
            properties,
        }
    }
}

impl DisplayAs for UnresolvedShuffleExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                if self.broadcast {
                    write!(
                        f,
                        "UnresolvedShuffleExec: broadcast=true, upstream_partitions: {}",
                        self.upstream_partition_count,
                    )
                } else {
                    write!(
                        f,
                        "UnresolvedShuffleExec: partitioning: {}",
                        self.properties().output_partitioning()
                    )
                }
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "partitioning={}",
                    self.properties().output_partitioning()
                )
            }
        }
    }
}

impl ExecutionPlan for UnresolvedShuffleExec {
    fn name(&self) -> &str {
        "UnresolvedShuffleExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "Ballista UnresolvedShuffleExec does not support children plans"
                    .to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Plan(
            "Ballista UnresolvedShuffleExec does not support execution".to_owned(),
        ))
    }
}
