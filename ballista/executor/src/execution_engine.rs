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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::utils;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::Debug;
use std::sync::Arc;

/// Execution engine extension point

pub trait ExecutionEngine: Sync + Send {
    fn create_query_stage_exec(
        &self,
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<dyn QueryStageExecutor>>;
}

/// QueryStageExecutor executes a section of a query plan that has consistent partitioning and
/// can be executed as one unit with each partition being executed in parallel. The output of each
/// partition is re-partitioned and streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
#[async_trait]
pub trait QueryStageExecutor: Sync + Send + Debug {
    async fn execute_query_stage(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>>;

    fn collect_plan_metrics(&self) -> Vec<MetricsSet>;

    fn schema(&self) -> SchemaRef;
}

pub struct DefaultExecutionEngine {}

impl ExecutionEngine for DefaultExecutionEngine {
    fn create_query_stage_exec(
        &self,
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<dyn QueryStageExecutor>> {
        // the query plan created by the scheduler always starts with a ShuffleWriterExec
        let exec = if let Some(shuffle_writer) =
            plan.as_any().downcast_ref::<ShuffleWriterExec>()
        {
            // recreate the shuffle writer with the correct working directory
            ShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                work_dir.to_string(),
                shuffle_writer.shuffle_output_partitioning().cloned(),
            )
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to new_query_stage_exec is not a ShuffleWriterExec"
                    .to_string(),
            ))
        }?;
        Ok(Arc::new(DefaultQueryStageExec::new(exec)))
    }
}

#[derive(Debug)]
pub struct DefaultQueryStageExec {
    shuffle_writer: ShuffleWriterExec,
}

impl DefaultQueryStageExec {
    pub fn new(shuffle_writer: ShuffleWriterExec) -> Self {
        Self { shuffle_writer }
    }
}

#[async_trait]
impl QueryStageExecutor for DefaultQueryStageExec {
    async fn execute_query_stage(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        self.shuffle_writer
            .execute_shuffle_write(input_partition, context)
            .await
    }

    fn schema(&self) -> SchemaRef {
        self.shuffle_writer.schema()
    }

    fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
        utils::collect_plan_metrics(&self.shuffle_writer)
    }
}
