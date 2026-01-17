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

//! Execution engine abstraction for query stage execution.
//!
//! This module provides traits and default implementations for executing
//! query stages in a distributed setting. The execution engine is responsible
//! for creating query stage executors from physical plans.

use async_trait::async_trait;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::execution_plans::sort_shuffle::SortShuffleWriterExec;
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::utils;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use std::fmt::{Debug, Display};
use std::sync::Arc;

/// Extension point for customizing query stage execution.
///
/// Implement this trait to provide a custom execution engine that can
/// transform physical plans into query stage executors. This allows
/// for custom execution strategies beyond the default DataFusion-based
/// execution.
pub trait ExecutionEngine: Sync + Send {
    /// Creates a query stage executor from a physical plan.
    ///
    /// The returned executor will be responsible for executing the given
    /// plan partition and writing shuffle output to the specified work directory.
    fn create_query_stage_exec(
        &self,
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<dyn QueryStageExecutor>>;
}

/// Executor for a single query stage in a distributed query.
///
/// A query stage is a section of a query plan that has consistent partitioning
/// and can be executed as one unit with each partition running in parallel.
/// The output of each partition is re-partitioned and written to disk in
/// Arrow IPC format. Subsequent stages read these results via ShuffleReaderExec.
#[async_trait]
pub trait QueryStageExecutor: Sync + Send + Debug + Display {
    /// Executes a single partition of this query stage.
    ///
    /// Returns metadata about the shuffle partitions written to disk,
    /// including file paths and statistics.
    async fn execute_query_stage(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>>;

    /// Collects execution metrics from all operators in the plan.
    fn collect_plan_metrics(&self) -> Vec<MetricsSet>;
}

/// Default execution engine using DataFusion's ShuffleWriterExec.
///
/// This implementation expects the input plan to be wrapped in a
/// ShuffleWriterExec and creates a DefaultQueryStageExec to execute it.
pub struct DefaultExecutionEngine {}

impl ExecutionEngine for DefaultExecutionEngine {
    fn create_query_stage_exec(
        &self,
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<dyn QueryStageExecutor>> {
        // the query plan created by the scheduler always starts with a shuffle writer
        // (either ShuffleWriterExec or SortShuffleWriterExec)
        if let Some(shuffle_writer) = plan.as_any().downcast_ref::<ShuffleWriterExec>() {
            // recreate the shuffle writer with the correct working directory
            let exec = ShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                work_dir.to_string(),
                shuffle_writer.shuffle_output_partitioning().cloned(),
            )?;
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Hash(exec),
            )))
        } else if let Some(sort_shuffle_writer) =
            plan.as_any().downcast_ref::<SortShuffleWriterExec>()
        {
            // recreate the sort shuffle writer with the correct working directory
            let exec = SortShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                work_dir.to_string(),
                sort_shuffle_writer.shuffle_output_partitioning().clone(),
                sort_shuffle_writer.config().clone(),
            )?;
            Ok(Arc::new(DefaultQueryStageExec::new(
                ShuffleWriterVariant::Sort(exec),
            )))
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to new_query_stage_exec is not a ShuffleWriterExec or SortShuffleWriterExec"
                    .to_string(),
            ))
        }
    }
}

/// Enum representing the different shuffle writer implementations.
#[derive(Debug, Clone)]
pub enum ShuffleWriterVariant {
    /// Hash-based shuffle writer (original implementation).
    Hash(ShuffleWriterExec),
    /// Sort-based shuffle writer.
    Sort(SortShuffleWriterExec),
}

/// Default query stage executor that wraps a shuffle writer.
///
/// This executor delegates to the underlying shuffle writer to perform the actual
/// shuffle write operation, which partitions the data and writes it to disk.
#[derive(Debug)]
pub struct DefaultQueryStageExec {
    /// The underlying shuffle writer execution plan.
    shuffle_writer: ShuffleWriterVariant,
}

impl DefaultQueryStageExec {
    /// Creates a new DefaultQueryStageExec wrapping the given shuffle writer.
    pub fn new(shuffle_writer: ShuffleWriterVariant) -> Self {
        Self { shuffle_writer }
    }
}

impl Display for DefaultQueryStageExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Hash): ({})\n{}",
                    stage_metrics.join(", "),
                    writer
                )
            }
            ShuffleWriterVariant::Sort(writer) => {
                let stage_metrics: Vec<String> = writer
                    .metrics()
                    .unwrap_or_default()
                    .iter()
                    .map(|m| m.to_string())
                    .collect();
                write!(
                    f,
                    "DefaultQueryStageExec(Sort): ({})\n{:?}",
                    stage_metrics.join(", "),
                    writer
                )
            }
        }
    }
}

#[async_trait]
impl QueryStageExecutor for DefaultQueryStageExec {
    async fn execute_query_stage(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => {
                writer
                    .clone()
                    .execute_shuffle_write(input_partition, context)
                    .await
            }
            ShuffleWriterVariant::Sort(writer) => {
                writer
                    .clone()
                    .execute_shuffle_write(input_partition, context)
                    .await
            }
        }
    }

    fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
        match &self.shuffle_writer {
            ShuffleWriterVariant::Hash(writer) => utils::collect_plan_metrics(writer),
            ShuffleWriterVariant::Sort(writer) => utils::collect_plan_metrics(writer),
        }
    }
}
