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

//! Ballista executor logic

use std::collections::HashMap;
use std::sync::Arc;

use crate::metrics::ExecutorMetricsCollector;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::ExecutorRegistration;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;

use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::future::AbortHandle;

use ballista_core::serde::scheduler::PartitionId;
use tokio::sync::Mutex;

type AbortHandles = Arc<Mutex<HashMap<(usize, PartitionId), AbortHandle>>>;

/// Ballista executor
#[derive(Clone)]
pub struct Executor {
    /// Metadata
    pub metadata: ExecutorRegistration,

    /// Directory for storing partial results
    pub work_dir: String,

    /// Scalar functions that are registered in the Executor
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,

    /// Aggregate functions registered in the Executor
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,

    /// Runtime environment for Executor
    pub runtime: Arc<RuntimeEnv>,

    /// Collector for runtime execution metrics
    pub metrics_collector: Arc<dyn ExecutorMetricsCollector>,

    /// Concurrent tasks can run in executor
    pub concurrent_tasks: usize,

    /// Handles to abort executing tasks
    abort_handles: AbortHandles,
}

impl Executor {
    /// Create a new executor instance
    pub fn new(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime: Arc<RuntimeEnv>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        concurrent_tasks: usize,
    ) -> Self {
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            // TODO add logic to dynamically load UDF/UDAFs libs from files
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            runtime,
            metrics_collector,
            concurrent_tasks,
            abort_handles: Default::default(),
        }
    }
}

impl Executor {
    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    pub async fn execute_shuffle_write(
        &self,
        task_id: usize,
        partition: PartitionId,
        shuffle_writer: Arc<ShuffleWriterExec>,
        task_ctx: Arc<TaskContext>,
        _shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Vec<protobuf::ShuffleWritePartition>, BallistaError> {
        let (task, abort_handle) = futures::future::abortable(
            shuffle_writer.execute_shuffle_write(partition.partition_id, task_ctx),
        );

        {
            let mut abort_handles = self.abort_handles.lock().await;
            abort_handles.insert((task_id, partition.clone()), abort_handle);
        }

        let partitions = task.await??;

        self.abort_handles
            .lock()
            .await
            .remove(&(task_id, partition.clone()));

        self.metrics_collector.record_stage(
            &partition.job_id,
            partition.stage_id,
            partition.partition_id,
            shuffle_writer,
        );

        Ok(partitions)
    }

    /// Recreate the shuffle writer with the correct working directory.
    pub fn new_shuffle_writer(
        &self,
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<ShuffleWriterExec>, BallistaError> {
        let exec = if let Some(shuffle_writer) =
            plan.as_any().downcast_ref::<ShuffleWriterExec>()
        {
            // recreate the shuffle writer with the correct working directory
            ShuffleWriterExec::try_new(
                job_id,
                stage_id,
                plan.children()[0].clone(),
                self.work_dir.clone(),
                shuffle_writer.shuffle_output_partitioning().cloned(),
            )
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to execute_shuffle_write is not a ShuffleWriterExec"
                    .to_string(),
            ))
        }?;
        Ok(Arc::new(exec))
    }

    pub async fn cancel_task(
        &self,
        task_id: usize,
        job_id: String,
        stage_id: usize,
        partition_id: usize,
    ) -> Result<bool, BallistaError> {
        if let Some(handle) = self.abort_handles.lock().await.remove(&(
            task_id,
            PartitionId {
                job_id,
                stage_id,
                partition_id,
            },
        )) {
            handle.abort();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }
}

#[cfg(test)]
mod test {
    use crate::executor::Executor;
    use crate::metrics::LoggingMetricsCollector;
    use arrow::datatypes::{Schema, SchemaRef};
    use arrow::error::ArrowError;
    use arrow::record_batch::RecordBatch;
    use ballista_core::execution_plans::ShuffleWriterExec;
    use ballista_core::serde::protobuf::ExecutorRegistration;
    use datafusion::execution::context::TaskContext;

    use ballista_core::serde::scheduler::PartitionId;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::{
        ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
        Statistics,
    };
    use datafusion::prelude::SessionContext;
    use futures::Stream;
    use std::any::Any;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tempfile::TempDir;

    /// A RecordBatchStream that will never terminate
    struct NeverendingRecordBatchStream;

    impl RecordBatchStream for NeverendingRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }
    }

    impl Stream for NeverendingRecordBatchStream {
        type Item = Result<RecordBatch, ArrowError>;

        fn poll_next(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    /// An ExecutionPlan which will never terminate
    #[derive(Debug)]
    pub struct NeverendingOperator;

    impl ExecutionPlan for NeverendingOperator {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            Ok(Box::pin(NeverendingRecordBatchStream))
        }

        fn statistics(&self) -> Statistics {
            Statistics::default()
        }
    }

    #[tokio::test]
    async fn test_task_cancellation() {
        let work_dir = TempDir::new()
            .unwrap()
            .into_path()
            .into_os_string()
            .into_string()
            .unwrap();

        let shuffle_write = ShuffleWriterExec::try_new(
            "job-id".to_owned(),
            1,
            Arc::new(NeverendingOperator),
            work_dir.clone(),
            None,
        )
        .expect("creating shuffle writer");

        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            port: 0,
            grpc_port: 0,
            specification: None,
            optional_host: None,
        };

        let ctx = SessionContext::new();

        let executor = Executor::new(
            executor_registration,
            &work_dir,
            ctx.runtime_env(),
            Arc::new(LoggingMetricsCollector {}),
            2,
        );

        let (sender, receiver) = tokio::sync::oneshot::channel();

        // Spawn our non-terminating task on a separate fiber.
        let executor_clone = executor.clone();
        tokio::task::spawn(async move {
            let part = PartitionId {
                job_id: "job-id".to_owned(),
                stage_id: 1,
                partition_id: 0,
            };
            let task_result = executor_clone
                .execute_shuffle_write(
                    1,
                    part,
                    Arc::new(shuffle_write),
                    ctx.task_ctx(),
                    None,
                )
                .await;
            sender.send(task_result).expect("sending result");
        });

        // Now cancel the task. We can only cancel once the task has been executed and has an `AbortHandle` registered, so
        // poll until that happens.
        for _ in 0..20 {
            if executor
                .cancel_task(1, "job-id".to_owned(), 1, 0)
                .await
                .expect("cancelling task")
            {
                break;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        // Wait for our task to complete
        let result = tokio::time::timeout(Duration::from_secs(5), receiver).await;

        // Make sure the task didn't timeout
        assert!(result.is_ok());

        // Make sure the actual task failed
        let inner_result = result.unwrap().unwrap();
        assert!(inner_result.is_err());
    }
}
