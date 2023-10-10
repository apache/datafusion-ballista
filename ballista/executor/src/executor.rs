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

use crate::execution_engine::DefaultExecutionEngine;
use crate::execution_engine::ExecutionEngine;
use crate::execution_engine::QueryStageExecutor;
use crate::metrics::ExecutorMetricsCollector;
use ballista_core::error::BallistaError;
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::ExecutorRegistration;

use crate::metrics::load::RUNNING_TASKS;
use dashmap::DashMap;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::WindowUDF;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::Partitioning;
use futures::future::AbortHandle;
use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_int_gauge, Histogram, HistogramTimer, IntGauge,
};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::info;

lazy_static! {
    static ref ACTIVE_TASKS: IntGauge =
        register_int_gauge!("active_task_count", "Number of currently active tasks")
            .unwrap();
    static ref TASK_DURATION: Histogram = register_histogram!(
        "task_duration_seconds",
        "Histogram of executor task duration in seconds",
        vec![0.5_f64, 1_f64, 5_f64, 30_f64, 60_f64, 120_f64, 180_f64, 240_f64, 300_f64],
    )
    .unwrap();
}

struct ActiveTaskMetricGuard(usize, HistogramTimer);

impl ActiveTaskMetricGuard {
    fn new(partitions: usize) -> Self {
        RUNNING_TASKS.fetch_add(partitions, Ordering::Relaxed);
        ACTIVE_TASKS.add(partitions as i64);
        Self(partitions, TASK_DURATION.start_timer())
    }
}

impl Drop for ActiveTaskMetricGuard {
    fn drop(&mut self) {
        RUNNING_TASKS.fetch_sub(self.0, Ordering::Relaxed);
        ACTIVE_TASKS.sub(self.0 as i64);
    }
}

/// Map from (Job ID, task ID) -> AbortHandle for running task
type AbortHandles = Arc<DashMap<(String, usize), AbortHandle>>;

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

    /// Window functions registered in the Executor
    pub window_functions: HashMap<String, Arc<WindowUDF>>,

    /// Runtime environment for Executor
    pub runtime: Arc<RuntimeEnv>,

    /// Collector for runtime execution metrics
    pub metrics_collector: Arc<dyn ExecutorMetricsCollector>,

    /// Concurrent tasks can run in executor
    pub concurrent_tasks: usize,

    /// Handles to abort executing tasks
    abort_handles: AbortHandles,

    /// Execution engine that the executor will delegate to
    /// for executing query stages
    pub(crate) execution_engine: Arc<dyn ExecutionEngine>,

    drained: Arc<watch::Sender<()>>,
    check_drained: watch::Receiver<()>,
}

impl Executor {
    /// Create a new executor instance
    pub fn new(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime: Arc<RuntimeEnv>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        concurrent_tasks: usize,
        execution_engine: Option<Arc<dyn ExecutionEngine>>,
    ) -> Self {
        Self::with_functions(
            metadata,
            work_dir,
            runtime,
            metrics_collector,
            concurrent_tasks,
            execution_engine,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_functions(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime: Arc<RuntimeEnv>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        concurrent_tasks: usize,
        execution_engine: Option<Arc<dyn ExecutionEngine>>,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        window_functions: HashMap<String, Arc<WindowUDF>>,
    ) -> Self {
        let (drained, check_drained) = watch::channel(());

        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            scalar_functions,
            aggregate_functions,
            window_functions,
            runtime,
            metrics_collector,
            concurrent_tasks,
            abort_handles: Default::default(),
            execution_engine: execution_engine
                .unwrap_or_else(|| Arc::new(DefaultExecutionEngine {})),
            drained: Arc::new(drained),
            check_drained,
        }
    }
}

impl Executor {
    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_query_stage(
        &self,
        job_id: &str,
        stage_id: usize,
        task_id: usize,
        partitions: &[usize],
        query_stage_exec: Arc<dyn QueryStageExecutor>,
        task_ctx: Arc<TaskContext>,
        _shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Vec<protobuf::ShuffleWritePartition>, BallistaError> {
        info!(
            executor_id = self.metadata.id,
            job_id, stage_id, task_id, "executing query stage"
        );

        let _metric_guard = ActiveTaskMetricGuard::new(partitions.len());

        let (task, abort_handle) = futures::future::abortable(
            query_stage_exec.execute_query_stage(partitions.to_vec(), task_ctx),
        );

        self.abort_handles
            .insert((job_id.to_string(), task_id), abort_handle);

        let shuffle_partitions = task.await;

        self.remove_handle(job_id.to_string(), task_id);

        let shuffle_partitions = shuffle_partitions??;

        self.metrics_collector.record_stage(
            job_id,
            stage_id,
            partitions,
            query_stage_exec,
        );

        Ok(shuffle_partitions)
    }

    pub async fn cancel_task(
        &self,
        task_id: usize,
        job_id: String,
        stage_id: usize,
    ) -> Result<bool, BallistaError> {
        info!(
            executor_id = self.metadata.id,
            task_id, job_id, stage_id, "cancelling task"
        );
        if let Some((_, handle)) = self.remove_handle(job_id, task_id) {
            handle.abort();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }

    pub fn active_task_count(&self) -> usize {
        self.abort_handles.len()
    }

    pub async fn wait_drained(&self) {
        let mut check_drained = self.check_drained.clone();
        loop {
            if self.active_task_count() == 0 {
                break;
            }

            if check_drained.changed().await.is_err() {
                break;
            };
        }
    }

    fn remove_handle(
        &self,
        job_id: String,
        task_id: usize,
    ) -> Option<((String, usize), AbortHandle)> {
        let removed = self.abort_handles.remove(&(job_id, task_id));

        if self.active_task_count() == 0 {
            self.drained.send_replace(());
        }

        removed
    }
}

#[cfg(test)]
mod test {
    use crate::executor::Executor;
    use crate::metrics::LoggingMetricsCollector;
    use arrow::datatypes::{Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use ballista_core::execution_plans::{CoalesceTasksExec, ShuffleWriterExec};
    use ballista_core::serde::protobuf::ExecutorRegistration;
    use datafusion::execution::context::TaskContext;

    use crate::execution_engine::DefaultQueryStageExec;
    use datafusion::error::DataFusionError;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
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
        type Item = Result<RecordBatch, DataFusionError>;

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

    impl DisplayAs for NeverendingOperator {
        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "NeverendingOperator")
                }
            }
        }
    }

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
            vec![0],
            Arc::new(CoalesceTasksExec::new(
                Arc::new(NeverendingOperator),
                vec![0],
            )),
            work_dir.clone(),
            None,
        )
        .expect("creating shuffle writer");

        let query_stage_exec = DefaultQueryStageExec::new(shuffle_write);

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
            None,
        );

        let (sender, receiver) = tokio::sync::oneshot::channel();

        // Spawn our non-terminating task on a separate fiber.
        let executor_clone = executor.clone();
        tokio::task::spawn(async move {
            let task_result = executor_clone
                .execute_query_stage(
                    "job-id",
                    1,
                    1,
                    &[0],
                    Arc::new(query_stage_exec),
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
                .cancel_task(1, "job-id".to_owned(), 1)
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
