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
use ballista_core::serde::scheduler::PartitionId;
use ballista_core::ConfigProducer;
use ballista_core::RuntimeProducer;
use dashmap::DashMap;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionState;
use datafusion::functions::all_default_functions;
use datafusion::functions_aggregate::all_default_aggregate_functions;
use datafusion::functions_window::all_default_window_functions;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion::prelude::SessionConfig;
use futures::future::AbortHandle;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct TasksDrainedFuture(pub Arc<Executor>);

impl Future for TasksDrainedFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.abort_handles.len() > 0 {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

type AbortHandles = Arc<DashMap<(usize, PartitionId), AbortHandle>>;

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

    pub runtime_producer: RuntimeProducer,

    pub config_producer: ConfigProducer,

    /// Collector for runtime execution metrics
    pub metrics_collector: Arc<dyn ExecutorMetricsCollector>,

    /// Concurrent tasks can run in executor
    pub concurrent_tasks: usize,

    /// Handles to abort executing tasks
    abort_handles: AbortHandles,

    /// Execution engine that the executor will delegate to
    /// for executing query stages
    pub(crate) execution_engine: Arc<dyn ExecutionEngine>,
}

impl Executor {
    /// Create a new executor instance with given [RuntimeEnv]
    /// It will use default scalar, aggregate and window functions
    pub fn new_from_runtime(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        concurrent_tasks: usize,
        execution_engine: Option<Arc<dyn ExecutionEngine>>,
    ) -> Self {
        let scalar_functions = all_default_functions()
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect();

        let aggregate_functions = all_default_aggregate_functions()
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect();

        let window_functions = all_default_window_functions()
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect();

        Self::new(
            metadata,
            work_dir,
            runtime_producer,
            config_producer,
            scalar_functions,
            aggregate_functions,
            window_functions,
            metrics_collector,
            concurrent_tasks,
            execution_engine,
        )
    }

    /// Create a new executor instance with given [RuntimeEnv],
    /// [ScalarUDF], [AggregateUDF] and [WindowUDF]
    #[allow(clippy::too_many_arguments)]
    fn new(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        window_functions: HashMap<String, Arc<WindowUDF>>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        concurrent_tasks: usize,
        execution_engine: Option<Arc<dyn ExecutionEngine>>,
    ) -> Self {
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            scalar_functions,
            aggregate_functions,
            window_functions,
            runtime_producer,
            config_producer,
            metrics_collector,
            concurrent_tasks,
            abort_handles: Default::default(),
            execution_engine: execution_engine
                .unwrap_or_else(|| Arc::new(DefaultExecutionEngine {})),
        }
    }
    /// Create a new executor instance from [SessionState].
    /// [ScalarUDF], [AggregateUDF] and [WindowUDF]
    #[allow(clippy::too_many_arguments)]
    pub fn new_from_state(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        state: &SessionState, // TODO MM narrow state down
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        concurrent_tasks: usize,
        execution_engine: Option<Arc<dyn ExecutionEngine>>,
    ) -> Self {
        let scalar_functions = state.scalar_functions().clone();
        let aggregate_functions = state.aggregate_functions().clone();
        let window_functions = state.window_functions().clone();

        Self::new(
            metadata,
            work_dir,
            runtime_producer,
            config_producer,
            scalar_functions,
            aggregate_functions,
            window_functions,
            metrics_collector,
            concurrent_tasks,
            execution_engine,
        )
    }
}

impl Executor {
    pub fn produce_runtime(
        &self,
        config: &SessionConfig,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        (self.runtime_producer)(config)
    }

    pub fn produce_config(&self) -> SessionConfig {
        (self.config_producer)()
    }

    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    pub async fn execute_query_stage(
        &self,
        task_id: usize,
        partition: PartitionId,
        query_stage_exec: Arc<dyn QueryStageExecutor>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Vec<protobuf::ShuffleWritePartition>, BallistaError> {
        let (task, abort_handle) = futures::future::abortable(
            query_stage_exec.execute_query_stage(partition.partition_id, task_ctx),
        );

        self.abort_handles
            .insert((task_id, partition.clone()), abort_handle);

        let partitions = task.await??;

        self.abort_handles.remove(&(task_id, partition.clone()));

        self.metrics_collector.record_stage(
            &partition.job_id,
            partition.stage_id,
            partition.partition_id,
            query_stage_exec,
        );

        Ok(partitions)
    }

    pub async fn cancel_task(
        &self,
        task_id: usize,
        job_id: String,
        stage_id: usize,
        partition_id: usize,
    ) -> Result<bool, BallistaError> {
        if let Some((_, handle)) = self.abort_handles.remove(&(
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

    pub fn active_task_count(&self) -> usize {
        self.abort_handles.len()
    }
}

#[cfg(test)]
mod test {
    use crate::execution_engine::DefaultQueryStageExec;
    use crate::executor::Executor;
    use crate::metrics::LoggingMetricsCollector;
    use arrow::datatypes::{Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use ballista_core::config::BallistaConfig;
    use ballista_core::execution_plans::ShuffleWriterExec;
    use ballista_core::serde::protobuf::ExecutorRegistration;
    use ballista_core::serde::scheduler::PartitionId;
    use ballista_core::RuntimeProducer;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::execution::context::TaskContext;

    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream, Statistics,
    };
    use datafusion::prelude::{SessionConfig, SessionContext};
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
    pub struct NeverendingOperator {
        properties: PlanProperties,
    }

    impl NeverendingOperator {
        fn new() -> Self {
            NeverendingOperator {
                properties: PlanProperties::new(
                    datafusion::physical_expr::EquivalenceProperties::new(Arc::new(
                        Schema::empty(),
                    )),
                    Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::ExecutionMode::Bounded,
                ),
            }
        }
    }

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
        fn name(&self) -> &str {
            "NeverendingOperator"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }

        fn properties(&self) -> &PlanProperties {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

        fn statistics(&self) -> Result<Statistics> {
            Ok(Statistics::new_unknown(&self.schema()))
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
            Arc::new(NeverendingOperator::new()),
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
        let config_producer = Arc::new(|| {
            SessionConfig::new().with_option_extension(BallistaConfig::new().unwrap())
        });
        let ctx = SessionContext::new();
        let runtime_env = ctx.runtime_env().clone();
        let runtime_producer: RuntimeProducer =
            Arc::new(move |_| Ok(runtime_env.clone()));

        let executor = Executor::new_from_runtime(
            executor_registration,
            &work_dir,
            runtime_producer,
            config_producer,
            Arc::new(LoggingMetricsCollector {}),
            2,
            None,
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
                .execute_query_stage(1, part, Arc::new(query_stage_exec), ctx.task_ctx())
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
