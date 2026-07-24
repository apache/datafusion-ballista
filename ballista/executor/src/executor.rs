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
use crate::execution_loop::any_to_string;
use crate::metrics::ExecutorMetricsCollector;
use crate::metrics::LoggingMetricsCollector;
use crate::runtime_cache::SessionRuntimeCache;
use ballista_core::ConfigProducer;
use ballista_core::JobId;
use ballista_core::RuntimeProducer;
use ballista_core::error::BallistaError;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::ExecutorRegistration;
use ballista_core::serde::scheduler::TaskKey;
use dashmap::DashMap;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use futures::FutureExt;
use futures::future::AbortHandle;
use log::error;
use log::warn;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A future that resolves when all active tasks on an executor have completed.
///
/// This is used during graceful shutdown to wait for in-flight tasks to drain
/// before terminating the executor process.
pub struct TasksDrainedFuture(
    /// The executor instance to monitor for task completion.
    pub Arc<Executor>,
);

impl Future for TasksDrainedFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.0.abort_handles.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

type AbortHandles = Arc<DashMap<TaskKey, AbortHandle>>;

/// Ballista executor
#[derive(Clone)]
pub struct Executor {
    /// Metadata
    pub metadata: ExecutorRegistration,

    /// Directory for storing partial results
    pub work_dir: String,

    /// Function registry
    pub function_registry: Arc<BallistaFunctionRegistry>,

    /// Creates [RuntimeEnv] based on [SessionConfig]
    pub runtime_producer: RuntimeProducer,

    /// Creates default [SessionConfig]
    pub config_producer: ConfigProducer,

    /// Collector for runtime execution metrics
    pub metrics_collector: Arc<dyn ExecutorMetricsCollector>,

    /// Virtual cores assigned to this executor. See CLI docs on `--vcores`.
    pub vcores: usize,

    /// Handles to abort executing tasks
    abort_handles: AbortHandles,

    /// Execution engine that the executor will delegate to
    /// for executing query stages
    pub(crate) execution_engine: Arc<dyn ExecutionEngine>,

    /// Optional session-keyed cache of shared base runtime envs. When set,
    /// `produce_runtime_for_session` reuses read-side state across a session's
    /// tasks; when `None`, each task builds a runtime from `runtime_producer`.
    session_runtime_cache: Option<Arc<dyn SessionRuntimeCache>>,
}

impl Executor {
    /// Create a new executor instance with given [RuntimeEnv]
    /// It will use default scalar, aggregate and window functions
    pub fn new_basic(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        vcores: usize,
    ) -> Self {
        Self::new(
            metadata,
            work_dir,
            runtime_producer,
            config_producer,
            Arc::new(BallistaFunctionRegistry::default()),
            Arc::new(LoggingMetricsCollector::default()),
            vcores,
            Arc::new(DefaultExecutionEngine::new()),
        )
    }

    /// Create a new executor instance with given [RuntimeEnv],
    /// [datafusion::logical_expr::ScalarUDF], [datafusion::logical_expr::AggregateUDF] and [datafusion::logical_expr::WindowUDF]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        function_registry: Arc<BallistaFunctionRegistry>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        vcores: usize,
        execution_engine: Arc<dyn ExecutionEngine>,
    ) -> Self {
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            function_registry,
            runtime_producer,
            config_producer,
            metrics_collector,
            vcores,
            abort_handles: Default::default(),
            execution_engine,
            session_runtime_cache: None,
        }
    }
    /// Creates new Executor with default `ExecutionEngine`.
    /// Default `ExecutionEngine` does not cache client connections.
    pub fn with_default_execution_engine(
        metadata: ExecutorRegistration,
        work_dir: &str,
        runtime_producer: RuntimeProducer,
        config_producer: ConfigProducer,
        function_registry: Arc<BallistaFunctionRegistry>,
        metrics_collector: Arc<dyn ExecutorMetricsCollector>,
        vcores: usize,
    ) -> Self {
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            function_registry,
            runtime_producer,
            config_producer,
            metrics_collector,
            vcores,
            abort_handles: Default::default(),
            execution_engine: Arc::new(DefaultExecutionEngine::new()),
            session_runtime_cache: None,
        }
    }
}

impl Executor {
    /// Creates a [`RuntimeEnv`] using the configured runtime producer.
    pub fn produce_runtime(
        &self,
        config: &SessionConfig,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        (self.runtime_producer)(config)
    }

    /// Attaches (or clears) the session-keyed runtime cache. Cloning the
    /// `Executor` shares the same cache via `Arc`.
    pub fn with_session_runtime_cache(
        mut self,
        cache: Option<Arc<dyn SessionRuntimeCache>>,
    ) -> Self {
        self.session_runtime_cache = cache;
        self
    }

    /// Produces the runtime for a task, reusing the session's shared read-side
    /// state when a session cache is attached; otherwise builds a runtime per
    /// task via the `runtime_producer`. `vcores_consumed` is the number of
    /// vcores this task claimed at bind time; the memory pool policy uses it
    /// to size the per-task pool proportionally.
    pub fn produce_runtime_for_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
        vcores_consumed: u32,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        match &self.session_runtime_cache {
            Some(cache) => cache.produce_runtime(session_id, config, vcores_consumed),
            None => (self.runtime_producer)(config),
        }
    }

    /// Creates a default [`SessionConfig`] using the configured config producer.
    pub fn produce_config(&self) -> SessionConfig {
        (self.config_producer)()
    }

    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    pub async fn execute_query_stage(
        &self,
        key: TaskKey,
        query_stage_exec: Arc<dyn QueryStageExecutor>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Vec<protobuf::ShuffleWritePartition>, BallistaError> {
        let (task, abort_handle) = futures::future::abortable(
            query_stage_exec.execute_query_stage(key.task_id, task_ctx),
        );

        self.abort_handles.insert(key.clone(), abort_handle);

        let partitions = match std::panic::AssertUnwindSafe(task).catch_unwind().await {
            Ok(Ok(result)) => {
                result.map_err(|e| BallistaError::DataFusionError(Box::new(e)))
            }
            Ok(Err(_)) => {
                warn!("Task has been aborted!");
                Err(BallistaError::Cancelled)
            }
            Err(p) => {
                let error_msg = format!("{:#?}", any_to_string(&p));
                error!("{error_msg}");
                Err(BallistaError::Internal(error_msg))
            }
        };

        self.abort_handles.remove(&key);

        self.metrics_collector.record_stage(
            &key.job_id,
            key.stage_id,
            key.task_id,
            query_stage_exec,
        );

        partitions
    }

    /// Cancels a running task by aborting its execution.
    ///
    /// Returns `Ok(true)` if the task was found and cancelled, `Ok(false)` if not found.
    pub async fn cancel_task(
        &self,
        job_id: JobId,
        stage_id: usize,
        task_id: usize,
    ) -> Result<bool, BallistaError> {
        if let Some((_, handle)) = self.abort_handles.remove(&TaskKey {
            job_id,
            stage_id,
            task_id,
        }) {
            handle.abort();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns the working directory path for this executor.
    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }

    /// Returns the number of tasks currently executing on this executor.
    pub fn active_task_count(&self) -> usize {
        self.abort_handles.len()
    }
}

#[cfg(test)]
mod test {
    use crate::execution_engine::{DefaultQueryStageExec, ShuffleWriterVariant};
    use crate::executor::Executor;
    use crate::runtime_cache::{
        DefaultSessionRuntimeCache, MemoryPoolPolicy, SessionRuntimeCache,
    };
    use ballista_core::RuntimeProducer;
    use ballista_core::execution_plans::ShuffleWriterExec;
    use ballista_core::serde::protobuf::ExecutorRegistration;
    use ballista_core::serde::scheduler::TaskKey;
    use ballista_core::utils::default_config_producer;
    use datafusion::arrow::datatypes::{Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::execution::context::TaskContext;
    use datafusion::execution::runtime_env::RuntimeEnv;

    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    };
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use futures::Stream;
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
        properties: Arc<PlanProperties>,
    }

    impl NeverendingOperator {
        fn new() -> Self {
            NeverendingOperator {
                properties: Arc::new(PlanProperties::new(
                    datafusion::physical_expr::EquivalenceProperties::new(Arc::new(
                        Schema::empty(),
                    )),
                    Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                    datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                )),
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
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => {
                    write!(f, "NeverendingOperator")
                }
            }
        }
    }

    impl ExecutionPlan for NeverendingOperator {
        fn name(&self) -> &str {
            "NeverendingOperator"
        }

        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
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
    }

    #[tokio::test]
    async fn test_task_cancellation() {
        let work_dir = TempDir::new().unwrap().path().to_str().unwrap().to_string();

        let shuffle_write = ShuffleWriterExec::try_new(
            "job-id".into(),
            1,
            Arc::new(NeverendingOperator::new()),
            work_dir.clone(),
        )
        .expect("creating shuffle writer");

        let query_stage_exec =
            DefaultQueryStageExec::new(ShuffleWriterVariant::Passthrough(shuffle_write));

        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        };
        let config_producer = Arc::new(default_config_producer);
        let ctx = SessionContext::new();
        let runtime_env = ctx.runtime_env().clone();
        let runtime_producer: RuntimeProducer =
            Arc::new(move |_| Ok(runtime_env.clone()));

        let executor = Executor::new_basic(
            executor_registration,
            &work_dir,
            runtime_producer,
            config_producer,
            2,
        );

        let (sender, receiver) = tokio::sync::oneshot::channel();

        // Spawn our non-terminating task on a separate fiber.
        let executor_clone = executor.clone();
        tokio::task::spawn(async move {
            let key = TaskKey {
                job_id: "job-id".into(),
                stage_id: 1,
                task_id: 0,
            };
            let task_result = executor_clone
                .execute_query_stage(key, Arc::new(query_stage_exec), ctx.task_ctx())
                .await;
            sender.send(task_result).expect("sending result");
        });

        // Now cancel the task. We can only cancel once the task has been executed and has an `AbortHandle` registered, so
        // poll until that happens.
        for _ in 0..20 {
            if executor
                .cancel_task("job-id".into(), 1, 0)
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

    #[test]
    fn produce_runtime_for_session_shares_read_side_state() {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        };
        let config_producer = Arc::new(default_config_producer);

        // A base producer that builds a fresh env each call, plus an identity
        // pool policy, so shared read-side state is observable via ptr equality.
        let base_producer: RuntimeProducer =
            Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())));
        let identity: MemoryPoolPolicy = Arc::new(|base, _, _| Ok(base));
        let cache: Arc<dyn SessionRuntimeCache> = Arc::new(
            DefaultSessionRuntimeCache::new(base_producer.clone(), identity, 4),
        );

        let executor = Executor::new_basic(
            executor_registration,
            "/tmp",
            base_producer,
            config_producer,
            2,
        )
        .with_session_runtime_cache(Some(cache));

        let cfg = SessionConfig::new();
        let e1 = executor.produce_runtime_for_session("s1", &cfg, 1).unwrap();
        let e2 = executor.produce_runtime_for_session("s1", &cfg, 1).unwrap();
        let e3 = executor.produce_runtime_for_session("s2", &cfg, 1).unwrap();

        assert!(Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e3.cache_manager));
    }

    #[test]
    fn produce_runtime_for_session_falls_back_without_cache() {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        };
        let config_producer = Arc::new(default_config_producer);
        let base_producer: RuntimeProducer =
            Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())));

        // No cache attached: each call builds a fresh env.
        let executor = Executor::new_basic(
            executor_registration,
            "/tmp",
            base_producer,
            config_producer,
            2,
        );

        let cfg = SessionConfig::new();
        let e1 = executor.produce_runtime_for_session("s1", &cfg, 1).unwrap();
        let e2 = executor.produce_runtime_for_session("s1", &cfg, 1).unwrap();
        assert!(!Arc::ptr_eq(&e1.cache_manager, &e2.cache_manager));
    }
}
