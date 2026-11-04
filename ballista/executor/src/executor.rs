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
use crate::metrics::LoggingMetricsCollector;
use crate::metrics::{ExecutorMemoryMetrics, ExecutorMetricsCollector};
use crate::runtime_cache::SessionRuntimeCache;
use ballista_core::ConfigProducer;
use ballista_core::JobId;
use ballista_core::RuntimeProducer;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::PipelinedRegistry;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::protobuf::{ExecutorRegistration, ShuffleWritePartition};
use ballista_core::serde::scheduler::TaskKey;
use ballista_core::work_dir_lifetime::{WorkDirActivity, WorkDirLease};
use dashmap::DashMap;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use futures::FutureExt;
use futures::future::AbortHandle;
use futures::task::AtomicWaker;
use log::error;
use log::warn;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.tasks_drained_waker.register(cx.waker());
        if let Some(activity) = &self.0.work_dir_activity {
            activity.register_waker(cx.waker());
        }
        if !self.0.abort_handles.is_empty()
            || self
                .0
                .work_dir_activity
                .as_ref()
                .is_some_and(|a| !a.is_idle())
        {
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

    tasks_drained_waker: Arc<AtomicWaker>,

    /// Includes queued launch requests and detached blocking shuffle writers.
    work_dir_activity: Option<Arc<WorkDirActivity>>,

    /// Execution engine that the executor will delegate to
    /// for executing query stages
    pub(crate) execution_engine: Arc<dyn ExecutionEngine>,

    /// Optional session-keyed cache of shared base runtime envs. When set,
    /// `produce_runtime_for_session` reuses read-side state across a session's
    /// tasks; when `None`, each task builds a runtime from `runtime_producer`.
    session_runtime_cache: Option<Arc<dyn SessionRuntimeCache>>,

    /// Executor-wide memory usage, sampled for heartbeats. Set when the
    /// executor was started with memory metrics enabled.
    memory_metrics: Option<ExecutorMemoryMetrics>,

    /// Output partitions of the pipelined producer tasks running here, served
    /// to consumers by the Flight service.
    pipelined_registry: Arc<PipelinedRegistry>,

    /// The runtime each pipelined job's tasks here share, by job id. See
    /// [`Executor::produce_runtime_for_pipelined_task`].
    pipelined_runtimes: Arc<parking_lot::Mutex<HashMap<String, Weak<RuntimeEnv>>>>,
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
        let pipelined_registry = Arc::new(PipelinedRegistry::new(metadata.id.clone()));
        Self {
            metadata,
            work_dir: work_dir.to_owned(),
            function_registry,
            runtime_producer,
            config_producer,
            metrics_collector,
            vcores,
            abort_handles: Default::default(),
            tasks_drained_waker: Default::default(),
            work_dir_activity: None,
            execution_engine,
            session_runtime_cache: None,
            memory_metrics: None,
            pipelined_registry,
            pipelined_runtimes: Default::default(),
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
        Self::new(
            metadata,
            work_dir,
            runtime_producer,
            config_producer,
            function_registry,
            metrics_collector,
            vcores,
            Arc::new(DefaultExecutionEngine::new()),
        )
    }
}

impl Executor {
    fn wake_tasks_drained(&self) {
        if self.abort_handles.is_empty() {
            self.tasks_drained_waker.wake();
        }
    }

    /// Installs tracking only for automatically created executor directories.
    pub(crate) fn with_work_dir_activity(
        mut self,
        activity: Option<Arc<WorkDirActivity>>,
    ) -> Self {
        self.work_dir_activity = activity;
        self
    }

    /// Reserve a lease before sending work to DedicatedExecutor's queue.
    pub(crate) fn begin_queued_work(
        &self,
    ) -> Result<Option<WorkDirLease>, BallistaError> {
        self.work_dir_activity
            .as_ref()
            .map(|a| a.lease().map_err(|_| BallistaError::Cancelled))
            .transpose()
    }

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

    /// Attaches (or clears) the executor-wide memory metrics reported in
    /// heartbeats. It should be the same handle the memory pool policy
    /// registers task pools with.
    pub(crate) fn with_memory_metrics(
        mut self,
        metrics: Option<ExecutorMemoryMetrics>,
    ) -> Self {
        self.memory_metrics = metrics;
        self
    }

    /// The executor-wide memory metrics, when memory metrics are enabled.
    pub(crate) fn memory_metrics(&self) -> Option<&ExecutorMemoryMetrics> {
        self.memory_metrics.as_ref()
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

    /// Produces the runtime for a task of the pipelined job `job_id`.
    ///
    /// A pipelined job's tasks on this executor run at the same time, on one
    /// reservation of `reserved_vcores`. So they share one runtime, and with it
    /// one memory pool sized by that reservation: pools sized task by task
    /// would add up to more than the executor's memory.
    pub fn produce_runtime_for_pipelined_task(
        &self,
        job_id: &str,
        session_id: &str,
        config: &SessionConfig,
        reserved_vcores: u32,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        let mut runtimes = self.pipelined_runtimes.lock();
        if let Some(runtime) = runtimes.get(job_id).and_then(Weak::upgrade) {
            return Ok(runtime);
        }
        // Forget jobs whose tasks here have all finished.
        runtimes.retain(|_, runtime| runtime.strong_count() > 0);
        let runtime =
            self.produce_runtime_for_session(session_id, config, reserved_vcores)?;
        runtimes.insert(job_id.to_owned(), Arc::downgrade(&runtime));
        Ok(runtime)
    }

    /// Creates a default [`SessionConfig`] using the configured config producer,
    /// with this executor's pipelined registry attached: that is how a task's
    /// pipelined writer and readers find it.
    pub fn produce_config(&self) -> SessionConfig {
        (self.config_producer)().with_extension(self.pipelined_registry.clone())
    }

    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    pub async fn execute_query_stage(
        &self,
        key: TaskKey,
        query_stage_exec: Arc<dyn QueryStageExecutor>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>, BallistaError> {
        // The registration race itself is an activity that can create shuffle paths.
        let _stage_work = self.begin_queued_work()?;
        let (task, abort_handle) = futures::future::abortable(
            query_stage_exec.execute_query_stage(key.task_id, task_ctx),
        );

        self.abort_handles.insert(key.clone(), abort_handle);

        let partitions = match std::panic::AssertUnwindSafe(task).catch_unwind().await {
            Ok(Ok(result)) => result.map_err(BallistaError::from),
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

        // cancel_task only signals the abort; this task owns removal after unwinding.
        self.abort_handles.remove(&key);
        self.wake_tasks_drained();

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
        // execute_query_stage removes the handle after the aborted task unwinds.
        if let Some(handle) = self.abort_handles.get(&TaskKey {
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

    /// The output partitions of the pipelined producer tasks running on this
    /// executor.
    pub fn pipelined_registry(&self) -> &Arc<PipelinedRegistry> {
        &self.pipelined_registry
    }

    /// Forgets the pipelined outputs of `job_id`, or of just `stage_ids` when
    /// given, once the scheduler has cleaned the job up.
    pub fn remove_pipelined_job_data(&self, job_id: &JobId, stage_ids: &[u32]) {
        if stage_ids.is_empty() {
            self.pipelined_registry.remove_job(job_id);
            self.pipelined_runtimes.lock().remove(job_id.as_str());
        } else {
            for stage_id in stage_ids {
                self.pipelined_registry
                    .remove_stage(job_id, *stage_id as usize);
            }
        }
    }

    /// Returns the number of tasks currently executing on this executor.
    pub fn active_task_count(&self) -> usize {
        self.abort_handles.len()
    }
}

#[cfg(test)]
mod test {
    use crate::execution_engine::{DefaultQueryStageExec, ShuffleWriterVariant};
    use crate::executor::{Executor, TasksDrainedFuture};
    use crate::runtime_cache::{
        DefaultSessionRuntimeCache, MemoryPoolPolicy, SessionRuntimeCache,
    };
    use ballista_core::RuntimeProducer;
    use ballista_core::error::BallistaError;
    use ballista_core::execution_plans::ShuffleWriterExec;
    use ballista_core::serde::protobuf::{ExecutorRegistration, ShuffleWritePartition};
    use ballista_core::serde::scheduler::TaskKey;
    use ballista_core::utils::default_config_producer;
    use datafusion::arrow::datatypes::{Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::execution::context::TaskContext;
    use datafusion::execution::runtime_env::RuntimeEnv;

    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    };
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use futures::Stream;
    use futures::task::{ArcWake, waker_ref};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tempfile::TempDir;

    /// A RecordBatchStream that will never terminate
    struct NeverendingRecordBatchStream;

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl ArcWake for WakeCounter {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

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

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
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

    /// The result `execute_query_stage` hands back once a spawned task unwinds.
    type TaskOutcome = Result<Vec<ShuffleWritePartition>, BallistaError>;

    /// Builds an executor over `work_dir`, along with the session context whose
    /// runtime its tasks run on.
    fn never_ending_executor(work_dir: &str) -> (Arc<Executor>, SessionContext) {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        };
        let config_producer = Arc::new(default_config_producer);
        let ctx = SessionContext::new();
        let runtime_env = ctx.runtime_env().clone();
        let runtime_producer: RuntimeProducer =
            Arc::new(move |_| Ok(runtime_env.clone()));

        let executor = Arc::new(Executor::new_basic(
            executor_registration,
            work_dir,
            runtime_producer,
            config_producer,
            2,
        ));

        (executor, ctx)
    }

    /// Spawns a task that never yields a batch on a separate fiber. The returned
    /// channel fires once `execute_query_stage` has unwound, which is after it has
    /// removed its own abort handle.
    fn spawn_never_ending_task(
        executor: &Arc<Executor>,
        ctx: &SessionContext,
        work_dir: &str,
        key: TaskKey,
    ) -> tokio::sync::oneshot::Receiver<TaskOutcome> {
        let shuffle_write = ShuffleWriterExec::try_new(
            key.job_id.clone(),
            key.stage_id,
            Arc::new(NeverendingOperator::new()),
            work_dir.to_string(),
        )
        .expect("creating shuffle writer");
        let query_stage_exec =
            DefaultQueryStageExec::new(ShuffleWriterVariant::Passthrough(shuffle_write));

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let executor = executor.clone();
        let task_ctx = ctx.task_ctx();
        tokio::task::spawn(async move {
            let task_result = executor
                .execute_query_stage(key, Arc::new(query_stage_exec), task_ctx)
                .await;
            sender.send(task_result).expect("sending result");
        });

        receiver
    }

    /// A task is only registered once it starts executing, so poll until the
    /// executor reports the count the test is waiting on.
    async fn await_active_task_count(executor: &Executor, expected: usize) {
        for _ in 0..20 {
            if executor.active_task_count() == expected {
                break;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        assert_eq!(executor.active_task_count(), expected);
    }

    /// Awaits a cancelled task's unwind and asserts it reported failure.
    async fn await_cancelled_task(receiver: tokio::sync::oneshot::Receiver<TaskOutcome>) {
        tokio::time::timeout(Duration::from_secs(5), receiver)
            .await
            .expect("task unwinding before the timeout")
            .expect("receiving task result")
            .expect_err("a cancelled task fails");
    }

    #[tokio::test]
    async fn test_task_cancellation() {
        let work_dir = TempDir::new().unwrap().path().to_str().unwrap().to_string();
        let (executor, ctx) = never_ending_executor(&work_dir);

        let receiver = spawn_never_ending_task(
            &executor,
            &ctx,
            &work_dir,
            TaskKey {
                job_id: "job-id".into(),
                stage_id: 1,
                task_id: 0,
            },
        );
        await_active_task_count(&executor, 1).await;

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = waker_ref(&wake_counter);
        let mut context = Context::from_waker(&waker);
        let mut tasks_drained = Box::pin(TasksDrainedFuture(executor.clone()));
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Pending);
        assert!(
            executor
                .cancel_task("job-id".into(), 1, 0)
                .await
                .expect("cancelling task")
        );
        assert_eq!(executor.active_task_count(), 1);

        await_cancelled_task(receiver).await;

        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Ready(()));
    }

    #[tokio::test]
    async fn test_tasks_drained_waits_for_last_task() {
        let work_dir = TempDir::new().unwrap().path().to_str().unwrap().to_string();
        let (executor, ctx) = never_ending_executor(&work_dir);

        let first = spawn_never_ending_task(
            &executor,
            &ctx,
            &work_dir,
            TaskKey {
                job_id: "job-id".into(),
                stage_id: 1,
                task_id: 0,
            },
        );
        let second = spawn_never_ending_task(
            &executor,
            &ctx,
            &work_dir,
            TaskKey {
                job_id: "job-id".into(),
                stage_id: 1,
                task_id: 1,
            },
        );
        await_active_task_count(&executor, 2).await;

        let wake_counter = Arc::new(WakeCounter::default());
        let waker = waker_ref(&wake_counter);
        let mut context = Context::from_waker(&waker);
        let mut tasks_drained = Box::pin(TasksDrainedFuture(executor.clone()));
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Pending);

        assert!(
            executor
                .cancel_task("job-id".into(), 1, 0)
                .await
                .expect("cancelling the first task")
        );
        await_cancelled_task(first).await;

        // Draining a task that is not the last one may wake the future, but it must
        // not resolve it, so re-poll rather than counting wakes.
        assert_eq!(executor.active_task_count(), 1);
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Pending);

        let wakes_before_last = wake_counter.0.load(Ordering::SeqCst);
        assert!(
            executor
                .cancel_task("job-id".into(), 1, 1)
                .await
                .expect("cancelling the second task")
        );
        await_cancelled_task(second).await;

        // Draining the last task has to wake the waker registered by the re-poll
        // above; without that, shutdown would never look at the map again.
        assert!(wake_counter.0.load(Ordering::SeqCst) > wakes_before_last);
        assert_eq!(executor.active_task_count(), 0);
        assert_eq!(tasks_drained.as_mut().poll(&mut context), Poll::Ready(()));
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

    #[test]
    fn pipelined_tasks_of_a_job_share_one_runtime_sized_by_its_reservation() {
        let executor_registration = ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        };
        let config_producer = Arc::new(default_config_producer);
        let base_producer: RuntimeProducer =
            Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())));
        // Records the vcores each new pool is sized for.
        let sized: Arc<std::sync::Mutex<Vec<u32>>> = Default::default();
        let recorder = sized.clone();
        let policy: MemoryPoolPolicy = Arc::new(move |_, _, vcores| {
            recorder.lock().unwrap().push(vcores);
            Ok(Arc::new(RuntimeEnv::default()))
        });
        let cache: Arc<dyn SessionRuntimeCache> = Arc::new(
            DefaultSessionRuntimeCache::new(base_producer.clone(), policy, 4),
        );
        let executor = Executor::new_basic(
            executor_registration,
            "/tmp",
            base_producer,
            config_producer,
            4,
        )
        .with_session_runtime_cache(Some(cache));

        let cfg = SessionConfig::new();
        let first = executor
            .produce_runtime_for_pipelined_task("job1", "s", &cfg, 3)
            .unwrap();
        let second = executor
            .produce_runtime_for_pipelined_task("job1", "s", &cfg, 3)
            .unwrap();
        let other = executor
            .produce_runtime_for_pipelined_task("job2", "s", &cfg, 1)
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &other));
        assert_eq!(*sized.lock().unwrap(), vec![3, 1]);

        // Once the job's tasks here have finished, its runtime is gone, and
        // a later task gets a new one.
        drop((first, second));
        let _again = executor
            .produce_runtime_for_pipelined_task("job1", "s", &cfg, 3)
            .unwrap();
        assert_eq!(*sized.lock().unwrap(), vec![3, 1, 3]);
    }

    /// R11: the actual Executor and IPC writer must own completion independently
    /// of an abortable stage future. Test a queued blocking worker against
    /// normal, timeout and cancelled-owner directory lifetime decisions.
    #[cfg(unix)]
    async fn r11_actual_shuffle_writer_lifecycle(
        scenario: &str,
    ) -> Option<(
        String,
        Arc<ballista_core::work_dir_lifetime::WorkDirActivity>,
    )> {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::path::Path;

        fn arrow_file_exists(dir: &Path) -> bool {
            let Ok(entries) = fs::read_dir(dir) else {
                return false;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && arrow_file_exists(&path) {
                    return true;
                }
                if path.extension().and_then(|s| s.to_str()) == Some("arrow") {
                    return true;
                }
            }
            false
        }

        let (work_dir, mut directory_guard) =
            crate::executor_process::resolve_executor_work_dir(None).unwrap();
        directory_guard.as_mut().unwrap().arm();
        let activity = directory_guard.as_ref().unwrap().activity();
        let root = Path::new(&work_dir);
        assert_eq!(
            fs::metadata(root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let (raw_executor, ctx) = never_ending_executor(&work_dir);
        let executor = Arc::new(
            Arc::try_unwrap(raw_executor)
                .ok()
                .unwrap()
                .with_work_dir_activity(Some(activity.clone())),
        );

        let (blocker_started_tx, blocker_started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = blocker_started_tx.send(());
            let _ = release_rx.recv_timeout(Duration::from_secs(8));
        });
        blocker_started_rx.await.unwrap();

        let key = TaskKey {
            job_id: "job-id".into(),
            stage_id: 1,
            task_id: 0,
        };
        // Normal case is a finite real physical plan. Abnormal cases keep
        // the existing never-ending R10 stream and prove retention, not false
        // completion, after their async parent has been cancelled.
        let mut receiver = if scenario == "normal" {
            let finite = ShuffleWriterExec::try_new(
                key.job_id.clone(),
                key.stage_id,
                Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                    Schema::empty(),
                ))),
                work_dir.clone(),
            )
            .unwrap();
            let stage =
                DefaultQueryStageExec::new(ShuffleWriterVariant::Passthrough(finite));
            let exec = executor.clone();
            let task_ctx = ctx.task_ctx();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let key = key.clone();
            tokio::spawn(async move {
                let result = exec
                    .execute_query_stage(key, Arc::new(stage), task_ctx)
                    .await;
                let _ = tx.send(result);
            });
            rx
        } else {
            spawn_never_ending_task(&executor, &ctx, &work_dir, key.clone())
        };
        let mut registered = false;
        let mut prepared = false;
        let mut writer_admitted = false;
        for _ in 0..150 {
            registered |= executor.active_task_count() == 1;
            prepared |= root.join("job-id").exists();
            // The test holds the sole blocking thread. These three leases
            // prove stage + detached coordinator + queued real IPC worker.
            writer_admitted |= activity.active_count() >= 3;
            if registered && prepared && writer_admitted {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let cancelled = if scenario == "normal" {
            false
        } else {
            executor
                .cancel_task(key.job_id.clone(), key.stage_id, key.task_id)
                .await
                .unwrap_or(false)
        };
        let cancelled_result = if scenario == "normal" {
            None
        } else {
            Some(tokio::time::timeout(Duration::from_secs(3), &mut receiver).await)
        };
        let premature_drain = tokio::time::timeout(
            Duration::from_millis(300),
            TasksDrainedFuture(executor.clone()),
        )
        .await
        .is_ok();
        let arrow_while_blocked = arrow_file_exists(root);
        let finish_while_blocked = directory_guard.as_mut().unwrap().finish();

        if scenario == "timeout" {
            let timed_out = tokio::time::timeout(
                Duration::from_millis(20),
                std::future::pending::<()>(),
            )
            .await;
            assert!(timed_out.is_err());
            drop(directory_guard.take());
        } else if scenario == "cancelled_owner" {
            let guard = directory_guard.take().unwrap();
            let owner = tokio::spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            owner.abort();
            assert!(owner.await.unwrap_err().is_cancelled());
        }

        // All assertions deferred until the blocking worker has been released,
        // preventing an assertion failure from leaving a blocked worker behind.
        let _ = release_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(3), blocker).await;
        let mut wrote_arrow = arrow_file_exists(root);
        for _ in 0..100 {
            if wrote_arrow {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            wrote_arrow = arrow_file_exists(root);
        }
        let normal_result = if scenario == "normal" {
            Some(tokio::time::timeout(Duration::from_secs(3), &mut receiver).await)
        } else {
            None
        };
        let fully_drained = tokio::time::timeout(
            Duration::from_millis(if scenario == "normal" { 3000 } else { 100 }),
            TasksDrainedFuture(executor.clone()),
        )
        .await
        .is_ok();
        let private_after_worker = fs::metadata(root)
            .map(|m| m.permissions().mode() & 0o777 == 0o700)
            .unwrap_or(false);

        // Normal cleanup is conditioned on the real writer lease releasing.
        let normal_cleanup = if scenario == "normal" {
            let finished = directory_guard.as_mut().unwrap().finish();
            drop(directory_guard.take());
            finished && !root.exists()
        } else {
            false
        };
        if scenario != "normal" {
            // Never delete a private directory while its coordinator continues.
            // The fixture is cleaned only after dropping the Tokio runtime.
            assert!(root.is_dir(), "abnormal shutdown removed active workdir");
        }

        assert!(registered, "stage never registered");
        assert!(prepared, "real ShuffleWriterExec never prepared a path");
        assert!(
            writer_admitted,
            "no proved live coordinator and queued real IPC writer"
        );
        if scenario == "normal" {
            assert!(
                matches!(normal_result, Some(Ok(Ok(Ok(_))))),
                "normal finite real stage did not complete"
            );
            assert!(
                fully_drained,
                "finite stage did not reach full real writer quiescence"
            );
            assert!(
                normal_cleanup,
                "normal shutdown failed to remove drained workdir"
            );
        } else {
            assert!(cancelled, "stage cancellation was not accepted");
            assert!(
                matches!(cancelled_result, Some(Ok(Ok(Err(_))))),
                "cancelled stage did not unwind"
            );
            assert!(
                !fully_drained,
                "infinite detached writer falsely reported completed after cancelled owner"
            );
        }
        assert!(
            !premature_drain,
            "F1 regression: false quiescence under blocked IPC"
        );
        assert!(
            !finish_while_blocked,
            "F1 regression: guard finished while writer active"
        );
        assert!(!arrow_while_blocked, "blocking worker started prematurely");
        assert!(wrote_arrow, "real IPC worker did not create an Arrow file");
        assert!(private_after_worker, "workdir lost 0700 protection");
        if scenario == "normal" {
            None
        } else {
            Some((work_dir, activity))
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_r11_real_shuffle_normal_quiescence_and_cleanup() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        assert!(
            rt.block_on(r11_actual_shuffle_writer_lifecycle("normal"))
                .is_none()
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_r11_real_shuffle_timeout_retains_private_directory() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let retained = rt
            .block_on(r11_actual_shuffle_writer_lifecycle("timeout"))
            .expect("abnormal shutdown must retain private workdir");
        drop(rt);
        // The executor/runtime is now shut down; the detached coordinator
        // has been cancelled and the blocking writer's channel is closed.
        assert!(
            retained.1.is_idle(),
            "writer still active after runtime shutdown"
        );
        std::fs::remove_dir_all(&retained.0).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn test_r11_real_shuffle_cancelled_owner_retains_private_directory() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let retained = rt
            .block_on(r11_actual_shuffle_writer_lifecycle("cancelled_owner"))
            .expect("abnormal shutdown must retain private workdir");
        drop(rt);
        // The executor/runtime is now shut down; the detached coordinator
        // has been cancelled and the blocking writer's channel is closed.
        assert!(
            retained.1.is_idle(),
            "writer still active after runtime shutdown"
        );
        std::fs::remove_dir_all(&retained.0).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_r11_queued_dedicated_task_blocks_directory_seal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let activity = ballista_core::work_dir_lifetime::WorkDirActivity::register(
            tmp.path().to_path_buf(),
        );
        let (raw_executor, _ctx) = never_ending_executor(tmp.path().to_str().unwrap());
        let executor = Arc::new(
            Arc::try_unwrap(raw_executor)
                .ok()
                .unwrap()
                .with_work_dir_activity(Some(activity.clone())),
        );
        let queued_work = executor.begin_queued_work().unwrap();
        let dedicated = crate::cpu_bound_executor::DedicatedExecutor::new("r11-queue", 1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let task = dedicated.spawn(async move {
            let _queued_work = queued_work;
            let _ = release_rx.await;
        });
        let premature = tokio::time::timeout(
            Duration::from_millis(100),
            TasksDrainedFuture(executor.clone()),
        )
        .await
        .is_ok();
        let premature_seal = activity.seal_if_idle();
        let _ = release_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        let drained =
            tokio::time::timeout(Duration::from_secs(3), TasksDrainedFuture(executor))
                .await
                .is_ok();
        dedicated.join();
        assert!(!premature, "queued task incorrectly considered drained");
        assert!(
            !premature_seal,
            "queued task failed to keep directory active"
        );
        assert!(drained, "queued task failed to release lease");
        assert!(
            activity.seal_if_idle(),
            "directory failed to seal after queue drained"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_r11_m1_reachable_late_shuffle_after_last_owner() {
        use crate::execution_engine::QueryStageExecutor;
        use std::os::unix::fs::PermissionsExt;
        use std::path::Path;
        let (root, mut guard) =
            crate::executor_process::resolve_executor_work_dir(None).unwrap();
        guard.as_mut().unwrap().arm();
        let activity = guard.as_ref().unwrap().activity();
        let weak = Arc::downgrade(&activity);
        let (_, ctx) = never_ending_executor(&root);
        // This is an actually executable real ShuffleWriter stage. It is
        // created before the owner can be sealed, then retained across guard drop.
        let stage = DefaultQueryStageExec::new(ShuffleWriterVariant::Passthrough(
            ShuffleWriterExec::try_new(
                "m1-real-job".into(),
                1,
                Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                    Schema::empty(),
                ))),
                root.clone(),
            )
            .unwrap(),
        ));
        assert!(
            !guard.as_mut().unwrap().finish(),
            "reachable real stage must stop private cleanup"
        );
        drop(guard);
        drop(activity);
        assert!(
            weak.upgrade().is_some(),
            "the real stage must retain the owner until it finishes"
        );
        assert!(Path::new(&root).exists(), "unsafe private root removal");
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let result =
            QueryStageExecutor::execute_query_stage(&stage, 0, ctx.task_ctx()).await;
        assert!(result.is_ok(), "real stage must execute: {result:?}");
        assert!(
            Path::new(&root).exists(),
            "real stage must not race with cleanup"
        );
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        drop(stage);
        assert!(
            weak.upgrade().is_none(),
            "last strong Arc must disappear after real producer release"
        );
        // No reachable producer now: an unrelated stale pathname is not proof
        // of executor admission and must not keep an unbounded tombstone.
        assert!(
            ballista_core::work_dir_lifetime::lease_for_writer(
                &Path::new(&root).join("m1-real-job/1/result.arrow"),
            )
            .unwrap()
            .is_none()
        );
        std::fs::remove_dir_all(&root).unwrap();
        assert!(
            !Path::new(&root).exists(),
            "explicit private residual cleanup failed"
        );

        // Configured caller-owned paths remain usable without private registry.
        let caller = tempfile::TempDir::new().unwrap();
        let (caller_root, owner) = crate::executor_process::resolve_executor_work_dir(
            Some(caller.path().to_str().unwrap().to_owned()),
        )
        .unwrap();
        assert!(owner.is_none());
        assert!(
            ballista_core::work_dir_lifetime::lease_for_writer(
                &Path::new(&caller_root).join("caller.arrow"),
            )
            .unwrap()
            .is_none()
        );
        std::fs::write(Path::new(&caller_root).join("caller.arrow"), b"caller").unwrap();
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_r11_m2_admission_between_drain_and_real_cleanup() {
        let (root, mut guard) =
            crate::executor_process::resolve_executor_work_dir(None).unwrap();
        guard.as_mut().unwrap().arm();
        let activity = guard.as_ref().unwrap().activity();
        let (raw, ctx) = never_ending_executor(&root);
        let executor = Arc::new(
            Arc::try_unwrap(raw)
                .ok()
                .unwrap()
                .with_work_dir_activity(Some(activity.clone())),
        );
        let drained_snapshot =
            futures::FutureExt::now_or_never(TasksDrainedFuture(executor.clone()))
                .is_some();
        assert!(
            drained_snapshot,
            "pre-admission drain snapshot must be ready"
        );
        let queued = executor.begin_queued_work().unwrap().unwrap();
        let key = TaskKey {
            job_id: "m2-real-job".into(),
            stage_id: 1,
            task_id: 0,
        };
        let stage = DefaultQueryStageExec::new(ShuffleWriterVariant::Passthrough(
            ShuffleWriterExec::try_new(
                key.job_id.clone(),
                key.stage_id,
                Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                    Schema::empty(),
                ))),
                root.clone(),
            )
            .unwrap(),
        ));
        let (release, admitted) = tokio::sync::oneshot::channel::<()>();
        let task_executor = executor.clone();
        let task_ctx = ctx.task_ctx();
        let real = tokio::spawn(async move {
            let _queued = queued;
            admitted.await.expect("release queued real task");
            task_executor
                .execute_query_stage(key, Arc::new(stage), task_ctx)
                .await
        });
        let sentinel = std::path::Path::new(&root).join("m2-real-job/1/existing.arrow");
        std::fs::create_dir_all(sentinel.parent().unwrap()).unwrap();
        std::fs::write(&sentinel, b"written-before-cleanup").unwrap();
        assert!(
            activity.active_count() >= 2,
            "real queued task and stage must both own leases"
        );
        let cleaned_during_admission =
            crate::executor_process::r11_m2_real_cleanup_test_only(
                &root, &executor, &mut guard,
            )
            .await
            .unwrap();
        assert!(
            !cleaned_during_admission,
            "active admission must reject cleanup"
        );
        let lost_during_active_admission = !sentinel.exists();
        let _ = release.send(());
        let completed = tokio::time::timeout(Duration::from_secs(5), real)
            .await
            .expect("stage completion")
            .expect("join");
        let stage_ok = completed.is_ok();
        assert!(stage_ok, "a real ShuffleWriter stage must complete");
        assert!(
            !lost_during_active_admission,
            "active real admission must protect existing shuffle data"
        );
        assert!(
            crate::executor_process::r11_m2_real_cleanup_test_only(
                &root, &executor, &mut guard,
            )
            .await
            .unwrap(),
            "cleaner must run after the final lease releases"
        );
        assert!(
            executor.begin_queued_work().is_err(),
            "no further real task admission is allowed after final seal"
        );
        drop(guard);
        assert!(
            !std::path::Path::new(&root).exists(),
            "sealed private root must be removed"
        );
    }

    // Exact counterexample RED #41819433053, now asserted safe on Core itself.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_r11_m1_direct_core_writer_retains_private_owner_and_clones() {
        use futures::TryStreamExt;
        use std::os::unix::fs::PermissionsExt;
        use std::path::Path;

        let (root, mut guard) =
            crate::executor_process::resolve_executor_work_dir(None).unwrap();
        guard.as_mut().unwrap().arm();
        let activity = guard.as_ref().unwrap().activity();
        let weak = Arc::downgrade(&activity);
        let (_, ctx) = never_ending_executor(&root);
        // Construct the public Core plan WITHOUT DefaultQueryStageExec.
        let writer = ShuffleWriterExec::try_new(
            "m1-direct-core".into(),
            1,
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                Schema::empty(),
            ))),
            root.clone(),
        )
        .unwrap();
        let retained = writer.clone();
        drop(writer);
        assert!(
            !guard.as_mut().unwrap().finish(),
            "unwrapped retained core writer must block private deletion"
        );
        drop(guard);
        drop(activity);
        assert!(
            weak.upgrade().is_some(),
            "retained public core writer must retain its final owner"
        );
        assert!(Path::new(&root).exists(), "private root disappeared");
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );

        let mut stream = retained
            .execute(0, ctx.task_ctx())
            .expect("public ExecutionPlan execute");
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(_) = stream.try_next().await.expect("direct writer stream") {}
        })
        .await
        .expect("bounded direct-core writer completion");
        drop(stream);
        assert!(
            Path::new(&root).exists(),
            "core writer removed private root"
        );
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        drop(retained);
        assert!(
            weak.upgrade().is_none(),
            "final owner must expire after final public plan clone releases"
        );
        std::fs::remove_dir_all(&root).unwrap();
        assert!(
            !Path::new(&root).exists(),
            "explicit residual cleanup failed"
        );

        // Caller-supplied work directories remain unregistered and usable.
        let caller = tempfile::TempDir::new().unwrap();
        let (caller_root, owner) = crate::executor_process::resolve_executor_work_dir(
            Some(caller.path().to_str().unwrap().to_string()),
        )
        .unwrap();
        assert!(owner.is_none());
        let caller_writer = ShuffleWriterExec::try_new(
            "m1-caller-owned".into(),
            1,
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                Schema::empty(),
            ))),
            caller_root.clone(),
        )
        .unwrap();
        let mut caller_stream = caller_writer
            .execute(0, ctx.task_ctx())
            .expect("caller-owned writer execute");
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(_) =
                caller_stream.try_next().await.expect("caller-owned stream")
            {}
        })
        .await
        .expect("bounded caller-owned execution");
        assert!(
            Path::new(&caller_root).exists(),
            "caller-owned root deleted"
        );
    }

    // M1: both other public Core writer variants must retain private roots
    // across manual Clone and loss of the executor guard.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_r11_m1_range_sort_direct_core_clones_retain_owner() {
        use ballista_core::execution_plans::sort_shuffle::SortShuffleConfig;
        use ballista_core::execution_plans::{
            RangeShuffleWriterExec, SortShuffleWriterExec,
        };
        use datafusion::arrow::datatypes::{DataType, Field};
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::sorts::sort::SortExec;
        use datafusion::physical_plan::{ExecutionPlan, Partitioning};
        use std::os::unix::fs::PermissionsExt;

        let (root, mut guard) =
            crate::executor_process::resolve_executor_work_dir(None).unwrap();
        guard.as_mut().unwrap().arm();
        let activity = guard.as_ref().unwrap().activity();
        let weak = Arc::downgrade(&activity);

        let schema =
            Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("key", 0),
        ))])
        .unwrap();
        let ordered: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering, input.clone()));

        let range = RangeShuffleWriterExec::try_new(
            "m1-range-core".into(),
            1,
            ordered,
            root.clone(),
        )
        .unwrap();
        let sort = SortShuffleWriterExec::try_new(
            "m1-sort-core".into(),
            1,
            input,
            root.clone(),
            Partitioning::Hash(vec![Arc::new(Column::new("key", 0))], 1),
            SortShuffleConfig::default(),
        )
        .unwrap();

        let retained_range = range.clone();
        let retained_sort = sort.clone();
        drop(range);
        drop(sort);
        assert_eq!(
            activity.active_count(),
            2,
            "Range and Sort must each acquire one lifetime lease at construction"
        );
        assert!(
            !guard.as_mut().unwrap().finish(),
            "two retained core variants must prevent root cleanup"
        );
        drop(guard);
        drop(activity);
        assert!(
            weak.upgrade().is_some(),
            "cloned Range and Sort must survive loss of last external owner"
        );
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        drop(retained_range);
        assert!(
            weak.upgrade().is_some(),
            "Sort clone must still retain private-root registration"
        );
        drop(retained_sort);
        assert!(
            weak.upgrade().is_none(),
            "no unbounded retained ownership after both final clones release"
        );
        std::fs::remove_dir_all(&root).unwrap();
    }
}
