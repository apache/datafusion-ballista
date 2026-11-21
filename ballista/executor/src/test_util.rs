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

//! Fixtures shared by the unit tests of the executor.

use std::fmt::{self, Display};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::protobuf::{ExecutorRegistration, ShuffleWritePartition};
use ballista_core::serde::scheduler::ExecutorSpecification;
use ballista_core::utils::default_config_producer;
use ballista_core::{JobId, RuntimeProducer};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::prelude::SessionConfig;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::execution_engine::{ExecutionEngine, QueryStageExecutor};
use crate::executor::Executor;
use crate::metrics::LoggingMetricsCollector;
use crate::task_span::trace_spawned_tasks;

/// How long to wait for something a test expects to happen. Only reached
/// when it doesn't.
pub(crate) const TIMEOUT: Duration = Duration::from_secs(10);

/// A runtime producer that builds a new default `RuntimeEnv` on every call.
pub(crate) fn fresh_runtime_producer() -> RuntimeProducer {
    Arc::new(|_| Ok(Arc::new(RuntimeEnv::default())))
}

/// An executor with id `executor` and the default config producer, running
/// up to `concurrent_tasks` tasks at once.
pub(crate) fn test_executor(
    work_dir: &str,
    runtime_producer: RuntimeProducer,
    concurrent_tasks: usize,
) -> Executor {
    Executor::new_basic(
        ExecutorRegistration {
            id: "executor".to_string(),
            ..Default::default()
        },
        work_dir,
        runtime_producer,
        Arc::new(default_config_producer),
        concurrent_tasks,
    )
}

/// Execution engine whose tasks announce that they started and then run
/// until released, so a test can look at the executor mid-task. It is its
/// own query stage: the tasks it creates share its two signals.
#[derive(Clone, Debug, Default)]
pub(crate) struct BlockingEngine {
    pub(crate) started: Arc<Notify>,
    pub(crate) release: Arc<Notify>,
}

impl ExecutionEngine for BlockingEngine {
    fn create_query_stage_exec(
        &self,
        _job_id: JobId,
        _stage_id: usize,
        _task_id: usize,
        _global_output_partition_ids: Vec<usize>,
        _plan: Arc<dyn ExecutionPlan>,
        _work_dir: &str,
        _config: &SessionConfig,
    ) -> Result<Arc<dyn QueryStageExecutor>> {
        Ok(Arc::new(self.clone()))
    }
}

impl Display for BlockingEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockingEngine")
    }
}

#[async_trait::async_trait]
impl QueryStageExecutor for BlockingEngine {
    async fn execute_query_stage(
        &self,
        _task_id: usize,
        _context: Arc<TaskContext>,
    ) -> Result<Vec<ShuffleWritePartition>> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(vec![])
    }

    fn collect_plan_metrics(&self) -> Vec<MetricsSet> {
        vec![]
    }
}

/// An idle executor with `vcores` vcores whose tasks run on `engine`.
pub(crate) fn executor_with_engine(
    engine: &BlockingEngine,
    vcores: usize,
) -> Arc<Executor> {
    Arc::new(Executor::new(
        ExecutorRegistration {
            id: "executor".to_string(),
            specification: Some(
                ExecutorSpecification::default()
                    .with_vcores(vcores as u32)
                    .into(),
            ),
            ..Default::default()
        },
        "",
        fresh_runtime_producer(),
        Arc::new(default_config_producer),
        Arc::new(BallistaFunctionRegistry::default()),
        Arc::new(LoggingMetricsCollector::default()),
        vcores,
        Arc::new(engine.clone()),
    ))
}

/// Lines logged at info level or above since the first [`captured_logs`]
/// call, by any test of this binary, as the `tracing` formatter prints them.
#[derive(Clone, Default)]
pub(crate) struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    /// The log lines captured so far.
    pub(crate) fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }

    /// Whether a line holding `message` was logged inside `span`, given as
    /// the formatter prints it.
    pub(crate) fn has_line_in_span(&self, span: &str, message: &str) -> bool {
        self.contents()
            .lines()
            .any(|line| line.contains(span) && line.contains(message))
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Installs the formatter on first use, which also routes `log` records to
/// it, and makes spawned work inherit the span of its spawner.
pub(crate) fn captured_logs() -> CapturedLogs {
    static LOGS: OnceLock<CapturedLogs> = OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = CapturedLogs::default();
        let writer = logs.clone();
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .init();
        trace_spawned_tasks().expect("no other test sets a tracer");
        logs
    })
    .clone()
}
