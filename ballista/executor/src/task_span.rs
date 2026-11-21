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

//! Tags the log lines an executor writes while it runs a task with the job,
//! stage and task they belong to, through a `tracing` span.
//!
//! The span only shows up where a `tracing` subscriber formats the logs, as
//! the executor binary's does. A process that logs through a plain `log`
//! logger sees no difference.

use ballista_core::serde::protobuf;
use ballista_core::serde::scheduler::TaskDefinition;
use datafusion::common::runtime::{
    JoinSetTracer, JoinSetTracerError, set_join_set_tracer,
};
use futures::future::BoxFuture;
use std::any::Any;
use tracing::subscriber::NoSubscriber;
use tracing::{Instrument, Span};

/// Span a task the executor polled from the scheduler runs in.
pub(crate) fn polled_task_span(task: &protobuf::TaskDefinition) -> Span {
    task_span(
        &task.job_id,
        task.stage_id.into(),
        task.stage_attempt_num.into(),
        task.task_id.into(),
        task.task_attempt_num.into(),
    )
}

/// Span a task the scheduler pushed to the executor runs in.
pub(crate) fn pushed_task_span(task: &TaskDefinition) -> Span {
    task_span(
        task.job_id.as_str(),
        task.stage_id as u64,
        task.stage_attempt_num as u64,
        task.task_id as u64,
        task.task_attempt_num as u64,
    )
}

/// It is created at error level so that a subscriber that only lets warnings
/// and errors through still tags them. Without a subscriber there is no span:
/// `tracing` would otherwise write it to the `log` logger as an error line.
fn task_span(
    job_id: &str,
    stage_id: u64,
    stage_attempt: u64,
    task_id: u64,
    task_attempt: u64,
) -> Span {
    if tracing::dispatcher::get_default(|dispatch| dispatch.is::<NoSubscriber>()) {
        return Span::none();
    }
    tracing::error_span!(
        "ballista_task",
        %job_id,
        stage_id,
        stage_attempt,
        task_id,
        task_attempt
    )
}

/// Runs spawned work in the span of the code that spawned it.
struct SpanTracer;

impl JoinSetTracer for SpanTracer {
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        Box::pin(fut.in_current_span())
    }

    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        let span = Span::current();
        Box::new(move || span.in_scope(f))
    }
}

/// Installs a process-wide DataFusion tracer so that the work DataFusion and
/// Ballista spawn while a task runs logs with that task's ids as well.
///
/// A task hands most of its work to other tokio tasks and blocking threads,
/// which would otherwise log outside its span. DataFusion takes one tracer
/// per process, so this fails if the process has already set one. The
/// executor binary calls it at startup.
pub fn trace_spawned_tasks() -> Result<(), JoinSetTracerError> {
    set_join_set_tracer(&SpanTracer)
}

#[cfg(test)]
mod tests {
    use super::task_span;
    use tracing::subscriber::NoSubscriber;

    #[test]
    fn no_span_without_a_subscriber() {
        let span = tracing::subscriber::with_default(NoSubscriber::default(), || {
            task_span("job", 2, 1, 3, 4)
        });
        assert!(span.is_none());
    }
}
