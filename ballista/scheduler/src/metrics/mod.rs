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

#[cfg(feature = "prometheus")]
pub mod prometheus;

#[cfg(feature = "prometheus")]
use crate::metrics::prometheus::PrometheusMetricsCollector;
use ballista_core::error::Result;
use std::sync::Arc;

/// Interface for recording metrics events in the scheduler. An instance of `Arc<dyn SchedulerMetricsCollector>`
/// will be passed when constructing the `QueryStageScheduler` which is the core event loop of the scheduler.
/// The event loop will then record metric events through this trait.
pub trait SchedulerMetricsCollector: Send + Sync {
    /// Record that job with `job_id` was submitted. This will be invoked
    /// after the job's `ExecutionGraph` is created and it is ready to be scheduled
    /// on executors.
    /// When invoked should specify the timestamp in milliseconds when the job was originally
    /// queued and the timestamp in milliseconds when it was submitted
    fn record_submitted(&self, job_id: &str, queued_at: u64, submitted_at: u64);

    /// Record that job with `job_id` has completed successfully. This should only
    /// be invoked on successful job completion.
    /// When invoked should specify the timestamp in milliseconds when the job was originally
    /// queued and the timestamp in milliseconds when it was completed
    fn record_completed(&self, job_id: &str, queued_at: u64, completed_at: u64);

    /// Record that job with `job_id` has failed.
    /// When invoked should specify the timestamp in milliseconds when the job was originally
    /// queued and the timestamp in milliseconds when it failed.
    fn record_failed(&self, job_id: &str, queued_at: u64, failed_at: u64);

    /// Record that job with `job_id` was cancelled.
    fn record_cancelled(&self, job_id: &str);

    /// Set the current number of pending tasks in scheduler. A pending task is a task that is available
    /// to schedule on an executor but cannot be scheduled because no resources are available.
    fn set_pending_tasks_queue_size(&self, value: u64);

    /// Gather current metric set that should be returned when calling the scheduler's metrics API
    /// Should return a tuple containing the content of the metric set and the content type (e.g. `application/json`, `text/plain`, etc)
    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>>;
}

/// Implementation of `SchedulerMetricsCollector` that ignores all events. This can be used as
/// a default implementation when tracking scheduler metrics is not required (or performed through other means)
#[derive(Default)]
pub struct NoopMetricsCollector {}

impl SchedulerMetricsCollector for NoopMetricsCollector {
    fn record_submitted(&self, _job_id: &str, _queued_at: u64, _submitted_at: u64) {}
    fn record_completed(&self, _job_id: &str, _queued_at: u64, _completed_att: u64) {}
    fn record_failed(&self, _job_id: &str, _queued_at: u64, _failed_at: u64) {}
    fn record_cancelled(&self, _job_id: &str) {}
    fn set_pending_tasks_queue_size(&self, _value: u64) {}

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        Ok(None)
    }
}

/// Return a reference to the systems default metrics collector.
#[cfg(feature = "prometheus")]
pub fn default_metrics_collector() -> Result<Arc<dyn SchedulerMetricsCollector>> {
    PrometheusMetricsCollector::current()
}

#[cfg(not(feature = "prometheus"))]
pub fn default_metrics_collector() -> Result<Arc<dyn SchedulerMetricsCollector>> {
    Ok(Arc::new(NoopMetricsCollector::default()))
}
