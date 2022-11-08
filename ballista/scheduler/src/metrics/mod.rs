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

use crate::metrics::prometheus::PrometheusMetricsCollector;
use ballista_core::error::Result;
use std::sync::Arc;

pub trait SchedulerMetricsCollector: Send + Sync {
    fn record_submitted(&self, job_id: &str, queued_at: u64, submitted_at: u64);
    fn record_completed(&self, job_id: &str, queued_at: u64, completed_at: u64);
    fn record_failed(&self, job_id: &str, queued_at: u64, failed_at: u64);
    fn record_cancelled(&self, job_id: &str);
    fn set_pending_tasks_queue_size(&self, value: u64);
}

#[derive(Default)]
pub struct NoopMetricsCollector {}

impl SchedulerMetricsCollector for NoopMetricsCollector {
    fn record_submitted(&self, _job_id: &str, _queued_at: u64, _submitted_at: u64) {}
    fn record_completed(&self, _job_id: &str, _queued_at: u64, _completed_att: u64) {}
    fn record_failed(&self, _job_id: &str, _queued_at: u64, _failed_at: u64) {}
    fn record_cancelled(&self, _job_id: &str) {}
    fn set_pending_tasks_queue_size(&self, _value: u64) {}
}

#[cfg(feature = "prometheus")]
pub fn default_metrics_collector() -> Result<Arc<dyn SchedulerMetricsCollector>> {
    PrometheusMetricsCollector::current()
}

#[cfg(not(feature = "prometheus"))]
pub fn default_metrics_collector() -> Result<Arc<dyn SchedulerMetricsCollector>> {
    Ok(Arc::new(NoopMetricsCollector::default()))
}
