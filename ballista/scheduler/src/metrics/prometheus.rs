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

use crate::metrics::SchedulerMetricsCollector;
use ballista_core::error::{BallistaError, Result};

use once_cell::sync::OnceCell;
use prometheus::{
    register_counter_with_registry, register_gauge_with_registry,
    register_histogram_with_registry, Counter, Gauge, Histogram, Registry,
};
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;

static COLLECTOR: OnceCell<Arc<dyn SchedulerMetricsCollector>> = OnceCell::new();

/// SchedulerMetricsCollector implementation based on Prometheus. By default this will track
/// 7 metrics:
/// *job_exec_time_seconds* - Histogram of successful job execution time in seconds
/// *planning_time_ms* - Histogram of job planning time in milliseconds
/// *failed* - Counter of failed jobs
/// *job_failed_total* - Counter of failed jobs
/// *job_cancelled_total* - Counter of cancelled jobs
/// *job_completed_total* - Counter of completed jobs
/// *job_submitted_total* - Counter of submitted jobs
/// *pending_task_queue_size* - Number of pending tasks
pub struct PrometheusMetricsCollector {
    execution_time: Histogram,
    planning_time: Histogram,
    failed: Counter,
    cancelled: Counter,
    completed: Counter,
    submitted: Counter,
    pending_queue_size: Gauge,
}

impl PrometheusMetricsCollector {
    pub fn new(registry: &Registry) -> Result<Self> {
        let execution_time = register_histogram_with_registry!(
            "job_exec_time_seconds",
            "Histogram of successful job execution time in seconds",
            vec![0.5_f64, 1_f64, 5_f64, 30_f64, 60_f64],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let planning_time = register_histogram_with_registry!(
            "planning_time_ms",
            "Histogram of job planning time in milliseconds",
            vec![1.0_f64, 5.0_f64, 25.0_f64, 100.0_f64, 500.0_f64],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let failed = register_counter_with_registry!(
            "job_failed_total",
            "Counter of failed jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let cancelled = register_counter_with_registry!(
            "job_cancelled_total",
            "Counter of cancelled jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let completed = register_counter_with_registry!(
            "job_completed_total",
            "Counter of completed jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let submitted = register_counter_with_registry!(
            "job_submitted_total",
            "Counter of submitted jobs",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let pending_queue_size = register_gauge_with_registry!(
            "pending_task_queue_size",
            "Number of pending tasks",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        Ok(Self {
            execution_time,
            planning_time,
            failed,
            cancelled,
            completed,
            submitted,
            pending_queue_size,
        })
    }

    pub fn current() -> Result<Arc<dyn SchedulerMetricsCollector>> {
        COLLECTOR
            .get_or_try_init(|| {
                let collector = Self::new(::prometheus::default_registry())?;

                Ok(Arc::new(collector) as Arc<dyn SchedulerMetricsCollector>)
            })
            .map(|arc| arc.clone())
    }
}

impl SchedulerMetricsCollector for PrometheusMetricsCollector {
    fn record_submitted(&self, _job_id: &str, queued_at: u64, submitted_at: u64) {
        self.submitted.inc();
        self.planning_time
            .observe((submitted_at - queued_at) as f64);
    }

    fn record_completed(&self, _job_id: &str, queued_at: u64, completed_at: u64) {
        self.completed.inc();
        self.execution_time
            .observe((completed_at - queued_at) as f64 / 1000_f64)
    }

    fn record_failed(&self, _job_id: &str, _queued_at: u64, _failed_at: u64) {
        self.failed.inc()
    }

    fn record_cancelled(&self, _job_id: &str) {
        self.cancelled.inc();
    }

    fn set_pending_tasks_queue_size(&self, value: u64) {
        self.pending_queue_size.set(value as f64);
    }

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        let encoder = TextEncoder::new();

        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).map_err(|e| {
            BallistaError::Internal(format!("Error encoding prometheus metrics: {e:?}"))
        })?;

        Ok(Some((buffer, encoder.format_type().to_owned())))
    }
}
