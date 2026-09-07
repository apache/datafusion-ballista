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

use crate::cluster::affinity::LocalityStats;
use crate::metrics::SchedulerMetricsCollector;
use ballista_core::JobId;
use ballista_core::error::{BallistaError, Result};

use once_cell::sync::OnceCell;
use prometheus::{
    Counter, Gauge, Histogram, IntCounter, IntGauge, Registry,
    register_counter_with_registry, register_gauge_with_registry,
    register_histogram_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry,
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
///
/// The `shuffle-affinity` task distribution adds the *shuffle_locality_\**
/// counters, plus *shuffle_locality_imputed_bytes* saying whether the byte
/// counts can be trusted. The share of input read without a network hop is
/// `shuffle_locality_local_bytes_total / shuffle_locality_input_bytes_total`.
pub struct PrometheusMetricsCollector {
    execution_time: Histogram,
    planning_time: Histogram,
    failed: Counter,
    cancelled: Counter,
    completed: Counter,
    submitted: Counter,
    pending_queue_size: Gauge,
    locality_tasks: IntCounter,
    locality_partitions: IntCounter,
    locality_local_partitions: IntCounter,
    locality_input_bytes: IntCounter,
    locality_local_bytes: IntCounter,
    locality_imputed_bytes: IntGauge,
}

impl PrometheusMetricsCollector {
    /// Creates a new PrometheusMetricsCollector instance.
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

        let locality_tasks = register_int_counter_with_registry!(
            "shuffle_locality_tasks_total",
            "Counter of tasks bound by the shuffle-affinity task distribution",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let locality_partitions = register_int_counter_with_registry!(
            "shuffle_locality_partitions_total",
            "Counter of input partitions covered by shuffle-affinity bound tasks",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let locality_local_partitions = register_int_counter_with_registry!(
            "shuffle_locality_local_partitions_total",
            "Counter of input partitions bound to an executor holding some of their input",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let locality_input_bytes = register_int_counter_with_registry!(
            "shuffle_locality_input_bytes_total",
            "Counter of shuffle input bytes bound tasks will read, local and remote",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let locality_local_bytes = register_int_counter_with_registry!(
            "shuffle_locality_local_bytes_total",
            "Counter of shuffle input bytes bound tasks will read from the executor running them",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {e:?}"))
        })?;

        let locality_imputed_bytes = register_int_gauge_with_registry!(
            "shuffle_locality_imputed_bytes",
            "1 once any producer reported no size, leaving the byte counters padded with placeholders and the local share untrustworthy",
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
            locality_tasks,
            locality_partitions,
            locality_local_partitions,
            locality_input_bytes,
            locality_local_bytes,
            locality_imputed_bytes,
        })
    }

    /// Returns the current global prometheus collector.
    pub fn current() -> Result<Arc<dyn SchedulerMetricsCollector>> {
        COLLECTOR
            .get_or_try_init(|| {
                let collector = Self::new(::prometheus::default_registry())?;

                Ok(Arc::new(collector) as Arc<dyn SchedulerMetricsCollector>)
            })
            .cloned()
    }
}

impl SchedulerMetricsCollector for PrometheusMetricsCollector {
    fn record_submitted(&self, _job_id: &JobId, queued_at: u64, submitted_at: u64) {
        self.submitted.inc();
        self.planning_time
            .observe((submitted_at - queued_at) as f64);
    }

    fn record_completed(&self, _job_id: &JobId, queued_at: u64, completed_at: u64) {
        self.completed.inc();
        self.execution_time
            .observe((completed_at - queued_at) as f64 / 1000_f64)
    }

    fn record_failed(&self, _job_id: &JobId, _queued_at: u64, _failed_at: u64) {
        self.failed.inc()
    }

    fn record_cancelled(&self, _job_id: &JobId) {
        self.cancelled.inc();
    }

    fn set_pending_tasks_queue_size(&self, value: u64) {
        self.pending_queue_size.set(value as f64);
    }

    fn record_shuffle_locality(&self, round: &LocalityStats) {
        self.locality_tasks.inc_by(round.tasks);
        self.locality_partitions.inc_by(round.partitions);
        self.locality_local_partitions
            .inc_by(round.local_partitions);
        self.locality_input_bytes.inc_by(round.total_bytes);
        self.locality_local_bytes.inc_by(round.local_bytes);
        if round.imputed_bytes {
            // Sticky, like `LocalityStats::imputed_bytes`.
            self.locality_imputed_bytes.set(1);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Registered on a private registry so the test does not race the
    /// process-wide default one.
    #[test]
    fn shuffle_locality_rounds_accumulate_into_the_counters() {
        let registry = Registry::new();
        let collector = PrometheusMetricsCollector::new(&registry).unwrap();

        collector.record_shuffle_locality(&LocalityStats {
            tasks: 2,
            partitions: 8,
            local_partitions: 6,
            local_bytes: 900,
            total_bytes: 1_000,
            imputed_bytes: false,
        });
        collector.record_shuffle_locality(&LocalityStats {
            tasks: 1,
            partitions: 4,
            local_partitions: 0,
            local_bytes: 0,
            total_bytes: 500,
            imputed_bytes: false,
        });

        // Deltas add up, so the local share is a ratio of two counters.
        assert_eq!(3, collector.locality_tasks.get());
        assert_eq!(12, collector.locality_partitions.get());
        assert_eq!(6, collector.locality_local_partitions.get());
        assert_eq!(900, collector.locality_local_bytes.get());
        assert_eq!(1_500, collector.locality_input_bytes.get());
        assert_eq!(0, collector.locality_imputed_bytes.get());

        // One unsized producer flags every later ratio, and stays flagged.
        collector.record_shuffle_locality(&LocalityStats {
            tasks: 1,
            imputed_bytes: true,
            ..Default::default()
        });
        assert_eq!(1, collector.locality_imputed_bytes.get());
        collector.record_shuffle_locality(&LocalityStats {
            tasks: 1,
            imputed_bytes: false,
            ..Default::default()
        });
        assert_eq!(1, collector.locality_imputed_bytes.get());
    }

    /// A registered metric that never reaches the exposition format is
    /// invisible to the scraper, whatever its value.
    #[test]
    fn the_locality_counters_are_exported() {
        let registry = Registry::new();
        let collector = PrometheusMetricsCollector::new(&registry).unwrap();
        collector.record_shuffle_locality(&LocalityStats {
            tasks: 1,
            partitions: 1,
            local_partitions: 1,
            local_bytes: 42,
            total_bytes: 42,
            imputed_bytes: false,
        });

        let mut buffer = vec![];
        TextEncoder::new()
            .encode(&registry.gather(), &mut buffer)
            .unwrap();
        let exposed = String::from_utf8(buffer).unwrap();

        for metric in [
            "shuffle_locality_tasks_total",
            "shuffle_locality_partitions_total",
            "shuffle_locality_local_partitions_total",
            "shuffle_locality_input_bytes_total",
            "shuffle_locality_local_bytes_total",
            "shuffle_locality_imputed_bytes",
        ] {
            assert!(exposed.contains(metric), "{metric} was not exported");
        }
        assert!(exposed.contains("shuffle_locality_local_bytes_total 42"));
    }
}
