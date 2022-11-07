use crate::metrics::SchedulerMetricsCollector;
use ballista_core::error::{BallistaError, Result};
use hyper::header::CONTENT_TYPE;
use prometheus::{
    default_registry, register_counter_with_registry, register_gauge_with_registry,
    register_histogram_with_registry, Counter, Gauge, Histogram, Registry,
};
use prometheus::{Encoder, TextEncoder};
use warp::reply::WithHeader;
use warp::Reply;

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
            "query_time_seconds",
            "Histogram of query execution time in seconds",
            vec![0.5_f64, 1_f64, 5_f64, 30_f64, 60_f64],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {:?}", e))
        })?;

        let planning_time = register_histogram_with_registry!(
            "planning_time_ms",
            "Histogram of query planning time in milliseconds",
            vec![1.0_f64, 5.0_f64, 25.0_f64, 100.0_f64, 500.0_f64],
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {:?}", e))
        })?;

        let failed = register_counter_with_registry!(
            "query_failed_total",
            "Counter of failed queries",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {:?}", e))
        })?;

        let cancelled = register_counter_with_registry!(
            "query_cancelled_total",
            "Counter of cancelled queries",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {:?}", e))
        })?;

        let completed = register_counter_with_registry!(
            "query_completed_total",
            "Counter of completed queries",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {:?}", e))
        })?;

        let submitted = register_counter_with_registry!(
            "query_submitted_total",
            "Counter of submitted queries",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {:?}", e))
        })?;

        let pending_queue_size = register_gauge_with_registry!(
            "pending_queue_size",
            "Number of pending tasks",
            registry
        )
        .map_err(|e| {
            BallistaError::Internal(format!("Error registering metric: {:?}", e))
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
}

impl SchedulerMetricsCollector for PrometheusMetricsCollector {
    fn record_submitted(&self, _job_id: &str, queued_at: u64, submitted_at: u64) {
        self.submitted.inc();
        self.planning_time
            .observe((submitted_at - queued_at) as f64 / 1000_f64);
    }

    fn record_completed(&self, _job_id: &str, queued_at: u64, completed_at: u64) {
        self.completed.inc();
        self.execution_time
            .observe((completed_at - queued_at) as f64)
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
}

pub fn get_metrics() -> Result<impl Reply> {
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).map_err(|e| {
        BallistaError::Internal(format!("Error encoding prometheus metrics: {:?}", e))
    })?;

    Ok(warp::reply::with_header(
        buffer,
        CONTENT_TYPE,
        encoder.format_type(),
    ))
}
