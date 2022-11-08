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
    Ok(Arc::new(PrometheusMetricsCollector::new(
        ::prometheus::default_registry(),
    )?))
}

#[cfg(not(feature = "prometheus"))]
pub fn default_metrics_collector() -> Result<Arc<dyn SchedulerMetricsCollector>> {
    Ok(Arc::new(NoopMetricsCollector::default()))
}
