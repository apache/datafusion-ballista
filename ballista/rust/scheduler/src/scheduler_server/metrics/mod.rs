pub trait SchedulerMetricsCollector: Send + Sync {
    fn record_submitted(&self, job_id: &str, queued_at: u64, submitted_at: u64);
    fn record_completed(&self, job_id: &str, queued_at: u64, completed_at: u64);
    fn record_failed(&self, job_id: &str, queued_at: u64, failed_at: u64);
}

#[derive(Default)]
pub struct NoopMetricsCollector {}

impl SchedulerMetricsCollector for NoopMetricsCollector {
    fn record_submitted(&self, _job_id: &str, _queued_at: u64, _submitted_at: u64) {}
    fn record_completed(&self, _job_id: &str, _queued_at: u64, _completed_att: u64) {}
    fn record_failed(&self, _job_id: &str, _queued_at: u64, _failed_at: u64) {}
}
