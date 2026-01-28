use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct SchedulerState {
    pub started: i64,
    pub version: String,
}

#[derive(bon::Builder, Clone, Debug)]
pub struct DashboardData {
    pub scheduler_state: Option<SchedulerState>,
}

impl DashboardData {
    pub fn with_scheduler_state(
        mut self,
        scheduler_state: Option<SchedulerState>,
    ) -> Self {
        self.scheduler_state = scheduler_state;
        self
    }
}
