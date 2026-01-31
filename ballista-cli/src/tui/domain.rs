use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct SchedulerState {
    pub started: i64,
    pub version: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ExecutorsData {
    pub host: String,
    pub port: u16,
    pub id: String,
    pub last_seen: i64,
}

#[derive(bon::Builder, Clone, Debug)]
pub struct DashboardData {
    pub scheduler_state: Option<SchedulerState>,
    pub executors_data: Option<Vec<ExecutorsData>>,
}

impl DashboardData {
    pub fn with_scheduler_state(
        mut self,
        scheduler_state: Option<SchedulerState>,
    ) -> Self {
        self.scheduler_state = scheduler_state;
        self
    }

    pub fn with_executors_data(
        mut self,
        executors_data: Option<Vec<ExecutorsData>>,
    ) -> Self {
        self.executors_data = executors_data;
        self
    }
}
