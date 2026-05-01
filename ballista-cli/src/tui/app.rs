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

use crate::tui::TuiResult;
use crate::tui::{
    TuiError,
    domain::{
        SortOrder,
        executors::{ExecutorsData, SortColumn as ExecutorsSortColumn},
        jobs::{
            CancelJobResult, JobDetails, JobsData, SortColumn as JobsSortColumn,
            stages::{JobStagesPopup, StagesGraph},
        },
        metrics::MetricsData,
        metrics::SortColumn as MetricsSortColumn,
    },
    event::Event,
    infrastructure::Settings,
};
use crossterm::event::{KeyCode, KeyEvent};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use crate::tui::http_client::HttpClient;
use crate::tui::ui::{
    load_executors_data, load_job_details, load_job_dot, load_job_stages_popup,
    load_jobs_data, load_metrics_data,
};

#[derive(Debug, PartialEq)]
enum Views {
    Executors,
    Jobs,
    Metrics,
}

#[derive(Debug, PartialEq)]
enum InputMode {
    View,
    Edit,
}

#[derive(Debug, PartialEq)]
pub(crate) enum PlanTab {
    Stage,
    Physical,
    Logical,
}

pub(crate) struct App {
    should_quit: bool,

    event_tx: Option<Sender<Event>>,

    current_view: Views,
    input_mode: InputMode,

    pub jobs_data: JobsData,
    pub executors_data: ExecutorsData,
    pub metrics_data: MetricsData,

    // Popups
    pub show_help: bool,
    pub show_scheduler_info: bool,

    pub cancel_job_result: Option<CancelJobResult>,

    pub search_term: String,

    pub job_details: Option<JobDetails>,

    pub job_dot_popup: Option<StagesGraph>,

    pub job_plan_popup: Option<(JobDetails, PlanTab)>,
    pub job_plan_popup_scroll: u16,

    pub job_stages_popup: Option<JobStagesPopup>,

    pub http_client: Arc<HttpClient>,
}

impl App {
    pub fn new(config: Settings) -> TuiResult<Self> {
        Ok(Self {
            should_quit: false,
            event_tx: None,
            current_view: Views::Jobs,
            input_mode: InputMode::View,
            search_term: String::new(),
            show_help: false,
            show_scheduler_info: false,
            cancel_job_result: None,
            job_details: None,
            job_dot_popup: None,
            job_plan_popup: None,
            job_plan_popup_scroll: 0,
            job_stages_popup: None,
            executors_data: ExecutorsData::new(),
            jobs_data: JobsData::new(),
            metrics_data: MetricsData::new(),
            http_client: Arc::new(HttpClient::new(config)?),
        })
    }

    pub fn is_scheduler_up(&self) -> bool {
        self.executors_data.scheduler_state.is_some()
    }

    pub fn is_executors_view(&self) -> bool {
        self.current_view == Views::Executors
    }

    pub fn is_jobs_view(&self) -> bool {
        self.current_view == Views::Jobs
    }

    pub fn is_metrics_view(&self) -> bool {
        self.current_view == Views::Metrics
    }

    pub fn is_edit_mode(&self) -> bool {
        self.input_mode == InputMode::Edit
    }

    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    pub fn set_event_tx(&mut self, tx: Sender<Event>) {
        self.event_tx = Some(tx);
    }

    pub async fn send_event(&self, event: Event) -> TuiResult<()> {
        if let Some(tx) = &self.event_tx {
            tx.send(event)
                .await
                .inspect_err(|e| tracing::warn!("Failed to send event: {e:?}"))
                .map_err(TuiError::from)
        } else {
            tracing::warn!("Event_tx is not set");
            Ok(())
        }
    }

    pub async fn on_tick(&mut self) {
        if self.is_executors_view() {
            self.load_executors_data().await;
        } else if self.is_jobs_view() {
            self.load_jobs_data().await;
            let selected_job_id = self
                .jobs_data
                .selected_job(&self.search_term)
                .map(|j| j.job_id.clone());
            let current_details_id = self.job_details.as_ref().map(|d| d.job_id.clone());
            if selected_job_id != current_details_id {
                self.job_details = None;
                if let Some(job_id) = selected_job_id {
                    self.load_selected_job_details(&job_id).await;
                }
            }
        } else if self.is_metrics_view() {
            self.load_metrics_data().await;
        }
    }

    pub async fn on_key(&mut self, key: KeyEvent) -> TuiResult<()> {
        // Edit mode takes priority over everything
        if self.is_edit_mode() {
            match key.code {
                KeyCode::Esc => {
                    self.search_term.clear();
                    self.input_mode = InputMode::View;
                }
                KeyCode::Backspace => {
                    self.search_term.pop();
                }
                KeyCode::Char(c) => {
                    self.search_term.push(c);
                }
                _ => {}
            }
            return Ok(());
        }

        // The cancellation result is rendered just once
        if self.cancel_job_result.is_some() {
            self.cancel_job_result = None;
            return Ok(());
        }

        if let Some(popup) = &mut self.job_stages_popup {
            if popup.is_tasks_view()
                && let KeyCode::Esc = key.code
            {
                popup.set_no_details_view();
            } else if popup.is_plan_view() {
                popup.set_no_details_view();
            } else if popup.is_no_details_view() {
                match key.code {
                    KeyCode::Up => popup.scroll_up(),
                    KeyCode::Down => popup.scroll_down(),
                    KeyCode::Enter => {
                        popup.set_tasks_view();
                    }
                    KeyCode::Char('p') => {
                        popup.set_plan_view();
                    }
                    KeyCode::Esc => {
                        self.job_stages_popup = None;
                    }
                    _ => {}
                }
            }
            return Ok(());
        }

        if let Some(ref mut job_graph_popup) = self.job_dot_popup {
            match key.code {
                KeyCode::Up => {
                    job_graph_popup.scroll_up();
                }
                KeyCode::Down => {
                    job_graph_popup.scroll_down();
                }
                KeyCode::Esc => {
                    self.job_dot_popup = None;
                }
                _ => {}
            }
            return Ok(());
        }

        if self.job_plan_popup.is_some() {
            match key.code {
                KeyCode::Up => {
                    self.job_plan_popup_scroll =
                        self.job_plan_popup_scroll.saturating_sub(1);
                }
                KeyCode::Down => {
                    self.job_plan_popup_scroll += 1;
                }
                KeyCode::Char('s') => {
                    if let Some((_, tab)) = &mut self.job_plan_popup {
                        *tab = PlanTab::Stage;
                        self.job_plan_popup_scroll = 0;
                    }
                }
                KeyCode::Char('p') => {
                    if let Some((_, tab)) = &mut self.job_plan_popup {
                        *tab = PlanTab::Physical;
                        self.job_plan_popup_scroll = 0;
                    }
                }
                KeyCode::Char('l') => {
                    if let Some((_, tab)) = &mut self.job_plan_popup {
                        *tab = PlanTab::Logical;
                        self.job_plan_popup_scroll = 0;
                    }
                }
                KeyCode::Esc => {
                    self.job_plan_popup = None;
                    self.job_plan_popup_scroll = 0;
                }
                _ => {}
            }
            return Ok(());
        }

        if self.show_help || self.show_scheduler_info {
            self.show_help = false;
            self.show_scheduler_info = false;
            return Ok(());
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.should_quit = true;
            }
            KeyCode::Char('?') | KeyCode::Char('h') => {
                self.show_help = true;
            }
            KeyCode::Char('i') => {
                self.show_scheduler_info = true;
            }
            KeyCode::Enter if self.is_jobs_view() => {
                self.load_job_stages_popup_data().await;
            }
            KeyCode::Char('g') if self.is_jobs_view() => {
                self.load_job_dot_data().await;
            }
            KeyCode::Char('p') if self.is_jobs_view() => {
                self.open_job_plan_popup();
            }
            KeyCode::Char('e') if self.is_scheduler_up() => {
                self.current_view = Views::Executors;
                self.load_executors_data().await;
            }
            KeyCode::Char('j') if self.is_scheduler_up() => {
                self.current_view = Views::Jobs;
                self.load_jobs_data().await;
            }
            KeyCode::Char('m') if self.is_scheduler_up() => {
                self.current_view = Views::Metrics;
                self.load_metrics_data().await;
            }
            KeyCode::Char('/') if self.is_jobs_view() || self.is_metrics_view() => {
                self.input_mode = InputMode::Edit;
            }
            KeyCode::Char('1') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::Id);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::Host);
                } else if self.is_metrics_view() {
                    self.sort_metrics_by(MetricsSortColumn::Name);
                }
            }
            KeyCode::Char('2') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::Name);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::Id);
                }
            }
            KeyCode::Char('3') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::Status);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::LastSeen);
                }
            }
            KeyCode::Char('4') if self.is_jobs_view() => {
                self.sort_jobs_by(JobsSortColumn::StagesCompleted);
            }
            KeyCode::Char('5') if self.is_jobs_view() => {
                self.sort_jobs_by(JobsSortColumn::PercentComplete);
            }
            KeyCode::Char('6') if self.is_jobs_view() => {
                self.sort_jobs_by(JobsSortColumn::StartTime);
            }
            KeyCode::Char('c')
                if self.is_jobs_view() && self.input_mode == InputMode::View =>
            {
                self.cancel_selected_job().await;
            }
            KeyCode::Down => {
                if self.is_jobs_view() {
                    self.jobs_data.scroll_down();
                    self.update_selected_job_details().await;
                } else if self.is_executors_view() {
                    self.executors_data.scroll_down();
                } else if self.is_metrics_view() {
                    self.metrics_data.scroll_down();
                }
            }
            KeyCode::Up => {
                if self.is_jobs_view() {
                    self.jobs_data.scroll_up();
                    self.update_selected_job_details().await;
                } else if self.is_executors_view() {
                    self.executors_data.scroll_up();
                } else if self.is_metrics_view() {
                    self.metrics_data.scroll_up();
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn update_selected_job_details(&mut self) {
        self.job_details = None;
        if let Some(job_id) = self
            .jobs_data
            .selected_job(&self.search_term)
            .map(|j| j.job_id.clone())
        {
            self.load_selected_job_details(&job_id).await;
        }
    }

    async fn load_selected_job_details(&self, job_id: &str) {
        if let Err(e) = load_job_details(self, job_id).await {
            tracing::error!("Failed to load job details: {e:?}");
        }
    }

    async fn load_job_dot_data(&self) {
        if let Some(selected_job) = self.jobs_data.selected_job(&self.search_term) {
            if selected_job.status == "Completed"
                && let Err(e) = load_job_dot(self, &selected_job.job_id).await
            {
                tracing::error!("Failed to load job dot: {e:?}");
            }
        } else {
            tracing::trace!("No job selected");
        }
    }

    async fn load_job_stages_popup_data(&self) {
        if let Some(job) = self.jobs_data.selected_job(&self.search_term) {
            let job_id = job.job_id.clone();
            if let Err(e) = load_job_stages_popup(self, &job_id).await {
                tracing::error!("Failed to load job stages popup for '{job_id}': {e:?}");
            }
        }
    }

    async fn load_executors_data(&mut self) {
        if let Err(e) = load_executors_data(self).await {
            tracing::error!("Failed to load executors data on tick: {e:?}");
        }
    }

    async fn load_jobs_data(&mut self) {
        if let Err(e) = load_jobs_data(self).await {
            tracing::error!("Failed to load jobs data on tick: {e:?}");
        }
    }

    async fn load_metrics_data(&mut self) {
        if let Err(e) = load_metrics_data(self).await {
            tracing::error!("Failed to load metrics data on tick: {e:?}");
        }
    }

    fn sort_jobs_by(&mut self, sort_column: JobsSortColumn) {
        if self.jobs_data.sort_column == sort_column {
            match self.jobs_data.sort_order {
                SortOrder::Ascending => {
                    self.jobs_data.sort_order = SortOrder::Descending;
                }
                SortOrder::Descending => {
                    self.jobs_data.sort_column = JobsSortColumn::None;
                }
            }
        } else {
            self.jobs_data.sort_column = sort_column;
            self.jobs_data.sort_order = SortOrder::Ascending;
        }
    }

    fn sort_executors_by(&mut self, sort_column: ExecutorsSortColumn) {
        if self.executors_data.sort_column == sort_column {
            match self.executors_data.sort_order {
                SortOrder::Ascending => {
                    self.executors_data.sort_order = SortOrder::Descending;
                }
                SortOrder::Descending => {
                    self.executors_data.sort_column = ExecutorsSortColumn::None;
                }
            }
        } else {
            self.executors_data.sort_column = sort_column;
            self.executors_data.sort_order = SortOrder::Ascending;
        }
        self.executors_data.sort();
    }

    fn sort_metrics_by(&mut self, sort_column: MetricsSortColumn) {
        if self.metrics_data.sort_column == sort_column {
            match self.metrics_data.sort_order {
                SortOrder::Ascending => {
                    self.metrics_data.sort_order = SortOrder::Descending;
                }
                SortOrder::Descending => {
                    self.metrics_data.sort_column = MetricsSortColumn::None;
                }
            }
        } else {
            self.metrics_data.sort_column = sort_column;
            self.metrics_data.sort_order = SortOrder::Ascending;
        }
        self.metrics_data.sort();
    }

    async fn cancel_selected_job(&mut self) {
        if let Some(job) = self.jobs_data.selected_job(&self.search_term)
            && (job.status == "Running" || job.status == "Queued")
        {
            let job_id = job.job_id.clone();
            self.cancel_job_result =
                Some(match self.http_client.cancel_job(&job_id).await {
                    Ok(resp) if resp.canceled => CancelJobResult::Success { job_id },
                    Ok(_) => CancelJobResult::NotCanceled { job_id },
                    Err(e) => CancelJobResult::Failure {
                        job_id,
                        error: e.to_string(),
                    },
                });
        }
    }

    pub fn has_selected_job(&self) -> bool {
        self.jobs_data.selected_job(&self.search_term).is_some()
    }

    pub fn is_selected_job_completed(&self) -> bool {
        self.jobs_data
            .selected_job(&self.search_term)
            .is_some_and(|j| j.status == "Completed")
    }

    pub fn is_selected_job_cancelable(&self) -> bool {
        self.jobs_data
            .selected_job(&self.search_term)
            .is_some_and(|j| j.status == "Running" || j.status == "Queued")
    }

    pub fn has_more_than_one_job(&self) -> bool {
        self.jobs_data.jobs.len() > 1
    }

    pub fn is_job_stages_popup_open(&self) -> bool {
        self.job_stages_popup.is_some()
    }

    pub fn is_job_stage_no_details_popup_open(&self) -> bool {
        self.job_stages_popup
            .as_ref()
            .is_some_and(|popup| popup.is_no_details_view())
    }

    pub fn is_job_stage_tasks_popup_open(&self) -> bool {
        self.job_stages_popup
            .as_ref()
            .is_some_and(|popup| popup.is_tasks_view())
    }

    pub fn is_job_stage_plan_popup_open(&self) -> bool {
        self.job_stages_popup
            .as_ref()
            .is_some_and(|popup| popup.is_plan_view())
    }

    fn open_job_plan_popup(&mut self) {
        let is_completed = self
            .jobs_data
            .selected_job(&self.search_term)
            .is_some_and(|j| j.status == "Completed");
        if is_completed && let Some(details) = &self.job_details {
            self.job_plan_popup = Some((details.clone(), PlanTab::Stage));
            self.job_plan_popup_scroll = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tui::App;
    use crate::tui::Settings;
    use crate::tui::app::{ExecutorsSortColumn, JobsSortColumn, MetricsSortColumn};
    use crate::tui::domain::{SchedulerState, SortOrder, jobs::Job};

    fn make_app() -> App {
        let settings =
            Settings::new().expect("Settings::new should succeed with defaults");
        App::new(settings).expect("App::new should succeed with valid settings")
    }

    fn make_job(id: &str, status: &str) -> Job {
        Job {
            job_id: id.to_string(),
            job_name: format!("Job {id}"),
            status: status.to_string(),
            start_time: 0,
            num_stages: 1,
            completed_stages: 0,
            percent_complete: 0,
        }
    }

    fn make_scheduler_state() -> SchedulerState {
        SchedulerState {
            started: 0,
            version: "0.1.0".to_string(),
            datafusion_version: "40.0.0".to_string(),
            substrait_support: false,
            keda_support: false,
            prometheus_support: false,
            graphviz_support: false,
            spark_support: false,
            scheduling_policy: "round-robin".to_string(),
        }
    }

    // --- Initial state tests ---

    #[test]
    fn new_app_starts_in_jobs_view() {
        let app = make_app();
        assert!(app.is_jobs_view());
        assert!(!app.is_executors_view());
        assert!(!app.is_metrics_view());
    }

    #[test]
    fn new_app_scheduler_is_not_up() {
        let app = make_app();
        assert!(!app.is_scheduler_up());
    }

    #[test]
    fn new_app_should_not_quit() {
        let app = make_app();
        assert!(!app.should_quit());
    }

    #[test]
    fn new_app_is_not_edit_mode() {
        let app = make_app();
        assert!(!app.is_edit_mode());
    }

    // --- Scheduler state ---

    #[test]
    fn is_scheduler_up_when_scheduler_state_is_some() {
        let mut app = make_app();
        app.executors_data.scheduler_state = Some(make_scheduler_state());
        assert!(app.is_scheduler_up());
    }

    // --- Job helper methods ---

    #[test]
    fn has_no_selected_job_when_empty() {
        let app = make_app();
        assert!(!app.has_selected_job());
    }

    #[test]
    fn has_more_than_one_job_false_when_empty() {
        let app = make_app();
        assert!(!app.has_more_than_one_job());
    }

    #[test]
    fn has_more_than_one_job_true_with_two_jobs() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Running"), make_job("j2", "Running")];
        assert!(app.has_more_than_one_job());
    }

    #[test]
    fn is_selected_job_completed_true_when_completed_job_selected() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Completed")];
        app.jobs_data.table_state.select(Some(0));
        assert!(app.is_selected_job_completed());
    }

    #[test]
    fn is_selected_job_completed_false_for_running_job() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Running")];
        app.jobs_data.table_state.select(Some(0));
        assert!(!app.is_selected_job_completed());
    }

    // --- sort_jobs_by toggle tests ---

    #[test]
    fn sort_jobs_by_new_column_sets_ascending() {
        let mut app = make_app();
        app.sort_jobs_by(JobsSortColumn::Id);
        assert_eq!(app.jobs_data.sort_column, JobsSortColumn::Id);
        assert_eq!(app.jobs_data.sort_order, SortOrder::Ascending);
    }

    #[test]
    fn sort_jobs_by_same_column_ascending_toggles_to_descending() {
        let mut app = make_app();
        app.sort_jobs_by(JobsSortColumn::Name);
        app.sort_jobs_by(JobsSortColumn::Name);
        assert_eq!(app.jobs_data.sort_column, JobsSortColumn::Name);
        assert_eq!(app.jobs_data.sort_order, SortOrder::Descending);
    }

    #[test]
    fn sort_jobs_by_same_column_descending_resets_to_none() {
        let mut app = make_app();
        app.sort_jobs_by(JobsSortColumn::Status);
        app.sort_jobs_by(JobsSortColumn::Status);
        app.sort_jobs_by(JobsSortColumn::Status);
        assert_eq!(app.jobs_data.sort_column, JobsSortColumn::None);
    }

    // --- sort_executors_by toggle tests ---

    #[test]
    fn sort_executors_by_new_column_sets_ascending() {
        let mut app = make_app();
        app.sort_executors_by(ExecutorsSortColumn::Host);
        assert_eq!(app.executors_data.sort_column, ExecutorsSortColumn::Host);
        assert_eq!(app.executors_data.sort_order, SortOrder::Ascending);
    }

    #[test]
    fn sort_executors_by_same_column_ascending_toggles_to_descending() {
        let mut app = make_app();
        app.sort_executors_by(ExecutorsSortColumn::Id);
        app.sort_executors_by(ExecutorsSortColumn::Id);
        assert_eq!(app.executors_data.sort_column, ExecutorsSortColumn::Id);
        assert_eq!(app.executors_data.sort_order, SortOrder::Descending);
    }

    #[test]
    fn sort_executors_by_same_column_descending_resets_to_none() {
        let mut app = make_app();
        app.sort_executors_by(ExecutorsSortColumn::LastSeen);
        app.sort_executors_by(ExecutorsSortColumn::LastSeen);
        app.sort_executors_by(ExecutorsSortColumn::LastSeen);
        assert_eq!(app.executors_data.sort_column, ExecutorsSortColumn::None);
    }

    // --- sort_metrics_by toggle tests ---

    #[test]
    fn sort_metrics_by_new_column_sets_ascending() {
        let mut app = make_app();
        app.sort_metrics_by(MetricsSortColumn::Name);
        assert_eq!(app.metrics_data.sort_column, MetricsSortColumn::Name);
        assert_eq!(app.metrics_data.sort_order, SortOrder::Ascending);
    }

    #[test]
    fn sort_metrics_by_same_column_ascending_toggles_to_descending() {
        let mut app = make_app();
        app.sort_metrics_by(MetricsSortColumn::Name);
        app.sort_metrics_by(MetricsSortColumn::Name);
        assert_eq!(app.metrics_data.sort_column, MetricsSortColumn::Name);
        assert_eq!(app.metrics_data.sort_order, SortOrder::Descending);
    }

    #[test]
    fn sort_metrics_by_same_column_descending_resets_to_none() {
        let mut app = make_app();
        app.sort_metrics_by(MetricsSortColumn::Name);
        app.sort_metrics_by(MetricsSortColumn::Name);
        app.sort_metrics_by(MetricsSortColumn::Name);
        assert_eq!(app.metrics_data.sort_column, MetricsSortColumn::None);
    }
}
