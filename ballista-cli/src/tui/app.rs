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
        CancelJobResult, DashboardData, JobDetails, JobsData, MetricsData, SortColumn,
        SortOrder, StagesGraph,
    },
    event::Event,
    infrastructure::Settings,
};
use crossterm::event::{KeyCode, KeyEvent};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use crate::tui::http_client::HttpClient;
use crate::tui::ui::{
    load_dashboard_data, load_job_details, load_job_dot, load_jobs_data,
    load_metrics_data,
};

#[derive(Debug, PartialEq)]
enum Views {
    Dashboard,
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

    pub dashboard_data: DashboardData,
    pub metrics_data: MetricsData,
    pub jobs_data: JobsData,

    // Popups
    pub show_help: bool,
    pub show_scheduler_info: bool,

    pub cancel_job_result: Option<CancelJobResult>,

    pub search_term: String,

    pub job_details: Option<JobDetails>,

    pub job_dot_popup: Option<StagesGraph>,
    pub job_dot_scroll: u16,

    pub job_plan_popup: Option<(JobDetails, PlanTab)>,
    pub job_plan_popup_scroll: u16,

    pub http_client: Arc<HttpClient>,
}

impl App {
    pub fn new(config: Settings) -> TuiResult<Self> {
        Ok(Self {
            should_quit: false,
            event_tx: None,
            current_view: Views::Dashboard,
            input_mode: InputMode::View,
            search_term: String::new(),
            show_help: false,
            show_scheduler_info: false,
            cancel_job_result: None,
            job_details: None,
            job_dot_popup: None,
            job_dot_scroll: 0,
            job_plan_popup: None,
            job_plan_popup_scroll: 0,
            dashboard_data: DashboardData::new(),
            jobs_data: JobsData::new(),
            metrics_data: MetricsData::new(),
            http_client: Arc::new(HttpClient::new(config)?),
        })
    }

    pub fn is_scheduler_up(&self) -> bool {
        self.dashboard_data.scheduler_state.is_some()
    }

    pub fn is_dashboard_view(&self) -> bool {
        self.current_view == Views::Dashboard
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
        if self.is_dashboard_view() {
            self.load_dashboard_data().await;
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

        if self.cancel_job_result.is_some() {
            self.cancel_job_result = None;
            return Ok(());
        }

        if self.job_dot_popup.is_some() {
            match key.code {
                KeyCode::Up => {
                    self.job_dot_scroll = self.job_dot_scroll.saturating_sub(1);
                }
                KeyCode::Down => {
                    self.job_dot_scroll += 1;
                }
                _ => {
                    self.job_dot_popup = None;
                    self.job_dot_scroll = 0;
                }
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
            KeyCode::Char('g') if self.is_jobs_view() => {
                self.load_job_dot_data().await;
            }
            KeyCode::Char('D') if self.is_jobs_view() => {
                self.open_job_plan_popup();
            }
            KeyCode::Char('d') if self.is_scheduler_up() => {
                self.current_view = Views::Dashboard;
                self.load_dashboard_data().await;
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
            KeyCode::Char('s') if self.is_jobs_view() => {
                self.sort_jobs_by(SortColumn::Status);
            }
            KeyCode::Char('p') if self.is_jobs_view() => {
                self.sort_jobs_by(SortColumn::PercentComplete);
            }
            KeyCode::Char('t') if self.is_jobs_view() => {
                self.sort_jobs_by(SortColumn::StartTime);
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
                } else if self.is_metrics_view() {
                    self.metrics_data.scroll_down();
                }
            }
            KeyCode::Up => {
                if self.is_jobs_view() {
                    self.jobs_data.scroll_up();
                    self.update_selected_job_details().await;
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

    async fn load_dashboard_data(&mut self) {
        if let Err(e) = load_dashboard_data(self).await {
            tracing::error!("Failed to load dashboard data on tick: {e:?}");
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

    fn sort_jobs_by(&mut self, sort_column: SortColumn) {
        if self.jobs_data.sort_column == sort_column {
            match self.jobs_data.sort_order {
                SortOrder::Ascending => {
                    self.jobs_data.sort_order = SortOrder::Descending;
                }
                SortOrder::Descending => {
                    self.jobs_data.sort_column = SortColumn::None;
                }
            }
        } else {
            self.jobs_data.sort_column = sort_column;
            self.jobs_data.sort_order = SortOrder::Ascending;
        }
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

    pub fn has_selected_completed_job(&self) -> bool {
        self.jobs_data
            .selected_job(&self.search_term)
            .is_some_and(|j| j.status == "Completed")
    }

    pub fn has_more_than_one_job(&self) -> bool {
        self.jobs_data.jobs.len() > 1
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
