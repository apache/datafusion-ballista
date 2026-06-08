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

#[cfg(not(feature = "web"))]
use crate::tui::TuiError;
use crate::tui::TuiResult;
#[cfg(feature = "web")]
use crate::tui::domain::jobs::stages::StagePlanTab;
use crate::tui::event::Event;
#[cfg(feature = "web")]
use crate::tui::event::web::Sender;
use crate::tui::{
    domain::{
        SortOrder,
        executors::{
            ExecutorDetailsPopup, ExecutorsData, SortColumn as ExecutorsSortColumn,
        },
        jobs::{
            CancelJobResult, JobDetails, JobPlansPopup, JobsData, PhysicalFormat,
            PlanTab, SortColumn as JobsSortColumn,
            stages::{JobStagesPopup, StagesGraph},
        },
        metrics::MetricsData,
        metrics::SortColumn as MetricsSortColumn,
    },
    event::UiData,
    infrastructure::Settings,
};
use chrono::{DateTime, Utc};
#[cfg(not(feature = "web"))]
use crossterm::event::{KeyCode, KeyEvent};
#[cfg(feature = "web")]
use ratzilla::event::{KeyCode, KeyEvent};
use std::string::ToString;
use std::sync::Arc;
#[cfg(not(feature = "web"))]
use tokio::sync::mpsc::Sender;

use crate::tui::http_client::HttpClient;
#[cfg(not(feature = "web"))]
use crate::tui::ui::{
    load_executor_details_popup, load_executors_data, load_job_details, load_job_dot,
    load_job_stages_popup, load_jobs_data, load_metrics_data, load_stage_plan,
};

const INVALID_DATE: &str = "Invalid date";

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

pub(crate) struct App {
    should_quit: bool,

    event_tx: Option<Sender<Event>>,

    current_view: Views,
    input_mode: InputMode,

    pub jobs_data: JobsData,
    pub executors_data: ExecutorsData,
    pub metrics_data: MetricsData,

    pub cancel_job_result: Option<CancelJobResult>,

    pub search_term: String,

    pub job_details: Option<JobDetails>,

    // Popups
    pub show_help: bool,
    pub show_scheduler_info: bool,
    pub job_dot_popup: Option<StagesGraph>,
    pub job_plan_popup: Option<JobPlansPopup>,
    pub job_stages_popup: Option<JobStagesPopup>,
    pub executor_details_popup: Option<ExecutorDetailsPopup>,

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
            job_stages_popup: None,
            executor_details_popup: None,
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

    #[cfg(not(feature = "web"))]
    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    pub fn set_event_tx(&mut self, tx: Sender<Event>) {
        self.event_tx = Some(tx);
    }

    #[cfg(not(feature = "web"))]
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

    #[cfg(not(feature = "web"))]
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

    #[cfg(not(feature = "web"))]
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
            if popup.is_tasks_view() {
                match key.code {
                    KeyCode::Esc => popup.set_no_details_view(),
                    KeyCode::Up => popup.scroll_up(),
                    KeyCode::Down => popup.scroll_down(),
                    _ => {}
                }
            } else if popup.is_plan_view() {
                use crate::tui::domain::jobs::stages::StagePlanTab;

                // Collect job_id + tab to fetch while popup is still borrowed,
                // then drop the borrow before the async call.
                let fetch = match key.code {
                    KeyCode::Char('d') => popup
                        .set_tab(StagePlanTab::Default)
                        .map(|tab| (popup.job_id.clone(), tab)),
                    KeyCode::Char('t') => popup
                        .set_tab(StagePlanTab::Tree)
                        .map(|tab| (popup.job_id.clone(), tab)),
                    KeyCode::Char('m') => popup
                        .set_tab(StagePlanTab::Metrics)
                        .map(|tab| (popup.job_id.clone(), tab)),
                    KeyCode::Esc => {
                        popup.set_no_details_view();
                        None
                    }
                    KeyCode::Up => {
                        popup.scroll_up();
                        None
                    }
                    KeyCode::Down => {
                        popup.scroll_down();
                        None
                    }
                    KeyCode::Left => {
                        popup.scroll_left();
                        None
                    }
                    KeyCode::Right => {
                        popup.scroll_right();
                        None
                    }
                    _ => None,
                };

                if let Some((job_id, tab)) = fetch
                    && let Err(e) = load_stage_plan(self, &job_id, tab).await
                {
                    tracing::error!("Failed to load stage plan: {e:?}");
                }
            } else if popup.is_no_details_view() {
                match key.code {
                    KeyCode::Up => popup.scroll_up(),
                    KeyCode::Down => popup.scroll_down(),
                    KeyCode::Enter if popup.selected_stage().is_some() => {
                        popup.set_tasks_view();
                    }
                    KeyCode::Char('p') if popup.selected_stage().is_some() => {
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

        if let Some(ref mut plans_popup) = self.job_plan_popup {
            let fetch_tree = if key.code == KeyCode::Char('t')
                && plans_popup.get_tab() == &PlanTab::Physical
            {
                plans_popup
                    .set_physical_format(PhysicalFormat::Tree)
                    .map(|_| plans_popup.details.job_id.clone())
            } else {
                match key.code {
                    KeyCode::Up => {
                        plans_popup.scroll_up();
                    }
                    KeyCode::Down => {
                        plans_popup.scroll_down();
                    }
                    KeyCode::Left => {
                        plans_popup.scroll_left();
                    }
                    KeyCode::Right => {
                        plans_popup.scroll_right();
                    }
                    KeyCode::Char('s') => plans_popup.set_tab(PlanTab::Stage),
                    KeyCode::Char('p') => plans_popup.set_tab(PlanTab::Physical),
                    KeyCode::Char('l') => plans_popup.set_tab(PlanTab::Logical),
                    KeyCode::Char('d') if plans_popup.get_tab() == &PlanTab::Physical => {
                        plans_popup.set_physical_format(PhysicalFormat::Default);
                    }
                    KeyCode::Esc => {
                        self.job_plan_popup = None;
                    }
                    _ => {}
                }
                None
            };

            if let Some(job_id) = fetch_tree {
                match self
                    .http_client
                    .get_job_details(&job_id, Some("tree"))
                    .await
                {
                    Ok(details) => {
                        if let Some(p) = &mut self.job_plan_popup {
                            p.details.physical_plan_tree = details.physical_plan;
                        }
                    }
                    Err(e) => tracing::error!("Failed to load tree physical plan: {e:?}"),
                }
            }

            return Ok(());
        }

        if let Some(ref mut executor_popup) = self.executor_details_popup {
            match key.code {
                KeyCode::Up => executor_popup.scroll_up(),
                KeyCode::Down => executor_popup.scroll_down(),
                KeyCode::Esc => {
                    self.executor_details_popup = None;
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
            KeyCode::Enter if self.is_executors_view() => {
                self.load_executor_details_popup_data().await;
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
                    self.sort_executors_by(ExecutorsSortColumn::CpuCores);
                }
            }
            KeyCode::Char('4') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::StagesCompleted);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::TaskSlots);
                }
            }
            KeyCode::Char('5') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::PercentComplete);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::ProcPhysicalMemoryUsage);
                }
            }
            KeyCode::Char('6') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::StartTime);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::PeakPhysicalMemoryUsage);
                }
            }
            KeyCode::Char('7') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::Duration);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::LastSeen);
                }
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

    #[cfg(not(feature = "web"))]
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

    #[cfg(not(feature = "web"))]
    async fn load_selected_job_details(&self, job_id: &str) {
        if let Err(e) = load_job_details(self, job_id).await {
            tracing::error!("Failed to load job details: {e:?}");
        }
    }

    #[cfg(not(feature = "web"))]
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

    #[cfg(not(feature = "web"))]
    async fn load_job_stages_popup_data(&self) {
        if let Some(job) = self.jobs_data.selected_job(&self.search_term) {
            let job_id = job.job_id.clone();
            if let Err(e) = load_job_stages_popup(self, &job_id).await {
                tracing::error!("Failed to load job stages popup for '{job_id}': {e:?}");
            }
        }
    }

    #[cfg(not(feature = "web"))]
    async fn load_executor_details_popup_data(&self) {
        if let Some(executor) = self.executors_data.selected_executor()
            && let Err(e) = load_executor_details_popup(self, &executor.id).await
        {
            tracing::error!(
                "Failed to load executor details for '{}': {e:?}",
                &executor.id
            );
        }
    }

    #[cfg(not(feature = "web"))]
    async fn load_executors_data(&mut self) {
        if let Err(e) = load_executors_data(self).await {
            tracing::error!("Failed to load executors data on tick: {e:?}");
        }
    }

    #[cfg(not(feature = "web"))]
    async fn load_jobs_data(&mut self) {
        if let Err(e) = load_jobs_data(self).await {
            tracing::error!("Failed to load jobs data on tick: {e:?}");
        }
    }

    #[cfg(not(feature = "web"))]
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

    #[cfg(not(feature = "web"))]
    async fn cancel_selected_job(&mut self) {
        if let Some(job) = self.jobs_data.selected_job(&self.search_term)
            && (job.status == "Running" || job.status == "Queued")
        {
            let job_id = job.job_id.clone();
            let cancel_job_result = match self.http_client.cancel_job(&job_id).await {
                Ok(resp) if resp.canceled => CancelJobResult::Success { job_id },
                Ok(_) => CancelJobResult::NotCanceled { job_id },
                Err(e) => CancelJobResult::Failure {
                    job_id,
                    error: e.to_string(),
                },
            };
            if let Err(err) = self
                .send_event(Event::DataLoaded {
                    data: UiData::CancelJobResult(cancel_job_result),
                })
                .await
            {
                tracing::error!("Failed to send CancelJobResult event: {err:?}")
            };
        }
    }

    pub fn has_selected_job(&self) -> bool {
        self.jobs_data.selected_job(&self.search_term).is_some()
    }

    pub fn is_selected_job_completed_or_running(&self) -> bool {
        self.jobs_data
            .selected_job(&self.search_term)
            .is_some_and(|j| j.status == "Completed" || j.status == "Running")
    }

    pub fn is_selected_job_cancelable(&self) -> bool {
        self.jobs_data
            .selected_job(&self.search_term)
            .is_some_and(|j| j.status == "Running" || j.status == "Queued")
    }

    pub fn has_more_than_one_job(&self) -> bool {
        self.jobs_data.jobs.len() > 1
    }

    pub fn has_selected_executor(&self) -> bool {
        self.executors_data.table_state.selected().is_some()
    }

    pub fn is_executor_details_popup_open(&self) -> bool {
        self.executor_details_popup.is_some()
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
        if self.is_selected_job_completed_or_running()
            && let Some(details) = &self.job_details
        {
            self.job_plan_popup =
                Some(JobPlansPopup::new(details.clone(), PlanTab::Stage));
        }
    }

    pub fn format_datetime(&self, timestamp: i64) -> String {
        DateTime::from_timestamp_millis(timestamp)
            .map(|dt| {
                dt.with_timezone(&chrono::Local)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            })
            .unwrap_or_else(|| INVALID_DATE.to_string())
    }

    // copied from DataFusion Commons to avoid depending on it
    pub fn format_duration(&self, duration_ms: u64) -> String {
        const NANOS_PER_SEC: f64 = 1_000_000_000.0;
        const NANOS_PER_MILLI: f64 = 1_000_000.0;
        const NANOS_PER_MICRO: f64 = 1_000.0;

        let nanos = duration_ms as f64 * NANOS_PER_MILLI;

        if nanos >= NANOS_PER_SEC {
            // >= 1 second: show in seconds
            format!("{:.2}s", nanos / NANOS_PER_SEC)
        } else if nanos >= NANOS_PER_MILLI {
            // >= 1 millisecond: show in milliseconds
            format!("{:.2}ms", nanos / NANOS_PER_MILLI)
        } else if nanos >= NANOS_PER_MICRO {
            // >= 1 microsecond: show in microseconds
            format!("{:.2}µs", nanos / NANOS_PER_MICRO)
        } else {
            // < 1 microsecond: show in nanoseconds
            format!("{nanos}ns")
        }
    }

    // copied from DataFusion Commons to avoid depending on it
    // and to remove a space between the number and the unit
    pub fn format_size(&self, size: usize) -> String {
        const TB: u64 = 1 << 40;
        const GB: u64 = 1 << 30;
        const MB: u64 = 1 << 20;
        const KB: u64 = 1 << 10;

        let size = size as u64;
        let (value, unit) = {
            if size >= 2 * TB {
                (size as f64 / TB as f64, "TB")
            } else if size >= 2 * GB {
                (size as f64 / GB as f64, "GB")
            } else if size >= 2 * MB {
                (size as f64 / MB as f64, "MB")
            } else if size >= 2 * KB {
                (size as f64 / KB as f64, "KB")
            } else {
                (size as f64, "B")
            }
        };
        format!("{value:.1}{unit}")
    }

    // copied from DataFusion Commons to avoid depending on it
    // and to remove a space between the number and the unit
    pub fn format_count(&self, count: usize) -> String {
        let count = count as u64;
        let (value, unit) = {
            if count >= 1_000_000_000_000 {
                (count as f64 / 1_000_000_000_000.0, "T")
            } else if count >= 1_000_000_000 {
                (count as f64 / 1_000_000_000.0, "B")
            } else if count >= 1_000_000 {
                (count as f64 / 1_000_000.0, "M")
            } else if count >= 1_000 {
                (count as f64 / 1_000.0, "K")
            } else {
                return count.to_string();
            }
        };

        // Format with appropriate precision
        // For values >= 100, show 1 decimal place (e.g., 123.4 K)
        // For values < 100, show 2 decimal places (e.g., 10.12 K)
        if value >= 100.0 {
            format!("{value:.1}{unit}")
        } else {
            format!("{value:.2}{unit}")
        }
    }

    #[cfg(not(feature = "web"))]
    pub(crate) fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[cfg(feature = "web")]
    pub(crate) fn now() -> DateTime<Utc> {
        let timestamp = js_sys::Date::now();
        DateTime::from_timestamp_millis(timestamp as i64).unwrap_or_else(|| {
            tracing::warn!("Failed to convert JS timestamp to DateTime. Falling back to Unix epoch for metrics' timestamps.");
            DateTime::from_timestamp_millis(0).unwrap()
        })
    }

    /// Applies a loaded data payload to the app state directly.
    /// Used by the WASM path where data is loaded without going through the event channel.
    pub fn apply_ui_data(&mut self, data: UiData) {
        use ratatui::widgets::ScrollbarState;
        match data {
            UiData::SchedulerState(state) => {
                self.executors_data.scheduler_state = state;
            }
            UiData::Executors(state, executors, jobs) => {
                let old_pos = self.executors_data.scrollbar_state.get_position();
                let scrollbar_state =
                    ScrollbarState::new(executors.len()).position(old_pos);
                let table_state = self.executors_data.table_state;
                let sort_column = self.executors_data.sort_column.clone();
                let sort_order = self.executors_data.sort_order.clone();
                self.executors_data = ExecutorsData {
                    executors,
                    scrollbar_state,
                    table_state,
                    sort_column,
                    sort_order,
                    scheduler_state: state,
                    jobs,
                };
                self.executors_data.sort();
            }
            UiData::Metrics(metrics) => {
                let old_pos = self.metrics_data.scrollbar_state.get_position();
                let scrollbar_state =
                    ScrollbarState::new(metrics.len()).position(old_pos);
                let table_state = self.metrics_data.table_state;
                let sort_column = self.metrics_data.sort_column.clone();
                let sort_order = self.metrics_data.sort_order.clone();
                self.metrics_data = MetricsData {
                    metrics,
                    scrollbar_state,
                    table_state,
                    sort_column,
                    sort_order,
                };
                self.metrics_data.sort();
            }
            UiData::Jobs(jobs) => {
                let old_pos = self.jobs_data.scrollbar_state.get_position();
                let scrollbar_state = ScrollbarState::new(jobs.len()).position(old_pos);
                let table_state = self.jobs_data.table_state;
                let sort_column = self.jobs_data.sort_column.clone();
                let sort_order = self.jobs_data.sort_order.clone();
                self.jobs_data = JobsData {
                    jobs,
                    scrollbar_state,
                    table_state,
                    sort_column,
                    sort_order,
                };
            }
            UiData::JobDetails(details) => {
                if details.physical_plan_tree.is_some() {
                    if let Some(popup) = &mut self.job_plan_popup {
                        popup.details.physical_plan_tree = details.physical_plan_tree;
                    }
                } else {
                    self.job_details = Some(details);
                }
            }
            UiData::JobStagesGraph(graph) => {
                self.job_dot_popup = Some(graph);
            }
            UiData::JobStagesData(job_id, stages) => {
                self.job_stages_popup = Some(JobStagesPopup::new(job_id, stages));
            }
            UiData::JobStagesPlanData(tab, stages) => {
                if let Some(popup) = &mut self.job_stages_popup {
                    popup.cache_plan_response(tab, stages);
                }
            }
            UiData::ExecutorDetails(executor) => {
                self.executor_details_popup = Some(ExecutorDetailsPopup::new(executor));
            }
            UiData::CancelJobResult(result) => {
                self.cancel_job_result = Some(result);
            }
        }
    }

    /// Synchronous portion of key handling for the WASM path. Returns an async action
    /// descriptor when further async work (e.g. HTTP calls) is needed.
    #[cfg(feature = "web")]
    pub fn on_key_sync(&mut self, key: &KeyEvent) -> Option<WebKeyAsyncAction> {
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
            return None;
        }

        if self.cancel_job_result.is_some() {
            self.cancel_job_result = None;
            return None;
        }

        if let Some(popup) = &mut self.job_stages_popup {
            if popup.is_tasks_view() {
                match key.code {
                    KeyCode::Esc => popup.set_no_details_view(),
                    KeyCode::Up => popup.scroll_up(),
                    KeyCode::Down => popup.scroll_down(),
                    _ => {}
                }
            } else if popup.is_plan_view() {
                use crate::tui::domain::jobs::stages::StagePlanTab;

                let fetch = match key.code {
                    KeyCode::Char('d') => popup
                        .set_tab(StagePlanTab::Default)
                        .map(|tab| (popup.job_id.clone(), tab)),
                    KeyCode::Char('t') => popup
                        .set_tab(StagePlanTab::Tree)
                        .map(|tab| (popup.job_id.clone(), tab)),
                    KeyCode::Char('m') => popup
                        .set_tab(StagePlanTab::Metrics)
                        .map(|tab| (popup.job_id.clone(), tab)),
                    KeyCode::Esc => {
                        popup.set_no_details_view();
                        None
                    }
                    KeyCode::Up => {
                        popup.scroll_up();
                        None
                    }
                    KeyCode::Down => {
                        popup.scroll_down();
                        None
                    }
                    KeyCode::Left => {
                        popup.scroll_left();
                        None
                    }
                    KeyCode::Right => {
                        popup.scroll_right();
                        None
                    }
                    _ => None,
                };

                return fetch
                    .map(|(job_id, tab)| WebKeyAsyncAction::LoadStagePlan(job_id, tab));
            } else if popup.is_no_details_view() {
                match key.code {
                    KeyCode::Up => popup.scroll_up(),
                    KeyCode::Down => popup.scroll_down(),
                    KeyCode::Enter if popup.selected_stage().is_some() => {
                        popup.set_tasks_view();
                    }
                    KeyCode::Char('p') if popup.selected_stage().is_some() => {
                        popup.set_plan_view();
                    }
                    KeyCode::Esc => {
                        self.job_stages_popup = None;
                    }
                    _ => {}
                }
            }
            return None;
        }

        if let Some(ref mut job_graph_popup) = self.job_dot_popup {
            match key.code {
                KeyCode::Up => job_graph_popup.scroll_up(),
                KeyCode::Down => job_graph_popup.scroll_down(),
                KeyCode::Esc => self.job_dot_popup = None,
                _ => {}
            }
            return None;
        }

        if let Some(ref mut plans_popup) = self.job_plan_popup {
            let fetch_tree = if key.code == KeyCode::Char('t')
                && plans_popup.get_tab() == &PlanTab::Physical
            {
                plans_popup
                    .set_physical_format(PhysicalFormat::Tree)
                    .map(|_| plans_popup.details.job_id.clone())
            } else {
                match key.code {
                    KeyCode::Up => {
                        plans_popup.scroll_up();
                    }
                    KeyCode::Down => {
                        plans_popup.scroll_down();
                    }
                    KeyCode::Left => {
                        plans_popup.scroll_left();
                    }
                    KeyCode::Right => {
                        plans_popup.scroll_right();
                    }
                    KeyCode::Char('s') => plans_popup.set_tab(PlanTab::Stage),
                    KeyCode::Char('p') => plans_popup.set_tab(PlanTab::Physical),
                    KeyCode::Char('l') => plans_popup.set_tab(PlanTab::Logical),
                    KeyCode::Char('d') if plans_popup.get_tab() == &PlanTab::Physical => {
                        plans_popup.set_physical_format(PhysicalFormat::Default);
                    }
                    KeyCode::Esc => self.job_plan_popup = None,
                    _ => {}
                }
                None
            };

            return fetch_tree.map(WebKeyAsyncAction::LoadJobPlanTree);
        }

        if let Some(ref mut executor_popup) = self.executor_details_popup {
            match key.code {
                KeyCode::Up => executor_popup.scroll_up(),
                KeyCode::Down => executor_popup.scroll_down(),
                KeyCode::Esc => self.executor_details_popup = None,
                _ => {}
            }
            return None;
        }

        if self.show_help || self.show_scheduler_info {
            self.show_help = false;
            self.show_scheduler_info = false;
            return None;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.should_quit = true;
                None
            }
            KeyCode::Char('?') | KeyCode::Char('h') => {
                self.show_help = true;
                None
            }
            KeyCode::Char('i') => {
                self.show_scheduler_info = true;
                None
            }
            KeyCode::Enter if self.is_jobs_view() => {
                let job_id = self
                    .jobs_data
                    .selected_job(&self.search_term)
                    .map(|j| j.job_id.clone());
                job_id.map(WebKeyAsyncAction::LoadJobStages)
            }
            KeyCode::Enter if self.is_executors_view() => {
                let executor_id = self
                    .executors_data
                    .selected_executor()
                    .map(|e| e.id.clone());
                executor_id.map(WebKeyAsyncAction::LoadExecutorDetails)
            }
            KeyCode::Char('g') if self.is_jobs_view() => {
                let job_id = self
                    .jobs_data
                    .selected_job(&self.search_term)
                    .filter(|j| j.status == "Completed")
                    .map(|j| j.job_id.clone());
                job_id.map(WebKeyAsyncAction::LoadJobDot)
            }
            KeyCode::Char('p') if self.is_jobs_view() => {
                self.open_job_plan_popup();
                None
            }
            KeyCode::Char('e') if self.is_scheduler_up() => {
                self.current_view = Views::Executors;
                Some(WebKeyAsyncAction::ReloadView)
            }
            KeyCode::Char('j') if self.is_scheduler_up() => {
                self.current_view = Views::Jobs;
                Some(WebKeyAsyncAction::ReloadView)
            }
            KeyCode::Char('m') if self.is_scheduler_up() => {
                self.current_view = Views::Metrics;
                Some(WebKeyAsyncAction::ReloadView)
            }
            KeyCode::Char('/') if self.is_jobs_view() || self.is_metrics_view() => {
                self.input_mode = InputMode::Edit;
                None
            }
            KeyCode::Char('1') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::Id);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::Host);
                } else if self.is_metrics_view() {
                    self.sort_metrics_by(MetricsSortColumn::Name);
                }
                None
            }
            KeyCode::Char('2') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::Name);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::Id);
                }
                None
            }
            KeyCode::Char('3') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::Status);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::CpuCores);
                }
                None
            }
            KeyCode::Char('4') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::StagesCompleted);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::TaskSlots);
                }
                None
            }
            KeyCode::Char('5') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::PercentComplete);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::ProcPhysicalMemoryUsage);
                }
                None
            }
            KeyCode::Char('6') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::StartTime);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::PeakPhysicalMemoryUsage);
                }
                None
            }
            KeyCode::Char('7') => {
                if self.is_jobs_view() {
                    self.sort_jobs_by(JobsSortColumn::Duration);
                } else if self.is_executors_view() {
                    self.sort_executors_by(ExecutorsSortColumn::LastSeen);
                }
                None
            }
            KeyCode::Char('c')
                if self.is_jobs_view() && self.input_mode == InputMode::View =>
            {
                let job_id = self
                    .jobs_data
                    .selected_job(&self.search_term)
                    .filter(|j| j.status == "Running" || j.status == "Queued")
                    .map(|j| j.job_id.clone());
                job_id.map(WebKeyAsyncAction::CancelJob)
            }
            KeyCode::Down => {
                if self.is_jobs_view() {
                    self.jobs_data.scroll_down();
                    let job_id = self
                        .jobs_data
                        .selected_job(&self.search_term)
                        .map(|j| j.job_id.clone());
                    self.job_details = None;
                    Some(WebKeyAsyncAction::UpdateJobDetails(job_id))
                } else if self.is_executors_view() {
                    self.executors_data.scroll_down();
                    None
                } else if self.is_metrics_view() {
                    self.metrics_data.scroll_down();
                    None
                } else {
                    None
                }
            }
            KeyCode::Up => {
                if self.is_jobs_view() {
                    self.jobs_data.scroll_up();
                    let job_id = self
                        .jobs_data
                        .selected_job(&self.search_term)
                        .map(|j| j.job_id.clone());
                    self.job_details = None;
                    Some(WebKeyAsyncAction::UpdateJobDetails(job_id))
                } else if self.is_executors_view() {
                    self.executors_data.scroll_up();
                    None
                } else if self.is_metrics_view() {
                    self.metrics_data.scroll_up();
                    None
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Async action descriptors returned by [`App::on_key_sync`] on the WASM path.
#[cfg(feature = "web")]
pub enum WebKeyAsyncAction {
    LoadJobStages(String),
    LoadExecutorDetails(String),
    LoadJobDot(String),
    CancelJob(String),
    UpdateJobDetails(Option<String>),
    ReloadView,
    LoadJobPlanTree(String),
    LoadStagePlan(String, StagePlanTab),
}

#[cfg(test)]
mod tests {
    use crate::tui::app::App;
    use crate::tui::app::{
        ExecutorsSortColumn, INVALID_DATE, JobsSortColumn, MetricsSortColumn,
    };
    use crate::tui::domain::{
        SchedulerState, SortOrder,
        executors::{Executor, ExecutorDetailsPopup, OsInfo, Specification},
        jobs::Job,
        jobs::stages::{JobStagesPopup, JobStagesResponse},
    };
    use crate::tui::infrastructure::Settings;

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
            end_time: 1,
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

    #[cfg(not(feature = "web"))]
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

    // --- open_job_plan_popup tests ---

    fn make_job_details(job_id: &str) -> crate::tui::domain::jobs::JobDetails {
        crate::tui::domain::jobs::JobDetails {
            job_id: job_id.to_string(),
            logical_plan: Some("logical".to_string()),
            physical_plan: Some("physical".to_string()),
            physical_plan_tree: Some("tree".to_string()),
            stage_plan: Some("stage".to_string()),
        }
    }

    #[test]
    fn open_job_plan_popup_opens_for_running_job_with_details() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Running")];
        app.jobs_data.table_state.select(Some(0));
        app.job_details = Some(make_job_details("j1"));
        app.open_job_plan_popup();
        assert!(app.job_plan_popup.is_some());
    }

    #[test]
    fn open_job_plan_popup_opens_for_completed_job_with_details() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Completed")];
        app.jobs_data.table_state.select(Some(0));
        app.job_details = Some(make_job_details("j1"));
        app.open_job_plan_popup();
        assert!(app.job_plan_popup.is_some());
    }

    #[test]
    fn open_job_plan_popup_does_not_open_without_details() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Running")];
        app.jobs_data.table_state.select(Some(0));
        app.job_details = None;
        app.open_job_plan_popup();
        assert!(app.job_plan_popup.is_none());
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

    // --- Popup state tests ---

    fn make_stages_popup() -> JobStagesPopup {
        JobStagesPopup::new("job1".to_string(), JobStagesResponse { stages: Vec::new() })
    }

    #[test]
    fn is_job_stages_popup_open_false_when_none() {
        let app = make_app();
        assert!(!app.is_job_stages_popup_open());
    }

    #[test]
    fn is_job_stages_popup_open_true_when_some() {
        let mut app = make_app();
        app.job_stages_popup = Some(make_stages_popup());
        assert!(app.is_job_stages_popup_open());
    }

    #[test]
    fn is_job_stage_no_details_popup_open_true() {
        let mut app = make_app();
        app.job_stages_popup = Some(make_stages_popup());
        assert!(app.is_job_stage_no_details_popup_open());
    }

    #[test]
    fn is_job_stage_tasks_popup_open_true() {
        let mut app = make_app();
        let mut popup = make_stages_popup();
        popup.set_tasks_view();
        app.job_stages_popup = Some(popup);
        assert!(app.is_job_stage_tasks_popup_open());
    }

    #[test]
    fn is_job_stage_plan_popup_open_true() {
        let mut app = make_app();
        let mut popup = make_stages_popup();
        popup.set_plan_view();
        app.job_stages_popup = Some(popup);
        assert!(app.is_job_stage_plan_popup_open());
    }

    #[test]
    fn is_job_stage_no_details_when_tasks_returns_false() {
        let mut app = make_app();
        let mut popup = make_stages_popup();
        popup.set_tasks_view();
        app.job_stages_popup = Some(popup);
        assert!(!app.is_job_stage_no_details_popup_open());
    }

    // --- has_selected_executor / is_executor_details_popup_open tests ---

    fn make_executor(id: &str) -> Executor {
        Executor {
            host: "host".to_string(),
            port: 8080,
            id: id.to_string(),
            last_seen: None,
            specification: Specification { task_slots: 4 },
            metrics: vec![],
            os_info: OsInfo {
                kernel_ver: "5.15".to_string(),
                num_disks: 1,
                open_files_limit: 1024,
                os_ver: "Ubuntu 22.04".to_string(),
                os_ver_long: "Ubuntu 22.04.1 LTS".to_string(),
                physical_cores: 4,
                system_name: "Linux".to_string(),
                total_available_disk_space: 50_000_000_000,
                total_disk_space: 100_000_000_000,
            },
        }
    }

    #[test]
    fn has_selected_executor_false_when_no_executors() {
        let app = make_app();
        assert!(!app.has_selected_executor());
    }

    #[test]
    fn has_selected_executor_false_when_no_selection() {
        let mut app = make_app();
        app.executors_data.executors = vec![make_executor("e1")];
        assert!(!app.has_selected_executor());
    }

    #[test]
    fn has_selected_executor_true_when_selected() {
        let mut app = make_app();
        app.executors_data.executors = vec![make_executor("e1")];
        app.executors_data.table_state.select(Some(0));
        assert!(app.has_selected_executor());
    }

    #[test]
    fn is_executor_details_popup_open_false_when_none() {
        let app = make_app();
        assert!(!app.is_executor_details_popup_open());
    }

    #[test]
    fn is_executor_details_popup_open_true_when_some() {
        let mut app = make_app();
        app.executor_details_popup = Some(ExecutorDetailsPopup::new(make_executor("e1")));
        assert!(app.is_executor_details_popup_open());
    }

    // --- format_size tests ---

    #[test]
    fn format_size_zero_bytes() {
        let app = make_app();
        assert_eq!(app.format_size(0), "0.0B");
    }

    #[test]
    fn format_size_bytes_below_kb_threshold() {
        let app = make_app();
        assert_eq!(app.format_size(1024), "1024.0B");
    }

    #[test]
    fn format_size_kilobytes() {
        let app = make_app();
        assert_eq!(app.format_size(2 * 1024), "2.0KB");
    }

    #[test]
    fn format_size_megabytes() {
        let app = make_app();
        assert_eq!(app.format_size(2 * 1024 * 1024), "2.0MB");
    }

    #[test]
    fn format_size_gigabytes() {
        let app = make_app();
        assert_eq!(app.format_size(2 * 1024 * 1024 * 1024), "2.0GB");
    }

    // --- is_selected_job_cancelable tests ---

    #[test]
    fn is_selected_job_cancelable_true_for_running() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Running")];
        app.jobs_data.table_state.select(Some(0));
        assert!(app.is_selected_job_cancelable());
    }

    #[test]
    fn is_selected_job_cancelable_true_for_queued() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Queued")];
        app.jobs_data.table_state.select(Some(0));
        assert!(app.is_selected_job_cancelable());
    }

    #[test]
    fn is_selected_job_cancelable_false_for_completed() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Completed")];
        app.jobs_data.table_state.select(Some(0));
        assert!(!app.is_selected_job_cancelable());
    }

    #[test]
    fn is_selected_job_completed_or_running_for_completed() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Completed")];
        app.jobs_data.table_state.select(Some(0));
        assert!(app.is_selected_job_completed_or_running());
    }

    #[test]
    fn is_selected_job_completed_or_running_for_running() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Running")];
        app.jobs_data.table_state.select(Some(0));
        assert!(app.is_selected_job_completed_or_running());
    }

    #[test]
    fn is_selected_job_completed_or_running_for_queued() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Queued")];
        app.jobs_data.table_state.select(Some(0));
        assert!(!app.is_selected_job_completed_or_running());
    }

    #[test]
    fn is_selected_job_completed_or_running_for_failed() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Failed")];
        app.jobs_data.table_state.select(Some(0));
        assert!(!app.is_selected_job_completed_or_running());
    }

    // --- has_selected_job with selection ---

    #[test]
    fn has_selected_job_true_when_selected() {
        let mut app = make_app();
        app.jobs_data.jobs = vec![make_job("j1", "Running")];
        app.jobs_data.table_state.select(Some(0));
        assert!(app.has_selected_job());
    }

    // --- format_datetime tests ---

    #[test]
    fn format_datetime_zero_timestamp_is_valid_format() {
        let app = make_app();
        let result = app.format_datetime(0);
        assert_ne!(result, INVALID_DATE);
    }

    #[test]
    fn format_datetime_known_timestamp_contains_year() {
        // 1_000_000_000_000 ms = 2001-09-09T01:46:40
        let app = make_app();
        let result = app.format_datetime(1_000_000_000_000);
        assert!(result.contains("2001"), "{result} must contain year 2001");
    }

    #[test]
    fn format_datetime_out_of_range_returns_invalid() {
        let app = make_app();
        assert_eq!(app.format_datetime(i64::MAX), "Invalid date");
    }

    // --- format_duration tests ---

    #[test]
    fn format_duration_zero_ms_returns_nanoseconds() {
        let app = make_app();
        assert_eq!(app.format_duration(0), "0ns");
    }

    #[test]
    fn format_duration_one_ms_returns_milliseconds() {
        let app = make_app();
        assert_eq!(app.format_duration(1), "1.00ms");
    }

    #[test]
    fn format_duration_one_second() {
        let app = make_app();
        assert_eq!(app.format_duration(1_000), "1.00s");
    }

    #[test]
    fn format_duration_large_value_returns_seconds() {
        let app = make_app();
        assert_eq!(app.format_duration(90_000), "90.00s");
    }

    // --- format_count tests ---

    #[test]
    fn format_count_zero() {
        let app = make_app();
        assert_eq!(app.format_count(0), "0");
    }

    #[test]
    fn format_count_below_thousand_returns_raw() {
        let app = make_app();
        assert_eq!(app.format_count(999), "999");
    }

    #[test]
    fn format_count_thousands_two_decimals() {
        let app = make_app();
        assert_eq!(app.format_count(1_000), "1.00K");
    }

    #[test]
    fn format_count_thousands_large_one_decimal() {
        let app = make_app();
        assert_eq!(app.format_count(100_000), "100.0K");
    }

    #[test]
    fn format_count_millions() {
        let app = make_app();
        assert_eq!(app.format_count(1_000_000), "1.00M");
    }

    #[test]
    fn format_count_billions() {
        let app = make_app();
        assert_eq!(app.format_count(1_000_000_000), "1.00B");
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn format_count_trillions() {
        let app = make_app();
        assert_eq!(app.format_count(1_000_000_000_000), "1.00T");
    }

    // --- format_size additional boundary ---

    #[test]
    fn format_size_terabytes() {
        let app = make_app();
        assert_eq!(app.format_size(2 * 1024 * 1024 * 1024 * 1024), "2.0TB");
    }
}
