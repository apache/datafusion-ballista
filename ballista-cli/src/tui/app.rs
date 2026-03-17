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
    domain::{
        CancelJobResult, DashboardData, JobsData, MetricsData, SortColumn, SortOrder,
    },
    event::Event,
    infrastructure::Settings,
};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::widgets::TableState;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use crate::tui::http_client::HttpClient;

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

pub(crate) struct App {
    pub should_quit: bool,
    pub event_tx: Option<Sender<Event>>,
    current_view: Views,

    pub dashboard_data: DashboardData,
    pub metrics_data: MetricsData,
    pub jobs_data: JobsData,

    // Popups
    pub show_help: bool,
    pub show_scheduler_info: bool,
    pub cancel_job_result: Option<CancelJobResult>,

    input_mode: InputMode,
    pub search_term: String,

    pub http_client: Arc<HttpClient>,
}

impl App {
    pub fn new(config: Settings) -> TuiResult<Self> {
        Ok(Self {
            current_view: Views::Dashboard,
            should_quit: false,
            event_tx: None,
            show_help: false,
            show_scheduler_info: false,
            cancel_job_result: None,
            input_mode: InputMode::View,
            search_term: String::new(),
            dashboard_data: DashboardData::new(),
            jobs_data: JobsData {
                jobs: Vec::new(),
                table_state: TableState::default(),
                sort_column: SortColumn::None,
                sort_order: SortOrder::Ascending,
            },
            metrics_data: MetricsData {
                metrics: Vec::new(),
                table_state: TableState::default(),
            },
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

    pub fn set_event_tx(&mut self, tx: Sender<Event>) {
        self.event_tx = Some(tx);
    }

    pub async fn on_tick(&mut self) {
        if self.current_view == Views::Dashboard {
            self.load_dashboard_data().await;
        } else if self.current_view == Views::Jobs {
            self.load_jobs_data().await;
        } else if self.current_view == Views::Metrics {
            self.load_metrics_data().await;
        }
    }

    pub async fn on_key(&mut self, key: KeyEvent) -> TuiResult<()> {
        // Edit mode takes priority over everything
        if self.input_mode == InputMode::Edit {
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
            KeyCode::Char('/')
                if self.current_view == Views::Jobs
                    || self.current_view == Views::Metrics =>
            {
                self.input_mode = InputMode::Edit;
            }
            KeyCode::Char('s') if self.current_view == Views::Jobs => {
                self.sort_jobs_by(SortColumn::Status);
            }
            KeyCode::Char('p') if self.current_view == Views::Jobs => {
                self.sort_jobs_by(SortColumn::PercentComplete);
            }
            KeyCode::Char('t') if self.current_view == Views::Jobs => {
                self.sort_jobs_by(SortColumn::StartTime);
            }
            KeyCode::Char('c')
                if self.current_view == Views::Jobs
                    && self.input_mode == InputMode::View =>
            {
                self.cancel_selected_job().await;
            }
            KeyCode::Down => {
                if self.current_view == Views::Jobs {
                    self.jobs_data.table_state.scroll_down_by(1);
                } else if self.current_view == Views::Metrics {
                    self.metrics_data.table_state.scroll_down_by(1);
                }
            }
            KeyCode::Up => {
                if self.current_view == Views::Jobs {
                    self.jobs_data.table_state.scroll_up_by(1);
                } else if self.current_view == Views::Metrics {
                    self.metrics_data.table_state.scroll_up_by(1);
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn load_dashboard_data(&mut self) {
        if let Err(e) = crate::tui::ui::load_dashboard_data(self).await {
            tracing::error!("Failed to load dashboard data on tick: {e:?}");
        }
    }

    async fn load_jobs_data(&mut self) {
        if let Err(e) = crate::tui::ui::load_jobs_data(self).await {
            tracing::error!("Failed to load jobs data on tick: {e:?}");
        }
    }

    async fn load_metrics_data(&mut self) {
        if let Err(e) = crate::tui::ui::load_metrics_data(self).await {
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
}
