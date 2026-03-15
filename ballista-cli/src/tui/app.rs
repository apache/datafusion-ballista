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

use crate::tui::{
    domain::{DashboardData, JobsData, MetricsData},
    event::Event,
    infrastructure::Settings,
};
use color_eyre::eyre::{Ok, Result};
use crossterm::event::{KeyCode, KeyEvent};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use crate::tui::http_client::HttpClient;

#[derive(Debug, PartialEq)]
pub(crate) enum Views {
    Dashboard,
    Jobs,
    Metrics,
}

#[derive(Debug, PartialEq)]
pub(crate) enum InputMode {
    View,
    Edit,
}

pub(crate) struct App {
    pub should_quit: bool,
    pub event_tx: Option<Sender<Event>>,
    pub current_view: Views,

    pub dashboard_data: DashboardData,
    pub metrics_data: MetricsData,
    pub jobs_data: JobsData,

    // Help panel
    pub show_help: bool,

    pub input_mode: InputMode,
    pub search_term: String,

    pub http_client: Arc<HttpClient>,
}

impl App {
    pub fn new(config: Settings) -> Result<Self> {
        Ok(Self {
            current_view: Views::Dashboard,
            should_quit: false,
            event_tx: None,
            show_help: false,
            input_mode: InputMode::View,
            search_term: String::new(),
            dashboard_data: DashboardData::new(),
            jobs_data: JobsData { jobs: Vec::new() },
            metrics_data: MetricsData {
                metrics: Vec::new(),
            },
            http_client: Arc::new(HttpClient::new(config)?),
        })
    }

    pub fn is_scheduler_up(&self) -> bool {
        self.dashboard_data.scheduler_state.is_some()
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

    pub async fn on_key(&mut self, key: KeyEvent) -> Result<()> {
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

        if self.show_help {
            self.show_help = false;
            return Ok(());
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.should_quit = true;
            }
            KeyCode::Char('?') | KeyCode::Char('h') => {
                self.show_help = true;
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
}
