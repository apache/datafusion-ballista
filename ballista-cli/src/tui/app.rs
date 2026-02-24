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

use crate::tui::{domain::DashboardData, event::Event, infrastructure::Settings};
use color_eyre::eyre::{Ok, Result};
use crossterm::event::{KeyCode, KeyEvent};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

use crate::tui::http_client::HttpClient;

#[derive(Debug, PartialEq)]
pub enum Views {
    Dashboard,
    Jobs,
    Metrics,
}

pub struct App {
    pub should_quit: bool,
    pub event_tx: Option<UnboundedSender<Event>>,
    pub current_view: Views,

    pub dashboard_data: DashboardData,

    // Help panel
    pub show_help: bool,

    pub http_client: Arc<HttpClient>,
}

impl App {
    pub fn new(config: Settings) -> Result<Self> {
        Ok(Self {
            current_view: Views::Dashboard,
            should_quit: false,
            event_tx: None,
            show_help: false,
            dashboard_data: DashboardData::new(),
            http_client: Arc::new(HttpClient::new(config)?),
        })
    }

    pub fn is_scheduler_up(&self) -> bool {
        self.dashboard_data.scheduler_state.is_some()
    }

    pub fn set_event_tx(&mut self, tx: UnboundedSender<Event>) {
        self.event_tx = Some(tx);
        // self.load_data();
    }

    pub async fn on_tick(&mut self) {
        self.load_dashboard_data().await;
    }

    pub async fn on_key(&mut self, key: KeyEvent) -> Result<()> {
        // Help panel takes priority
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
            // KeyCode::Up | KeyCode::Char('k') => {
            //     self.previous();
            // }
            // KeyCode::Down | KeyCode::Char('j') => {
            //     self.next();
            // }
            KeyCode::Char('d') if self.is_scheduler_up() => {
                self.current_view = Views::Dashboard;
                self.load_dashboard_data().await;
            }
            KeyCode::Char('j') if self.is_scheduler_up() => {
                self.current_view = Views::Jobs;
            }
            KeyCode::Char('m') if self.is_scheduler_up() => {
                self.current_view = Views::Metrics;
            }
            _ => {}
        }
        Ok(())
    }

    async fn load_dashboard_data(&mut self) {
        if let Err(e) = crate::tui::ui::load_dashboard_data(self).await {
            tracing::error!("Failed to load dashboard data on tick: {:?}", e);
        }
    }
}
