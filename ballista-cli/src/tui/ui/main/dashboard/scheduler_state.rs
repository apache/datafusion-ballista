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

use chrono::DateTime;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    widgets::{Block, Borders, Paragraph},
};

use crate::tui::app::App;

pub fn render_scheduler_state(f: &mut Frame, area: Rect, app: &App) {
    let (started, version) = match &app.dashboard_data.scheduler_state {
        Some(state) => {
            let started = DateTime::from_timestamp_millis(state.started)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Invalid Date".to_string());
            (started, state.version.clone())
        }
        None => ("-".to_string(), "unknown".to_string()),
    };

    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(13),
            Constraint::Percentage(13),
            Constraint::Percentage(10),
            Constraint::Min(0),
        ])
        .split(vertical_chunks[0]);

    let scheduler_url_block = Block::default()
        .borders(Borders::ALL)
        .title("Scheduler URL");
    let scheduler_url_paragraph = Paragraph::new(app.http_client.scheduler_url())
        .block(scheduler_url_block)
        .alignment(Alignment::Left);
    f.render_widget(scheduler_url_paragraph, chunks[0]);

    let started_block = Block::default().borders(Borders::ALL).title("Started at");
    let started_paragraph = Paragraph::new(started)
        .block(started_block)
        .alignment(Alignment::Left);
    f.render_widget(started_paragraph, chunks[1]);

    let version_block = Block::default().borders(Borders::ALL).title("Version");
    let version_paragraph = Paragraph::new(version)
        .block(version_block)
        .alignment(Alignment::Left);
    f.render_widget(version_paragraph, chunks[2]);
}
