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

use crate::tui::app::App;
use chrono::DateTime;
use ratatui::style::Style;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    widgets::{Block, Borders, Paragraph},
};

pub fn render_scheduler_state(f: &mut Frame, area: Rect, app: &App) -> bool {
    let (started, version, is_up) = match &app.dashboard_data.scheduler_state {
        Some(state) => {
            let started = DateTime::from_timestamp_millis(state.started)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Invalid Date".to_string());
            (started, state.version.clone(), true)
        }
        None => ("-".to_string(), "unknown".to_string(), false),
    };

    let scheduler_url = app.http_client.scheduler_url();

    if is_up {
        render_scheduler_state_up(f, area, scheduler_url, started, version);
    } else {
        render_scheduler_state_down(f, area, scheduler_url);
    }

    is_up
}

fn render_scheduler_state_down(f: &mut Frame, area: Rect, scheduler_url: &str) {
    let scheduler_url_block = Block::default()
        .borders(Borders::ALL)
        .style(Style::new().red())
        .title("Scheduler down");
    let scheduler_url_paragraph = Paragraph::new(scheduler_url)
        .block(scheduler_url_block)
        .alignment(Alignment::Left);
    f.render_widget(scheduler_url_paragraph, area);
}

fn render_scheduler_state_up(
    f: &mut Frame,
    area: Rect,
    scheduler_url: &str,
    started: String,
    version: String,
) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33), // url
            Constraint::Percentage(33), // started at
            Constraint::Percentage(33), // version
        ])
        .split(area);

    let scheduler_url_block = Block::default()
        .borders(Borders::ALL)
        .title("Scheduler URL");
    let scheduler_url_paragraph = Paragraph::new(scheduler_url)
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
