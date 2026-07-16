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
use crate::tui::ui::header::scheduler_state::render_scheduler_state;
use ratatui::style::Stylize;
use ratatui::widgets::BorderType;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    text::{Line, Text},
    widgets::{Block, Borders, Paragraph},
};

pub mod scheduler_state;

const MENU_ITEMS: [&str; 3] = ["Jobs", "Executors", "Metrics"];
const MENU_CONSTRAINTS: [Constraint; MENU_ITEMS.len()] = [
    Constraint::Percentage(33),
    Constraint::Percentage(34),
    Constraint::Percentage(33),
];

pub(super) fn render_header(f: &mut Frame, area: Rect, app: &App) {
    render_navbar(f, area, app);
}

fn render_navbar(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0), // Scheduler info
            Constraint::Min(0), // navigation
        ])
        .split(area);

    render_scheduler_state(f, chunks[0], app);
    render_menu(f, chunks[1], app);
}

fn render_menu(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(MENU_CONSTRAINTS)
        .split(area);

    for (index, menu_item) in MENU_ITEMS.iter().enumerate() {
        let mut block = Block::default()
            .borders(Borders::ALL)
            .border_style(app.theme.nav_inactive);

        let first_char = menu_item.chars().next().unwrap().underlined();
        let rest_chars = menu_item.chars().skip(1).collect::<String>();
        let line = Line::from(vec![first_char, rest_chars.into()]);
        let text = Text::from(line);

        let is_active = (app.is_executors_view() && *menu_item == "Executors")
            || (app.is_jobs_view() && *menu_item == "Jobs")
            || (app.is_metrics_view() && *menu_item == "Metrics");

        let style = if is_active && app.is_scheduler_up() {
            block = block
                .border_style(app.theme.nav_active)
                .border_type(BorderType::Thick);
            app.theme.nav_active
        } else {
            app.theme.nav_inactive
        };

        let paragraph = Paragraph::new(text)
            .style(style)
            .block(block.clone())
            .alignment(Alignment::Center);

        f.render_widget(paragraph, chunks[index]);
    }
}
