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
use crate::tui::domain::executors::{Executor, SortColumn};
use crate::tui::ui::vertical_scrollbar::render_scrollbar;
use ratatui::layout::Constraint;
use ratatui::prelude::{Color, Text};
use ratatui::style::Style;
use ratatui::widgets::{Cell, HighlightSpacing, Row, Table};
use ratatui::{
    Frame,
    layout::Rect,
    widgets::{Block, Borders, Paragraph},
};

pub fn render_executors(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default().borders(Borders::ALL).title("Executors");

    match &app.executors_data.executors {
        executors if !executors.is_empty() => {
            let mut scroll_state = app.executors_data.scrollbar_state;
            render_executors_table(f, area, app);
            render_scrollbar(f, area, &mut scroll_state);
        }
        _no_executors => {
            f.render_widget(no_live_executors(block), area);
        }
    };
}

fn render_executors_table(frame: &mut Frame, area: Rect, app: &App) {
    let header_style = Style::default().fg(Color::Yellow).bg(Color::Black);

    let sort_column = &app.executors_data.sort_column;
    let sort_order = &app.executors_data.sort_order;

    let host_suffix = match (sort_column, sort_order) {
        (SortColumn::Host, crate::tui::domain::SortOrder::Ascending) => " ▲",
        (SortColumn::Host, crate::tui::domain::SortOrder::Descending) => " ▼",
        _ => "",
    };
    let id_suffix = match (sort_column, sort_order) {
        (SortColumn::Id, crate::tui::domain::SortOrder::Ascending) => " ▲",
        (SortColumn::Id, crate::tui::domain::SortOrder::Descending) => " ▼",
        _ => "",
    };
    let last_seen_suffix = match (sort_column, sort_order) {
        (SortColumn::LastSeen, crate::tui::domain::SortOrder::Ascending) => " ▲",
        (SortColumn::LastSeen, crate::tui::domain::SortOrder::Descending) => " ▼",
        _ => "",
    };

    let header = [
        format!("Host{host_suffix}"),
        format!("Id{id_suffix}"),
        format!("Last seen{last_seen_suffix}"),
    ]
    .into_iter()
    .map(|item| Text::from(item).centered())
    .map(Cell::from)
    .collect::<Row>()
    .style(header_style)
    .height(1);

    let rows = app
        .executors_data
        .executors
        .iter()
        .enumerate()
        .map(|(i, executor)| {
            let color = match i % 2 {
                0 => Color::DarkGray,
                _ => Color::Black,
            };

            let host_cell = Cell::from(
                Text::from(format!("{}:{}", executor.host, executor.port)).centered(),
            );
            let id_cell = Cell::from(Text::from(executor.id.clone()).centered());
            let last_seen_cell = render_last_seen_cell(executor);

            let cells = vec![host_cell, id_cell, last_seen_cell];
            Row::new(cells).style(Style::default().bg(color))
        });

    let t = Table::new(
        rows,
        [
            Constraint::Percentage(33), // Host
            Constraint::Percentage(33), // Id
            Constraint::Percentage(33), // Last seen
        ],
    )
    .block(Block::default().borders(Borders::all()))
    .header(header)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);
    let mut table_state = app.executors_data.table_state;
    frame.render_stateful_widget(t, area, &mut table_state);
}

fn render_last_seen_cell(executor: &Executor) -> Cell<'_> {
    let last_seen = chrono::DateTime::from_timestamp_millis(executor.last_seen)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "Invalid Date".to_string());
    Cell::from(Text::from(last_seen).centered())
}

fn no_live_executors(block: Block<'_>) -> Paragraph<'_> {
    Paragraph::new("No live executors")
        .block(block.border_style(Style::new().red()))
        .centered()
}
