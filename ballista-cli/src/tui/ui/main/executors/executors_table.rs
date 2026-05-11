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

    macro_rules! sort_suffix {
        ($column:path, $sort_column:expr, $sort_order:expr) => {
            match ($sort_column, $sort_order) {
                ($column, crate::tui::domain::SortOrder::Ascending) => " ▲",
                ($column, crate::tui::domain::SortOrder::Descending) => " ▼",
                _ => "",
            }
        };
    }

    let host_suffix = sort_suffix!(SortColumn::Host, sort_column, sort_order);
    let id_suffix = sort_suffix!(SortColumn::Id, sort_column, sort_order);
    let task_slots_suffix = sort_suffix!(SortColumn::TaskSlots, sort_column, sort_order);
    let proc_physical_memory_suffix =
        sort_suffix!(SortColumn::ProcPhysicalMemoryUsage, sort_column, sort_order);
    let peak_physical_memory_suffix =
        sort_suffix!(SortColumn::PeakPhysicalMemoryUsage, sort_column, sort_order);
    let last_seen_suffix = sort_suffix!(SortColumn::LastSeen, sort_column, sort_order);

    let header = [
        format!("Host{host_suffix}"),
        format!("Id{id_suffix}"),
        format!("Task Slots{task_slots_suffix}"),
        format!("Physical Memory{proc_physical_memory_suffix}"),
        format!("Peak Physical Memory{peak_physical_memory_suffix}"),
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
            let task_slots_cell = Cell::from(
                Text::from(app.format_count(executor.specification.task_slots as usize))
                    .centered(),
            );
            let proc_physical_memory_cell = Cell::from(
                Text::from(
                    app.format_size(executor.proc_physical_memory_usage() as usize),
                )
                .centered(),
            );
            let peak_physical_memory_cell = Cell::from(
                Text::from(
                    app.format_size(executor.peak_physical_memory_usage() as usize),
                )
                .centered(),
            );
            let last_seen_cell = render_last_seen_cell(executor, app);

            let cells = vec![
                host_cell,
                id_cell,
                task_slots_cell,
                proc_physical_memory_cell,
                peak_physical_memory_cell,
                last_seen_cell,
            ];
            Row::new(cells).style(Style::default().bg(color))
        });

    let t = Table::new(
        rows,
        [
            Constraint::Percentage(10), // Host
            Constraint::Percentage(15), // Id
            Constraint::Percentage(15), // Task slots
            Constraint::Percentage(20), // Proc physical memory
            Constraint::Percentage(20), // Peak physical memory
            Constraint::Percentage(20), // Last seen
        ],
    )
    .block(Block::default().borders(Borders::all()))
    .header(header)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);
    let mut table_state = app.executors_data.table_state;
    frame.render_stateful_widget(t, area, &mut table_state);
}

fn render_last_seen_cell<'a>(executor: &'a Executor, app: &App) -> Cell<'a> {
    let last_seen = super::format_last_seen(executor, app);
    Cell::from(Text::from(last_seen).centered())
}

fn no_live_executors(block: Block<'_>) -> Paragraph<'_> {
    Paragraph::new("No live executors")
        .block(block.border_style(Style::new().red()))
        .centered()
}
