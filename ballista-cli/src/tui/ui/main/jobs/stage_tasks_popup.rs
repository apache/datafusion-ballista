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
use crate::tui::domain::jobs::stages::StageTaskResponse;
use datafusion::common::human_readable_count;
use ratatui::Frame;
use ratatui::layout::Constraint;
use ratatui::prelude::{Color, Style};
use ratatui::text::Text;
use ratatui::widgets::{Block, Borders, Cell, Clear, HighlightSpacing, Row, Table};

pub(crate) fn render_stage_tasks_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_stages_popup else {
        return;
    };

    let Some(stage) = popup.selected_stage() else {
        return;
    };

    let area = crate::tui::ui::centered_rect(98, 55, f.area());
    f.render_widget(Clear, area);

    let header_style = Style::default().fg(Color::Yellow).bg(Color::Black);
    let header = [
        "Task ID",
        "Status",
        "Input Rows",
        "Output Rows",
        "Partition ID",
        // "Scheduled Time",
        "Launch Time",
        "Start Time",
        "End Time",
        "Duration",
        "Finish Time",
    ]
    .into_iter()
    .map(|h| Cell::from(Text::from(h).centered()))
    .collect::<Row>()
    .style(header_style)
    .height(1);

    let rows = stage
        .tasks
        .iter()
        .enumerate()
        .map(|(i, task)| build_stage_task_row(i, task));

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(5),
            Constraint::Percentage(9),
            Constraint::Percentage(7),
            Constraint::Percentage(7),
            // Constraint::Percentage(9),
            Constraint::Percentage(8),
            Constraint::Percentage(12),
            Constraint::Percentage(12),
            Constraint::Percentage(9),
            Constraint::Percentage(9),
            Constraint::Percentage(9),
        ],
    )
    .block(
        Block::default()
            .title(format!(
                " Tasks for stage '{}' of job '{}' (Esc close) ",
                stage.id,
                popup.job_id.clone()
            ))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::LightYellow)),
    )
    .header(header)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);

    f.render_widget(table, area);
}

fn build_stage_task_row(i: usize, task: &StageTaskResponse) -> Row<'static> {
    let bg = if i.is_multiple_of(2) {
        Color::DarkGray
    } else {
        Color::Black
    };

    let status_color = match task.status.as_str() {
        "Running" => Color::LightBlue,
        "Successful" | "Completed" => Color::Green,
        "Failed" => Color::Red,
        _ => Color::Gray,
    };

    Row::new(vec![
        Cell::from(Text::from(task.id.to_string()).centered()),
        Cell::from(
            Text::from(task.status.clone())
                .style(Style::default().fg(status_color))
                .centered(),
        ),
        Cell::from(Text::from(human_readable_count(task.input_rows)).centered()),
        Cell::from(Text::from(human_readable_count(task.output_rows)).centered()),
        Cell::from(Text::from(task.partition_id.to_string()).centered()),
        // Cell::from(Text::from(format_datetime(task.scheduled_time)).centered()),
        Cell::from(Text::from(format_datetime(task.launch_time)).centered()),
        Cell::from(Text::from(format_datetime(task.start_exec_time)).centered()),
        Cell::from(Text::from(format_time(task.end_exec_time)).centered()),
        Cell::from(Text::from(task.exec_duration.to_string()).centered()),
        Cell::from(Text::from(format_time(task.finish_time)).centered()),
    ])
    .style(Style::default().bg(bg))
}

fn format_datetime(dt: u64) -> String {
    chrono::DateTime::from_timestamp_millis(dt.try_into().unwrap_or(0))
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "Invalid Date".to_string())
}

fn format_time(dt: u64) -> String {
    chrono::DateTime::from_timestamp_millis(dt.try_into().unwrap_or(0))
        .map(|dt| dt.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "Invalid Date".to_string())
}
