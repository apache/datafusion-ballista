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
use ratatui::Frame;
use ratatui::layout::Constraint;
use ratatui::prelude::{Color, Style};
use ratatui::text::Text;
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, HighlightSpacing, Row, Table,
};

pub(crate) fn render_stage_tasks_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_stages_popup else {
        return;
    };

    let Some(stage) = popup.selected_stage() else {
        return;
    };

    let area = crate::tui::ui::centered_rect(98, 70, f.area());
    f.render_widget(Clear, area);

    let header_style = Style::default()
        .fg(Color::LightYellow)
        .bg(Color::Black)
        .bold();
    let header = [
        "Task ID",
        "Status",
        "Input Rows",
        "Output Rows",
        "Partition ID",
        "Scheduled Time",
        "Launch latency",
        "Start latency",
        // "End Time",
        "Duration",
        "Total Time",
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
        .map(|(i, task)| build_stage_task_row(i, task, app));

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(7),  // Task ID
            Constraint::Percentage(9),  // Status
            Constraint::Percentage(9),  // Input Rows
            Constraint::Percentage(9),  // Output Rows
            Constraint::Percentage(9),  // Partition ID
            Constraint::Percentage(18), // Scheduled Time
            Constraint::Percentage(9),  // Launch latency
            Constraint::Percentage(9),  // Start latency
            Constraint::Percentage(9),  // Duration
            Constraint::Percentage(9),  // Total Time
        ],
    )
    .block(
        Block::default()
            .title(format!(
                " Tasks for stage '{}' of job '{}' ",
                stage.id, &popup.job_id
            ))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Indexed(193)).bold())
            .border_type(BorderType::Thick),
    )
    .header(header)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);

    f.render_widget(table, area);
}

fn build_stage_task_row(i: usize, task: &StageTaskResponse, app: &App) -> Row<'static> {
    let bg = if i.is_multiple_of(2) {
        Color::DarkGray
    } else {
        Color::Black
    };

    let status_color = match task.status.as_str() {
        "Running" => Color::LightBlue,
        "Successful" | "Completed" => Color::LightGreen,
        "Failed" => Color::LightRed,
        _ => Color::Gray,
    };

    Row::new(vec![
        Cell::from(Text::from(task.id.to_string()).centered()),
        Cell::from(
            Text::from(task.status.clone())
                .style(Style::default().fg(status_color).bold())
                .centered(),
        ),
        Cell::from(Text::from(app.format_count(task.input_rows)).centered()),
        Cell::from(Text::from(app.format_count(task.output_rows)).centered()),
        Cell::from(Text::from(task.partition_id.to_string()).centered()),
        Cell::from(Text::from(format_datetime(task.scheduled_time, app)).centered()),
        Cell::from(
            Text::from(app.format_duration(task.launch_time - task.scheduled_time))
                .centered(),
        ),
        Cell::from(
            Text::from(app.format_duration(task.start_exec_time - task.scheduled_time))
                .centered(),
        ),
        Cell::from(
            Text::from(app.format_duration(task.end_exec_time - task.start_exec_time))
                .centered(),
        ),
        Cell::from(
            Text::from(app.format_duration(task.finish_time - task.scheduled_time))
                .centered(),
        ),
    ])
    .style(Style::default().bg(bg))
}

fn format_datetime(dt: u64, app: &App) -> String {
    app.format_datetime(dt.try_into().unwrap_or(0))
}
