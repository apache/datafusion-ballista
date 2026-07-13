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
use crate::tui::domain::jobs::stages::{StageTaskResponse, StageTaskStatus};
use crate::tui::ui::components::clear_area::clear_area;
use crate::tui::ui::vertical_scrollbar;
use ratatui::Frame;
use ratatui::layout::Constraint;
use ratatui::text::Text;
use ratatui::widgets::{Block, BorderType, Borders, Cell, HighlightSpacing, Row, Table};

pub(crate) fn render_stage_tasks_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_stages_popup else {
        return;
    };

    let Some(stage) = popup.selected_stage() else {
        return;
    };

    let area = crate::tui::ui::centered_rect(98, 70, f.area());
    clear_area(f, area, app);

    let header_row = Row::new(vec![
        Cell::from(Text::from("Task ID".to_string()).right_aligned()),
        Cell::from(Text::from("Status".to_string()).centered()),
        Cell::from(Text::from("Input Rows".to_string()).right_aligned()),
        Cell::from(Text::from("Output Rows".to_string()).right_aligned()),
        Cell::from(Text::from("Partition ID".to_string()).right_aligned()),
        Cell::from(Text::from("Scheduled Time".to_string()).centered()),
        Cell::from(Text::from("Launch Latency".to_string()).right_aligned()),
        Cell::from(Text::from("Start Latency".to_string()).right_aligned()),
        Cell::from(Text::from("Duration".to_string()).right_aligned()),
        Cell::from(Text::from("Total Time".to_string()).right_aligned()),
    ])
    .style(app.theme.table_header)
    .height(1);

    let rows = stage
        .tasks
        .iter()
        .flatten()
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
                stage.id, popup.job_id
            ))
            .borders(Borders::ALL)
            .border_style(app.theme.popup_border_alt)
            .border_type(BorderType::Thick),
    )
    .header(header_row)
    .row_highlight_style(app.theme.row_selected)
    .highlight_spacing(HighlightSpacing::Always);

    let mut table_state = popup.tasks_table_state;
    let mut scroll_state = popup.tasks_scrollbar_state;
    let rects = vertical_scrollbar::split_area(area);
    f.render_stateful_widget(table, rects[0], &mut table_state);
    crate::tui::ui::vertical_scrollbar::render_scrollbar(f, rects[1], &mut scroll_state);
}

fn build_stage_task_row(i: usize, task: &StageTaskResponse, app: &App) -> Row<'static> {
    let row_style = if i.is_multiple_of(2) {
        app.theme.row_even
    } else {
        app.theme.row_odd
    };

    let status_text = match &task.status {
        StageTaskStatus::Running => Text::from("Running").style(app.theme.status_running),
        StageTaskStatus::Successful => {
            Text::from("Successful").style(app.theme.status_completed)
        }
        StageTaskStatus::Failed { reason } => {
            Text::from(reason.clone()).style(app.theme.status_failed)
        }
    };

    Row::new(vec![
        Cell::from(Text::from(task.id.to_string()).right_aligned()),
        Cell::from(status_text.centered()),
        Cell::from(Text::from(app.format_count(task.input_rows)).right_aligned()),
        Cell::from(Text::from(app.format_count(task.output_rows)).right_aligned()),
        Cell::from(Text::from(task.partition_id.to_string()).right_aligned()),
        Cell::from(Text::from(format_datetime(task.scheduled_time, app)).centered()),
        Cell::from(
            Text::from(
                app.format_duration(task.launch_time.saturating_sub(task.scheduled_time)),
            )
            .right_aligned(),
        ),
        Cell::from(
            Text::from(app.format_duration(
                task.start_exec_time.saturating_sub(task.scheduled_time),
            ))
            .right_aligned(),
        ),
        Cell::from(
            Text::from(app.format_duration(
                task.end_exec_time.saturating_sub(task.start_exec_time),
            ))
            .right_aligned(),
        ),
        Cell::from(
            Text::from(
                app.format_duration(task.finish_time.saturating_sub(task.scheduled_time)),
            )
            .right_aligned(),
        ),
    ])
    .style(row_style)
}

fn format_datetime(dt: u64, app: &App) -> String {
    app.format_datetime(dt.try_into().unwrap_or(0))
}
