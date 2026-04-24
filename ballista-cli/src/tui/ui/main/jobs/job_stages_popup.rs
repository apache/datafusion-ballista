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
use crate::tui::domain::jobs::stages::JobStageResponse;
use ratatui::Frame;
use ratatui::layout::Constraint;
use ratatui::prelude::{Color, Style};
use ratatui::text::Text;
use ratatui::widgets::{Block, Borders, Cell, Clear, HighlightSpacing, Row, Table};

pub(crate) fn render_job_stages_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_stages_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(80, 70, f.area());
    f.render_widget(Clear, area);

    let job_id = popup.stages.stages.first().map(|_| "").unwrap_or("");
    let _ = job_id;

    let header_style = Style::default().fg(Color::Yellow).bg(Color::Black);
    let header = [
        "Stage ID",
        "Status",
        "Input Rows",
        "Output Rows",
        "Elapsed Compute",
        "Task Durations (min/med/max)",
    ]
    .into_iter()
    .map(|h| Cell::from(Text::from(h).centered()))
    .collect::<Row>()
    .style(header_style)
    .height(1);

    let rows = popup
        .stages
        .stages
        .iter()
        .enumerate()
        .map(|(i, stage)| build_stage_row(i, stage));

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(15),
            Constraint::Percentage(12),
            Constraint::Percentage(13),
            Constraint::Percentage(13),
            Constraint::Percentage(20),
            Constraint::Percentage(27),
        ],
    )
    .block(
        Block::default()
            .title(" Job Stages (↑↓ navigate | Enter tasks | Esc close) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    )
    .header(header)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);

    let mut table_state = popup.table_state.clone();
    f.render_stateful_widget(table, area, &mut table_state);
}

fn build_stage_row(i: usize, stage: &JobStageResponse) -> Row<'static> {
    let bg = if i % 2 == 0 {
        Color::DarkGray
    } else {
        Color::Black
    };

    let status_color = match stage.status.as_str() {
        "Running" => Color::LightBlue,
        "Completed" => Color::Green,
        "Failed" => Color::Red,
        _ => Color::Gray,
    };

    let p = &stage.task_duration_percentiles;
    let task_summary = format!("min={}ms med={}ms max={}ms", p.min, p.median, p.max);

    Row::new(vec![
        Cell::from(Text::from(stage.id.clone()).centered()),
        Cell::from(
            Text::from(stage.status.clone())
                .style(Style::default().fg(status_color))
                .centered(),
        ),
        Cell::from(Text::from(stage.input_rows.to_string()).centered()),
        Cell::from(Text::from(stage.output_rows.to_string()).centered()),
        Cell::from(Text::from(stage.elapsed_compute.clone()).centered()),
        Cell::from(Text::from(task_summary).centered()),
    ])
    .style(Style::default().bg(bg))
}
