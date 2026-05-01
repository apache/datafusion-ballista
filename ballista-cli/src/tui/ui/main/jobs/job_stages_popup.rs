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
use datafusion::common::human_readable_count;
use ratatui::Frame;
use ratatui::layout::Constraint;
use ratatui::prelude::{Color, Style};
use ratatui::text::Text;
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, HighlightSpacing, Row, Table,
};

pub(crate) fn render_job_stages_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_stages_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(80, 70, f.area());
    f.render_widget(Clear, area);

    let header_style = Style::default().fg(Color::Yellow).bg(Color::Black);
    let header = [
        "Stage ID",
        "Status",
        "Input Rows",
        "Output Rows",
        "Elapsed Compute",
        "Input percentiles\n(min/p25/med/p75/max ms)",
        "Duration percentiles\n(min/p25/med/p75/max ms)",
    ]
    .into_iter()
    .map(|h| Cell::from(Text::from(h).centered()))
    .collect::<Row>()
    .style(header_style)
    .height(2);

    let rows = popup
        .stages
        .stages
        .iter()
        .enumerate()
        .map(|(i, stage)| build_stage_row(i, stage));

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(12),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
            Constraint::Percentage(17),
            Constraint::Percentage(17),
            Constraint::Percentage(24),
        ],
    )
    .block(
        Block::default()
            .title(format!(" Stages for job '{}' ", popup.job_id.clone()))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::LightBlue))
            .border_type(BorderType::Thick),
    )
    .header(header)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);

    let mut table_state = popup.table_state;
    f.render_stateful_widget(table, area, &mut table_state);
}

fn build_stage_row(i: usize, stage: &JobStageResponse) -> Row<'static> {
    let bg = if i.is_multiple_of(2) {
        Color::DarkGray
    } else {
        Color::Black
    };

    let status_color = match stage.status.as_str() {
        "Running" => Color::LightBlue,
        "Successful" | "Completed" => Color::Green,
        "Failed" => Color::Red,
        _ => Color::Gray,
    };

    let p = &stage.task_duration_percentiles;
    let duration_percentiles =
        format!("{}/{}/{}/{}/{}", p.min, p.p25, p.median, p.p75, p.max);
    let p = &stage.task_input_percentiles;
    let input_percentiles =
        format!("{}/{}/{}/{}/{}", p.min, p.p25, p.median, p.p75, p.max);

    Row::new(vec![
        Cell::from(Text::from(stage.id.clone()).centered()),
        Cell::from(
            Text::from(stage.status.clone())
                .style(Style::default().fg(status_color))
                .centered(),
        ),
        Cell::from(Text::from(human_readable_count(stage.input_rows)).centered()),
        Cell::from(Text::from(human_readable_count(stage.output_rows)).centered()),
        Cell::from(Text::from(stage.elapsed_compute.clone()).centered()),
        Cell::from(Text::from(input_percentiles).centered()),
        Cell::from(Text::from(duration_percentiles).centered()),
    ])
    .style(Style::default().bg(bg))
}
