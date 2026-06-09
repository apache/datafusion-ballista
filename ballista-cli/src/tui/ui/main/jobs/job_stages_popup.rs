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
use crate::tui::ui::vertical_scrollbar;
use ratatui::Frame;
use ratatui::layout::Constraint;
use ratatui::text::Text;
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, HighlightSpacing, Row, Table,
};

pub(crate) fn render_job_stages_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_stages_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(85, 70, f.area());
    f.render_widget(Clear, area);

    let header_row = Row::new(vec![
        Cell::from(Text::from("Stage ID").right_aligned()),
        Cell::from(Text::from("Status").centered()),
        Cell::from(Text::from("Input Rows").right_aligned()),
        Cell::from(Text::from("Output Rows").right_aligned()),
        Cell::from(Text::from("Compute Time").right_aligned()),
        Cell::from(Text::from("Input percentiles\n(min/p25/med/p75/max)").centered()),
        Cell::from(Text::from("Duration percentiles\n(min/p25/med/p75/max)").centered()),
    ])
    .style(app.theme.table_header)
    .height(2);

    let rows = popup
        .stages
        .stages
        .iter()
        .enumerate()
        .map(|(i, stage)| build_stage_row(i, stage, app));

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(7),  // stage id
            Constraint::Percentage(7),  // status
            Constraint::Percentage(10), // input rows
            Constraint::Percentage(10), // output rows
            Constraint::Percentage(10), // elapsed compute
            Constraint::Percentage(28), // input percentiles
            Constraint::Percentage(28), // duration percentiles
        ],
    )
    .block(
        Block::default()
            .title(format!(" Stages for job '{}' ", popup.job_id.clone()))
            .borders(Borders::ALL)
            .border_style(app.theme.popup_border_jobs_stages)
            .border_type(BorderType::Thick),
    )
    .header(header_row)
    .row_highlight_style(app.theme.row_selected)
    .highlight_spacing(HighlightSpacing::Always);

    let mut table_state = popup.table_state;
    let mut scroll_state = popup.scrollbar_state;
    let rects = vertical_scrollbar::split_area(area);
    f.render_stateful_widget(table, rects[0], &mut table_state);
    vertical_scrollbar::render_scrollbar(f, rects[1], &mut scroll_state);
}

fn build_stage_row(i: usize, stage: &JobStageResponse, app: &App) -> Row<'static> {
    let row_style = if i.is_multiple_of(2) {
        app.theme.row_even
    } else {
        app.theme.row_odd
    };

    let status_style = match stage.status.as_str() {
        "Running" => app.theme.status_running,
        "Successful" | "Completed" => app.theme.status_completed,
        "Failed" => app.theme.status_failed,
        _ => app.theme.status_unknown,
    };

    let duration_percentiles = stage.task_duration_percentiles.as_ref().map_or_else(
        || "N/A".to_string(),
        |p| {
            format!(
                "{}/{}/{}/{}/{}",
                app.format_duration(p.min),
                app.format_duration(p.p25),
                app.format_duration(p.median),
                app.format_duration(p.p75),
                app.format_duration(p.max)
            )
        },
    );
    let input_percentiles = stage.task_input_percentiles.as_ref().map_or_else(
        || "N/A".to_string(),
        |p| {
            format!(
                "{}/{}/{}/{}/{}",
                app.format_count(p.min.try_into().unwrap_or(0)),
                app.format_count(p.p25.try_into().unwrap_or(0)),
                app.format_count(p.median.try_into().unwrap_or(0)),
                app.format_count(p.p75.try_into().unwrap_or(0)),
                app.format_count(p.max.try_into().unwrap_or(0))
            )
        },
    );

    Row::new(vec![
        Cell::from(Text::from(stage.id.clone()).right_aligned()),
        Cell::from(
            Text::from(stage.status.clone())
                .style(status_style)
                .centered(),
        ),
        Cell::from(Text::from(app.format_count(stage.input_rows)).right_aligned()),
        Cell::from(Text::from(app.format_count(stage.output_rows)).right_aligned()),
        Cell::from(
            Text::from(stage.elapsed_compute.clone().unwrap_or("N/A".to_string()))
                .right_aligned(),
        ),
        Cell::from(Text::from(input_percentiles).centered()),
        Cell::from(Text::from(duration_percentiles).centered()),
    ])
    .style(row_style)
}
