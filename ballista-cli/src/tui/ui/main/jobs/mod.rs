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

use crate::tui::TuiResult;
use crate::tui::app::App;
use crate::tui::domain::Job;
use crate::tui::error::TuiError;
use crate::tui::event::Event;
use crate::tui::event::UiData;

use ratatui::style::Color;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Margin, Rect},
    style::Style,
    text::Text,
    widgets::{
        Block, Borders, Cell, Clear, HighlightSpacing, Paragraph, Row, Scrollbar,
        ScrollbarOrientation, ScrollbarState, Table, TableState,
    },
};

pub async fn load_jobs_data(app: &App) -> TuiResult<()> {
    let jobs = match app.http_client.get_jobs().await {
        Ok(jobs) => jobs,
        Err(e) => {
            tracing::error!("Failed to load the jobs: {e:?}");
            Vec::new()
        }
    };

    match &app.event_tx {
        Some(event_tx) => {
            event_tx
                .send(Event::DataLoaded {
                    data: UiData::Jobs(jobs),
                })
                .map_err(TuiError::SendError)?;
        }
        None => {
            tracing::warn!("Jobs data loaded but event_tx is not set");
        }
    }

    Ok(())
}

pub fn render_jobs(f: &mut Frame, area: Rect, app: &App) {
    f.render_widget(Clear, area);

    match &app.jobs_data.jobs {
        jobs if !jobs.is_empty() => {
            let vertical = &Layout::vertical([
                Constraint::Min(5),    // Table
                Constraint::Length(4), // Scrollbar
            ]);
            let rects = vertical.split(area);

            let mut scroll_state = ScrollbarState::new((jobs.len() - 1) * 2);
            let mut table_state = TableState::default().with_selected(0);
            render_jobs_table(f, rects[0], jobs, &mut table_state);
            render_scrollbar(f, rects[0], &mut scroll_state);
        }
        _no_jobs => render_no_jobs(f, area),
    }
}

fn render_no_jobs(f: &mut Frame, area: Rect) {
    let block = Block::default().borders(Borders::all());
    let paragraph = Paragraph::new("No registered jobs in the scheduler!")
        .style(Style::default().bold())
        .centered()
        .block(block);
    f.render_widget(paragraph, area);
}

fn render_jobs_table(
    frame: &mut Frame,
    area: Rect,
    jobs: &[Job],
    state: &mut TableState,
) {
    let header_style = Style::default().fg(Color::Yellow).bg(Color::Black);

    let header = [
        "Id",
        "Name",
        "Status",
        "Stages Completed",
        "Percent Completed",
    ]
    .into_iter()
    .map(|item| Text::from(item).centered())
    .map(Cell::from)
    .collect::<Row>()
    .style(header_style)
    .height(1);

    let rows = jobs.iter().enumerate().map(|(i, job)| {
        let color = match i % 2 {
            0 => Color::DarkGray,
            _ => Color::Black,
        };

        let id_cell = Cell::from(Text::from(job.job_id.clone()).centered());
        let name_cell = Cell::from(Text::from(job.job_name.clone()).centered());
        let status_cell = render_job_status_cell(job);
        let stage_completion_cell = render_job_stage_completion_cell(job);
        let percent_completion_cell = render_job_percent_completion_cell(job);

        Row::new(vec![
            id_cell,
            name_cell,
            status_cell,
            stage_completion_cell,
            percent_completion_cell,
        ])
        .style(Style::default().bg(color))
    });

    let bar = " █ ";
    let t = Table::new(
        rows,
        [
            Constraint::Percentage(20), // Id
            Constraint::Percentage(20), // Name
            Constraint::Percentage(20), // Status
            Constraint::Percentage(20), // Stages Completed
            Constraint::Percentage(20), // Percent Completed
        ],
    )
    .block(Block::default().borders(Borders::all()))
    .header(header)
    // .row_highlight_style(selected_row_style)
    // .column_highlight_style(selected_col_style)
    // .cell_highlight_style(selected_cell_style)
    .highlight_symbol(Text::from(vec![
        "".into(),
        bar.into(),
        bar.into(),
        "".into(),
    ]))
    // .bg(self.colors.buffer_bg)
    .highlight_spacing(HighlightSpacing::Always);
    frame.render_stateful_widget(t, area, state);
}

fn render_job_percent_completion_cell(job: &Job) -> Cell<'_> {
    Cell::from(Text::from(format!("{}%", job.percent_complete)).centered())
}

fn render_job_stage_completion_cell(job: &Job) -> Cell<'_> {
    let stages_completion = job.completed_stages as f32 / job.num_stages as f32;
    let stage_completion = format!(
        "{:.2}% ({} / {})",
        stages_completion * 100.0,
        job.completed_stages,
        job.num_stages
    );
    Cell::from(Text::from(stage_completion).centered())
}

fn render_job_status_cell(job: &Job) -> Cell<'_> {
    let color = match job.status.as_str() {
        "Running" => Color::LightBlue,
        "Queued" => Color::Magenta,
        "Failed" => Color::Red,
        "Completed" => Color::Green,
        _ => Color::Gray,
    };
    let text = Text::from(job.status.clone()).style(Style::default().fg(color));
    Cell::from(text.centered())
}

fn render_scrollbar(frame: &mut Frame, area: Rect, scroll_state: &mut ScrollbarState) {
    frame.render_stateful_widget(
        Scrollbar::default()
            .orientation(ScrollbarOrientation::VerticalRight)
            .begin_symbol(None)
            .end_symbol(None),
        area.inner(Margin {
            vertical: 1,
            horizontal: 1,
        }),
        scroll_state,
    );
}
