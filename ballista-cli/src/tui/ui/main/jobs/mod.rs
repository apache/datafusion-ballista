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

mod dot_parser;
pub mod job_dot_popup;
pub mod job_plan_popup;
pub mod job_stages_popup;
pub mod stage_tasks_popup;

use crate::tui::{
    TuiResult,
    app::App,
    domain::{
        SortOrder,
        jobs::{Job, SortColumn},
    },
    event::{Event, UiData},
    ui::search_box::render_search_box,
    ui::vertical_scrollbar::render_scrollbar,
};

use ratatui::style::Color;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::Style,
    text::Text,
    widgets::{
        Block, Borders, Cell, Clear, HighlightSpacing, Paragraph, Row, Table, TableState,
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

    app.send_event(Event::DataLoaded {
        data: UiData::Jobs(jobs),
    })
    .await
}

pub async fn load_job_dot(app: &App, job_id: &str) -> TuiResult<()> {
    match app.http_client.get_job_dot(job_id).await {
        Ok(dot_content) => {
            let graph = dot_parser::parse_dot(job_id, &dot_content);
            app.send_event(Event::DataLoaded {
                data: UiData::JobStagesGraph(graph),
            })
            .await
        }
        Err(e) => {
            tracing::error!("Failed to load job dot for {job_id}: {e:?}");
            Ok(())
        }
    }
}

pub async fn load_job_stages_popup(app: &App, job_id: &str) -> TuiResult<()> {
    let mut stages = app
        .http_client
        .get_job_stages(job_id)
        .await
        .inspect(|stages| tracing::trace!("Loaded stages for job '{job_id}': {stages:?}"))
        .inspect_err(|e| {
            tracing::error!("Failed to load stages for job '{job_id}': {e:?}")
        })?;

    stages.stages.sort_by(|a, b| a.id.cmp(&b.id));

    app.send_event(Event::DataLoaded {
        data: UiData::JobStagesData(job_id.to_owned(), stages),
    })
    .await
}

pub async fn load_job_details(app: &App, job_id: &str) -> TuiResult<()> {
    let details = match app.http_client.get_job_details(job_id).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!("Failed to load job details for {job_id}: {e:?}");
            return Ok(());
        }
    };
    app.send_event(Event::DataLoaded {
        data: UiData::JobDetails(details),
    })
    .await
}

pub fn render_jobs(f: &mut Frame, area: Rect, app: &App) {
    f.render_widget(Clear, area);

    let search_term = app.search_term.to_lowercase();
    let filtered_jobs: Vec<&Job> = if search_term.is_empty() {
        app.jobs_data.jobs.iter().collect()
    } else {
        app.jobs_data
            .jobs
            .iter()
            .filter(|j| {
                j.job_id.to_lowercase().contains(&search_term)
                    || j.job_name.to_lowercase().contains(&search_term)
            })
            .collect()
    };

    let rects = Layout::vertical([
        Constraint::Length(3), // Search box
        Constraint::Min(5),    // Table
        Constraint::Length(4), // Scrollbar padding
    ])
    .split(area);

    render_search_box(f, rects[0], app);

    let mut sorted_jobs = filtered_jobs;
    app.jobs_data.sort_jobs(&mut sorted_jobs);

    if !sorted_jobs.is_empty() {
        let mut scroll_state = app.jobs_data.scrollbar_state;
        let mut table_state = app.jobs_data.table_state;
        render_jobs_table(
            f,
            rects[1],
            &sorted_jobs,
            &mut table_state,
            &app.jobs_data.sort_column,
            &app.jobs_data.sort_order,
        );
        render_scrollbar(f, rects[1], &mut scroll_state);
    } else {
        render_no_jobs(f, rects[1]);
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

fn column_suffix(
    active_sort_column: &SortColumn,
    sort_order: &SortOrder,
    sort_column: &SortColumn,
) -> &'static str {
    match (active_sort_column, sort_order) {
        (sc, SortOrder::Ascending) if sc == sort_column => " ▲",
        (sc, SortOrder::Descending) if sc == sort_column => " ▼",
        _ => "",
    }
}

fn render_jobs_table(
    frame: &mut Frame,
    area: Rect,
    jobs: &[&Job],
    state: &mut TableState,
    sort_column: &SortColumn,
    sort_order: &SortOrder,
) {
    let header_style = Style::default().fg(Color::Yellow).bg(Color::Black);

    let id_suffix = column_suffix(sort_column, sort_order, &SortColumn::Id);
    let name_suffix = column_suffix(sort_column, sort_order, &SortColumn::Name);
    let status_suffix = column_suffix(sort_column, sort_order, &SortColumn::Status);
    let stages_suffix =
        column_suffix(sort_column, sort_order, &SortColumn::StagesCompleted);
    let percent_suffix =
        column_suffix(sort_column, sort_order, &SortColumn::PercentComplete);
    let start_time_suffix =
        column_suffix(sort_column, sort_order, &SortColumn::StartTime);

    let header = [
        format!("Id{id_suffix}"),
        format!("Name{name_suffix}"),
        format!("Status{status_suffix}"),
        format!("Stages Completes{stages_suffix}"),
        format!("Percent Completed{percent_suffix}"),
        format!("Start time{start_time_suffix}"),
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
        let start_time_cell = render_job_start_time_cell(job);

        let cells = vec![
            id_cell,
            name_cell,
            status_cell,
            stage_completion_cell,
            percent_completion_cell,
            start_time_cell,
        ];
        Row::new(cells).style(Style::default().bg(color))
    });

    let t = Table::new(
        rows,
        [
            Constraint::Percentage(10), // Id
            Constraint::Percentage(20), // Name
            Constraint::Percentage(10), // Status
            Constraint::Percentage(20), // Stages Completed
            Constraint::Percentage(20), // Percent Completed
            Constraint::Percentage(20), // Start time
        ],
    )
    .block(Block::default().borders(Borders::all()))
    .header(header)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);
    frame.render_stateful_widget(t, area, state);
}

fn render_job_start_time_cell(job: &Job) -> Cell<'_> {
    let start_time = chrono::DateTime::from_timestamp_millis(job.start_time)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "Invalid Date".to_string());
    Cell::from(Text::from(start_time).centered())
}

fn render_job_percent_completion_cell(job: &Job) -> Cell<'_> {
    Cell::from(Text::from(format!("{}%", job.percent_complete)).centered())
}

fn render_job_stage_completion_cell(job: &Job) -> Cell<'_> {
    let stage_completion = if job.num_stages == 0 {
        format!("0.00% ({} / {})", job.completed_stages, job.num_stages)
    } else {
        let stages_completion = job.completed_stages as f32 / job.num_stages as f32;
        format!(
            "{:.2}% ({} / {})",
            stages_completion * 100.0,
            job.completed_stages,
            job.num_stages
        )
    };
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

#[cfg(test)]
mod tests {
    use super::column_suffix;
    use crate::tui::domain::{SortOrder, jobs::SortColumn};

    #[test]
    fn column_suffix_active_ascending_returns_up_arrow() {
        assert_eq!(
            column_suffix(&SortColumn::Id, &SortOrder::Ascending, &SortColumn::Id),
            " ▲"
        );
    }

    #[test]
    fn column_suffix_active_descending_returns_down_arrow() {
        assert_eq!(
            column_suffix(&SortColumn::Id, &SortOrder::Descending, &SortColumn::Id),
            " ▼"
        );
    }

    #[test]
    fn column_suffix_different_column_returns_empty() {
        assert_eq!(
            column_suffix(&SortColumn::Name, &SortOrder::Ascending, &SortColumn::Id),
            ""
        );
    }

    #[test]
    fn column_suffix_none_vs_id_returns_empty() {
        assert_eq!(
            column_suffix(&SortColumn::None, &SortOrder::Ascending, &SortColumn::Id),
            ""
        );
    }
}
