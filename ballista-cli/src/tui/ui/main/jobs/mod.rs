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

pub(crate) mod dot_parser;
pub mod job_config_popup;
pub mod job_dot_popup;
pub mod job_plan_popup;
pub mod job_stages_popup;
pub mod stage_plan_popup;
pub mod stage_tasks_popup;

#[cfg(not(feature = "web"))]
use crate::tui::{
    TuiResult,
    domain::jobs::{JobConfigEntry, JobConfigPopup, stages::StagePlanTab},
    event::{Event, UiData},
};
use crate::tui::{
    app::App,
    domain::{
        SortOrder,
        jobs::{Job, SortColumn},
    },
    ui::search_box::render_search_box,
    ui::vertical_scrollbar::render_scrollbar,
};

use crate::tui::ui::components::loading_indicator::shimmer_spans_with_style;
use crate::tui::ui::vertical_scrollbar;
use ratatui::style::Color;
use ratatui::text::Line;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::Style,
    text::Text,
    widgets::{
        Block, Borders, Cell, Clear, HighlightSpacing, Paragraph, Row, Table, TableState,
    },
};

#[cfg(not(feature = "web"))]
pub async fn load_jobs_data(app: &App) -> TuiResult<()> {
    let jobs = app.http_client.get_jobs().await.unwrap_or_else(|e| {
        tracing::error!("Failed to load the jobs: {e:?}");
        Vec::new()
    });

    app.send_event(Event::DataLoaded {
        data: UiData::Jobs(jobs),
    })
    .await
}

#[cfg(not(feature = "web"))]
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

/// Loading the whole job's stages to render the popup window
#[cfg(not(feature = "web"))]
pub async fn load_job_config_popup(app: &App, job_id: &str) -> TuiResult<()> {
    let config = match app.http_client.get_job_config(job_id).await {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("Failed to load job config for {job_id}: {e:?}");
            return Ok(());
        }
    };

    let entries = config
        .into_iter()
        .map(|(key, value)| JobConfigEntry { key, value })
        .collect();

    app.send_event(Event::DataLoaded {
        data: UiData::JobConfig(JobConfigPopup::new(job_id.to_string(), entries)),
    })
    .await
}

#[cfg(not(feature = "web"))]
pub async fn load_job_stages_popup(app: &App, job_id: &str) -> TuiResult<()> {
    let mut stages = app
        .http_client
        .get_job_stages(job_id, &StagePlanTab::Default)
        .await
        .inspect(|stages| tracing::trace!("Loaded stages for job '{job_id}': {stages:?}"))
        .inspect_err(|e| {
            tracing::error!("Failed to load stages for job '{job_id}': {e:?}")
        })?;

    stages
        .stages
        .sort_by_key(|s| s.id.parse::<u64>().unwrap_or(u64::MAX));

    app.send_event(Event::DataLoaded {
        data: UiData::JobStagesData(job_id.to_owned(), stages),
    })
    .await
}

/// Loading stage's plan to render the popup window
#[cfg(not(feature = "web"))]
pub async fn load_stage_plan(
    app: &App,
    job_id: &str,
    tab: &StagePlanTab,
) -> TuiResult<()> {
    let mut stages = app
        .http_client
        .get_job_stages(job_id, tab)
        .await
        .inspect(|s| tracing::trace!("Loaded the {tab:?} plan for job '{job_id}': {s:?}"))
        .inspect_err(|e| {
            tracing::error!("Failed to load the {tab:?} plan for job '{job_id}': {e:?}")
        })?;

    stages
        .stages
        .sort_by_key(|s| s.id.parse::<u64>().unwrap_or(u64::MAX));

    app.send_event(Event::DataLoaded {
        data: UiData::JobStagesPlanData(tab.clone(), stages),
    })
    .await
}

#[cfg(not(feature = "web"))]
pub async fn load_job_details(app: &App, job_id: &str) -> TuiResult<()> {
    let details = match app
        .http_client
        .get_job_details(job_id, &StagePlanTab::Default)
        .await
    {
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
    ])
    .split(area);

    render_search_box(f, rects[0], app);

    let mut sorted_jobs = filtered_jobs;
    app.jobs_data.sort_jobs(&mut sorted_jobs);

    if !sorted_jobs.is_empty() {
        let mut scroll_state = app.jobs_data.scrollbar_state;
        let mut table_state = app.jobs_data.table_state;
        let table_area = vertical_scrollbar::split_area(rects[1]);
        render_jobs_table(
            f,
            table_area[0],
            &sorted_jobs,
            &mut table_state,
            &app.jobs_data.sort_column,
            &app.jobs_data.sort_order,
            app,
        );
        render_scrollbar(f, table_area[1], &mut scroll_state);
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
    app: &App,
) {
    let header_style = Style::default()
        .fg(Color::LightYellow)
        .bg(Color::Black)
        .bold();

    let id_suffix = column_suffix(sort_column, sort_order, &SortColumn::Id);
    let name_suffix = column_suffix(sort_column, sort_order, &SortColumn::Name);
    let status_suffix = column_suffix(sort_column, sort_order, &SortColumn::Status);
    let stages_suffix =
        column_suffix(sort_column, sort_order, &SortColumn::StagesCompleted);
    let percent_suffix =
        column_suffix(sort_column, sort_order, &SortColumn::PercentComplete);
    let start_time_suffix =
        column_suffix(sort_column, sort_order, &SortColumn::StartTime);
    let duration_suffix = column_suffix(sort_column, sort_order, &SortColumn::Duration);

    let header_row = Row::new(vec![
        Cell::from(Text::from(format!("Id{id_suffix}")).right_aligned()),
        Cell::from(Text::from(format!("Name{name_suffix}")).centered()),
        Cell::from(Text::from(format!("Status{status_suffix}")).centered()),
        Cell::from(Text::from(format!("Stages Completed{stages_suffix}")).centered()),
        Cell::from(Text::from(format!("Percent Completed{percent_suffix}")).centered()),
        Cell::from(Text::from(format!("Start Time{start_time_suffix}")).centered()),
        Cell::from(Text::from(format!("Duration{duration_suffix}")).right_aligned()),
    ])
    .style(header_style)
    .height(1);

    let rows = jobs.iter().enumerate().map(|(i, job)| {
        let color = match i % 2 {
            0 => Color::DarkGray,
            _ => Color::Black,
        };

        let id_cell = Cell::from(Text::from(job.job_id.clone()).right_aligned());
        let name_cell = Cell::from(Text::from(job.job_name.clone()).centered());
        let status_cell = render_job_status_cell(job);
        let stage_completion_cell = render_job_stage_completion_cell(job);
        let percent_completion_cell = render_job_percent_completion_cell(job);
        let start_time_cell = render_job_start_time_cell(job, app);
        let duration_cell = render_job_duration_cell(job, app);

        let cells = vec![
            id_cell,
            name_cell,
            status_cell,
            stage_completion_cell,
            percent_completion_cell,
            start_time_cell,
            duration_cell,
        ];
        Row::new(cells).style(Style::default().bg(color))
    });

    let t = Table::new(
        rows,
        [
            Constraint::Percentage(10), // Id
            Constraint::Percentage(15), // Name
            Constraint::Percentage(10), // Status
            Constraint::Percentage(15), // Stages Completed
            Constraint::Percentage(15), // Percent Completed
            Constraint::Percentage(15), // Start time
            Constraint::Percentage(15), // Duration
        ],
    )
    .block(Block::default().borders(Borders::all()))
    .header(header_row)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);
    frame.render_stateful_widget(t, area, state);
}

fn render_job_start_time_cell<'a>(job: &'a Job, app: &App) -> Cell<'a> {
    let start_time = app.format_datetime(job.start_time);
    Cell::from(Text::from(start_time).centered())
}

fn render_job_duration_cell<'a>(job: &'a Job, app: &App) -> Cell<'a> {
    let duration = if job.end_time > 0 {
        let duration = job.end_time - job.start_time;
        app.format_duration(duration as u64)
    } else {
        "N/A".to_string()
    };
    Cell::from(Text::from(duration).right_aligned())
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
    fn content_wo_animation(text: &str, color: Color) -> Text<'_> {
        Text::from(text).style(Style::default().fg(color).bold())
    }

    fn content_with_animation(text: &str, color: Color) -> Text<'_> {
        let text = shimmer_spans_with_style(text, Style::default().fg(color).bold());
        Line::from(text).into()
    }

    let content = match job.status.as_str() {
        "Running" => content_with_animation("Running", Color::LightBlue),
        "Queued" => content_with_animation("Queued", Color::LightMagenta),
        "Failed" => content_wo_animation("Failed", Color::LightRed),
        "Completed" => content_wo_animation("Completed", Color::LightGreen),
        other => content_wo_animation(other, Color::Gray),
    };

    Cell::from(content.centered())
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
