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
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::Style;
use ratatui::{
    Frame,
    layout::Rect,
    widgets::{Block, Borders, Paragraph},
};

pub fn render_jobs(f: &mut Frame, area: Rect, app: &App) {
    fn no_jobs(block: Block<'_>) -> Paragraph<'_> {
        Paragraph::new("No Jobs data")
            .block(block.border_style(Style::new().gray()))
            .centered()
    }

    let block = Block::default().borders(Borders::ALL).title("Jobs");

    match &app.dashboard_data.jobs_data {
        Some(jobs) => {
            if jobs.is_empty() {
                f.render_widget(no_jobs(block), area);
                return;
            }

            let mut running_jobs = 0;
            let mut completed_jobs = 0;
            let mut failed_jobs = 0;
            let mut queued_jobs = 0;

            for job in jobs {
                if job.job_status.eq_ignore_ascii_case("Running") {
                    running_jobs += 1;
                } else if job.job_status.starts_with("Completed") {
                    completed_jobs += 1;
                } else if job.job_status.starts_with("Failed") {
                    failed_jobs += 1;
                } else if job.job_status.eq_ignore_ascii_case("Queued") {
                    queued_jobs += 1;
                }
            }

            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(25), // Running jobs
                    Constraint::Percentage(25), // Queued jobs
                    Constraint::Percentage(25), // Completed jobs
                    Constraint::Percentage(25), // Failed jobs
                ])
                .split(area);

            render_running_jobs(f, chunks[0], running_jobs);
            render_queued_jobs(f, chunks[1], queued_jobs);
            render_completed_jobs(f, chunks[2], completed_jobs);
            render_failed_jobs(f, chunks[3], failed_jobs);
        }
        None => {
            f.render_widget(no_jobs(block), area);
        }
    }
}

fn render_running_jobs(f: &mut Frame, area: Rect, running_jobs: usize) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Running Jobs ")
        .style(Style::new().light_blue());
    f.render_widget(
        Paragraph::new(format!("Running jobs: {running_jobs}")).block(block),
        area,
    );
}

fn render_queued_jobs(f: &mut Frame, area: Rect, queued_jobs: usize) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Queued Jobs ")
        .style(Style::new().magenta());
    f.render_widget(
        Paragraph::new(format!("Queued jobs: {queued_jobs}")).block(block),
        area,
    );
}

fn render_completed_jobs(f: &mut Frame, area: Rect, completed_jobs: usize) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Completed Jobs ")
        .style(Style::new().green());
    f.render_widget(
        Paragraph::new(format!("Completed jobs: {completed_jobs}")).block(block),
        area,
    );
}

fn render_failed_jobs(f: &mut Frame, area: Rect, failed_jobs: usize) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Failed Jobs ")
        .style(Style::new().red());
    f.render_widget(
        Paragraph::new(format!("Failed jobs: {failed_jobs}")).block(block),
        area,
    );
}
