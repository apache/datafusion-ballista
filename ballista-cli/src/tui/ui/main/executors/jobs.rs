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
    fn no_jobs(block: Block<'_>, style: Style) -> Paragraph<'_> {
        Paragraph::new("No Jobs data")
            .block(block.border_style(style))
            .centered()
    }

    let block = Block::default().borders(Borders::ALL).title("Jobs");

    match &app.executors_data.jobs {
        jobs if !jobs.is_empty() => {
            let mut running_jobs = 0;
            let mut completed_jobs = 0;
            let mut failed_jobs = 0;
            let mut queued_jobs = 0;

            for job in jobs {
                match job.status.as_str() {
                    "Running" => running_jobs += 1,
                    "Completed" => completed_jobs += 1,
                    "Failed" => failed_jobs += 1,
                    "Queued" => queued_jobs += 1,
                    _ => {}
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

            render_tile(
                f,
                chunks[0],
                "Running Jobs",
                running_jobs,
                app.theme.tile_running,
            );
            render_tile(
                f,
                chunks[1],
                "Queued Jobs",
                queued_jobs,
                app.theme.tile_queued,
            );
            render_tile(
                f,
                chunks[2],
                "Completed Jobs",
                completed_jobs,
                app.theme.tile_completed,
            );
            render_tile(
                f,
                chunks[3],
                "Failed Jobs",
                failed_jobs,
                app.theme.tile_failed,
            );
        }
        _no_jobs => {
            f.render_widget(no_jobs(block, app.theme.nav_inactive), area);
        }
    }
}

fn render_tile(f: &mut Frame, area: Rect, title: &str, count: usize, style: Style) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {title} "))
        .style(style);
    f.render_widget(
        Paragraph::new(format!("{title}: {count}")).block(block),
        area,
    );
}
