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

mod cancel_result_popup;
mod footer;
mod header;
mod help_overlay;
mod main;
mod scheduler_info_popup;
mod search_box;

use crate::tui::app::App;
use crate::tui::ui::header::render_header;
use footer::render_footer;
pub use main::{
    job_dot_popup, job_plan_popup, load_dashboard_data, load_job_details, load_job_dot,
    load_jobs_data, load_metrics_data,
};
use main::{render_dashboard, render_jobs, render_metrics};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
};

pub(crate) fn render(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6), // Header
            Constraint::Min(0),    // Main view
            Constraint::Length(2), // Footer
        ])
        .split(f.area());

    render_header(f, chunks[0], app);
    render_main_view(f, app, chunks[1]);
    render_footer(f, chunks[2], app);

    // Overlay help if active
    if app.show_help {
        help_overlay::render_help_overlay(f, app);
    } else if app.show_scheduler_info {
        scheduler_info_popup::render_scheduler_info(f, app);
    } else if app.cancel_job_result.is_some() {
        cancel_result_popup::render_cancel_result_popup(f, app);
    } else if app.job_dot_popup.is_some() {
        job_dot_popup::render_job_dot_popup(f, app);
    } else if app.job_plan_popup.is_some() {
        job_plan_popup::render_job_plan_popup(f, app);
    }
}

fn render_main_view(f: &mut Frame, app: &App, area: Rect) {
    if app.is_dashboard_view() {
        render_dashboard(f, area, app);
    } else if app.is_jobs_view() {
        render_jobs(f, area, app);
    } else if app.is_metrics_view() {
        render_metrics(f, area, app);
    }
}

// Helper functions

pub(crate) fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
