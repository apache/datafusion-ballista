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

mod footer;
mod header;
mod main;

use crate::tui::app::{App, Views};
use crate::tui::ui::header::render_header;
use footer::render_footer;
pub use main::load_dashboard_data;
use main::{render_dashboard, render_jobs, render_metrics};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

pub(crate) fn render(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6), // Header
            Constraint::Min(0),    // Main view
            Constraint::Length(1), // Footer
        ])
        .split(f.area());

    render_header(f, chunks[0], app);
    render_main_view(f, app, chunks[1]);
    render_footer(f, chunks[2]);

    // Overlay help if active
    if app.show_help {
        render_help_overlay(f);
    }
}

fn render_main_view(f: &mut Frame, app: &App, area: Rect) {
    if app.current_view == Views::Dashboard {
        render_dashboard(f, area, app);
    } else if app.current_view == Views::Jobs {
        render_jobs(f, area, app);
    } else if app.current_view == Views::Metrics {
        render_metrics(f, area, app);
    }
}

fn render_help_overlay(f: &mut Frame) {
    let area = centered_rect(25, 35, f.area());

    f.render_widget(Clear, area);

    let help_text = vec![
        Line::from(""),
        Line::from(Span::styled(
            "KEYBOARD SHORTCUTS",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Navigation",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from("  d       Show Dashboard"),
        Line::from("  j       Show Jobs"),
        Line::from("  m       Show Metrics"),
        Line::from(""),
        Line::from(vec![Span::styled(
            "  General",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from("  ?/h       Show this help"),
        Line::from("  q/Esc     Quit"),
        Line::from(""),
        Line::from(Span::styled(
            "Press any key to close",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let block = Block::default()
        .title(" Help ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let para = Paragraph::new(help_text)
        .block(block)
        .wrap(Wrap { trim: false });

    f.render_widget(para, area);
}

// Helper functions

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
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
