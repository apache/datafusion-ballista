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
use ratatui::Frame;
use ratatui::prelude::{Color, Line, Modifier, Span, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

pub(crate) fn render_help_overlay(f: &mut Frame, app: &App) {
    let area = crate::tui::ui::centered_rect(25, 50, f.area());

    f.render_widget(Clear, area);

    let style = if app.is_scheduler_up() {
        Style::default()
    } else {
        Style::default().fg(Color::Gray)
    };

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
            " Navigation",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from(Span::styled("  d       Show Dashboard", style)),
        Line::from(Span::styled("  j       Show Jobs", style)),
        Line::from(Span::styled("  m       Show Metrics", style)),
        Line::from(Span::styled("  /       Search in Jobs or Metrics", style)),
        Line::from(""),
        Line::from(vec![Span::styled(
            " Jobs view:",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from(Span::styled("  s       Sort by Status", style)),
        Line::from(Span::styled("  p       Sort by % Completed", style)),
        Line::from(Span::styled("  t       Sort by Start time", style)),
        Line::from(Span::styled(
            "  g       Dot graph if a completed job is selected",
            style,
        )),
        Line::from(Span::styled(
            "  D View plans if a completed job is selected",
            style,
        )),
        Line::from(Span::styled(
            "  c       Cancel selected Running/Queued job",
            style,
        )),
        Line::from(""),
        Line::from(vec![Span::styled(
            " General",
            Style::default().fg(Color::Yellow),
        )]),
        Line::from(Span::styled("  i       Show Scheduler Info", style)),
        Line::from("  ?/h     Show this help"),
        Line::from("  q/Esc   Quit"),
    ];

    let block = Block::default()
        .title(" Help (Press any key to close) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let para = Paragraph::new(help_text)
        .block(block)
        .wrap(Wrap { trim: false });

    f.render_widget(para, area);
}
