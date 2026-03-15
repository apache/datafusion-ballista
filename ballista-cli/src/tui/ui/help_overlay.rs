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

use ratatui::Frame;
use ratatui::prelude::{Color, Line, Modifier, Span, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

pub(crate) fn render_help_overlay(f: &mut Frame) {
    let area = crate::tui::ui::centered_rect(25, 35, f.area());

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
        Line::from("  /       Search in Jobs or Metrics"),
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
