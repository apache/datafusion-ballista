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
use ratatui::prelude::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph, Wrap};

pub(crate) fn render_help_overlay(f: &mut Frame, app: &App) {
    let area = crate::tui::ui::centered_rect(45, 50, f.area());

    f.render_widget(Clear, area);

    let style = if app.is_scheduler_up() {
        app.theme.help_item
    } else {
        app.theme.help_item_dim
    };

    let help_text = vec![
        Line::from(""),
        Line::from(Span::styled("KEYBOARD SHORTCUTS", app.theme.help_header)),
        Line::from(""),
        Line::from(vec![Span::styled(" Navigation", app.theme.help_section)]),
        Line::from(Span::styled("  j       Show Jobs", style)),
        Line::from(Span::styled("  e       Show Executors", style)),
        Line::from(Span::styled("  m       Show Metrics", style)),
        Line::from(Span::styled("  /       Search in Jobs or Metrics", style)),
        Line::from(""),
        Line::from(vec![Span::styled(" Jobs view:", app.theme.help_section)]),
        Line::from(Span::styled(
            "  1/2/... Sort by first/second/... column",
            style,
        )),
        Line::from(Span::styled("  o       Show job config items", style)),
        Line::from(Span::styled(
            "  g       Dot graph if a completed job is selected",
            style,
        )),
        Line::from(Span::styled(
            "  p       View plans if a completed job is selected",
            style,
        )),
        Line::from(Span::styled(
            "  c       Cancel selected Running/Queued job",
            style,
        )),
        Line::from(""),
        Line::from(vec![Span::styled(" General", app.theme.help_section)]),
        Line::from(Span::styled("  i       Show Scheduler Info", style)),
        Line::from("  ?/h     Show this help"),
        Line::from("  q/Esc   Quit"),
    ];

    let block = Block::default()
        .title(" Help (Press any key to close) ")
        .borders(Borders::ALL)
        .border_style(app.theme.popup_border)
        .border_type(BorderType::Thick);

    let para = Paragraph::new(help_text)
        .block(block)
        .wrap(Wrap { trim: false });

    f.render_widget(para, area);
}
