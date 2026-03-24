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
use ratatui::prelude::{Color, Line, Span, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

pub(crate) fn render_job_dot_popup(f: &mut Frame, app: &App) {
    let Some(graph) = &app.job_dot_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(60, 60, f.area());
    f.render_widget(Clear, area);

    let block = Block::default()
        .title(format!(
            " Job Stages: {} (↑↓ scroll, any other key to close) ",
            graph.job_id
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let mut lines: Vec<Line> = Vec::new();

    for stage in &graph.stages {
        lines.push(Line::from(Span::styled(
            format!(" {}:", stage.label),
            Style::default().fg(Color::Yellow),
        )));
        for node in &stage.nodes {
            lines.push(Line::from(format!("   {} ({})", node.label, node.id)));
        }
        lines.push(Line::from(""));
    }

    if !graph.edges.is_empty() {
        lines.push(Line::from(Span::styled(
            " Edges:",
            Style::default().fg(Color::Yellow),
        )));
        for (from, to) in &graph.edges {
            lines.push(Line::from(format!("   {from}  →  {to}")));
        }
    }

    let paragraph = Paragraph::new(lines)
        .block(block)
        .scroll((app.job_dot_scroll, 0))
        .wrap(Wrap { trim: false });

    f.render_widget(paragraph, area);
}
