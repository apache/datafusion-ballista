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
use crate::tui::domain::CancelJobResult;
use ratatui::Frame;
use ratatui::prelude::{Color, Line, Span, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

pub(crate) fn render_cancel_result_popup(f: &mut Frame, app: &App) {
    if let Some(result) = app.cancel_job_result.as_ref() {
        let area = crate::tui::ui::centered_rect(40, 20, f.area());
        f.render_widget(Clear, area);

        let (message, color) = match result {
            CancelJobResult::Success { job_id } => (
                format!("Job '{job_id}' was successfully canceled."),
                Color::Green,
            ),
            CancelJobResult::NotCanceled { job_id } => (
                format!("Job '{job_id}' could not be canceled."),
                Color::Yellow,
            ),
            CancelJobResult::Failure { job_id, error } => (
                format!("Failed to cancel job '{job_id}': {error}"),
                Color::Red,
            ),
        };

        let text = vec![
            Line::from(""),
            Line::from(Span::styled(message, Style::default().fg(color))),
        ];

        let block = Block::default()
            .title(" Cancel Job (Press any key to close) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        let para = Paragraph::new(text).block(block).wrap(Wrap { trim: false });

        f.render_widget(para, area);
    }
}
