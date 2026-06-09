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
use crate::tui::domain::jobs::CancelJobResult;
use ratatui::Frame;
use ratatui::prelude::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph, Wrap};

pub(crate) fn render_cancel_result_popup(f: &mut Frame, app: &App) {
    if let Some(result) = app.cancel_job_result.as_ref() {
        let area = crate::tui::ui::centered_rect(40, 20, f.area());
        f.render_widget(Clear, area);

        let (message, style) = match result {
            CancelJobResult::Success { job_id } => (
                format!("Job '{job_id}' was successfully canceled."),
                app.theme.cancel_success,
            ),
            CancelJobResult::NotCanceled { job_id } => (
                format!("Job '{job_id}' could not be canceled."),
                app.theme.cancel_not_done,
            ),
            CancelJobResult::Failure { job_id, error } => (
                format!("Failed to cancel job '{job_id}': {error}"),
                app.theme.cancel_failure,
            ),
        };

        let text = vec![Line::from(""), Line::from(Span::styled(message, style))];

        let block = Block::default()
            .title(" Cancel Job (Press any key to close) ")
            .borders(Borders::ALL)
            .border_style(app.theme.popup_border)
            .border_type(BorderType::Thick);

        let para = Paragraph::new(text).block(block).wrap(Wrap { trim: false });

        f.render_widget(para, area);
    }
}
