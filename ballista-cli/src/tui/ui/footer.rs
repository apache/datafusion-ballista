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
use ratatui::layout::Rect;
use ratatui::prelude::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph};

pub(super) fn render_footer(f: &mut Frame, area: Rect, app: &App) {
    let mut key_bindings = Vec::with_capacity(10);

    if app.is_edit_mode() {
        key_bindings.push(Span::from("[Esc] Quit edit mode, "));
    } else {
        key_bindings.push(Span::from("Key bindings: "));

        if app.is_scheduler_up() {
            key_bindings.push(Span::from("[d] Dashboard, "));
            key_bindings.push(Span::from("[j] Jobs, "));
            key_bindings.push(Span::from("[m] Metrics, "));
            if app.is_jobs_view() {
                key_bindings.push(Span::from("[/] Search jobs, "));
                key_bindings.push(Span::from("[s] Sort by Status, "));
                key_bindings.push(Span::from("[p] Sort by % Completed, "));
                key_bindings.push(Span::from("[t] Sort by Start time, "));
                if app.has_selected_job() {
                    key_bindings.push(Span::from("[c] Cancel job, "));
                }
            } else if app.is_metrics_view() {
                key_bindings.push(Span::from("[/] Search metrics, "));
            }
            key_bindings.push(Span::from("[i] Scheduler info, "));
        }

        key_bindings.push(Span::from("[?/h] Help, "));
        key_bindings.push(Span::from("[q/Esc] Quit"));
    }

    let line = Line::from(key_bindings);

    let block = Block::default();
    let paragraph = Paragraph::new(line)
        .style(Style::default().bold())
        .block(block)
        .centered();
    f.render_widget(paragraph, area);
}
