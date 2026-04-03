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
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph};

pub(super) fn render_footer(f: &mut Frame, area: Rect, app: &App) {
    let mut page_key_bindings = Vec::with_capacity(10);
    let mut global_key_bindings = Vec::with_capacity(10);

    if app.is_edit_mode() {
        page_key_bindings.push(Span::from("[Esc] Quit edit mode, "));
    } else {
        global_key_bindings.push(Span::from("Global key bindings: "));

        if app.is_scheduler_up() {
            global_key_bindings.push(Span::from("[d] Dashboard, "));
            global_key_bindings.push(Span::from("[j] Jobs, "));
            global_key_bindings.push(Span::from("[m] Metrics, "));
            if app.is_jobs_view() {
                if app.has_more_than_one_job() {
                    page_key_bindings.push(Span::from("[/] Search jobs, "));
                    page_key_bindings.push(Span::from("[s] Sort by Status, "));
                    page_key_bindings.push(Span::from("[p] Sort by % Completed, "));
                    page_key_bindings.push(Span::from("[t] Sort by Start time, "));
                }
                if app.has_selected_job() {
                    page_key_bindings.push(Span::from("[g] View job stages, "));
                    page_key_bindings.push(Span::from("[c] Cancel job, "));
                }
                if app.has_selected_completed_job() {
                    page_key_bindings.push(Span::from("[D] View job plans, "));
                }
                if !page_key_bindings.is_empty() {
                    page_key_bindings.insert(0, Span::from("Jobs key bindings: "));
                }
            } else if app.is_metrics_view() {
                page_key_bindings.push(Span::from("Metrics key bindings: "));
                page_key_bindings.push(Span::from("[/] Search metrics, "));
            }
            global_key_bindings.push(Span::from("[i] Scheduler info, "));
        }

        global_key_bindings.push(Span::from("[?/h] Help, "));
        global_key_bindings.push(Span::from("[q/Esc] Quit"));
    }

    let global_area = if !page_key_bindings.is_empty() {
        let areas = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1), // page keybindings
                Constraint::Min(1), // global keybindings
            ])
            .split(area);

        let line = Line::from(page_key_bindings);

        let block = Block::default();
        let paragraph = Paragraph::new(line)
            .style(Style::default().bold())
            .block(block)
            .centered();
        f.render_widget(paragraph, areas[0]);

        areas[1]
    } else {
        area
    };

    let line = Line::from(global_key_bindings);

    let block = Block::default();
    let paragraph = Paragraph::new(line)
        .style(Style::default().bold())
        .block(block)
        .centered();
    f.render_widget(paragraph, global_area);
}
