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
use crate::tui::domain::jobs::stages::JobStageResponse;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::prelude::{Color, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

pub(crate) fn render_stage_plan_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_stages_popup else {
        return;
    };

    let Some(stage) = popup.selected_stage() else {
        return;
    };

    let area = crate::tui::ui::centered_rect(80, 70, f.area());
    f.render_widget(Clear, area);

    let areas = Layout::vertical([
        Constraint::Min(0),    // Plans
        Constraint::Length(3), // Navigation
    ])
    .split(area);

    render_plans(f, areas[0], stage, &popup.job_id, app);
}

fn render_plans(
    f: &mut Frame,
    area: Rect,
    stage: &JobStageResponse,
    job_id: &str,
    app: &App,
) {
    let title = format!(" Plan for stage '{}' of job '{}' ", stage.id, job_id,);

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let paragraph = Paragraph::new(stage.plan.clone())
        .block(block)
        .scroll((app.job_plan_popup_scroll, 0));

    f.render_widget(paragraph, area);
}
