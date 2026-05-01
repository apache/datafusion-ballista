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
use crate::tui::domain::jobs::{JobPlansPopup, PlanTab};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::prelude::{Color, Style};
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph};

pub(crate) fn render_job_plan_popup(f: &mut Frame, app: &App) {
    let Some(job_plans) = &app.job_plan_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(80, 70, f.area());
    f.render_widget(Clear, area);

    let areas = Layout::vertical([
        Constraint::Min(0), // Plans
    ])
    .split(area);

    render_plans(f, areas[0], job_plans);
}

fn render_plans(f: &mut Frame, area: Rect, job_plans: &JobPlansPopup) {
    let details = &job_plans.details;
    let tab = &job_plans.tab;

    let plan = match tab {
        PlanTab::Stage => details.stage_plan.as_deref().unwrap_or("N/A"),
        PlanTab::Physical => details.physical_plan.as_deref().unwrap_or("N/A"),
        PlanTab::Logical => details.logical_plan.as_deref().unwrap_or("N/A"),
    };

    let title = format!(" {:?} plan for job '{}' ", tab, details.job_id);

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .border_type(BorderType::Thick);

    let paragraph = Paragraph::new(plan)
        .block(block)
        .scroll((job_plans.scroll_position, 0));

    f.render_widget(paragraph, area);
}
