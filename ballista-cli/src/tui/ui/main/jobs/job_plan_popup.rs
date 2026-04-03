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

use crate::tui::app::{App, PlanTab};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::prelude::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

pub(crate) fn render_job_plan_popup(f: &mut Frame, app: &App) {
    let Some((details, tab)) = &app.job_plan_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(80, 70, f.area());
    f.render_widget(Clear, area);

    let areas = Layout::vertical([
        Constraint::Min(0),    // Plans
        Constraint::Length(3), // Navigation
    ])
    .split(area);

    render_plans(f, areas[0], details, tab, app);
    render_navigation(f, areas[1], tab);
}

fn render_navigation(f: &mut Frame, area: Rect, tab: &PlanTab) {
    let stage_label = if *tab == PlanTab::Stage {
        Span::styled(" [s] Stage", Style::default().bold())
    } else {
        Span::from(" [s] Stage")
    };
    let physical_label = if *tab == PlanTab::Physical {
        Span::styled("[p] Physical", Style::default().bold())
    } else {
        Span::from("[p] Physical")
    };
    let logical_label = if *tab == PlanTab::Logical {
        Span::styled("[l] Logical", Style::default().bold())
    } else {
        Span::from("[l] Logical")
    };
    let navigation = Line::from(vec![
        stage_label,
        Span::from(" | "),
        physical_label,
        Span::from(" | "),
        logical_label,
        Span::from(" | ↑↓ scroll | Esc close "),
    ]);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let paragraph = Paragraph::new(navigation).block(block);

    f.render_widget(paragraph, area);
}

fn render_plans(
    f: &mut Frame,
    area: ratatui::layout::Rect,
    details: &crate::tui::domain::JobDetails,
    tab: &PlanTab,
    app: &App,
) {
    let plan = match tab {
        PlanTab::Stage => details.stage_plan.as_deref().unwrap_or("N/A"),
        PlanTab::Physical => details.physical_plan.as_deref().unwrap_or("N/A"),
        PlanTab::Logical => details.logical_plan.as_deref().unwrap_or("N/A"),
    };

    let title = format!(" Job: {} ", details.job_id,);

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let paragraph = Paragraph::new(plan)
        .block(block)
        .wrap(Wrap { trim: false })
        .scroll((app.job_plan_popup_scroll, 0));

    f.render_widget(paragraph, area);
}
