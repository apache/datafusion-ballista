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
use ratatui::layout::Constraint;
use ratatui::prelude::{Color, Style};
use ratatui::text::Text;
use ratatui::widgets::{Block, Borders, Cell, Clear, Row, Table};

pub(crate) fn render_stage_tasks_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_stages_popup else {
        return;
    };

    let Some(stage) = popup.selected_stage() else {
        return;
    };

    let area = crate::tui::ui::centered_rect(50, 55, f.area());
    f.render_widget(Clear, area);

    let header_style = Style::default().fg(Color::Yellow).bg(Color::Black);
    let header = ["Metric", "Value"]
        .into_iter()
        .map(|h| Cell::from(Text::from(h).centered()))
        .collect::<Row>()
        .style(header_style)
        .height(1);

    let p = &stage.task_duration_percentiles;
    let metrics: &[(&str, String)] = &[
        ("Status", stage.status.clone()),
        ("Input Rows", stage.input_rows.to_string()),
        ("Output Rows", stage.output_rows.to_string()),
        ("Elapsed Compute", stage.elapsed_compute.clone()),
        ("Min Task Duration", format!("{} ms", p.min)),
        ("P25 Task Duration", format!("{} ms", p.p25)),
        ("Median Task Duration", format!("{} ms", p.median)),
        ("P75 Task Duration", format!("{} ms", p.p75)),
        ("Max Task Duration", format!("{} ms", p.max)),
    ];

    let rows = metrics.iter().enumerate().map(|(i, (metric, value))| {
        let bg = if i % 2 == 0 {
            Color::DarkGray
        } else {
            Color::Black
        };
        Row::new(vec![
            Cell::from(Text::from(*metric).style(Style::default().fg(Color::Cyan))),
            Cell::from(Text::from(value.clone())),
        ])
        .style(Style::default().bg(bg))
    });

    let title = format!(" Tasks for Stage '{}' (Esc close) ", stage.id);
    let table = Table::new(
        rows,
        [Constraint::Percentage(45), Constraint::Percentage(55)],
    )
    .block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    )
    .header(header);

    f.render_widget(table, area);
}
