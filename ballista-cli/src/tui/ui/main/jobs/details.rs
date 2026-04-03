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

use crate::tui::domain::JobDetails;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    widgets::{Block, Borders, Paragraph, Wrap},
};

pub fn render_job_details(f: &mut Frame, area: Rect, details: &JobDetails) {
    let vertical = Layout::vertical([
        // Constraint::Percentage(50), // Logical + Physical plans
        Constraint::Percentage(100), // Stage plan
    ]);
    let rows = vertical.split(area);

    // let horizontal = Layout::horizontal([
    //     Constraint::Percentage(50), // Logical plan
    //     Constraint::Percentage(50), // Physical plan
    // ]);
    // let top_cols = horizontal.split(rows[0]);

    // let logical_text = details.logical_plan.as_deref().unwrap_or("N/A").to_string();
    // let physical_text = details
    //     .physical_plan
    //     .as_deref()
    //     .unwrap_or("N/A")
    //     .to_string();
    let stage_text = details.stage_plan.as_deref().unwrap_or("N/A").to_string();

    // let logical_panel = Paragraph::new(logical_text)
    //     .block(
    //         Block::default()
    //             .borders(Borders::ALL)
    //             .title(" Logical Plan "),
    //     )
    //     .wrap(Wrap { trim: false });
    //
    // let physical_panel = Paragraph::new(physical_text)
    //     .block(
    //         Block::default()
    //             .borders(Borders::ALL)
    //             .title(" Physical Plan "),
    //     )
    //     .wrap(Wrap { trim: false });

    let stage_panel = Paragraph::new(stage_text)
        .block(Block::default().borders(Borders::ALL).title(" Stage Plan "))
        .wrap(Wrap { trim: false });

    // f.render_widget(logical_panel, top_cols[0]);
    // f.render_widget(physical_panel, top_cols[1]);
    f.render_widget(stage_panel, rows[0]);
}
