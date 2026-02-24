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
use ratatui::style::Style;
use ratatui::{
    layout::Rect,
    widgets::{Block, Borders, Paragraph},
    Frame,
};

pub fn render_jobs(f: &mut Frame, area: Rect, app: &App) {
    fn no_jobs(block: Block<'_>) -> Paragraph<'_> {
        Paragraph::new("No Jobs data")
            .block(block.border_style(Style::new().gray()))
            .centered()
    }

    let block = Block::default().borders(Borders::ALL).title("Jobs");

    match &app.dashboard_data.jobs_data {
        Some(jobs) => {
            if jobs.is_empty() {
                f.render_widget(no_jobs(block), area);
                return;
            }

            f.render_widget(Paragraph::new("Some jobs"), area);
        }
        None => {
            f.render_widget(no_jobs(block), area);
        }
    };
}
