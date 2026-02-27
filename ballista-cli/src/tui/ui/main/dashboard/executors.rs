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
use chrono::DateTime;
use ratatui::style::Style;
use ratatui::{
    Frame,
    layout::Rect,
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

pub fn render_executors(f: &mut Frame, area: Rect, app: &App) {
    fn no_live_executors(block: Block<'_>) -> Paragraph<'_> {
        Paragraph::new("No live executors")
            .block(block.border_style(Style::new().red()))
            .centered()
    }

    let block = Block::default().borders(Borders::ALL).title("Executors");

    match &app.dashboard_data.executors_data {
        Some(executors) => {
            if executors.is_empty() {
                f.render_widget(no_live_executors(block), area);
                return;
            }

            let items: Vec<ListItem> = executors
                .iter()
                .map(|ex| {
                    let datetime = DateTime::from_timestamp_millis(ex.last_seen);
                    let last_seen = datetime
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| "Invalid Date".to_string());

                    let line = format!(
                        "{}:{} ({}) — Last seen: {}",
                        ex.host, ex.port, ex.id, last_seen
                    );
                    ListItem::new(line)
                })
                .collect();

            let list = List::new(items).block(block);
            f.render_widget(list, area);
        }
        None => {
            f.render_widget(no_live_executors(block), area);
        }
    };
}
