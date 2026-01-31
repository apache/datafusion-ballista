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

use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::Style,
    widgets::{Block, Borders, Clear, Paragraph},
};

use crate::tui::app::App;

pub fn render_metrics(f: &mut Frame, area: Rect, _app: &App) {
    f.render_widget(Clear, area);
    let block = Block::default().borders(Borders::all());
    let paragraph = Paragraph::new("Metrics")
        .style(Style::default().bold())
        .centered()
        .block(block)
        .alignment(Alignment::Left);
    f.render_widget(paragraph, area);
}
