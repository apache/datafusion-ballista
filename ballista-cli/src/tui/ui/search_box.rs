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

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::prelude::{Color, Line, Span, Style};
use ratatui::widgets::{Block, Borders, Paragraph};

pub(crate) fn render_search_box(f: &mut Frame, area: Rect, app: &crate::tui::app::App) {
    let (title, border_style) = if app.is_edit_mode() {
        (" Search ", Style::default().fg(Color::Yellow))
    } else {
        (" Search [/ to activate] ", Style::default().dim())
    };

    let display_text = if app.is_edit_mode() {
        let search_term = Span::from(app.search_term.to_string());
        let cursor = Span::from("_").style(Style::default().bold().yellow());
        Line::from(vec![search_term, cursor])
    } else {
        Line::from(Span::from(app.search_term.clone()))
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title(title);

    let paragraph = Paragraph::new(display_text).block(block);
    f.render_widget(paragraph, area);
}
