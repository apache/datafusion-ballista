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
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Style, Stylize},
    text::{Line, Text},
    widgets::{Block, Borders, Paragraph},
};

use crate::tui::app::{App, Views};

const MENU_ITEMS: [&str; 3] = ["Dashboard", "Jobs", "Metrics"];
const PERCENTAGE: u16 = 100 / MENU_ITEMS.len() as u16;
const MENU_CONSTRAINTS: [Constraint; MENU_ITEMS.len()] =
    [Constraint::Percentage(PERCENTAGE); MENU_ITEMS.len()];

// Generated at https://manytools.org/hacker-tools/ascii-banner/
// Font: Shimrod
#[rustfmt::skip]
const BANNER: &str = r#"
,-.      .       ,--.                   ,-.      . .       .
|  \     |       |          o           |  )     | | o     |
|  | ,-: |-  ,-: |- . . ,-. . ,-. ;-.   |-<  ,-: | | . ,-. |-  ,-:
|  / | | |   | | |  | | `-. | | | | |   |  ) | | | | | `-. |   | |
`-'  `-` `-' `-` '  `-` `-' ' `-' ' '   `-'  `-` ' ' ' `-' `-' `-`
"#;

pub(super) fn render_header(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Min(0)])
        .split(area);

    render_block(f, chunks[0]);
    render_menu(f, chunks[1], app);
}

fn render_block(f: &mut Frame, area: Rect) {
    let block = Block::default().borders(Borders::empty());
    let paragraph = Paragraph::new(BANNER)
        .style(Style::default().bold())
        .block(block)
        .alignment(Alignment::Left);
    f.render_widget(paragraph, area);
}

fn render_menu(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(MENU_CONSTRAINTS)
        .split(area);

    for (index, menu_item) in MENU_ITEMS.iter().enumerate() {
        let mut block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().dark_gray());

        let first_char = menu_item.chars().next().unwrap().underlined();
        let rest_chars = menu_item.chars().skip(1).collect::<String>();
        let line = Line::from(vec![first_char, rest_chars.into()]);
        let text = Text::from(line);

        let mut paragraph = Paragraph::new(text)
            .style(Style::default().dark_gray())
            .block(block.clone())
            .alignment(Alignment::Center);

        let is_active = match app.current_view {
            Views::Dashboard => *menu_item == "Dashboard",
            Views::Jobs => *menu_item == "Jobs",
            Views::Metrics => *menu_item == "Metrics",
        };

        if is_active {
            block = block.border_style(Style::default().white());
            paragraph = paragraph.style(Style::default().white()).block(block);
        }

        f.render_widget(paragraph, chunks[index]);
    }
}
