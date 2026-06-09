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
use crate::tui::domain::jobs::JobConfigEntry;
use crate::tui::ui::search_box::render_search_input;
use crate::tui::ui::vertical_scrollbar::{render_scrollbar, split_area};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::prelude::{Color, Style};
use ratatui::text::Text;
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, HighlightSpacing, Paragraph, Row, Table,
};

pub(crate) fn render_job_config_popup(f: &mut Frame, app: &App) {
    let Some(popup) = &app.job_config_popup else {
        return;
    };

    let area = crate::tui::ui::centered_rect(85, 80, f.area());
    f.render_widget(Clear, area);

    let areas = Layout::vertical([Constraint::Length(3), Constraint::Min(0)]).split(area);

    render_search_input(
        f,
        areas[0],
        &popup.search_term,
        app.is_job_config_search_edit_mode(),
        " Search config [/ to activate] ",
        " Search config ",
    );

    let filtered = popup.filtered_entries();
    if filtered.is_empty() {
        let block = Block::default()
            .title(format!(" Job config for '{}' ", popup.job_id))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::LightCyan))
            .border_type(BorderType::Thick);
        let paragraph = Paragraph::new("No matching config entries").block(block);
        f.render_widget(paragraph, areas[1]);
        return;
    }

    let table_area = split_area(areas[1]);
    let mut table_state = popup.table_state;
    let mut scrollbar_state = popup.scrollbar_state;

    let header = Row::new(vec![
        Cell::from(Text::from("Key")),
        Cell::from(Text::from("Value")),
    ])
    .style(
        Style::default()
            .fg(Color::LightYellow)
            .bg(Color::Black)
            .bold(),
    );

    let rows = filtered
        .iter()
        .enumerate()
        .map(|(i, entry)| row_for_entry(i, entry));

    let table = Table::new(
        rows,
        [Constraint::Percentage(40), Constraint::Percentage(60)],
    )
    .header(header)
    .block(
        Block::default()
            .title(format!(" Job config for '{}' ", popup.job_id))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::LightCyan))
            .border_type(BorderType::Thick),
    )
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);

    f.render_stateful_widget(table, table_area[0], &mut table_state);
    render_scrollbar(f, table_area[1], &mut scrollbar_state);
}

fn row_for_entry<'a>(i: usize, entry: &&'a JobConfigEntry) -> Row<'a> {
    let color = if i.is_multiple_of(2) {
        Color::DarkGray
    } else {
        Color::Black
    };

    Row::new(vec![
        Cell::from(entry.key.as_str()),
        Cell::from(entry.value.as_str()),
    ])
    .style(Style::default().bg(color))
}
