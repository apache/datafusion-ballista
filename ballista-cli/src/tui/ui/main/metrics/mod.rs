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

use crate::tui::TuiResult;
use crate::tui::app::App;
use crate::tui::domain::Metric;
use crate::tui::error::TuiError;
use crate::tui::event::Event;
use crate::tui::event::UiData;

use ratatui::style::Color;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Margin, Rect},
    style::Style,
    text::Text,
    widgets::{
        Block, Borders, Cell, Clear, HighlightSpacing, Paragraph, Row, Scrollbar,
        ScrollbarOrientation, ScrollbarState, Table, TableState,
    },
};

pub async fn load_metrics_data(app: &App) -> TuiResult<()> {
    let metrics = match app.http_client.get_metrics().await {
        Ok(metrics) => metrics,
        Err(e) => {
            tracing::error!("Failed to load the metrics: {e:?}");
            None
        }
    };

    match &app.event_tx {
        Some(event_tx) => {
            event_tx
                .send(Event::DataLoaded {
                    data: UiData::Metrics(metrics),
                })
                .map_err(TuiError::SendError)?;
        }
        None => {
            tracing::warn!("Dashboard data loaded but event_tx is not set");
        }
    }

    Ok(())
}

pub fn render_metrics(f: &mut Frame, area: Rect, app: &App) {
    f.render_widget(Clear, area);

    match &app.metrics_data.metrics {
        Some(metrics) => {
            let vertical = &Layout::vertical([
                Constraint::Min(5),    // Table
                Constraint::Length(4), // Scrollbar
            ]);
            let rects = vertical.split(area);

            let mut scroll_state = ScrollbarState::new((metrics.len() - 1) * 2);
            let mut table_state = TableState::default().with_selected(0);
            render_metrics_table(f, rects[0], metrics, &mut table_state);
            render_scrollbar(f, rects[0], &mut scroll_state);
        }
        None => render_no_metrics(f, area),
    }
}

fn render_no_metrics(f: &mut Frame, area: Rect) {
    let block = Block::default().borders(Borders::all());
    let paragraph = Paragraph::new("No metrics")
        .style(Style::default().bold())
        .centered()
        .block(block);
    f.render_widget(paragraph, area);
}

fn render_metrics_table(
    frame: &mut Frame,
    area: Rect,
    metrics: &[Metric],
    state: &mut TableState,
) {
    let header_style = Style::default().fg(Color::Yellow).bg(Color::Black);

    let header = ["Name", "Value", "Description"]
        .into_iter()
        .map(Cell::from)
        .collect::<Row>()
        .style(header_style)
        .height(1);

    let rows = metrics.iter().enumerate().map(|(i, metric)| {
        use prometheus_parse::Value;

        let color = match i % 2 {
            0 => Color::DarkGray,
            _ => Color::Black,
        };

        let name_cell = Cell::from(Text::from(metric.metric.clone()));
        let value_cell = match &metric.value {
            Value::Counter(v) => Cell::from(Text::from(format!("Counter: {v}"))),
            Value::Gauge(v) => Cell::from(Text::from(format!("Gauge: {v}"))),
            Value::Histogram(v) => Cell::from(Text::from(format!("Histogram: {v:?}"))),
            Value::Summary(v) => Cell::from(Text::from(format!("Summary: {v:?}"))),
            Value::Untyped(v) => Cell::from(Text::from(format!("Untyped: {v}"))),
        };
        let description_cell = Cell::from(Text::from(format!("{}", metric.labels)));

        Row::new(vec![name_cell, value_cell, description_cell])
            .style(Style::default().bg(color))
    });

    let bar = " █ ";
    let t = Table::new(
        rows,
        [
            Constraint::Min(50),  // Name
            Constraint::Min(100), // Value
            Constraint::Min(60),  // Description
        ],
    )
    .header(header)
    // .row_highlight_style(selected_row_style)
    // .column_highlight_style(selected_col_style)
    // .cell_highlight_style(selected_cell_style)
    .highlight_symbol(Text::from(vec![
        "".into(),
        bar.into(),
        bar.into(),
        "".into(),
    ]))
    // .bg(self.colors.buffer_bg)
    .highlight_spacing(HighlightSpacing::Always);
    frame.render_stateful_widget(t, area, state);
}

fn render_scrollbar(frame: &mut Frame, area: Rect, scroll_state: &mut ScrollbarState) {
    frame.render_stateful_widget(
        Scrollbar::default()
            .orientation(ScrollbarOrientation::VerticalRight)
            .begin_symbol(None)
            .end_symbol(None),
        area.inner(Margin {
            vertical: 1,
            horizontal: 1,
        }),
        scroll_state,
    );
}
