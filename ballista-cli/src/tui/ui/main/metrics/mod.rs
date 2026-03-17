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

use crate::tui::{
    TuiResult,
    app::App,
    domain::Metric,
    event::{Event, UiData},
    ui::search_box::render_search_box,
};
use prometheus_parse::HistogramCount;

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
            Vec::new()
        }
    };

    app.send_event(Event::DataLoaded {
        data: UiData::Metrics(metrics),
    })
    .await
}

pub fn render_metrics(f: &mut Frame, area: Rect, app: &App) {
    f.render_widget(Clear, area);

    let search_term = app.search_term.to_lowercase();
    let filtered_metrics: Vec<&Metric> = if search_term.is_empty() {
        app.metrics_data.metrics.iter().collect()
    } else {
        app.metrics_data
            .metrics
            .iter()
            .filter(|m| m.sample.metric.to_lowercase().contains(&search_term))
            .collect()
    };

    let vertical = Layout::vertical([
        Constraint::Length(3), // Search box
        Constraint::Min(5),    // Table
        Constraint::Length(4), // Scrollbar
    ]);
    let rects = vertical.split(area);

    render_search_box(f, rects[0], app);

    if !filtered_metrics.is_empty() {
        let mut scroll_state = ScrollbarState::new((filtered_metrics.len() - 1) * 2);
        let mut table_state = app.metrics_data.table_state;
        render_metrics_table(f, rects[1], &filtered_metrics, &mut table_state);
        render_scrollbar(f, rects[1], &mut scroll_state);
    } else {
        if !are_metrics_enabled(app) {
            render_no_metrics(
                f,
                rects[1],
                "The scheduler is built with 'prometheus_metric' feature disabled.",
            );
        } else {
            render_no_metrics(f, rects[1], "No metrics.");
        }
    }
}

fn are_metrics_enabled(app: &App) -> bool {
    if let Some(scheduler_state) = app.dashboard_data.scheduler_state.as_ref() {
        scheduler_state.prometheus_support
    } else {
        false
    }
}

fn render_no_metrics(f: &mut Frame, area: Rect, reason: &str) {
    let block = Block::default()
        .borders(Borders::all())
        .style(Style::default().fg(Color::Red));
    let paragraph = Paragraph::new(reason)
        .style(Style::default().bold())
        .centered()
        .block(block);
    f.render_widget(paragraph, area);
}

fn render_metrics_table(
    frame: &mut Frame,
    area: Rect,
    metrics: &[&Metric],
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

        let name_cell = Cell::from(Text::from(metric.sample.metric.clone()));
        let value_cell = match &metric.sample.value {
            Value::Counter(v) => Cell::from(Text::from(format!("Counter: {v}"))),
            Value::Gauge(v) => Cell::from(Text::from(format!("Gauge: {v}"))),
            Value::Histogram(histograms) => histogram_cell(histograms),
            Value::Summary(v) => Cell::from(Text::from(format!("Summary: {v:?}"))),
            Value::Untyped(v) => Cell::from(Text::from(format!("Untyped: {v}"))),
        };
        let description_cell = Cell::from(Text::from(metric.help.clone()));

        Row::new(vec![name_cell, value_cell, description_cell])
            .style(Style::default().bg(color))
    });

    let t = Table::new(
        rows,
        [
            Constraint::Percentage(25), // Name
            Constraint::Percentage(35), // Value
            Constraint::Percentage(40), // Description
        ],
    )
    .block(Block::default().borders(Borders::all()))
    .header(header)
    .row_highlight_style(Style::default().bg(Color::Indexed(29)))
    .highlight_spacing(HighlightSpacing::Always);
    frame.render_stateful_widget(t, area, state);
}

fn histogram_cell(histograms: &[HistogramCount]) -> Cell<'_> {
    let mut data = Vec::new();
    for histogram in histograms {
        let item = format!("<{} ={}", histogram.less_than, histogram.count);
        data.push(item);
    }
    Cell::from(Text::from(format!("Histogram: {}", data.join(", "))))
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
