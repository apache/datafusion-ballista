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

#[cfg(not(feature = "web"))]
use crate::tui::{
    TuiResult,
    event::{Event, UiData},
};
use crate::tui::{
    app::App,
    domain::{
        SortOrder,
        metrics::{Metric, SortColumn},
    },
    ui::search_box::render_search_box,
    ui::vertical_scrollbar::render_scrollbar,
};

use prometheus_parse::HistogramCount;

use crate::tui::ui::vertical_scrollbar;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    text::Text,
    widgets::{
        Block, Borders, Cell, Clear, HighlightSpacing, Paragraph, Row, Table, TableState,
    },
};

#[cfg(not(feature = "web"))]
pub async fn load_metrics_data(app: &App) -> TuiResult<()> {
    let metrics = app.http_client.get_metrics().await.unwrap_or_else(|e| {
        tracing::error!("Failed to load the metrics: {e:?}");
        Vec::new()
    });

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

    let rects = Layout::vertical([
        Constraint::Length(3), // Search box
        Constraint::Min(5),    // Table
    ])
    .split(area);

    render_search_box(f, rects[0], app);

    if !filtered_metrics.is_empty() {
        let mut scroll_state = app.metrics_data.scrollbar_state;
        let mut table_state = app.metrics_data.table_state;
        let table_area = vertical_scrollbar::split_area(rects[1]);
        render_metrics_table(f, table_area[0], app, &filtered_metrics, &mut table_state);
        render_scrollbar(f, table_area[1], &mut scroll_state);
    } else if are_metrics_enabled(app) {
        render_no_metrics(f, rects[1], "No metrics.", app);
    } else {
        render_no_metrics(
            f,
            rects[1],
            "The scheduler is built with 'prometheus-metrics' feature disabled.",
            app,
        );
    }
}

fn are_metrics_enabled(app: &App) -> bool {
    if let Some(scheduler_state) = app.executors_data.scheduler_state.as_ref() {
        scheduler_state.prometheus_support
    } else {
        false
    }
}

fn render_no_metrics(f: &mut Frame, area: Rect, reason: &str, app: &App) {
    let block = Block::default()
        .borders(Borders::all())
        .style(app.theme.text_error);
    let paragraph = Paragraph::new(reason)
        .style(app.theme.text_error)
        .centered()
        .block(block);
    f.render_widget(paragraph, area);
}

fn column_suffix(
    active_sort_column: &SortColumn,
    sort_order: &SortOrder,
    sort_column: &SortColumn,
) -> &'static str {
    match (active_sort_column, sort_order) {
        (sc, SortOrder::Ascending) if sc == sort_column => " ▲",
        (sc, SortOrder::Descending) if sc == sort_column => " ▼",
        _ => "",
    }
}

fn render_metrics_table(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    metrics: &[&Metric],
    state: &mut TableState,
) {
    let sort_column = &app.metrics_data.sort_column;
    let sort_order = &app.metrics_data.sort_order;

    let name_suffix = column_suffix(sort_column, sort_order, &SortColumn::Name);

    let header = [
        format!("Name{name_suffix}"),
        "Value".to_string(),
        "Description".to_string(),
    ]
    .into_iter()
    .map(Cell::from)
    .collect::<Row>()
    .style(app.theme.table_header)
    .height(1);

    let rows = metrics.iter().enumerate().map(|(i, metric)| {
        use prometheus_parse::Value;

        let row_style = if i % 2 == 0 {
            app.theme.row_even
        } else {
            app.theme.row_odd
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

        Row::new(vec![name_cell, value_cell, description_cell]).style(row_style)
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
    .row_highlight_style(app.theme.row_selected)
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
