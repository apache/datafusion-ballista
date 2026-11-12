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
use crate::tui::domain::SortOrder;
use prometheus_parse::Sample;
use ratatui::widgets::{ScrollbarState, TableState};
use std::str::FromStr;

/// A Prometheus metric
///
/// Returned by the /api/metrics REST endpoint
#[derive(Clone, Debug)]
pub struct Metric {
    pub sample: Sample,
    pub help: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortColumn {
    None,
    Name,
}

#[derive(Clone, Debug)]
pub struct MetricsData {
    pub metrics: Vec<Metric>,
    pub scrollbar_state: ScrollbarState,
    pub table_state: TableState,
    pub sort_column: SortColumn,
    pub sort_order: SortOrder,
}

impl Default for MetricsData {
    fn default() -> Self {
        Self {
            metrics: vec![],
            scrollbar_state: ScrollbarState::new(0),
            table_state: TableState::default(),
            sort_column: SortColumn::None,
            sort_order: SortOrder::Ascending,
        }
    }
}

impl MetricsData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sort(&mut self) {
        match self.sort_column {
            SortColumn::Name => self.metrics.sort_by(|a, b| {
                let cmp = a.sample.metric.cmp(&b.sample.metric);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::None => {}
        }
    }

    pub fn scroll_down(&mut self) {
        super::scroll_down(
            &mut self.table_state,
            &mut self.scrollbar_state,
            self.metrics.len(),
        );
    }

    pub fn scroll_up(&mut self) {
        super::scroll_up(
            &mut self.table_state,
            &mut self.scrollbar_state,
            self.metrics.len(),
        );
    }
}

/// Newtype struct for parsing the HTTP response into a vec
pub(crate) struct MetricsResponse {
    pub metrics: Vec<Metric>,
}

impl FromStr for MetricsResponse {
    type Err = std::io::Error;

    fn from_str(http_response: &str) -> Result<Self, Self::Err> {
        let mut metrics: Vec<Metric> = Vec::new();

        let lines: Vec<std::io::Result<String>> = http_response
            .lines()
            .map(|line| Ok(line.to_string()))
            .collect();
        let scrape = prometheus_parse::Scrape::parse_at(lines.into_iter(), App::now())?;
        for sample in scrape.samples {
            let metric = Metric {
                sample: sample.clone(),
                help: scrape
                    .docs
                    .get(&sample.metric)
                    .unwrap_or(&String::new())
                    .to_string(),
            };
            metrics.push(metric);
        }
        metrics.sort_by(|a, b| a.sample.metric.cmp(&b.sample.metric));

        Ok(MetricsResponse { metrics })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const PROMETHEUS_TEXT: &str = "\
# HELP ballista_jobs_total Total number of jobs submitted
# TYPE ballista_jobs_total counter
ballista_jobs_total 42
# HELP ballista_executors_active Active executor count
# TYPE ballista_executors_active gauge
ballista_executors_active 3
";

    fn parse_metrics(text: &str) -> Vec<Metric> {
        MetricsResponse::from_str(text).unwrap().metrics
    }

    // --- MetricsResponse::from_str tests ---

    #[test]
    fn parse_valid_prometheus_text_produces_metrics() {
        let metrics = parse_metrics(PROMETHEUS_TEXT);
        assert!(!metrics.is_empty());
        let names: Vec<&str> = metrics.iter().map(|m| m.sample.metric.as_str()).collect();
        assert!(names.contains(&"ballista_jobs_total"));
        assert!(names.contains(&"ballista_executors_active"));
    }

    #[test]
    fn parse_empty_string_produces_empty_vec() {
        let metrics = parse_metrics("");
        assert!(metrics.is_empty());
    }

    #[test]
    fn parse_with_help_text_sets_help_field() {
        let metrics = parse_metrics(PROMETHEUS_TEXT);
        let jobs_metric = metrics
            .iter()
            .find(|m| m.sample.metric == "ballista_jobs_total")
            .unwrap();
        assert_eq!(jobs_metric.help, "Total number of jobs submitted");
    }

    // --- sort tests ---

    fn make_metrics_data(
        metrics: Vec<Metric>,
        sort_column: SortColumn,
        sort_order: SortOrder,
    ) -> MetricsData {
        MetricsData {
            metrics,
            sort_column,
            sort_order,
            ..MetricsData::new()
        }
    }

    #[test]
    fn sort_by_none_preserves_order() {
        let metrics = parse_metrics(PROMETHEUS_TEXT);
        let original_names: Vec<String> =
            metrics.iter().map(|m| m.sample.metric.clone()).collect();
        let mut data = make_metrics_data(metrics, SortColumn::None, SortOrder::Ascending);
        data.sort();
        let after_names: Vec<String> = data
            .metrics
            .iter()
            .map(|m| m.sample.metric.clone())
            .collect();
        assert_eq!(original_names, after_names);
    }

    #[test]
    fn sort_by_name_ascending() {
        let metrics = parse_metrics(PROMETHEUS_TEXT);
        let mut data = make_metrics_data(metrics, SortColumn::Name, SortOrder::Ascending);
        data.sort();
        let names: Vec<&str> = data
            .metrics
            .iter()
            .map(|m| m.sample.metric.as_str())
            .collect();
        // "ballista_executors_active" < "ballista_jobs_total" lexicographically
        assert_eq!(names[0], "ballista_executors_active");
        assert_eq!(names[1], "ballista_jobs_total");
    }

    #[test]
    fn sort_by_name_descending() {
        let metrics = parse_metrics(PROMETHEUS_TEXT);
        let mut data =
            make_metrics_data(metrics, SortColumn::Name, SortOrder::Descending);
        data.sort();
        let names: Vec<&str> = data
            .metrics
            .iter()
            .map(|m| m.sample.metric.as_str())
            .collect();
        assert_eq!(names[0], "ballista_jobs_total");
        assert_eq!(names[1], "ballista_executors_active");
    }
}
