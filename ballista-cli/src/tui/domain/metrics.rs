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

use prometheus_parse::Sample;
use ratatui::widgets::TableState;
use std::str::FromStr;

/// A Prometheus metric
///
/// Returned by the /api/metrics REST endpoint
#[derive(Clone, Debug)]
pub struct Metric {
    pub sample: Sample,
    pub help: String,
}

#[derive(Clone, Debug, Default)]
pub struct MetricsData {
    pub metrics: Vec<Metric>,
    pub table_state: TableState,
}

impl MetricsData {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_selected_metric_index(&self) -> Option<usize> {
        self.table_state.selected()
    }

    pub fn scroll_down(&mut self) {
        if let Some(selected) = self.get_selected_metric_index() {
            if selected < self.metrics.len() - 1 {
                self.table_state.select(Some(selected + 1));
            } else {
                self.table_state.select(None);
            }
        } else {
            self.table_state.select(Some(0));
        }
    }

    pub fn scroll_up(&mut self) {
        if let Some(selected) = self.get_selected_metric_index() {
            if selected == 0 {
                self.table_state.select(None);
            } else {
                self.table_state.select(Some(selected - 1));
            }
        } else {
            self.table_state.select(Some(self.metrics.len() - 1));
        }
    }
}

impl FromStr for MetricsData {
    type Err = std::io::Error;

    fn from_str(http_response: &str) -> Result<Self, Self::Err> {
        let mut metrics: Vec<Metric> = Vec::new();

        let lines: Vec<std::io::Result<String>> = http_response
            .lines()
            .map(|line| Ok(line.to_string()))
            .collect();
        let scrape = prometheus_parse::Scrape::parse(lines.into_iter())?;
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

        Ok(MetricsData {
            metrics,
            table_state: ratatui::widgets::TableState::default(), // dummy state that is ignored
        })
    }
}
