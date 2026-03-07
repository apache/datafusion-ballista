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
use serde::Deserialize;
use std::str::FromStr;

#[derive(Deserialize, Clone, Debug)]
pub struct SchedulerState {
    pub started: i64,
    pub version: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ExecutorsData {
    pub host: String,
    pub port: u16,
    pub id: String,
    pub last_seen: i64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct JobsData {
    // pub job_id: String,
    // pub job_name: String,
    pub job_status: String,
    // pub num_stages: usize,
    // pub completed_stages: usize,
    // pub percent_complete: u8,
}

#[derive(Clone, Debug)]
pub struct DashboardData {
    pub scheduler_state: Option<SchedulerState>,
    pub executors_data: Option<Vec<ExecutorsData>>,
    pub jobs_data: Option<Vec<JobsData>>,
}

/// A Prometheus metric
///
/// Returned by the /api/metrics REST endpoint
#[derive(Clone, Debug)]
pub struct Metric {
    pub sample: Sample,
    pub help: String,
}

#[derive(Clone, Debug)]
pub struct MetricsData {
    pub metrics: Option<Vec<Metric>>,
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
            metrics: Some(metrics),
        })
    }
}

impl DashboardData {
    pub fn new() -> Self {
        Self {
            scheduler_state: None,
            executors_data: None,
            jobs_data: None,
        }
    }
}
