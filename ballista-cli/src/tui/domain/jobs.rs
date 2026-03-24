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

use ratatui::widgets::TableState;
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Job {
    pub job_id: String,
    pub job_name: String,
    pub status: String,     // Running, Completed, Failed, Canceled
    pub job_status: String, // Longer description of the status
    pub start_time: i64,
    pub num_stages: usize,
    pub completed_stages: usize,
    pub percent_complete: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortColumn {
    None,
    StartTime,
    Status,
    PercentComplete,
}

#[derive(Clone, Debug)]
pub struct JobsData {
    pub jobs: Vec<Job>,
    pub table_state: TableState,
    pub sort_column: SortColumn,
    pub sort_order: crate::tui::domain::SortOrder,
}

impl Default for JobsData {
    fn default() -> Self {
        Self {
            jobs: Vec::new(),
            table_state: TableState::default(),
            sort_column: SortColumn::None,
            sort_order: crate::tui::domain::SortOrder::Ascending,
        }
    }
}

impl JobsData {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Deserialize, Debug)]
pub struct CancelJobResponse {
    pub canceled: bool,
}

pub enum CancelJobResult {
    Success { job_id: String },
    NotCanceled { job_id: String },
    Failure { job_id: String, error: String },
}

#[derive(Clone, Debug)]
pub struct JobDetails {
    pub job_id: String,
    pub logical_plan: Option<String>,
    pub physical_plan: Option<String>,
    pub stage_plan: Option<String>,
}

#[derive(Clone, Debug)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct GraphStage {
    pub label: String,
    pub nodes: Vec<GraphNode>,
}

#[derive(Clone, Debug)]
pub struct StagesGraph {
    pub job_id: String,
    pub stages: Vec<GraphStage>,
    pub edges: Vec<(String, String)>,
}

impl JobsData {
    pub fn sort_jobs(&self, jobs: &mut Vec<&Job>) {
        match self.sort_column {
            SortColumn::Status => jobs.sort_by(|a, b| {
                let cmp = a.status.cmp(&b.status);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::PercentComplete => jobs.sort_by(|a, b| {
                let cmp = a.percent_complete.cmp(&b.percent_complete);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::StartTime => jobs.sort_by(|a, b| {
                let cmp = a.start_time.cmp(&b.start_time);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::None => {}
        }
    }

    pub fn selected_job<'a>(&'a self, search_term: &str) -> Option<&'a Job> {
        let search_term = search_term.to_lowercase();
        let mut filtered: Vec<&Job> = if search_term.is_empty() {
            self.jobs.iter().collect()
        } else {
            self.jobs
                .iter()
                .filter(|j| {
                    j.job_id.to_lowercase().contains(&search_term)
                        || j.job_name.to_lowercase().contains(&search_term)
                })
                .collect()
        };
        self.sort_jobs(&mut filtered);
        self.get_selected_job_index()
            .and_then(|idx| filtered.get(idx).copied())
    }

    fn get_selected_job_index(&self) -> Option<usize> {
        self.table_state.selected()
    }

    pub fn scroll_down(&mut self) {
        if let Some(selected) = self.get_selected_job_index() {
            if selected < self.jobs.len() - 1 {
                self.table_state.select(Some(selected + 1));
            } else {
                self.table_state.select(None);
            }
        } else {
            self.table_state.select(Some(0));
        }
    }

    pub fn scroll_up(&mut self) {
        if let Some(selected) = self.get_selected_job_index() {
            if selected == 0 {
                self.table_state.select(None);
            } else {
                self.table_state.select(Some(selected - 1));
            }
        } else {
            self.table_state.select(Some(self.jobs.len() - 1));
        }
    }
}
