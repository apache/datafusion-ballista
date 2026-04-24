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

pub mod stages;

use ratatui::widgets::{ScrollbarState, TableState};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Job {
    pub job_id: String,
    pub job_name: String,
    pub status: String, // Running, Completed, Failed, Canceled
    pub start_time: i64,
    pub num_stages: usize,
    pub completed_stages: usize,
    pub percent_complete: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortColumn {
    None,
    Id,
    Name,
    Status,
    StagesCompleted,
    PercentComplete,
    StartTime,
}

#[derive(Clone, Debug)]
pub struct JobsData {
    pub jobs: Vec<Job>,
    pub table_state: TableState,
    pub scrollbar_state: ScrollbarState,
    pub sort_column: SortColumn,
    pub sort_order: crate::tui::domain::SortOrder,
}

impl Default for JobsData {
    fn default() -> Self {
        Self {
            jobs: Vec::new(),
            table_state: TableState::default(),
            scrollbar_state: ScrollbarState::new(0).position(0),
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
            SortColumn::Id => jobs.sort_by(|a, b| {
                let cmp = a.job_id.cmp(&b.job_id);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::Name => jobs.sort_by(|a, b| {
                let cmp = a.job_name.cmp(&b.job_name);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::Status => jobs.sort_by(|a, b| {
                let cmp = a.status.cmp(&b.status);
                if self.sort_order == crate::tui::domain::SortOrder::Descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            }),
            SortColumn::StagesCompleted => jobs.sort_by(|a, b| {
                let a_stages = a.completed_stages / a.num_stages;
                let b_stages = b.completed_stages / b.num_stages;
                let cmp = a_stages.cmp(&b_stages);
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
        if self.jobs.is_empty() {
            self.table_state.select(None);
            return;
        }

        if let Some(selected) = self.get_selected_job_index() {
            if selected < self.jobs.len() - 1 {
                self.table_state.select(Some(selected + 1));
            } else {
                self.table_state.select(None);
            }
        } else {
            self.table_state.select(Some(0));
        }
        self.scrollbar_state = self
            .scrollbar_state
            .position(self.get_selected_job_index().unwrap_or(0));
    }

    pub fn scroll_up(&mut self) {
        if self.jobs.is_empty() {
            self.table_state.select(None);
            return;
        }

        if let Some(selected) = self.get_selected_job_index() {
            if selected == 0 {
                self.table_state.select(None);
            } else {
                self.table_state.select(Some(selected - 1));
            }
        } else {
            self.table_state.select(Some(self.jobs.len() - 1));
        }
        self.scrollbar_state = self
            .scrollbar_state
            .position(self.get_selected_job_index().unwrap_or(0));
    }
}

#[cfg(test)]
mod tests {
    use crate::tui::domain::jobs::{Job, JobsData, SortColumn};
    use crate::tui::domain::SortOrder;

    fn make_job(
        id: &str,
        name: &str,
        status: &str,
        start_time: i64,
        num_stages: usize,
        completed_stages: usize,
        percent_complete: u8,
    ) -> Job {
        Job {
            job_id: id.to_string(),
            job_name: name.to_string(),
            status: status.to_string(),
            start_time,
            num_stages,
            completed_stages,
            percent_complete,
        }
    }

    fn make_jobs_data(
        jobs: Vec<Job>,
        sort_column: SortColumn,
        sort_order: SortOrder,
    ) -> JobsData {
        JobsData {
            jobs,
            sort_column,
            sort_order,
            ..JobsData::new()
        }
    }

    // --- sort_jobs tests ---

    #[test]
    fn sort_by_none_preserves_order() {
        let jobs = vec![
            make_job("c", "Charlie", "Running", 3, 1, 0, 0),
            make_job("a", "Alpha", "Running", 1, 1, 0, 0),
            make_job("b", "Beta", "Running", 2, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].job_id, "c");
        assert_eq!(refs[1].job_id, "a");
        assert_eq!(refs[2].job_id, "b");
    }

    #[test]
    fn sort_by_id_ascending() {
        let jobs = vec![
            make_job("c", "C", "Running", 3, 1, 0, 0),
            make_job("a", "A", "Running", 1, 1, 0, 0),
            make_job("b", "B", "Running", 2, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Id, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].job_id, "a");
        assert_eq!(refs[1].job_id, "b");
        assert_eq!(refs[2].job_id, "c");
    }

    #[test]
    fn sort_by_id_descending() {
        let jobs = vec![
            make_job("a", "A", "Running", 1, 1, 0, 0),
            make_job("c", "C", "Running", 3, 1, 0, 0),
            make_job("b", "B", "Running", 2, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Id, SortOrder::Descending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].job_id, "c");
        assert_eq!(refs[1].job_id, "b");
        assert_eq!(refs[2].job_id, "a");
    }

    #[test]
    fn sort_by_name_ascending() {
        let jobs = vec![
            make_job("1", "Zeta", "Running", 1, 1, 0, 0),
            make_job("2", "Alpha", "Running", 2, 1, 0, 0),
            make_job("3", "Mu", "Running", 3, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Name, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].job_name, "Alpha");
        assert_eq!(refs[1].job_name, "Mu");
        assert_eq!(refs[2].job_name, "Zeta");
    }

    #[test]
    fn sort_by_name_descending() {
        let jobs = vec![
            make_job("1", "Alpha", "Running", 1, 1, 0, 0),
            make_job("2", "Zeta", "Running", 2, 1, 0, 0),
            make_job("3", "Mu", "Running", 3, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Name, SortOrder::Descending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].job_name, "Zeta");
        assert_eq!(refs[1].job_name, "Mu");
        assert_eq!(refs[2].job_name, "Alpha");
    }

    #[test]
    fn sort_by_status_ascending() {
        let jobs = vec![
            make_job("1", "A", "Running", 1, 1, 0, 0),
            make_job("2", "B", "Completed", 2, 1, 0, 0),
            make_job("3", "C", "Failed", 3, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Status, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].status, "Completed");
        assert_eq!(refs[1].status, "Failed");
        assert_eq!(refs[2].status, "Running");
    }

    #[test]
    fn sort_by_status_descending() {
        let jobs = vec![
            make_job("1", "A", "Completed", 1, 1, 0, 0),
            make_job("2", "B", "Running", 2, 1, 0, 0),
            make_job("3", "C", "Failed", 3, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Status, SortOrder::Descending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].status, "Running");
        assert_eq!(refs[1].status, "Failed");
        assert_eq!(refs[2].status, "Completed");
    }

    #[test]
    fn sort_by_percent_complete_ascending() {
        let jobs = vec![
            make_job("1", "A", "Running", 1, 1, 0, 75),
            make_job("2", "B", "Running", 2, 1, 0, 25),
            make_job("3", "C", "Running", 3, 1, 0, 50),
        ];
        let data =
            make_jobs_data(jobs, SortColumn::PercentComplete, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].percent_complete, 25);
        assert_eq!(refs[1].percent_complete, 50);
        assert_eq!(refs[2].percent_complete, 75);
    }

    #[test]
    fn sort_by_percent_complete_descending() {
        let jobs = vec![
            make_job("1", "A", "Running", 1, 1, 0, 25),
            make_job("2", "B", "Running", 2, 1, 0, 75),
            make_job("3", "C", "Running", 3, 1, 0, 50),
        ];
        let data =
            make_jobs_data(jobs, SortColumn::PercentComplete, SortOrder::Descending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].percent_complete, 75);
        assert_eq!(refs[1].percent_complete, 50);
        assert_eq!(refs[2].percent_complete, 25);
    }

    #[test]
    fn sort_by_start_time_ascending() {
        let jobs = vec![
            make_job("1", "A", "Running", 300, 1, 0, 0),
            make_job("2", "B", "Running", 100, 1, 0, 0),
            make_job("3", "C", "Running", 200, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::StartTime, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].start_time, 100);
        assert_eq!(refs[1].start_time, 200);
        assert_eq!(refs[2].start_time, 300);
    }

    #[test]
    fn sort_by_start_time_descending() {
        let jobs = vec![
            make_job("1", "A", "Running", 100, 1, 0, 0),
            make_job("2", "B", "Running", 300, 1, 0, 0),
            make_job("3", "C", "Running", 200, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::StartTime, SortOrder::Descending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].start_time, 300);
        assert_eq!(refs[1].start_time, 200);
        assert_eq!(refs[2].start_time, 100);
    }

    // --- selected_job tests ---

    #[test]
    fn selected_job_no_selection_returns_none() {
        let jobs = vec![make_job("j1", "Job One", "Running", 1, 1, 0, 0)];
        let data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        assert!(data.selected_job("").is_none());
    }

    #[test]
    fn selected_job_returns_correct_job() {
        let jobs = vec![
            make_job("j1", "Job One", "Running", 1, 1, 0, 0),
            make_job("j2", "Job Two", "Running", 2, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(1));
        let job = data.selected_job("").unwrap();
        assert_eq!(job.job_id, "j2");
    }

    #[test]
    fn selected_job_filters_by_search_term_on_id() {
        let jobs = vec![
            make_job("abc-123", "Job One", "Running", 1, 1, 0, 0),
            make_job("xyz-456", "Job Two", "Running", 2, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(0));
        // Only "xyz-456" matches the search; index 0 in filtered list is that job
        let job = data.selected_job("xyz").unwrap();
        assert_eq!(job.job_id, "xyz-456");
    }

    #[test]
    fn selected_job_filters_by_search_term_on_name() {
        let jobs = vec![
            make_job("j1", "Query Alpha", "Running", 1, 1, 0, 0),
            make_job("j2", "Query Beta", "Running", 2, 1, 0, 0),
            make_job("j3", "Other Job", "Running", 3, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(1));
        // Filtered by "beta": only "Query Beta" matches, index 0
        let job = data.selected_job("beta");
        assert!(job.is_none()); // index 1 out of bounds in filtered list of 1

        data.table_state.select(Some(0));
        let job = data.selected_job("beta").unwrap();
        assert_eq!(job.job_id, "j2");
    }

    #[test]
    fn selected_job_search_is_case_insensitive() {
        let jobs = vec![make_job("j1", "My QUERY", "Running", 1, 1, 0, 0)];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(0));
        assert!(data.selected_job("query").is_some());
        assert!(data.selected_job("QUERY").is_some());
        assert!(data.selected_job("Query").is_some());
    }

    #[test]
    fn selected_job_no_match_returns_none() {
        let jobs = vec![make_job("j1", "Job One", "Running", 1, 1, 0, 0)];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(0));
        assert!(data.selected_job("nonexistent").is_none());
    }

    // --- scroll_down tests ---

    #[test]
    fn scroll_down_empty_list_stays_none() {
        let mut data = JobsData::new();
        data.scroll_down();
        assert_eq!(data.table_state.selected(), None);
    }

    #[test]
    fn scroll_down_with_no_selection_selects_first() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.scroll_down();
        assert_eq!(data.table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_down_advances_selection() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 1, 0, 0),
            make_job("j3", "C", "Running", 3, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(0));
        data.scroll_down();
        assert_eq!(data.table_state.selected(), Some(1));
    }

    #[test]
    fn scroll_down_at_last_item_deselects() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(1));
        data.scroll_down();
        assert_eq!(data.table_state.selected(), None);
    }

    // --- scroll_up tests ---

    #[test]
    fn scroll_up_empty_list_stays_none() {
        let mut data = JobsData::new();
        data.scroll_up();
        assert_eq!(data.table_state.selected(), None);
    }

    #[test]
    fn scroll_up_with_no_selection_selects_last() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 1, 0, 0),
            make_job("j3", "C", "Running", 3, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.scroll_up();
        assert_eq!(data.table_state.selected(), Some(2));
    }

    #[test]
    fn scroll_up_moves_selection_back() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(1));
        data.scroll_up();
        assert_eq!(data.table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_up_at_first_item_deselects() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(0));
        data.scroll_up();
        assert_eq!(data.table_state.selected(), None);
    }
}
