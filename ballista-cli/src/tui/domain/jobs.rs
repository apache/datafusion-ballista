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
use std::collections::BTreeMap;

#[derive(Deserialize, Clone, Debug)]
pub struct Job {
    pub job_id: String,
    pub job_name: String,
    pub status: String,     // Running, Completed, Failed, Canceled
    pub job_status: String, // human-readable status/failure detail, e.g. "Failed: <reason>"
    pub start_time: i64,
    pub end_time: i64,
    pub num_stages: usize,
    pub completed_stages: usize,
    pub percent_complete: u8,
}

impl Job {
    pub fn is_queued(&self) -> bool {
        self.status == "Queued"
    }

    pub fn is_running(&self) -> bool {
        self.status == "Running"
    }

    pub fn is_completed(&self) -> bool {
        self.status == "Completed"
    }

    pub fn is_failed(&self) -> bool {
        self.status == "Failed"
    }
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
    Duration,
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
                let stage_completion = |job: &Job| {
                    if job.num_stages == 0 {
                        (0_u128, 1_u128)
                    } else {
                        (job.completed_stages as u128, job.num_stages as u128)
                    }
                };
                let (a_completed, a_total) = stage_completion(a);
                let (b_completed, b_total) = stage_completion(b);
                let cmp = (a_completed * b_total).cmp(&(b_completed * a_total));
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
            SortColumn::Duration => jobs.sort_by(|a, b| {
                let duration_a = if a.end_time > 0 {
                    a.end_time - a.start_time
                } else {
                    0
                };
                let duration_b = if b.end_time > 0 {
                    b.end_time - b.start_time
                } else {
                    0
                };
                let cmp = duration_a.cmp(&duration_b);
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

#[derive(Deserialize, Debug)]
pub struct CancelJobResponse {
    pub canceled: bool,
}

#[derive(Clone, Debug)]
pub enum CancelJobResult {
    Success { job_id: String },
    NotCanceled { job_id: String },
    Failure { job_id: String, error: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalFormat {
    Default,
    Tree,
}

#[derive(Clone, Debug)]
pub struct JobDetails {
    pub job_id: String,
    pub logical_plan: Option<String>,
    pub physical_plan: Option<String>,
    pub physical_plan_tree: Option<String>,
    pub stage_plan: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JobConfigEntry {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct JobConfigPopup {
    pub job_id: String,
    pub entries: Vec<JobConfigEntry>,
    pub search_term: String,
    pub table_state: TableState,
    pub scrollbar_state: ScrollbarState,
}

impl JobConfigPopup {
    pub fn new(job_id: String, entries: Vec<JobConfigEntry>) -> Self {
        let has_entries = !entries.is_empty();
        let len = entries.len();
        Self {
            job_id,
            entries,
            search_term: String::new(),
            table_state: TableState::default().with_selected(has_entries.then_some(0)),
            scrollbar_state: ScrollbarState::new(len).position(0),
        }
    }

    pub fn filtered_entries(&self) -> Vec<&JobConfigEntry> {
        let search_term = self.search_term.to_lowercase();
        if search_term.is_empty() {
            self.entries.iter().collect()
        } else {
            self.entries
                .iter()
                .filter(|entry| {
                    entry.key.to_lowercase().contains(&search_term)
                        || entry.value.to_lowercase().contains(&search_term)
                })
                .collect()
        }
    }

    pub fn scroll_down(&mut self) {
        let len = self.filtered_entries().len();
        if len == 0 {
            self.table_state.select(None);
            return;
        }

        let next = match self.table_state.selected() {
            Some(selected) if selected + 1 < len => Some(selected + 1),
            Some(_) => None,
            None => Some(0),
        };
        self.table_state.select(next);
        self.scrollbar_state = self.scrollbar_state.position(next.unwrap_or(0));
    }

    pub fn scroll_up(&mut self) {
        let len = self.filtered_entries().len();
        if len == 0 {
            self.table_state.select(None);
            return;
        }

        let next = match self.table_state.selected() {
            Some(0) => None,
            Some(selected) => Some(selected - 1),
            None => Some(len - 1),
        };
        self.table_state.select(next);
        self.scrollbar_state = self.scrollbar_state.position(next.unwrap_or(0));
    }

    pub fn push_search_char(&mut self, c: char) {
        self.search_term.push(c);
        self.reset_selection_for_filter();
    }

    pub fn pop_search_char(&mut self) {
        self.search_term.pop();
        self.reset_selection_for_filter();
    }

    pub fn clear_search(&mut self) {
        self.search_term.clear();
        self.reset_selection_for_filter();
    }

    fn reset_selection_for_filter(&mut self) {
        let len = self.filtered_entries().len();
        let selected = if len == 0 { None } else { Some(0) };
        self.table_state.select(selected);
        self.scrollbar_state = ScrollbarState::new(len).position(0);
    }
}

pub type JobConfigResponse = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlanTab {
    Stage,
    Physical,
    Logical,
}

#[derive(Clone, Debug)]
pub struct JobPlansPopup {
    pub details: JobDetails,
    tab: PlanTab,
    physical_format: PhysicalFormat,
    vertical_scroll_position: u16,
    horizontal_scroll_position: u16,
}

impl JobPlansPopup {
    pub fn new(details: JobDetails, tab: PlanTab) -> Self {
        Self {
            details,
            tab,
            physical_format: PhysicalFormat::Default,
            vertical_scroll_position: 0,
            horizontal_scroll_position: 0,
        }
    }

    pub fn get_tab(&self) -> &PlanTab {
        &self.tab
    }

    pub fn get_physical_format(&self) -> &PhysicalFormat {
        &self.physical_format
    }

    /// Returns Some(()) if a fetch is needed (tree not cached yet), None if already available.
    pub fn set_physical_format(&mut self, fmt: PhysicalFormat) -> Option<()> {
        self.physical_format = fmt;
        self.vertical_scroll_position = 0;
        self.horizontal_scroll_position = 0;
        if self.physical_format == PhysicalFormat::Tree
            && self.details.physical_plan_tree.is_none()
        {
            Some(())
        } else {
            None
        }
    }

    pub fn set_tab(&mut self, tab: PlanTab) {
        self.tab = tab;
        self.vertical_scroll_position = 0;
        self.horizontal_scroll_position = 0;
    }

    pub fn vertical_scroll_position(&self) -> u16 {
        self.vertical_scroll_position
    }

    pub fn horizontal_scroll_position(&self) -> u16 {
        self.horizontal_scroll_position
    }

    pub fn scroll_up(&mut self) {
        self.vertical_scroll_position = self.vertical_scroll_position.saturating_sub(1);
    }

    pub fn scroll_down(&mut self) {
        self.vertical_scroll_position = self.vertical_scroll_position.saturating_add(1);
    }

    pub fn scroll_left(&mut self) {
        self.horizontal_scroll_position =
            self.horizontal_scroll_position.saturating_sub(1);
    }

    pub fn scroll_right(&mut self) {
        self.horizontal_scroll_position =
            self.horizontal_scroll_position.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use crate::tui::domain::SortOrder;
    use crate::tui::domain::jobs::{
        Job, JobConfigEntry, JobConfigPopup, JobDetails, JobPlansPopup, JobsData,
        PlanTab, SortColumn,
    };

    #[expect(clippy::too_many_arguments)]
    fn make_job(
        id: &str,
        name: &str,
        status: &str,
        start_time: i64,
        end_time: i64,
        num_stages: usize,
        completed_stages: usize,
        percent_complete: u8,
    ) -> Job {
        Job {
            job_id: id.to_string(),
            job_name: name.to_string(),
            status: status.to_string(),
            job_status: status.to_string(),
            start_time,
            end_time,
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
            make_job("c", "Charlie", "Running", 3, 4, 1, 0, 0),
            make_job("a", "Alpha", "Running", 1, 3, 1, 0, 0),
            make_job("b", "Beta", "Running", 2, 3, 1, 0, 0),
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
            make_job("c", "C", "Running", 3, 4, 1, 0, 0),
            make_job("a", "A", "Running", 1, 2, 1, 0, 0),
            make_job("b", "B", "Running", 2, 4, 1, 0, 0),
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
            make_job("a", "A", "Running", 1, 3, 1, 0, 0),
            make_job("c", "C", "Running", 3, 4, 1, 0, 0),
            make_job("b", "B", "Running", 2, 5, 1, 0, 0),
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
            make_job("1", "Zeta", "Running", 1, 2, 1, 0, 0),
            make_job("2", "Alpha", "Running", 2, 3, 1, 0, 0),
            make_job("3", "Mu", "Running", 3, 4, 1, 0, 0),
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
            make_job("1", "Alpha", "Running", 1, 2, 1, 0, 0),
            make_job("2", "Zeta", "Running", 2, 3, 1, 0, 0),
            make_job("3", "Mu", "Running", 3, 4, 1, 0, 0),
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
            make_job("1", "A", "Running", 1, 2, 1, 0, 0),
            make_job("2", "B", "Completed", 2, 3, 1, 0, 0),
            make_job("3", "C", "Failed", 3, 4, 1, 0, 0),
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
            make_job("1", "A", "Completed", 1, 2, 1, 0, 0),
            make_job("2", "B", "Running", 2, 3, 1, 0, 0),
            make_job("3", "C", "Failed", 3, 4, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Status, SortOrder::Descending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].status, "Running");
        assert_eq!(refs[1].status, "Failed");
        assert_eq!(refs[2].status, "Completed");
    }

    #[test]
    fn sort_by_stages_completed_uses_completion_ratio() {
        let jobs = vec![
            make_job("quarter", "A", "Running", 1, 2, 4, 1, 0),
            make_job("three_quarters", "B", "Running", 2, 3, 4, 3, 0),
            make_job("half", "C", "Running", 3, 4, 4, 2, 0),
        ];
        let data =
            make_jobs_data(jobs, SortColumn::StagesCompleted, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].job_id, "quarter");
        assert_eq!(refs[1].job_id, "half");
        assert_eq!(refs[2].job_id, "three_quarters");
    }

    #[test]
    fn sort_by_stages_completed_handles_zero_stages() {
        let jobs = vec![
            make_job("zero", "A", "Running", 1, 2, 0, 0, 0),
            make_job("half", "B", "Running", 2, 3, 2, 1, 0),
        ];
        let data =
            make_jobs_data(jobs, SortColumn::StagesCompleted, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].job_id, "zero");
        assert_eq!(refs[1].job_id, "half");
    }

    #[test]
    fn sort_by_percent_complete_ascending() {
        let jobs = vec![
            make_job("1", "A", "Running", 1, 2, 1, 0, 75),
            make_job("2", "B", "Running", 2, 3, 1, 0, 25),
            make_job("3", "C", "Running", 3, 4, 1, 0, 50),
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
            make_job("1", "A", "Running", 1, 2, 1, 0, 25),
            make_job("2", "B", "Running", 2, 3, 1, 0, 75),
            make_job("3", "C", "Running", 3, 4, 1, 0, 50),
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
            make_job("1", "A", "Running", 300, 301, 1, 0, 0),
            make_job("2", "B", "Running", 100, 101, 1, 0, 0),
            make_job("3", "C", "Running", 200, 201, 1, 0, 0),
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
            make_job("1", "A", "Running", 100, 101, 1, 0, 0),
            make_job("2", "B", "Running", 300, 301, 1, 0, 0),
            make_job("3", "C", "Running", 200, 201, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::StartTime, SortOrder::Descending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].start_time, 300);
        assert_eq!(refs[1].start_time, 200);
        assert_eq!(refs[2].start_time, 100);
    }

    #[test]
    fn sort_by_duration_ascending() {
        let jobs = vec![
            make_job("1", "A", "Running", 300, 301, 1, 0, 0),
            make_job("2", "B", "Running", 100, 102, 1, 0, 0),
            make_job("3", "C", "Running", 200, 203, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Duration, SortOrder::Ascending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].start_time, 300);
        assert_eq!(refs[1].start_time, 100);
        assert_eq!(refs[2].start_time, 200);
    }

    #[test]
    fn sort_by_duration_descending() {
        let jobs = vec![
            make_job("1", "A", "Running", 100, 102, 1, 0, 0),
            make_job("2", "B", "Running", 300, 301, 1, 0, 0),
            make_job("3", "C", "Running", 200, 203, 1, 0, 0),
        ];
        let data = make_jobs_data(jobs, SortColumn::Duration, SortOrder::Descending);
        let mut refs: Vec<&Job> = data.jobs.iter().collect();
        data.sort_jobs(&mut refs);
        assert_eq!(refs[0].start_time, 200);
        assert_eq!(refs[1].start_time, 100);
        assert_eq!(refs[2].start_time, 300);
    }

    // --- selected_job tests ---

    #[test]
    fn selected_job_no_selection_returns_none() {
        let jobs = vec![make_job("j1", "Job One", "Running", 1, 2, 1, 0, 0)];
        let data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        assert!(data.selected_job("").is_none());
    }

    #[test]
    fn selected_job_returns_correct_job() {
        let jobs = vec![
            make_job("j1", "Job One", "Running", 1, 2, 1, 0, 0),
            make_job("j2", "Job Two", "Running", 2, 3, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(1));
        let job = data.selected_job("").unwrap();
        assert_eq!(job.job_id, "j2");
    }

    #[test]
    fn job_status_carries_failure_detail_independent_of_status() {
        let job = Job {
            job_id: "j1".to_string(),
            job_name: "Job One".to_string(),
            status: "Failed".to_string(),
            job_status: "Failed: division by zero".to_string(),
            start_time: 1,
            end_time: 2,
            num_stages: 1,
            completed_stages: 0,
            percent_complete: 0,
        };
        assert_eq!(job.status, "Failed");
        assert_eq!(job.job_status, "Failed: division by zero");
    }

    #[test]
    fn selected_job_filters_by_search_term_on_id() {
        let jobs = vec![
            make_job("abc-123", "Job One", "Running", 1, 2, 1, 0, 0),
            make_job("xyz-456", "Job Two", "Running", 2, 3, 1, 0, 0),
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
            make_job("j1", "Query Alpha", "Running", 1, 2, 1, 0, 0),
            make_job("j2", "Query Beta", "Running", 2, 3, 1, 0, 0),
            make_job("j3", "Other Job", "Running", 3, 4, 1, 0, 0),
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
        let jobs = vec![make_job("j1", "My QUERY", "Running", 1, 2, 1, 0, 0)];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(0));
        assert!(data.selected_job("query").is_some());
        assert!(data.selected_job("QUERY").is_some());
        assert!(data.selected_job("Query").is_some());
    }

    #[test]
    fn selected_job_no_match_returns_none() {
        let jobs = vec![make_job("j1", "Job One", "Running", 1, 2, 1, 0, 0)];
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
            make_job("j1", "A", "Running", 1, 2, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 3, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.scroll_down();
        assert_eq!(data.table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_down_advances_selection() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 2, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 1, 2, 0, 0),
            make_job("j3", "C", "Running", 3, 1, 2, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(0));
        data.scroll_down();
        assert_eq!(data.table_state.selected(), Some(1));
    }

    #[test]
    fn scroll_down_at_last_item_deselects() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 2, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 3, 1, 0, 0),
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
            make_job("j1", "A", "Running", 1, 2, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 3, 1, 0, 0),
            make_job("j3", "C", "Running", 3, 4, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.scroll_up();
        assert_eq!(data.table_state.selected(), Some(2));
    }

    #[test]
    fn scroll_up_moves_selection_back() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 2, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 2, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(1));
        data.scroll_up();
        assert_eq!(data.table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_up_at_first_item_deselects() {
        let jobs = vec![
            make_job("j1", "A", "Running", 1, 2, 1, 0, 0),
            make_job("j2", "B", "Running", 2, 3, 1, 0, 0),
        ];
        let mut data = make_jobs_data(jobs, SortColumn::None, SortOrder::Ascending);
        data.table_state.select(Some(0));
        data.scroll_up();
        assert_eq!(data.table_state.selected(), None);
    }

    // --- JobPlansPopup tests ---

    fn make_job_details(id: &str) -> JobDetails {
        JobDetails {
            job_id: id.to_string(),
            physical_plan_tree: None,
            logical_plan: None,
            physical_plan: None,
            stage_plan: None,
        }
    }

    #[test]
    fn job_plans_popup_new_scroll_position_is_zero() {
        let popup = JobPlansPopup::new(make_job_details("j1"), PlanTab::Stage);
        assert_eq!(popup.vertical_scroll_position, 0);
    }

    #[test]
    fn job_plans_popup_scroll_down_increments() {
        let mut popup = JobPlansPopup::new(make_job_details("j1"), PlanTab::Stage);
        popup.scroll_down();
        assert_eq!(popup.vertical_scroll_position, 1);
        popup.scroll_down();
        assert_eq!(popup.vertical_scroll_position, 2);
    }

    #[test]
    fn job_plans_popup_scroll_up_decrements() {
        let mut popup = JobPlansPopup::new(make_job_details("j1"), PlanTab::Stage);
        popup.scroll_down();
        popup.scroll_down();
        popup.scroll_up();
        assert_eq!(popup.vertical_scroll_position, 1);
    }

    #[test]
    fn job_plans_popup_scroll_up_saturates_at_zero() {
        let mut popup = JobPlansPopup::new(make_job_details("j1"), PlanTab::Stage);
        popup.scroll_up();
        assert_eq!(popup.vertical_scroll_position, 0);
    }

    #[test]
    fn job_plans_popup_scroll_right_increments() {
        let mut popup = JobPlansPopup::new(make_job_details("j1"), PlanTab::Stage);
        popup.scroll_right();
        assert_eq!(popup.horizontal_scroll_position, 1);
    }

    #[test]
    fn job_plans_popup_scroll_left_saturates_at_zero() {
        let mut popup = JobPlansPopup::new(make_job_details("j1"), PlanTab::Stage);
        popup.scroll_left();
        assert_eq!(popup.horizontal_scroll_position, 0);
    }

    #[test]
    fn job_plans_popup_set_tab_resets_scroll_positions() {
        let mut popup = JobPlansPopup::new(make_job_details("j1"), PlanTab::Stage);
        popup.scroll_down();
        popup.scroll_right();
        popup.set_tab(PlanTab::Physical);
        assert_eq!(popup.vertical_scroll_position, 0);
        assert_eq!(popup.horizontal_scroll_position, 0);
    }

    #[test]
    fn job_config_popup_filters_by_key_and_value() {
        let popup = JobConfigPopup::new(
            "j1".to_string(),
            vec![
                JobConfigEntry {
                    key: "ballista.job.name".to_string(),
                    value: "Remote SQL Example".to_string(),
                },
                JobConfigEntry {
                    key: "datafusion.execution.batch_size".to_string(),
                    value: "8192".to_string(),
                },
            ],
        );

        let mut popup = popup;
        popup.push_search_char('8');
        assert_eq!(popup.filtered_entries().len(), 1);
        assert_eq!(
            popup.filtered_entries()[0].key,
            "datafusion.execution.batch_size"
        );

        popup.clear_search();
        popup.push_search_char('n');
        popup.push_search_char('a');
        popup.push_search_char('m');
        popup.push_search_char('e');
        assert_eq!(popup.filtered_entries().len(), 1);
        assert_eq!(popup.filtered_entries()[0].key, "ballista.job.name");
    }

    #[test]
    fn job_config_popup_search_resets_selection() {
        let mut popup = JobConfigPopup::new(
            "j1".to_string(),
            vec![
                JobConfigEntry {
                    key: "a".to_string(),
                    value: "1".to_string(),
                },
                JobConfigEntry {
                    key: "b".to_string(),
                    value: "2".to_string(),
                },
            ],
        );
        popup.scroll_down();
        assert_eq!(popup.table_state.selected(), Some(1));
        popup.push_search_char('a');
        assert_eq!(popup.table_state.selected(), Some(0));
    }
}
