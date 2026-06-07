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
#![allow(unfulfilled_lint_expectations)]
use ratatui::widgets::{ScrollbarState, TableState};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct JobStagesResponse {
    pub stages: Vec<JobStageResponse>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct JobStageResponse {
    #[serde(rename = "stage_id")]
    pub id: String,
    #[serde(rename = "stage_status")]
    pub status: String,
    #[serde(rename = "stage_plan", default)]
    pub plan: String,
    pub input_rows: usize,
    pub output_rows: usize,
    pub elapsed_compute: Option<String>,
    #[serde(default)]
    pub task_duration_percentiles: Option<TaskPercentiles>,
    #[serde(default)]
    pub task_input_percentiles: Option<TaskPercentiles>,
    #[serde(default)]
    pub tasks: Vec<Option<StageTaskResponse>>,
}

// TaskSummary
#[derive(Deserialize, Clone, Debug)]
pub struct StageTaskResponse {
    pub id: usize,
    pub status: String,
    pub partition_id: u32,
    pub input_rows: usize,
    pub output_rows: usize,
    pub scheduled_time: u64,
    pub launch_time: u64,
    pub start_exec_time: u64,
    pub end_exec_time: u64,
    pub finish_time: u64,
}

// Percentiles
#[derive(Deserialize, Clone, Debug)]
pub struct TaskPercentiles {
    pub min: u64,
    pub max: u64,
    pub median: u64,
    pub p25: u64,
    pub p75: u64,
}

#[derive(Debug, Default)]
pub struct PlanCache {
    pub default: Option<JobStagesResponse>,
    pub tree: Option<JobStagesResponse>,
    pub metrics: Option<JobStagesResponse>,
}

#[derive(Debug, PartialEq)]
pub enum StageDetailsView {
    None,
    Tasks,
    Plan(StagePlanTab),
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum StagePlanTab {
    Default,
    Tree,
    Metrics,
}

#[derive(Debug)]
pub struct JobStagesPopup {
    pub job_id: String,
    pub stages: JobStagesResponse,
    pub plan_cache: PlanCache,
    pub table_state: TableState,
    pub scrollbar_state: ScrollbarState,
    pub tasks_table_state: TableState,
    pub tasks_scrollbar_state: ScrollbarState,
    details_view: StageDetailsView,
    plan_vertical_scroll_position: u16,
    plan_horizontal_scroll_position: u16,
}

impl JobStagesPopup {
    pub fn new(job_id: String, stages: JobStagesResponse) -> Self {
        Self {
            job_id,
            scrollbar_state: ScrollbarState::new(stages.stages.len()),
            plan_cache: PlanCache {
                default: Some(stages.clone()),
                ..Default::default()
            },
            stages,
            table_state: TableState::default(),
            tasks_table_state: TableState::default(),
            tasks_scrollbar_state: ScrollbarState::new(0),
            details_view: StageDetailsView::None,
            plan_vertical_scroll_position: 0,
            plan_horizontal_scroll_position: 0,
        }
    }

    pub fn cache_plan_response(&mut self, fmt: StagePlanTab, resp: JobStagesResponse) {
        match fmt {
            StagePlanTab::Default => self.plan_cache.default = Some(resp.clone()),
            StagePlanTab::Tree => self.plan_cache.tree = Some(resp.clone()),
            StagePlanTab::Metrics => self.plan_cache.metrics = Some(resp.clone()),
        }
        // If we're currently on that tab, update the live stages too so the
        // selection / scroll state is preserved.
        let active_fmt = self.active_plan_format();
        if active_fmt == Some(fmt) {
            self.stages = resp;
        }
    }

    #[allow(dead_code)]
    pub fn cached_response(&self, tab: &StagePlanTab) -> Option<JobStagesResponse> {
        match tab {
            StagePlanTab::Default => self.plan_cache.default.clone(),
            StagePlanTab::Tree => self.plan_cache.tree.clone(),
            StagePlanTab::Metrics => self.plan_cache.metrics.clone(),
        }
    }

    pub fn active_plan_format(&self) -> Option<StagePlanTab> {
        match &self.details_view {
            StageDetailsView::Plan(tab) => Some(tab.clone()),
            _ => None,
        }
    }

    pub fn plan_vertical_scroll_position(&self) -> u16 {
        self.plan_vertical_scroll_position
    }

    pub fn plan_horizontal_scroll_position(&self) -> u16 {
        self.plan_horizontal_scroll_position
    }

    pub fn set_tasks_view(&mut self) {
        self.details_view = StageDetailsView::Tasks;
        self.tasks_table_state = TableState::default().with_selected(None);
        self.tasks_scrollbar_state = ScrollbarState::new(self.tasks_count());
    }

    pub fn set_plan_view(&mut self) {
        self.details_view = StageDetailsView::Plan(StagePlanTab::Default);
        self.plan_vertical_scroll_position = 0;
        self.plan_horizontal_scroll_position = 0;
    }

    pub fn set_no_details_view(&mut self) {
        self.details_view = StageDetailsView::None;
    }

    pub fn is_no_details_view(&self) -> bool {
        self.details_view == StageDetailsView::None
    }

    pub fn is_tasks_view(&self) -> bool {
        self.details_view == StageDetailsView::Tasks
    }

    pub fn is_plan_view(&self) -> bool {
        matches!(self.details_view, StageDetailsView::Plan(_))
    }

    #[allow(dead_code)]
    pub fn set_tab(&mut self, tab: StagePlanTab) -> Option<StagePlanTab> {
        self.details_view = StageDetailsView::Plan(tab.clone());
        self.plan_vertical_scroll_position = 0;
        self.plan_horizontal_scroll_position = 0;

        if let Some(cached) = self.cached_response(&tab) {
            self.stages = cached;
            None
        } else {
            Some(tab)
        }
    }

    pub fn scroll_down(&mut self) {
        if self.is_no_details_view() {
            let len = self.stages.stages.len();
            if len == 0 {
                self.table_state.select(None);
                return;
            }
            if let Some(selected) = self.table_state.selected() {
                if selected < len - 1 {
                    self.table_state.select(Some(selected + 1));
                    self.scrollbar_state = self.scrollbar_state.position(selected + 1);
                } else {
                    self.table_state.select(None);
                    self.scrollbar_state = self.scrollbar_state.position(0);
                }
            } else {
                self.table_state.select(Some(0));
                self.scrollbar_state = self.scrollbar_state.position(0);
            }
        } else if self.is_tasks_view() {
            self.tasks_scroll_down();
        } else if self.is_plan_view() {
            self.plan_vertical_scroll_position =
                self.plan_vertical_scroll_position.saturating_add(1);
        }
    }

    pub fn scroll_up(&mut self) {
        if self.is_no_details_view() {
            let len = self.stages.stages.len();
            if len == 0 {
                self.table_state.select(None);
                return;
            }
            if let Some(selected) = self.table_state.selected() {
                if selected == 0 {
                    self.table_state.select(None);
                    self.scrollbar_state = self.scrollbar_state.position(0);
                } else {
                    self.table_state.select(Some(selected - 1));
                    self.scrollbar_state = self.scrollbar_state.position(selected - 1);
                }
            } else {
                self.table_state.select(Some(len - 1));
                self.scrollbar_state = self.scrollbar_state.position(len - 1);
            }
        } else if self.is_tasks_view() {
            self.tasks_scroll_up();
        } else if self.is_plan_view() {
            self.plan_vertical_scroll_position =
                self.plan_vertical_scroll_position.saturating_sub(1);
        }
    }

    fn tasks_scroll_down(&mut self) {
        let tasks_count = self.tasks_count();
        if tasks_count == 0 {
            return;
        }
        let next = match self.tasks_table_state.selected() {
            Some(i) if i < tasks_count - 1 => Some(i + 1),
            Some(_) => None,
            None => Some(0),
        };
        self.tasks_table_state.select(next);
        self.tasks_scrollbar_state =
            self.tasks_scrollbar_state.position(next.unwrap_or(0));
    }

    fn tasks_scroll_up(&mut self) {
        let tasks_count = self.tasks_count();
        if tasks_count == 0 {
            return;
        }
        let prev = match self.tasks_table_state.selected() {
            Some(i) if i > 0 => Some(i - 1),
            Some(_) => None,
            None => Some(tasks_count - 1),
        };
        self.tasks_table_state.select(prev);
        self.tasks_scrollbar_state =
            self.tasks_scrollbar_state.position(prev.unwrap_or(0));
    }

    pub fn scroll_left(&mut self) {
        if self.is_plan_view() {
            self.plan_horizontal_scroll_position =
                self.plan_horizontal_scroll_position.saturating_sub(1);
        }
    }

    pub fn scroll_right(&mut self) {
        if self.is_plan_view() {
            self.plan_horizontal_scroll_position =
                self.plan_horizontal_scroll_position.saturating_add(1);
        }
    }

    pub fn selected_stage(&self) -> Option<&JobStageResponse> {
        self.table_state
            .selected()
            .and_then(|i| self.stages.stages.get(i))
    }

    fn tasks_count(&self) -> usize {
        self.selected_stage()
            .map(|s| s.tasks.iter().flatten().count())
            .unwrap_or(0)
    }
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
    pub scroll_position: u16,
}

impl StagesGraph {
    pub fn scroll_up(&mut self) {
        self.scroll_position = self.scroll_position.saturating_sub(1);
    }

    pub fn scroll_down(&mut self) {
        self.scroll_position = self.scroll_position.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        JobStageResponse, JobStagesPopup, JobStagesResponse, StageTaskResponse,
        StagesGraph, TaskPercentiles,
    };

    fn make_percentiles() -> TaskPercentiles {
        TaskPercentiles {
            min: 0,
            max: 100,
            median: 50,
            p25: 25,
            p75: 75,
        }
    }

    fn make_stage(id: &str) -> JobStageResponse {
        JobStageResponse {
            id: id.to_string(),
            status: "Completed".to_string(),
            plan: String::new(),
            input_rows: 0,
            output_rows: 0,
            elapsed_compute: Some("1ns".to_string()),
            task_duration_percentiles: Some(make_percentiles()),
            task_input_percentiles: Some(make_percentiles()),
            tasks: Vec::new(),
        }
    }

    fn make_stages_response(n: usize) -> JobStagesResponse {
        JobStagesResponse {
            stages: (0..n).map(|i| make_stage(&i.to_string())).collect(),
        }
    }

    fn make_popup(n: usize) -> JobStagesPopup {
        JobStagesPopup::new("job1".to_string(), make_stages_response(n))
    }

    fn make_stages_graph(n_stages: usize) -> StagesGraph {
        StagesGraph {
            job_id: "job1".to_string(),
            stages: Vec::new(),
            edges: Vec::new(),
            scroll_position: n_stages as u16,
        }
    }

    // --- JobStagesPopup view-state transitions ---

    #[test]
    fn new_popup_is_no_details_view() {
        let popup = make_popup(2);
        assert!(popup.is_no_details_view());
        assert!(!popup.is_tasks_view());
        assert!(!popup.is_plan_view());
    }

    #[test]
    fn set_tasks_view_transitions_to_tasks() {
        let mut popup = make_popup(2);
        popup.set_tasks_view();
        assert!(popup.is_tasks_view());
        assert!(!popup.is_no_details_view());
        assert!(!popup.is_plan_view());
    }

    #[test]
    fn set_plan_view_transitions_to_plan() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        assert!(popup.is_plan_view());
        assert!(!popup.is_no_details_view());
        assert!(!popup.is_tasks_view());
    }

    #[test]
    fn set_no_details_view_resets_from_tasks() {
        let mut popup = make_popup(2);
        popup.set_tasks_view();
        popup.set_no_details_view();
        assert!(popup.is_no_details_view());
    }

    #[test]
    fn set_no_details_view_resets_from_plan() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        popup.set_no_details_view();
        assert!(popup.is_no_details_view());
    }

    #[test]
    fn is_tasks_view_false_when_plan() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        assert!(!popup.is_tasks_view());
    }

    #[test]
    fn is_plan_view_false_when_tasks() {
        let mut popup = make_popup(2);
        popup.set_tasks_view();
        assert!(!popup.is_plan_view());
    }

    #[test]
    fn only_one_view_active_at_a_time() {
        let mut popup = make_popup(2);
        popup.set_tasks_view();
        let active_count = [
            popup.is_no_details_view(),
            popup.is_tasks_view(),
            popup.is_plan_view(),
        ]
        .iter()
        .filter(|&&v| v)
        .count();
        assert_eq!(active_count, 1);
    }

    // --- JobStagesPopup scroll_down ---

    #[test]
    fn scroll_down_empty_stays_none() {
        let mut popup = make_popup(0);
        popup.scroll_down();
        assert_eq!(popup.table_state.selected(), None);
    }

    #[test]
    fn scroll_down_no_selection_selects_first() {
        let mut popup = make_popup(3);
        popup.scroll_down();
        assert_eq!(popup.table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_down_advances_selection() {
        let mut popup = make_popup(3);
        popup.table_state.select(Some(0));
        popup.scroll_down();
        assert_eq!(popup.table_state.selected(), Some(1));
    }

    #[test]
    fn scroll_down_at_last_deselects() {
        let mut popup = make_popup(3);
        popup.table_state.select(Some(2));
        popup.scroll_down();
        assert_eq!(popup.table_state.selected(), None);
    }

    // --- JobStagesPopup scroll_up ---

    #[test]
    fn scroll_up_empty_stays_none() {
        let mut popup = make_popup(0);
        popup.scroll_up();
        assert_eq!(popup.table_state.selected(), None);
    }

    #[test]
    fn scroll_up_no_selection_selects_last() {
        let mut popup = make_popup(3);
        popup.scroll_up();
        assert_eq!(popup.table_state.selected(), Some(2));
    }

    #[test]
    fn scroll_up_moves_back() {
        let mut popup = make_popup(3);
        popup.table_state.select(Some(2));
        popup.scroll_up();
        assert_eq!(popup.table_state.selected(), Some(1));
    }

    #[test]
    fn scroll_up_at_first_deselects() {
        let mut popup = make_popup(3);
        popup.table_state.select(Some(0));
        popup.scroll_up();
        assert_eq!(popup.table_state.selected(), None);
    }

    // --- JobStagesPopup::selected_stage ---

    #[test]
    fn selected_stage_none_when_no_selection() {
        let popup = make_popup(3);
        assert!(popup.selected_stage().is_none());
    }

    #[test]
    fn selected_stage_returns_correct_stage() {
        let mut popup = make_popup(3);
        popup.table_state.select(Some(1));
        let stage = popup.selected_stage().unwrap();
        assert_eq!(stage.id, "1");
    }

    #[test]
    fn selected_stage_none_after_deselect() {
        let mut popup = make_popup(3);
        popup.table_state.select(Some(0));
        popup.table_state.select(None);
        assert!(popup.selected_stage().is_none());
    }

    // --- StagesGraph scroll ---

    #[test]
    fn stages_graph_scroll_down_increments_position() {
        let mut graph = make_stages_graph(0);
        graph.scroll_down();
        assert_eq!(graph.scroll_position, 1);
    }

    #[test]
    fn stages_graph_scroll_down_multiple_times() {
        let mut graph = make_stages_graph(0);
        graph.scroll_down();
        graph.scroll_down();
        graph.scroll_down();
        assert_eq!(graph.scroll_position, 3);
    }

    #[test]
    fn stages_graph_scroll_up_decrements_position() {
        let mut graph = make_stages_graph(5);
        graph.scroll_up();
        assert_eq!(graph.scroll_position, 4);
    }

    #[test]
    fn stages_graph_scroll_up_saturates_at_zero() {
        let mut graph = make_stages_graph(0);
        graph.scroll_up();
        assert_eq!(graph.scroll_position, 0);
    }

    // --- Helpers for task-bearing stages ---

    fn make_task(id: usize) -> StageTaskResponse {
        StageTaskResponse {
            id,
            status: "Completed".to_string(),
            partition_id: id as u32,
            input_rows: 0,
            output_rows: 0,
            scheduled_time: 0,
            launch_time: 0,
            start_exec_time: 0,
            end_exec_time: 0,
            finish_time: 0,
        }
    }

    fn make_stage_with_tasks(id: &str, task_count: usize) -> JobStageResponse {
        let mut stage = make_stage(id);
        stage.tasks = (0..task_count).map(|i| Some(make_task(i))).collect();
        stage
    }

    // Creates a popup with one stage that has `task_count` tasks; the stage is pre-selected.
    fn make_popup_with_tasks(task_count: usize) -> JobStagesPopup {
        let stage = make_stage_with_tasks("0", task_count);
        let mut popup = JobStagesPopup::new(
            "job1".to_string(),
            JobStagesResponse {
                stages: vec![stage],
            },
        );
        popup.table_state.select(Some(0));
        popup
    }

    // --- set_tasks_view ---

    #[test]
    fn set_tasks_view_with_tasks_does_not_preselect() {
        let mut popup = make_popup_with_tasks(3);
        popup.set_tasks_view();
        assert_eq!(popup.tasks_table_state.selected(), None);
    }

    #[test]
    fn set_tasks_view_with_no_tasks_selects_none() {
        let mut popup = make_popup_with_tasks(0);
        popup.set_tasks_view();
        assert_eq!(popup.tasks_table_state.selected(), None);
    }

    // --- tasks_scroll_down (via scroll_down in tasks view) ---

    #[test]
    fn scroll_down_in_tasks_view_advances_selection() {
        let mut popup = make_popup_with_tasks(3);
        popup.set_tasks_view();
        // set_tasks_view does not pre-select a task; scrolling down should move to 0
        popup.scroll_down();
        assert_eq!(popup.tasks_table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_down_in_tasks_view_at_last_deselects() {
        let mut popup = make_popup_with_tasks(3);
        popup.set_tasks_view();
        popup.tasks_table_state.select(Some(2));
        popup.scroll_down();
        assert_eq!(popup.tasks_table_state.selected(), None);
    }

    #[test]
    fn scroll_down_in_tasks_view_from_none_selects_first() {
        let mut popup = make_popup_with_tasks(3);
        popup.set_tasks_view();
        popup.tasks_table_state.select(None);
        popup.scroll_down();
        assert_eq!(popup.tasks_table_state.selected(), Some(0));
    }

    #[test]
    fn scroll_down_in_tasks_view_empty_tasks_does_nothing() {
        let mut popup = make_popup_with_tasks(0);
        popup.set_tasks_view();
        popup.scroll_down();
        assert_eq!(popup.tasks_table_state.selected(), None);
    }

    // --- tasks_scroll_up (via scroll_up in tasks view) ---

    #[test]
    fn scroll_up_in_tasks_view_moves_back() {
        let mut popup = make_popup_with_tasks(3);
        popup.set_tasks_view();
        popup.tasks_table_state.select(Some(2));
        popup.scroll_up();
        assert_eq!(popup.tasks_table_state.selected(), Some(1));
    }

    #[test]
    fn scroll_up_in_tasks_view_at_first_selects_last() {
        let mut popup = make_popup_with_tasks(3);
        popup.set_tasks_view();
        // tasks_table_state is already at None after set_tasks_view
        popup.scroll_up();
        assert_eq!(popup.tasks_table_state.selected(), Some(2));
    }

    #[test]
    fn scroll_up_in_tasks_view_from_none_selects_last() {
        let mut popup = make_popup_with_tasks(3);
        popup.set_tasks_view();
        popup.tasks_table_state.select(None);
        popup.scroll_up();
        assert_eq!(popup.tasks_table_state.selected(), Some(2));
    }

    #[test]
    fn scroll_up_in_tasks_view_empty_tasks_does_nothing() {
        let mut popup = make_popup_with_tasks(0);
        popup.set_tasks_view();
        popup.scroll_up();
        assert_eq!(popup.tasks_table_state.selected(), None);
    }

    // --- Plan view scrolling ---

    #[test]
    fn set_plan_view_resets_scroll_positions() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        popup.scroll_right();
        popup.scroll_down();
        popup.set_plan_view();
        assert_eq!(popup.plan_horizontal_scroll_position(), 0);
        assert_eq!(popup.plan_vertical_scroll_position(), 0);
    }

    #[test]
    fn scroll_down_in_plan_view_increments_vertical() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        popup.scroll_down();
        assert_eq!(popup.plan_vertical_scroll_position(), 1);
    }

    #[test]
    fn scroll_up_in_plan_view_decrements_vertical() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        popup.scroll_down();
        popup.scroll_up();
        assert_eq!(popup.plan_vertical_scroll_position(), 0);
    }

    #[test]
    fn scroll_up_in_plan_view_saturates_at_zero() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        popup.scroll_up();
        assert_eq!(popup.plan_vertical_scroll_position(), 0);
    }

    #[test]
    fn scroll_right_increments_horizontal() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        popup.scroll_right();
        assert_eq!(popup.plan_horizontal_scroll_position(), 1);
    }

    #[test]
    fn scroll_left_decrements_horizontal() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        popup.scroll_right();
        popup.scroll_left();
        assert_eq!(popup.plan_horizontal_scroll_position(), 0);
    }

    #[test]
    fn scroll_left_saturates_at_zero() {
        let mut popup = make_popup(2);
        popup.set_plan_view();
        popup.scroll_left();
        assert_eq!(popup.plan_horizontal_scroll_position(), 0);
    }

    // --- scroll_left / scroll_right no-ops outside plan view ---

    #[test]
    fn scroll_left_in_no_details_view_does_nothing() {
        let mut popup = make_popup(2);
        popup.scroll_left();
        assert_eq!(popup.plan_horizontal_scroll_position(), 0);
    }

    #[test]
    fn scroll_right_in_no_details_view_does_nothing() {
        let mut popup = make_popup(2);
        popup.scroll_right();
        assert_eq!(popup.plan_horizontal_scroll_position(), 0);
    }
}
