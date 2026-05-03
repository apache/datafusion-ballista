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
pub struct JobStagesResponse {
    pub stages: Vec<JobStageResponse>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct JobStageResponse {
    #[serde(rename = "stage_id")]
    pub id: String,
    #[serde(rename = "stage_status")]
    pub status: String,
    #[serde(rename = "stage_plan")]
    pub plan: String,
    pub input_rows: usize,
    pub output_rows: usize,
    pub elapsed_compute: String,
    pub task_duration_percentiles: TaskPercentiles,
    pub task_input_percentiles: TaskPercentiles,
    pub tasks: Vec<StageTaskResponse>,
}

// TaskSummary
#[derive(Deserialize, Clone, Debug)]
pub struct StageTaskResponse {
    pub id: usize,
    pub status: String,
    pub partition_id: u32,
    pub input_rows: usize,
    pub output_rows: usize,
    #[expect(dead_code)]
    pub scheduled_time: u64,
    pub launch_time: u64,
    pub start_exec_time: u64,
    pub end_exec_time: u64,
    pub exec_duration: u64,
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

#[derive(Debug, PartialEq)]
pub enum StageDetailsView {
    None,
    Tasks,
    Plan,
}

#[derive(Debug)]
pub struct JobStagesPopup {
    pub job_id: String,
    pub stages: JobStagesResponse,
    pub table_state: TableState,
    details_view: StageDetailsView,
}

impl JobStagesPopup {
    pub fn new(job_id: String, stages: JobStagesResponse) -> Self {
        Self {
            job_id,
            stages,
            table_state: TableState::default(),
            details_view: StageDetailsView::None,
        }
    }

    pub fn set_tasks_view(&mut self) {
        self.details_view = StageDetailsView::Tasks;
    }

    pub fn set_plan_view(&mut self) {
        self.details_view = StageDetailsView::Plan;
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
        self.details_view == StageDetailsView::Plan
    }

    pub fn scroll_down(&mut self) {
        let len = self.stages.stages.len();
        if len == 0 {
            self.table_state.select(None);
            return;
        }
        if let Some(selected) = self.table_state.selected() {
            if selected < len - 1 {
                self.table_state.select(Some(selected + 1));
            } else {
                self.table_state.select(None);
            }
        } else {
            self.table_state.select(Some(0));
        }
    }

    pub fn scroll_up(&mut self) {
        let len = self.stages.stages.len();
        if len == 0 {
            self.table_state.select(None);
            return;
        }
        if let Some(selected) = self.table_state.selected() {
            if selected == 0 {
                self.table_state.select(None);
            } else {
                self.table_state.select(Some(selected - 1));
            }
        } else {
            self.table_state.select(Some(len - 1));
        }
    }

    pub fn selected_stage(&self) -> Option<&JobStageResponse> {
        self.table_state
            .selected()
            .and_then(|i| self.stages.stages.get(i))
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
        JobStageResponse, JobStagesPopup, JobStagesResponse, StagesGraph, TaskPercentiles,
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
            elapsed_compute: "0".to_string(),
            task_duration_percentiles: make_percentiles(),
            task_input_percentiles: make_percentiles(),
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
}
