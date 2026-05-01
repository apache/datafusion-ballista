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
