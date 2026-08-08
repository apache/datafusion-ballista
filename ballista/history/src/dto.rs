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

//! REST response DTOs shared by the live scheduler API handlers and the history
//! server, so both serialize byte-identical JSON.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResponse {
    /// A `String` rather than `ballista_core::JobId` so this crate stays a
    /// serde-only leaf. `JobId` is `#[serde(transparent)]` over `String`, so
    /// the serialized JSON is unchanged.
    pub job_id: String,
    pub job_name: String,
    pub job_status: String,
    pub status: String,
    pub num_stages: usize,
    pub completed_stages: usize,
    pub percent_complete: u8,
    pub start_time: u64,
    pub end_time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_plan: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    Running,
    Successful,
    Failed { reason: String, error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSummary {
    /// task id
    pub id: usize,
    /// Task status
    pub status: TaskStatus,
    /// Global partition ids covered by this task. For a single-partition
    /// task this is a one-element list — JSON-compatible with the old
    /// scalar `partition_id` for callers that read `partition_id[0]`, and
    /// honestly plural for multi-partition tasks.
    pub partition_id: Vec<u32>,
    /// Scheduler schedule time
    pub scheduled_time: u64,
    /// Scheduler launch time (ms since epoch)
    pub launch_time: u64,
    /// The time the Executor start to run the task (ms since epoch)
    pub start_exec_time: u64,
    /// The time the Executor finish the task (ms since epoch)
    pub end_exec_time: u64,
    /// total execution time (ms)
    pub exec_duration: u64,
    /// Scheduler side finish time (ms since epoch)
    pub finish_time: u64,
    /// Number of input rows
    pub input_rows: usize,
    /// Number of output rows
    pub output_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Percentiles {
    pub min: u64,
    pub p25: u64,
    pub median: u64,
    pub p75: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStageSummary {
    pub stage_id: String,
    pub stage_status: String,
    pub input_rows: usize,
    pub output_rows: usize,
    pub elapsed_compute: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_duration_percentiles: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_input_percentiles: Option<Percentiles>,
    pub tasks: Vec<Option<TaskSummary>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStagesResponse {
    pub stages: Vec<QueryStageSummary>,
}

/// Session config as flat key/value pairs (from `SessionConfig::to_props()`),
/// sorted for stable output.
pub type JobConfig = BTreeMap<String, String>;
