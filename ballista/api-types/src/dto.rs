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

/// Summary of one job, served by `GET /api/jobs` and `GET /api/job/{job_id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResponse {
    /// A `String` rather than `ballista_core::JobId` so this crate stays a
    /// serde-only leaf. `JobId` is `#[serde(transparent)]` over `String`, so
    /// the serialized JSON is unchanged.
    pub job_id: String,
    /// Human-readable job name.
    pub job_name: String,
    /// Verbose status, including the completion or failure detail.
    pub job_status: String,
    /// Plain status word: `Queued`, `Running`, `Completed`, `Failed`, or `Invalid`.
    pub status: String,
    /// Total number of stages in the job.
    pub num_stages: usize,
    /// Number of stages that finished successfully.
    pub completed_stages: usize,
    /// Progress as a percentage of completed stages.
    pub percent_complete: u8,
    /// Timestamp when the job started.
    pub start_time: u64,
    /// Timestamp when the job ended (0 if still running).
    pub end_time: u64,
    /// Rendered logical plan. Absent in the job list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_plan: Option<String>,
    /// Rendered physical plan. Absent in the job list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_plan: Option<String>,
    /// Rendered stage DAG. Absent in the job list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_plan: Option<String>,
}

/// Terminal or in-flight state of a single task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task is currently executing.
    Running,
    /// Task completed successfully.
    Successful,
    /// Task failed, with the classified reason and the underlying error.
    Failed {
        /// Failure category, e.g. `ExecutionError` or `FetchPartitionError`.
        reason: String,
        /// Underlying error message.
        error: String,
    },
}

/// Per-task detail within a stage.
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

/// Five-number summary over a stage's tasks, used to spot skew.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Percentiles {
    /// Smallest observed value.
    pub min: u64,
    /// 25th percentile.
    pub p25: u64,
    /// 50th percentile.
    pub median: u64,
    /// 75th percentile.
    pub p75: u64,
    /// Largest observed value.
    pub max: u64,
}

/// Summary of one query stage, served by `GET /api/job/{job_id}/stages`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStageSummary {
    /// Stage id, as a string.
    pub stage_id: String,
    /// Stage state, e.g. `Running`, `Successful`, `Failed`.
    pub stage_status: String,
    /// Rows read by the stage, summed across tasks.
    pub input_rows: usize,
    /// Rows produced by the stage, summed across tasks.
    pub output_rows: usize,
    /// Formatted wall time across the stage's tasks, if any have started.
    pub elapsed_compute: Option<String>,
    /// Rendered plan for this stage, in the requested [`PlanFormat`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_plan: Option<String>,
    /// Distribution of per-task execution time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_duration_percentiles: Option<Percentiles>,
    /// Distribution of per-task input row counts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_input_percentiles: Option<Percentiles>,
    /// One entry per task. Always `Some` today; the `Option` predates
    /// multi-partition tasks, when this list was indexed by partition and
    /// could be sparse.
    pub tasks: Vec<Option<TaskSummary>>,
}

/// Response body for `GET /api/job/{job_id}/stages`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStagesResponse {
    /// One summary per stage, in stage order.
    pub stages: Vec<QueryStageSummary>,
}

/// How a plan should be rendered. Parsed from the `?plan_format=` query
/// parameter, and part of the REST contract, so it lives alongside the
/// response types rather than with the HTTP handlers.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanFormat {
    /// `?plan_format=default` => plain indent, no metrics
    #[default]
    Default,
    /// `?plan_format=tree` => tree render, no metrics
    Tree,
    /// `?plan_format=metrics` => indent with aggregated metrics
    Metrics,
}

/// Session config for one job, as flat key/value pairs. Served by
/// `GET /api/job/{job_id}/config`.
///
/// A `BTreeMap` so key order is deterministic: the same job must serialize
/// identically whether it is served live or replayed from a stored log.
pub type JobConfig = BTreeMap<String, String>;
