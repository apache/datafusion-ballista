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

//! The on-disk event-log schema. One `HistoryEvent` is serialized per JSONL line.
//! This is a frozen public projection of the scheduler's internal events; the
//! embedded DTOs are the stable contract the history server serves.

use ballista_api_types::dto::{JobConfig, JobResponse, QueryStagesResponse, TaskStatus};
use serde::{Deserialize, Serialize};

/// Current on-disk schema version, stamped on `JobStart`/`JobEnd`.
pub const SCHEMA_VERSION: u32 = 1;

/// How a job reached its terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobEndStatus {
    /// Job completed successfully.
    Succeeded,
    /// Job terminated with the given error.
    Failed(String),
    /// Job was cancelled before completing.
    Cancelled,
}

/// Metrics captured per finished task on the incremental timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEndMetrics {
    /// Rows the task read.
    pub input_rows: u64,
    /// Rows the task produced.
    pub output_rows: u64,
    /// Time the task spent computing, in nanoseconds.
    pub elapsed_compute_nanos: u64,
}

/// A single record in a job's event log.
///
/// Stage and partition identifiers are fixed-width (`u32`) throughout rather
/// than `usize`: this is a durable, cross-machine format, so a log written by a
/// 64-bit scheduler must mean the same thing to any reader. Callers holding the
/// scheduler's own `usize` stage ids cast on the way in.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "ev")]
pub enum HistoryEvent {
    /// First record in every log. Written when the scheduler accepts the job.
    JobStart {
        /// Schema version, see [`SCHEMA_VERSION`].
        version: u32,
        /// Job identifier, matching the log's filename.
        job_id: String,
        /// Human-readable job name.
        job_name: String,
        /// When the job entered the queue.
        queued_at: u64,
        /// When the job was submitted for planning.
        submitted_at: u64,
        /// Rendered logical plan, if one was captured.
        logical_plan: Option<String>,
        /// Rendered physical plan, if one was captured.
        physical_plan: Option<String>,
    },
    /// A stage became runnable.
    StageStart {
        /// Stage identifier within the job.
        stage_id: u32,
        /// Number of partitions the stage will produce.
        partitions: u32,
    },
    /// A stage reached a terminal state.
    StageEnd {
        /// Stage identifier within the job.
        stage_id: u32,
        /// Terminal stage status.
        status: String,
    },
    /// A task finished, successfully or otherwise.
    TaskEnd {
        /// Stage the task belonged to.
        stage_id: u32,
        /// Task's slot within the stage. Under the multi-partition-task model a
        /// task owns a slice of partitions, so this names the task rather than
        /// a single partition.
        task_id: u32,
        /// Executor that ran the task.
        executor_id: String,
        /// Outcome of the task.
        status: TaskStatus,
        /// When the scheduler launched the task.
        launch_time: u64,
        /// When the executor began running it.
        start_exec_time: u64,
        /// When the executor finished it.
        end_exec_time: u64,
        /// Row counts and compute time for the task.
        metrics: TaskEndMetrics,
    },
    /// Terminal record, and the only one the history server serves from. It
    /// carries the finished REST responses so replay never re-derives them.
    JobEnd {
        /// Schema version, see [`SCHEMA_VERSION`].
        version: u32,
        /// How the job ended.
        status: JobEndStatus,
        /// When the job entered the queue.
        queued_at: u64,
        /// When the job started executing.
        started_at: u64,
        /// When the job reached its terminal state.
        completed_at: u64,
        /// Finished `GET /api/job/{job_id}` response.
        job: Box<JobResponse>,
        /// Finished `GET /api/job/{job_id}/stages` response.
        stages: Box<QueryStagesResponse>,
        /// Finished `GET /api/job/{job_id}/config` response.
        config: JobConfig,
        /// Rendered DOT graph of the stage DAG.
        dot: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_api_types::dto::{JobResponse, QueryStagesResponse};
    use std::collections::BTreeMap;

    #[test]
    fn job_end_round_trips_through_jsonl() {
        let job = JobResponse {
            job_id: "job-1".into(),
            job_name: "q1".into(),
            job_status: "COMPLETED".into(),
            status: "Successful".into(),
            num_stages: 2,
            completed_stages: 2,
            percent_complete: 100,
            start_time: 10,
            end_time: 20,
            logical_plan: Some("Projection".into()),
            physical_plan: Some("ProjectionExec".into()),
            stage_plan: Some("stage plan".into()),
        };
        let event = HistoryEvent::JobEnd {
            version: SCHEMA_VERSION,
            status: JobEndStatus::Succeeded,
            queued_at: 5,
            started_at: 10,
            completed_at: 20,
            job: Box::new(job),
            stages: Box::new(QueryStagesResponse { stages: vec![] }),
            config: BTreeMap::from([("k".to_string(), "v".to_string())]),
            dot: "digraph {}".into(),
        };
        let line = serde_json::to_string(&event).unwrap();
        assert!(line.contains("\"ev\":\"JobEnd\""));
        let back: HistoryEvent = serde_json::from_str(&line).unwrap();
        // Re-serialize and compare strings (stable, discriminating).
        assert_eq!(line, serde_json::to_string(&back).unwrap());
    }
}
