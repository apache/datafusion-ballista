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

use crate::dto::{JobConfig, JobResponse, QueryStagesResponse, TaskStatus};
use serde::{Deserialize, Serialize};

/// Current on-disk schema version, stamped on `JobStart`/`JobEnd`.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobEndStatus {
    Succeeded,
    Failed(String),
}

/// Metrics captured per finished task on the incremental timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEndMetrics {
    pub input_rows: u64,
    pub output_rows: u64,
    pub elapsed_compute_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "ev")]
pub enum HistoryEvent {
    JobStart {
        version: u32,
        job_id: String,
        job_name: String,
        queued_at: u64,
        submitted_at: u64,
        logical_plan: Option<String>,
        physical_plan: Option<String>,
    },
    StageStart {
        stage_id: usize,
        partitions: usize,
    },
    StageEnd {
        stage_id: usize,
        status: String,
    },
    TaskEnd {
        stage_id: u32,
        partition: u32,
        executor_id: String,
        status: TaskStatus,
        launch_time: u64,
        start_exec_time: u64,
        end_exec_time: u64,
        metrics: TaskEndMetrics,
    },
    JobEnd {
        version: u32,
        status: JobEndStatus,
        queued_at: u64,
        started_at: u64,
        completed_at: u64,
        job: Box<JobResponse>,
        stages: Box<QueryStagesResponse>,
        config: JobConfig,
        dot: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::{JobResponse, QueryStagesResponse};
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
