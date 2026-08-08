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

//! Reads a completed `<job_id>.eventlog` into the DTO bundle the history server
//! serves. A file is "completed" once it contains a `JobEnd` record.

use crate::event::HistoryEvent;
use ballista_api_types::dto::{JobConfig, JobResponse, QueryStagesResponse};
use std::io::BufRead;
use std::path::Path;

/// The served payload recovered from a completed job's event log.
#[derive(Debug, Clone)]
pub struct ReplayedJob {
    /// `GET /api/job/{job_id}` response.
    pub job: JobResponse,
    /// `GET /api/job/{job_id}/stages` response.
    pub stages: QueryStagesResponse,
    /// `GET /api/job/{job_id}/config` response.
    pub config: JobConfig,
    /// Rendered DOT graph of the stage DAG.
    pub dot: String,
}

/// Read a completed job's payload out of its event log.
///
/// Returns `Ok(None)` when the file has no `JobEnd` record, which means the job
/// is still running or the scheduler died before finishing it. Malformed lines
/// are skipped rather than treated as fatal, so a partially-written log still
/// yields its job if the terminal record survived.
pub fn read_completed_job(path: &Path) -> std::io::Result<Option<ReplayedJob>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        // Only JobEnd carries the served payload; other lines are the timeline
        // and are ignored here. Unknown/garbled lines are skipped, not fatal.
        if let Ok(HistoryEvent::JobEnd {
            job,
            stages,
            config,
            dot,
            ..
        }) = serde_json::from_str::<HistoryEvent>(&line)
        {
            return Ok(Some(ReplayedJob {
                job: *job,
                stages: *stages,
                config,
                dot,
            }));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{HistoryEvent, JobEndStatus, SCHEMA_VERSION};
    use ballista_api_types::dto::{JobResponse, QueryStagesResponse};
    use std::io::Write;

    fn job_end_line() -> String {
        let event = HistoryEvent::JobEnd {
            version: SCHEMA_VERSION,
            status: JobEndStatus::Succeeded,
            queued_at: 1,
            started_at: 2,
            completed_at: 3,
            job: Box::new(JobResponse {
                job_id: "job-1".into(),
                job_name: "q1".into(),
                job_status: "COMPLETED".into(),
                status: "Successful".into(),
                num_stages: 1,
                completed_stages: 1,
                percent_complete: 100,
                start_time: 2,
                end_time: 3,
                logical_plan: Some("Projection".into()),
                physical_plan: Some("ProjectionExec".into()),
                stage_plan: Some("stage".into()),
            }),
            stages: Box::new(QueryStagesResponse { stages: vec![] }),
            config: Default::default(),
            dot: "digraph {}".into(),
        };
        serde_json::to_string(&event).unwrap()
    }

    #[test]
    fn reads_job_end_and_ignores_unknown_timeline_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job-1.eventlog");
        let mut f = std::fs::File::create(&path).unwrap();
        // Unknown/other event lines before the JobEnd must be tolerated.
        writeln!(f, r#"{{"ev":"StageStart","stage_id":1,"partitions":4}}"#).unwrap();
        writeln!(f, "{}", job_end_line()).unwrap();
        drop(f);

        let replayed = read_completed_job(&path).unwrap().expect("completed");
        assert_eq!(replayed.job.job_id, "job-1");
        assert_eq!(
            replayed.job.physical_plan.as_deref(),
            Some("ProjectionExec")
        );
        assert_eq!(replayed.dot, "digraph {}");
    }

    #[test]
    fn returns_none_when_no_job_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job-2.eventlog");
        std::fs::write(
            &path,
            "{\"ev\":\"StageStart\",\"stage_id\":1,\"partitions\":4}\n",
        )
        .unwrap();
        assert!(read_completed_job(&path).unwrap().is_none());
    }
}
