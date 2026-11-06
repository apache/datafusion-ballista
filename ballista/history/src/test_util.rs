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

//! Event records shared by the unit tests of the writer, the reader and the
//! schema.

use crate::event::{JobEnd, JobEndStatus, JobIndex, JobStart};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

/// The `JobStart` of a job named `q1` that has no plans captured.
pub(crate) fn job_start(job_id: &str) -> JobStart {
    JobStart {
        job_id: job_id.into(),
        job_name: "q1".into(),
        queued_at: 1,
        submitted_at: 2,
        scheduler_id: None,
        logical_plan: None,
        physical_plan: None,
    }
}

/// The summary of a completed one-stage job named `q1`.
pub(crate) fn job_index(job_id: &str) -> JobIndex {
    JobIndex {
        job_id: job_id.into(),
        job_name: "q1".into(),
        status: "Completed".into(),
        job_status: "COMPLETED".into(),
        start_time: 2,
        end_time: 3,
        num_stages: 1,
        completed_stages: 1,
        percent_complete: 100,
    }
}

/// A succeeded `JobEnd` whose job payload names a `ProjectionExec`, whose
/// stages payload is empty and whose config is `{"k": "v"}`.
pub(crate) fn job_end(job_id: &str) -> JobEnd {
    JobEnd {
        status: JobEndStatus::Succeeded,
        queued_at: 1,
        started_at: 2,
        completed_at: 3,
        index: job_index(job_id),
        job: RawValue::from_string(format!(
            r#"{{"job_id":"{job_id}","physical_plan":"ProjectionExec"}}"#
        ))
        .unwrap(),
        stages: RawValue::from_string(r#"{"stages":[]}"#.to_string()).unwrap(),
        config: BTreeMap::from([("k".to_string(), "v".to_string())]),
        dot: "digraph {}".into(),
    }
}
