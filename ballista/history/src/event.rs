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

//! The on-disk event-log schema. One [`LogRecord`] is serialized per JSONL line.
//!
//! # Compatibility
//!
//! A log is written once and may be read years later by a much newer Ballista,
//! so the guarantee runs one way: **a reader accepts any record whose version is
//! less than or equal to its own**. There is no way to upgrade the writer of a
//! file that already exists.
//!
//! That is the opposite of `BALLISTA_PROTOCOL_VERSION`, the strict-equality
//! handshake between scheduler and executor. Both ends of that handshake are
//! live and upgraded together, so refusing to proceed is the safe move. Here
//! there is nothing to negotiate with.
//!
//! Three properties make the format survivable:
//!
//! 1. **Every line self-describes.** [`LogRecord`] carries `ev` and `version`
//!    next to an opaque `data` payload, so a reader can decide whether it
//!    understands a record before committing to its shape.
//! 2. **Unknown record types are skipped, not fatal.** A future scheduler can
//!    add event kinds without breaking today's reader.
//! 3. **The served responses are stored verbatim**, as raw JSON rather than
//!    typed structs. See [`JobEnd`].

use ballista_api_types::dto::{JobConfig, TaskStatus};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Current on-disk schema version, stamped on every record.
///
/// Bump this only for a **breaking** change: removing a field, changing a
/// field's type, or changing the meaning of an existing one. Additive changes
/// do not bump it and must remain readable from older logs, which in practice
/// means every new field carries `#[serde(default)]`.
pub const SCHEMA_VERSION: u32 = 1;

/// One line of the log: a self-describing envelope around an opaque payload.
///
/// Keeping the payload opaque at this level is what lets a reader route on
/// `ev` and check `version` *before* attempting to parse a shape it may not
/// understand. Deserializing the whole record up front would collapse "written
/// by a newer Ballista" and "corrupt" into the same indistinguishable failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Record kind, e.g. `JobStart` or `JobEnd`.
    pub ev: String,
    /// Schema version this record was written with. See [`SCHEMA_VERSION`].
    pub version: u32,
    /// Kind-specific payload, left unparsed.
    pub data: Box<RawValue>,
}

impl LogRecord {
    /// Wrap a payload in an envelope stamped with the current schema version.
    pub fn new<T: Serialize>(ev: &str, payload: &T) -> serde_json::Result<Self> {
        Ok(LogRecord {
            ev: ev.to_string(),
            version: SCHEMA_VERSION,
            data: serde_json::value::to_raw_value(payload)?,
        })
    }

    /// Parse the payload as `T`.
    pub fn decode<T: for<'de> Deserialize<'de>>(&self) -> serde_json::Result<T> {
        serde_json::from_str(self.data.get())
    }
}

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

/// Written when the scheduler accepts a job. First record in every log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStart {
    /// Job identifier, matching the log's filename.
    pub job_id: String,
    /// Human-readable job name.
    pub job_name: String,
    /// When the job entered the queue.
    pub queued_at: u64,
    /// When the job was submitted for planning.
    pub submitted_at: u64,
    /// Rendered logical plan, if one was captured.
    pub logical_plan: Option<String>,
    /// Rendered physical plan, if one was captured.
    pub physical_plan: Option<String>,
}

/// A stage became runnable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageStart {
    /// Stage identifier within the job.
    pub stage_id: u32,
    /// Number of partitions the stage will produce.
    pub partitions: u32,
}

/// A stage reached a terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageEnd {
    /// Stage identifier within the job.
    pub stage_id: u32,
    /// Terminal stage status.
    pub status: String,
}

/// A task finished, successfully or otherwise.
///
/// Identifiers are fixed-width `u32` rather than `usize`: this is a durable,
/// cross-machine format, so a log written by a 64-bit scheduler must mean the
/// same thing to any reader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnd {
    /// Stage the task belonged to.
    pub stage_id: u32,
    /// Task's slot within the stage. Under the multi-partition-task model a
    /// task owns a slice of partitions, so this names the task rather than a
    /// single partition.
    pub task_id: u32,
    /// Executor that ran the task.
    pub executor_id: String,
    /// Outcome of the task.
    pub status: TaskStatus,
    /// When the scheduler launched the task.
    pub launch_time: u64,
    /// When the executor began running it.
    pub start_exec_time: u64,
    /// When the executor finished it.
    pub end_exec_time: u64,
    /// Row counts and compute time for the task.
    pub metrics: TaskEndMetrics,
}

/// Frozen summary of a completed job, owned by this crate rather than shared
/// with the REST API.
///
/// The history server needs *some* structure to list and sort jobs, but it does
/// not need to understand the full responses. Keeping that structure minimal and
/// local decouples the part that must stay readable forever from
/// `ballista-api-types`, which evolves with the live REST contract.
///
/// These are exactly the fields `GET /api/jobs` renders, which is what lets the
/// history server build the job list without touching the payloads at all.
///
/// Adding fields here later is fine; each one needs `#[serde(default)]` so older
/// logs still parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobIndex {
    /// Job identifier.
    pub job_id: String,
    /// Human-readable job name.
    pub job_name: String,
    /// Plain status word, e.g. `Completed` or `Failed`.
    pub status: String,
    /// Verbose status, including completion or failure detail.
    pub job_status: String,
    /// When the job started executing.
    pub start_time: u64,
    /// When the job reached its terminal state.
    pub end_time: u64,
    /// Total number of stages in the job.
    pub num_stages: usize,
    /// Number of stages that finished successfully.
    pub completed_stages: usize,
    /// Progress as a percentage of completed stages.
    pub percent_complete: u8,
}

/// Terminal record, and the only one the history server serves from.
///
/// `job` and `stages` hold the finished REST responses **as raw JSON**, not as
/// typed structs. That is deliberate. Those responses are
/// `ballista-api-types` shapes, which change with the live REST contract:
/// `TaskSummary::partition_id` went from `u32` to `Vec<u32>`, and
/// `TaskStatus::Failed` gained a field, both within a single release cycle.
///
/// If this record stored them typed, a reader built after any such change would
/// fail to deserialize a log written before it, and the job would disappear.
/// Storing raw JSON means nothing ever parses the inner shape: the history
/// server relays the exact bytes the scheduler produced. The log is therefore
/// immune to REST type churn, and replayed output is byte-identical rather than
/// merely equivalent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEnd {
    /// How the job ended.
    pub status: JobEndStatus,
    /// When the job entered the queue.
    pub queued_at: u64,
    /// When the job started executing.
    pub started_at: u64,
    /// When the job reached its terminal state.
    pub completed_at: u64,
    /// Frozen summary, used to list and sort jobs without parsing the payloads.
    pub index: JobIndex,
    /// Finished `GET /api/job/{job_id}` response, stored verbatim.
    pub job: Box<RawValue>,
    /// Finished `GET /api/job/{job_id}/stages` response, stored verbatim.
    pub stages: Box<RawValue>,
    /// Finished `GET /api/job/{job_id}/config` response.
    pub config: JobConfig,
    /// Rendered DOT graph of the stage DAG.
    pub dot: String,
}

/// Record-kind discriminators, as written to the `ev` field.
pub mod kind {
    /// [`super::JobStart`]
    pub const JOB_START: &str = "JobStart";
    /// [`super::StageStart`]
    pub const STAGE_START: &str = "StageStart";
    /// [`super::StageEnd`]
    pub const STAGE_END: &str = "StageEnd";
    /// [`super::TaskEnd`]
    pub const TASK_END: &str = "TaskEnd";
    /// [`super::JobEnd`]
    pub const JOB_END: &str = "JobEnd";
}

/// An event to append to a job's log.
///
/// This is the in-memory shape callers build. It is encoded to a [`LogRecord`]
/// on the way to disk rather than serialized directly, so the envelope stays the
/// only thing a reader has to understand unconditionally.
#[derive(Debug, Clone)]
pub enum HistoryEvent {
    /// See [`JobStart`].
    JobStart(JobStart),
    /// See [`StageStart`].
    StageStart(StageStart),
    /// See [`StageEnd`].
    StageEnd(StageEnd),
    /// See [`TaskEnd`].
    TaskEnd(TaskEnd),
    /// See [`JobEnd`].
    JobEnd(Box<JobEnd>),
}

impl HistoryEvent {
    /// The `ev` discriminator this event is written with.
    pub fn kind(&self) -> &'static str {
        match self {
            HistoryEvent::JobStart(_) => kind::JOB_START,
            HistoryEvent::StageStart(_) => kind::STAGE_START,
            HistoryEvent::StageEnd(_) => kind::STAGE_END,
            HistoryEvent::TaskEnd(_) => kind::TASK_END,
            HistoryEvent::JobEnd(_) => kind::JOB_END,
        }
    }

    /// Encode to the envelope written to disk.
    pub fn to_record(&self) -> serde_json::Result<LogRecord> {
        match self {
            HistoryEvent::JobStart(p) => LogRecord::new(self.kind(), p),
            HistoryEvent::StageStart(p) => LogRecord::new(self.kind(), p),
            HistoryEvent::StageEnd(p) => LogRecord::new(self.kind(), p),
            HistoryEvent::TaskEnd(p) => LogRecord::new(self.kind(), p),
            HistoryEvent::JobEnd(p) => LogRecord::new(self.kind(), p),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sample_job_end() -> JobEnd {
        JobEnd {
            status: JobEndStatus::Succeeded,
            queued_at: 5,
            started_at: 10,
            completed_at: 20,
            index: JobIndex {
                job_id: "job-1".into(),
                job_name: "q1".into(),
                status: "Completed".into(),
                job_status: "COMPLETED".into(),
                start_time: 10,
                end_time: 20,
                num_stages: 1,
                completed_stages: 1,
                percent_complete: 100,
            },
            job: RawValue::from_string(r#"{"job_id":"job-1"}"#.to_string()).unwrap(),
            stages: RawValue::from_string(r#"{"stages":[]}"#.to_string()).unwrap(),
            config: BTreeMap::from([("k".to_string(), "v".to_string())]),
            dot: "digraph {}".into(),
        }
    }

    #[test]
    fn job_end_round_trips_through_jsonl() {
        let event = HistoryEvent::JobEnd(Box::new(sample_job_end()));
        let line = serde_json::to_string(&event.to_record().unwrap()).unwrap();

        assert!(line.contains(r#""ev":"JobEnd""#));
        assert!(line.contains(r#""version":1"#));

        let record: LogRecord = serde_json::from_str(&line).unwrap();
        assert_eq!(record.ev, kind::JOB_END);
        let back: JobEnd = record.decode().unwrap();
        assert_eq!(back.index.job_id, "job-1");
    }

    /// The stored responses are relayed as raw JSON and never round-tripped
    /// through a typed struct, so a log stays readable when the REST types it
    /// came from change shape. This is the property that keeps old logs alive
    /// across releases.
    #[test]
    fn stored_payloads_survive_shapes_this_build_cannot_model() {
        let mut end = sample_job_end();
        end.job = RawValue::from_string(
            r#"{"a_field_from_the_future":[1,2,3],"partition_id":{"nested":true}}"#
                .to_string(),
        )
        .unwrap();
        let line = serde_json::to_string(&end).unwrap();

        let back: JobEnd = serde_json::from_str(&line).unwrap();
        assert!(back.job.get().contains("a_field_from_the_future"));
        // Byte-for-byte, not merely equivalent.
        assert_eq!(back.job.get(), end.job.get());
    }

    /// The envelope is readable without knowing the payload's shape at all,
    /// which is what lets a reader check the version before parsing.
    #[test]
    fn envelope_is_readable_without_understanding_the_payload() {
        let line =
            r#"{"ev":"SomeFutureEvent","version":99,"data":{"anything":[1,{"x":null}]}}"#;
        let record: LogRecord = serde_json::from_str(line).unwrap();
        assert_eq!(record.ev, "SomeFutureEvent");
        assert_eq!(record.version, 99);
        assert!(record.data.get().contains("anything"));
    }
}
