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

//! Reads a completed `<job_id>.eventlog` into the payload the history server
//! serves. A file is "completed" once it contains a `JobEnd` record.
//!
//! Reading is deliberately forgiving about lines it does not recognise and
//! deliberately loud about a `JobEnd` it cannot use. An unreadable terminal
//! record means a job that exists on disk will not appear in the UI, and that
//! must never be indistinguishable from a job that is simply still running.

use crate::event::{JobEnd, JobIndex, LogRecord, SCHEMA_VERSION, kind};
use ballista_api_types::dto::JobConfig;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::io::BufRead;
use std::path::Path;

/// The served payload recovered from a completed job's event log.
#[derive(Debug, Clone)]
pub struct ReplayedJob {
    /// Frozen summary, used to list and sort jobs.
    pub index: JobIndex,
    /// `GET /api/job/{job_id}` response, verbatim as the scheduler wrote it.
    pub job: Box<RawValue>,
    /// `GET /api/job/{job_id}/stages` response, verbatim.
    pub stages: Box<RawValue>,
    /// `GET /api/job/{job_id}/config` response.
    pub config: JobConfig,
    /// Rendered DOT graph of the stage DAG.
    pub dot: String,
}

/// Why a log that exists on disk yielded no servable job.
#[derive(Debug)]
pub enum ReadError {
    /// The file could not be read at all.
    Io(std::io::Error),
    /// A `JobEnd` record is present but was written by a newer schema than
    /// this build understands.
    UnsupportedVersion {
        /// Version stamped on the record.
        found: u32,
        /// Highest version this build can read.
        supported: u32,
    },
    /// A `JobEnd` record is present and claims a supported version, but could
    /// not be deserialized. Distinct from a missing `JobEnd`: this is a real
    /// problem worth surfacing, not a job that is still running.
    Malformed(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Io(e) => write!(f, "{e}"),
            ReadError::UnsupportedVersion { found, supported } => write!(
                f,
                "event log schema version {found} is newer than the highest \
                 supported version {supported}; upgrade the history server"
            ),
            ReadError::Malformed(e) => write!(f, "malformed JobEnd record: {e}"),
        }
    }
}

impl std::error::Error for ReadError {}

impl From<std::io::Error> for ReadError {
    fn from(e: std::io::Error) -> Self {
        ReadError::Io(e)
    }
}

/// Read a completed job's payload out of its event log.
///
/// Returns `Ok(None)` only when the log genuinely has no `JobEnd` record, which
/// means the job is still running or the scheduler died before finishing it.
/// A `JobEnd` that is present but unusable is an `Err`, so callers can report it
/// rather than silently dropping the job.
pub fn read_completed_job(path: &Path) -> Result<Option<ReplayedJob>, ReadError> {
    Ok(read_job_end::<JobEnd>(path)?.map(|end| ReplayedJob {
        index: end.index,
        job: end.job,
        stages: end.stages,
        config: end.config,
        dot: end.dot,
    }))
}

/// The `JobEnd` fields needed to list a job, and nothing else.
///
/// Deserializing into this rather than [`JobEnd`] steps over the stored
/// `/api/job/{id}` and `/api/job/{id}/stages` payloads, the session config and
/// the DOT graph without ever allocating them. That is what lets the history
/// server index a directory of logs without holding their contents.
#[derive(Deserialize)]
struct JobEndIndex {
    index: JobIndex,
}

/// Read only the frozen summary out of a completed job's event log.
///
/// Same contract as [`read_completed_job`], including how a malformed terminal
/// record is reported, but it recovers only the fields the job list needs. Use
/// it to index a log directory, then [`read_completed_job`] to serve one job.
///
/// Because the payloads are never parsed, corruption confined to them is not
/// detected here. It surfaces when the job is actually read.
pub fn read_job_index(path: &Path) -> Result<Option<JobIndex>, ReadError> {
    Ok(read_job_end::<JobEndIndex>(path)?.map(|end| end.index))
}

/// Find a log's terminal record and decode it into `T`.
///
/// Lines that are not `JobEnd` are skipped without inspection, including ones
/// this build does not recognise: a future schema may add record types, and an
/// older reader must tolerate them rather than choke on the file.
fn read_job_end<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, ReadError> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        // Route on the envelope alone. A line we cannot even read an envelope
        // from is treated as a record we do not understand, not as a failure.
        let Ok(record) = serde_json::from_str::<LogRecord>(&line) else {
            continue;
        };
        if record.ev != kind::JOB_END {
            continue;
        }

        if record.version > SCHEMA_VERSION {
            return Err(ReadError::UnsupportedVersion {
                found: record.version,
                supported: SCHEMA_VERSION,
            });
        }

        return match record.decode::<T>() {
            Ok(end) => Ok(Some(end)),
            Err(e) => Err(ReadError::Malformed(e.to_string())),
        };
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{HistoryEvent, JobEnd, JobEndStatus, SCHEMA_VERSION};
    use std::io::Write;

    fn sample_index() -> JobIndex {
        JobIndex {
            job_id: "job-1".into(),
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

    fn job_end_line() -> String {
        let event = HistoryEvent::JobEnd(Box::new(JobEnd {
            status: JobEndStatus::Succeeded,
            queued_at: 1,
            started_at: 2,
            completed_at: 3,
            index: sample_index(),
            job: RawValue::from_string(
                r#"{"job_id":"job-1","physical_plan":"ProjectionExec"}"#.to_string(),
            )
            .unwrap(),
            stages: RawValue::from_string(r#"{"stages":[]}"#.to_string()).unwrap(),
            config: Default::default(),
            dot: "digraph {}".into(),
        }));
        serde_json::to_string(&event.to_record().unwrap()).unwrap()
    }

    fn write_log(
        dir: &tempfile::TempDir,
        name: &str,
        lines: &[&str],
    ) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        path
    }

    #[test]
    fn reads_job_end_and_ignores_unknown_timeline_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_log(
            &dir,
            "job-1.eventlog",
            &[
                r#"{"ev":"StageStart","version":1,"data":{"stage_id":1,"partitions":4}}"#,
                &job_end_line(),
            ],
        );

        let replayed = read_completed_job(&path).unwrap().expect("completed");
        assert_eq!(replayed.index.job_id, "job-1");
        assert_eq!(replayed.dot, "digraph {}");
        // The payload is re-served verbatim rather than round-tripped through a
        // typed struct.
        assert!(replayed.job.get().contains("ProjectionExec"));
    }

    /// The index-only read is what the history server builds its job list
    /// from, so it has to agree with the full read on every field it carries.
    /// If the two ever diverge, the list view and the detail view disagree
    /// about the same job.
    #[test]
    fn index_only_read_agrees_with_full_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_log(&dir, "job-1.eventlog", &[&job_end_line()]);

        let index = read_job_index(&path).unwrap().expect("completed");
        let full = read_completed_job(&path).unwrap().expect("completed");

        assert_eq!(
            serde_json::to_value(&index).unwrap(),
            serde_json::to_value(&full.index).unwrap()
        );
    }

    #[test]
    fn index_only_read_returns_none_when_no_job_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_log(
            &dir,
            "job-6.eventlog",
            &[r#"{"ev":"StageStart","version":1,"data":{"stage_id":1,"partitions":4}}"#],
        );
        assert!(read_job_index(&path).unwrap().is_none());
    }

    /// A terminal record too broken to yield a summary must still be an error
    /// rather than a silently missing job, exactly as for the full read.
    #[test]
    fn index_only_read_reports_a_malformed_job_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_log(
            &dir,
            "job-7.eventlog",
            &[r#"{"ev":"JobEnd","version":1,"data":{"status":"Succeeded"}}"#],
        );

        match read_job_index(&path) {
            Err(ReadError::Malformed(_)) => {}
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn returns_none_when_no_job_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_log(
            &dir,
            "job-2.eventlog",
            &[r#"{"ev":"StageStart","version":1,"data":{"stage_id":1,"partitions":4}}"#],
        );
        assert!(read_completed_job(&path).unwrap().is_none());
    }

    /// A record type this build has never heard of must not stop it reading the
    /// rest of the file. This is the forward-compatibility guarantee: an older
    /// history server keeps working against logs from a newer scheduler, as
    /// long as the schema version itself has not been bumped.
    #[test]
    fn skips_unrecognized_record_types() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_log(
            &dir,
            "job-3.eventlog",
            &[
                r#"{"ev":"SomeFutureEvent","version":1,"data":{"whatever":true}}"#,
                &job_end_line(),
            ],
        );
        assert!(read_completed_job(&path).unwrap().is_some());
    }

    /// A newer schema version is reported as such rather than being silently
    /// skipped, so an operator finds out their history server is too old.
    #[test]
    fn newer_schema_version_is_reported_not_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let line = format!(
            r#"{{"ev":"JobEnd","version":{},"data":{{}}}}"#,
            SCHEMA_VERSION + 1
        );
        let path = write_log(&dir, "job-4.eventlog", &[&line]);

        match read_completed_job(&path) {
            Err(ReadError::UnsupportedVersion { found, supported }) => {
                assert_eq!(found, SCHEMA_VERSION + 1);
                assert_eq!(supported, SCHEMA_VERSION);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    /// A corrupt terminal record must be distinguishable from a job that has
    /// simply not finished. Both used to surface as `Ok(None)`, which meant a
    /// job could vanish from the UI with nothing logged.
    #[test]
    fn malformed_job_end_is_an_error_not_a_missing_job() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_log(
            &dir,
            "job-5.eventlog",
            &[r#"{"ev":"JobEnd","version":1,"data":{"status":"Succeeded"}}"#],
        );

        match read_completed_job(&path) {
            Err(ReadError::Malformed(_)) => {}
            other => panic!("expected Malformed, got {other:?}"),
        }
    }
}

/// Compatibility tests against a checked-in log from an earlier schema version.
///
/// `testdata/schema-v1.eventlog` is a **frozen artifact**. It is never
/// regenerated: the whole point is that it was written by an older Ballista and
/// must stay readable forever. If a change here makes this module fail, the
/// change breaks every event log already on disk in the field.
///
/// Fixing such a failure by editing the fixture defeats the test. The options
/// are to make the change backward-compatible (usually `#[serde(default)]` on a
/// new field), or to bump [`SCHEMA_VERSION`] and keep a path that can still read
/// the old version.
#[cfg(test)]
mod compatibility {
    use super::*;

    fn golden_v1() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("schema-v1.eventlog")
    }

    #[test]
    fn reads_a_v1_log_written_by_an_earlier_ballista() {
        let replayed = read_completed_job(&golden_v1())
            .expect("a v1 log must remain readable")
            .expect("the fixture contains a JobEnd record");

        assert_eq!(replayed.index.job_id, "golden-v1");
        assert_eq!(replayed.index.job_name, "tpch-q1");
        assert_eq!(replayed.index.status, "Completed");
        assert_eq!(replayed.index.start_time, 1005);
        assert_eq!(replayed.index.end_time, 1100);
        assert_eq!(
            replayed
                .config
                .get("datafusion.execution.target_partitions"),
            Some(&"4".to_string())
        );
        assert!(replayed.dot.contains("digraph"));
    }

    /// The history server indexes a directory with the index-only read, so it
    /// has to work on a log written by an earlier Ballista too.
    #[test]
    fn indexes_a_v1_log_written_by_an_earlier_ballista() {
        let index = read_job_index(&golden_v1())
            .expect("a v1 log must remain indexable")
            .expect("the fixture contains a JobEnd record");

        assert_eq!(index.job_id, "golden-v1");
        assert_eq!(index.job_name, "tpch-q1");
        assert_eq!(index.status, "Completed");
    }

    /// The stored responses must come back byte-for-byte, because that is what
    /// lets the history server re-serve them without understanding them.
    ///
    /// Compared against the raw file text rather than a parsed
    /// `serde_json::Value`: `Value` sorts object keys, so round-tripping
    /// through it would reorder the payload and hide exactly the property
    /// under test.
    #[test]
    fn v1_payloads_are_relayed_verbatim() {
        let raw = std::fs::read_to_string(golden_v1()).unwrap();
        let replayed = read_completed_job(&golden_v1()).unwrap().unwrap();

        assert!(
            raw.contains(replayed.job.get()),
            "the job payload must appear verbatim in the log, got: {}",
            replayed.job.get()
        );
        assert!(
            raw.contains(replayed.stages.get()),
            "the stages payload must appear verbatim in the log"
        );
        // Key order survives, which a typed round trip would not preserve.
        assert!(
            replayed.job.get().starts_with(r#"{"job_id":"golden-v1""#),
            "original key order must be preserved, got: {}",
            replayed.job.get()
        );
    }

    /// The v1 fixture deliberately contains a record kind this build has never
    /// heard of, standing in for one a future scheduler might write. An older
    /// reader must step over it and still find the terminal record.
    #[test]
    fn unknown_record_kinds_in_a_v1_log_do_not_break_reading() {
        let raw = std::fs::read_to_string(golden_v1()).unwrap();
        assert!(
            raw.contains("AnEventKindFromTheFuture"),
            "fixture should exercise the unknown-record path"
        );
        assert!(read_completed_job(&golden_v1()).unwrap().is_some());
    }

    /// The stages payload in the fixture carries `partition_id` as an array,
    /// the shape that broke the TUI when it was declared as a scalar (#2257).
    /// Storing payloads opaquely means a change like that can never make an
    /// existing log unreadable.
    #[test]
    fn a_rest_shape_change_cannot_orphan_a_stored_log() {
        let replayed = read_completed_job(&golden_v1()).unwrap().unwrap();
        assert!(
            replayed.stages.get().contains(r#""partition_id":[0,1]"#),
            "fixture should carry the multi-partition shape verbatim"
        );
    }
}
