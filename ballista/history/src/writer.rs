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

//! Async, buffered event-log writer. Each job's events append to
//! `<log_dir>/<job_id>.eventlog.running` as JSONL while the job runs. Appends
//! are non-blocking; a background task performs the file I/O so the scheduler
//! hot path never waits on disk. When the job reaches a terminal state,
//! [`EventLogWriter::finish_job`] flushes, closes, and renames the file to
//! `<log_dir>/<job_id>.eventlog` — so the `.running` suffix alone marks a job
//! as still running (or abandoned by a crashed scheduler), without reading
//! the file.
//!
//! Once a job has been finalized, a late event for it (e.g. a straggler
//! terminal task status arriving after the job was cancelled or failed) is
//! dropped rather than reopening the log: recreating an orphan `.running` file
//! that nothing would ever rename would make that job read as crashed. See
//! `open_for`.

use crate::event::HistoryEvent;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

enum WriterMsg {
    Event {
        job_id: String,
        event: Box<HistoryEvent>,
    },
    Flush {
        job_id: String,
        done: oneshot::Sender<()>,
    },
    Finish {
        job_id: String,
        done: oneshot::Sender<()>,
    },
}

/// Handle to the background event-log writer task.
///
/// Cloning is cheap and every clone feeds the same task, so the scheduler can
/// hand one to each component that needs to record events.
#[derive(Clone)]
pub struct EventLogWriter {
    tx: mpsc::Sender<WriterMsg>,
}

impl EventLogWriter {
    /// Spawn the writer task, appending logs under `log_dir`.
    ///
    /// `buffer` bounds the in-flight event queue; beyond it, timeline events are
    /// dropped rather than allowed to stall the caller.
    pub fn new(log_dir: PathBuf, buffer: usize) -> EventLogWriter {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        tokio::spawn(run(log_dir, rx));
        EventLogWriter { tx }
    }

    /// Enqueue an event for `job_id`. Never blocks; drops (with a warning) if the
    /// channel is full, so logging cannot stall scheduling.
    pub fn append(&self, job_id: &str, event: HistoryEvent) {
        let msg = WriterMsg::Event {
            job_id: job_id.to_string(),
            event: Box::new(event),
        };
        if self.tx.try_send(msg).is_err() {
            log::warn!(
                "event-log writer: dropping event for {job_id} (channel full or closed)"
            );
        }
    }

    /// Await all currently-enqueued writes for `job_id` (best effort).
    pub async fn flush_job(&self, job_id: &str) {
        let (done, wait) = oneshot::channel();
        if self
            .tx
            .send(WriterMsg::Flush {
                job_id: job_id.to_string(),
                done,
            })
            .await
            .is_ok()
        {
            let _ = wait.await;
        }
    }

    /// Enqueue a terminal event (e.g. `JobEnd`) for `job_id`. Unlike `append`, this
    /// awaits channel capacity instead of dropping the event when the channel is
    /// full, so the terminal record is never silently lost. Still best-effort at
    /// the process boundary: if the channel is closed (background task gone) this
    /// logs and returns rather than panicking.
    pub async fn append_final(&self, job_id: &str, event: HistoryEvent) {
        let msg = WriterMsg::Event {
            job_id: job_id.to_string(),
            event: Box::new(event),
        };
        if self.tx.send(msg).await.is_err() {
            log::warn!(
                "event-log writer: failed to enqueue terminal event for {job_id} (channel closed)"
            );
        }
    }

    /// Flush and close the per-job file handle for `job_id`. Must be called after
    /// the terminal event has been enqueued (e.g. via `append_final`) so it is
    /// ordered after it on the single-consumer FIFO channel. Best effort: if the
    /// channel is closed this logs and returns.
    pub async fn finish_job(&self, job_id: &str) {
        let (done, wait) = oneshot::channel();
        if self
            .tx
            .send(WriterMsg::Finish {
                job_id: job_id.to_string(),
                done,
            })
            .await
            .is_ok()
        {
            let _ = wait.await;
        } else {
            log::warn!(
                "event-log writer: failed to enqueue finish for {job_id} (channel closed)"
            );
        }
    }
}

async fn run(log_dir: PathBuf, mut rx: mpsc::Receiver<WriterMsg>) {
    if let Err(e) = tokio::fs::create_dir_all(&log_dir).await {
        log::warn!("event-log writer: cannot create {}: {e}", log_dir.display());
        return;
    }
    // One open append handle per job for the life of the process.
    let mut handles: HashMap<String, tokio::fs::File> = HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            WriterMsg::Event { job_id, event } => {
                let file = match open_for(&log_dir, &mut handles, &job_id).await {
                    Some(f) => f,
                    None => continue,
                };
                match event.to_record().and_then(|r| serde_json::to_string(&r)) {
                    Ok(mut line) => {
                        line.push('\n');
                        if let Err(e) = file.write_all(line.as_bytes()).await {
                            log::warn!(
                                "event-log writer: write failed for {job_id}: {e}"
                            );
                        }
                    }
                    Err(e) => log::warn!("event-log writer: serialize failed: {e}"),
                }
            }
            WriterMsg::Flush { job_id, done } => {
                if let Some(file) = handles.get_mut(&job_id) {
                    let _ = file.flush().await;
                }
                let _ = done.send(());
            }
            WriterMsg::Finish { job_id, done } => {
                if let Some(mut file) = handles.remove(&job_id) {
                    let _ = file.flush().await;
                    // Must close before renaming: required on Windows, and
                    // makes the close-before-rename ordering explicit here on
                    // every platform.
                    drop(file);
                    let from = running_path(&log_dir, &job_id);
                    let to = final_path(&log_dir, &job_id);
                    if let Err(e) = tokio::fs::rename(&from, &to).await {
                        log::warn!(
                            "event-log writer: failed to rename {} to {}: {e}",
                            from.display(),
                            to.display()
                        );
                    }
                }
                let _ = done.send(());
            }
        }
    }
}

/// Where `job_id`'s log lives while the job is still running.
fn running_path(log_dir: &Path, job_id: &str) -> PathBuf {
    log_dir.join(format!("{job_id}.eventlog.running"))
}

/// Where `job_id`'s log lives once it has reached a terminal state.
fn final_path(log_dir: &Path, job_id: &str) -> PathBuf {
    log_dir.join(format!("{job_id}.eventlog"))
}

async fn open_for<'a>(
    log_dir: &Path,
    handles: &'a mut HashMap<String, tokio::fs::File>,
    job_id: &str,
) -> Option<&'a mut tokio::fs::File> {
    if !handles.contains_key(job_id) {
        // The job's log has already been finalized and renamed. A late event
        // (a straggler terminal task status arriving after JobCancel or
        // JobRunningFailed) must not recreate an orphan `.running` file that
        // nothing will ever rename — that filename is the "still running /
        // crashed" signal a directory scan relies on. `unwrap_or(false)` lets a
        // transient stat error fall through to the normal open path rather than
        // silently dropping a real event.
        //
        // A job that finished without a `JobEnd` and without an open handle
        // leaves no `.eventlog`, so a straggler for it is not covered here; that
        // job could not be recorded in the first place and the history server
        // skips it either way.
        if tokio::fs::try_exists(final_path(log_dir, job_id))
            .await
            .unwrap_or(false)
        {
            log::debug!(
                "event-log writer: dropping late event for {job_id}; its log is already finalized"
            );
            return None;
        }
        let path = running_path(log_dir, job_id);
        match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            Ok(f) => {
                handles.insert(job_id.to_string(), f);
            }
            Err(e) => {
                log::warn!("event-log writer: cannot open {}: {e}", path.display());
                return None;
            }
        }
    }
    handles.get_mut(job_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{
        HistoryEvent, JobEnd, JobEndStatus, JobIndex, JobStart, StageStart, TaskEnd,
        TaskEndMetrics,
    };
    use ballista_api_types::dto::TaskStatus;
    use serde_json::value::RawValue;
    use std::collections::BTreeMap;

    /// A minimal succeeded `JobEnd` for `job_id`; the payloads are placeholders,
    /// only the record's presence and the file lifecycle matter to these tests.
    fn succeeded_job_end(job_id: &str) -> HistoryEvent {
        HistoryEvent::JobEnd(Box::new(JobEnd {
            status: JobEndStatus::Succeeded,
            queued_at: 1,
            started_at: 2,
            completed_at: 20,
            index: JobIndex {
                job_id: job_id.into(),
                job_name: "q1".into(),
                status: "Completed".into(),
                job_status: "COMPLETED".into(),
                start_time: 10,
                end_time: 20,
                num_stages: 1,
                completed_stages: 1,
                percent_complete: 100,
            },
            job: RawValue::from_string(format!(r#"{{"job_id":"{job_id}"}}"#)).unwrap(),
            stages: RawValue::from_string(r#"{"stages":[]}"#.to_string()).unwrap(),
            config: BTreeMap::new(),
            dot: "digraph {}".into(),
        }))
    }

    #[tokio::test]
    async fn terminal_job_end_is_not_dropped_on_a_saturated_channel() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny buffer so the non-blocking `append` path would readily drop events
        // under load; `append_final` must still guarantee delivery.
        let writer = EventLogWriter::new(dir.path().to_path_buf(), 1);

        writer.append(
            "job-1",
            HistoryEvent::JobStart(JobStart {
                job_id: "job-1".into(),
                job_name: "q1".into(),
                queued_at: 1,
                submitted_at: 2,
                logical_plan: None,
                physical_plan: None,
            }),
        );
        for stage_id in 0..10 {
            writer.append(
                "job-1",
                HistoryEvent::StageStart(StageStart {
                    stage_id,
                    partitions: 4,
                }),
            );
        }

        let job_end = HistoryEvent::JobEnd(Box::new(JobEnd {
            status: JobEndStatus::Succeeded,
            queued_at: 1,
            started_at: 2,
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
            config: BTreeMap::new(),
            dot: "digraph {}".into(),
        }));
        writer.append_final("job-1", job_end).await;
        writer.finish_job("job-1").await;

        assert!(!dir.path().join("job-1.eventlog.running").exists());
        let path = dir.path().join("job-1.eventlog");
        assert!(path.exists());
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert!(
            lines.iter().any(|l| l.contains("\"ev\":\"JobEnd\"")),
            "expected a JobEnd line in the event log, got: {contents}"
        );
        assert_eq!(
            lines.last().map(|l| l.contains("\"ev\":\"JobEnd\"")),
            Some(true),
            "JobEnd should be the last line written"
        );
    }

    #[tokio::test]
    async fn append_writes_one_jsonl_line_per_event() {
        let dir = tempfile::tempdir().unwrap();
        let writer = EventLogWriter::new(dir.path().to_path_buf(), 16);
        writer.append(
            "job-1",
            HistoryEvent::JobStart(JobStart {
                job_id: "job-1".into(),
                job_name: "q1".into(),
                queued_at: 1,
                submitted_at: 2,
                logical_plan: None,
                physical_plan: None,
            }),
        );
        writer.append(
            "job-1",
            HistoryEvent::StageStart(StageStart {
                stage_id: 1,
                partitions: 4,
            }),
        );
        writer.flush_job("job-1").await;

        // Not finished yet, so still under the `.running` name.
        let path = dir.path().join("job-1.eventlog.running");
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"ev\":\"JobStart\""));
        assert!(lines[1].contains("\"ev\":\"StageStart\""));
    }

    #[tokio::test]
    async fn finish_job_is_a_noop_for_a_job_with_no_open_handle() {
        let dir = tempfile::tempdir().unwrap();
        let writer = EventLogWriter::new(dir.path().to_path_buf(), 16);

        writer.finish_job("never-appended").await;

        assert!(!dir.path().join("never-appended.eventlog.running").exists());
        assert!(!dir.path().join("never-appended.eventlog").exists());
    }

    #[tokio::test]
    async fn late_event_after_finish_does_not_recreate_the_running_log() {
        let dir = tempfile::tempdir().unwrap();
        let writer = EventLogWriter::new(dir.path().to_path_buf(), 16);

        writer.append(
            "job-1",
            HistoryEvent::JobStart(JobStart {
                job_id: "job-1".into(),
                job_name: "q1".into(),
                queued_at: 1,
                submitted_at: 2,
                logical_plan: None,
                physical_plan: None,
            }),
        );
        writer
            .append_final("job-1", succeeded_job_end("job-1"))
            .await;
        writer.finish_job("job-1").await;

        let running = dir.path().join("job-1.eventlog.running");
        let final_path = dir.path().join("job-1.eventlog");
        assert!(!running.exists());
        assert!(final_path.exists());
        let finalized = tokio::fs::read_to_string(&final_path).await.unwrap();

        // A straggler terminal task status for the same job, arriving after the
        // log was finalized (e.g. a task completing just after JobCancel).
        writer.append(
            "job-1",
            HistoryEvent::TaskEnd(TaskEnd {
                stage_id: 1,
                task_id: 7,
                executor_id: "exec-1".into(),
                status: TaskStatus::Successful,
                launch_time: 1,
                start_exec_time: 2,
                end_exec_time: 3,
                metrics: TaskEndMetrics {
                    input_rows: 0,
                    output_rows: 0,
                    elapsed_compute_nanos: 0,
                },
            }),
        );
        writer.flush_job("job-1").await;

        assert!(
            !running.exists(),
            "a late event must not recreate the .running log for a finalized job"
        );
        assert_eq!(
            tokio::fs::read_to_string(&final_path).await.unwrap(),
            finalized,
            "the finalized .eventlog must be left untouched"
        );
    }
}
