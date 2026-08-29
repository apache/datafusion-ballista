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

//! Async, buffered event-log writer. Each job's events accumulate in memory and
//! are persisted as a single `<job_id>.eventlog` JSONL object through the
//! [`ObjectStore`] abstraction. Appends are non-blocking; a background task owns
//! the store so the scheduler hot path never waits on I/O.
//!
//! Persistence goes through `object_store` rather than the local filesystem
//! directly, so an event log can live on any supported backend. The default is a
//! [`LocalFileSystem`] rooted at the configured directory; a caller can supply
//! its own store with [`EventLogWriter::with_object_store`].
//!
//! An object store has no append: `put` replaces a whole object. The writer
//! therefore keeps each running job's full serialized log in memory and writes
//! it out once, on [`EventLogWriter::flush_job`] and on the terminal
//! [`EventLogWriter::finish_job`]. Individual timeline events never touch the
//! store on their own. A scheduler that dies mid-job loses that job's buffered
//! timeline, but the history server needs the terminal `JobEnd` record to serve
//! a job at all, so nothing servable is lost.

use crate::event::HistoryEvent;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
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
    /// Spawn the writer task, persisting logs to a [`LocalFileSystem`] store
    /// rooted at `log_dir`. The directory is created if it does not exist.
    ///
    /// `buffer` bounds the in-flight event queue; beyond it, timeline events are
    /// dropped rather than allowed to stall the caller.
    pub fn new(log_dir: PathBuf, buffer: usize) -> EventLogWriter {
        let _ = std::fs::create_dir_all(&log_dir);
        let store: Arc<dyn ObjectStore> = match LocalFileSystem::new_with_prefix(&log_dir)
        {
            Ok(store) => Arc::new(store),
            Err(e) => {
                log::warn!(
                    "event-log writer: cannot root a store at {}: {e}",
                    log_dir.display()
                );
                Arc::new(LocalFileSystem::new())
            }
        };
        Self::with_object_store(store, Path::default(), buffer)
    }

    /// Spawn the writer task, persisting each `<job_id>.eventlog` object under
    /// `prefix` in `store`.
    ///
    /// This is the extension point for backends other than the local
    /// filesystem: hand in any `object_store` implementation and the writer will
    /// `put` completed job logs through it.
    pub fn with_object_store(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        buffer: usize,
    ) -> EventLogWriter {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        tokio::spawn(run(store, prefix, rx));
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

    /// Persist everything enqueued for `job_id` so far as its `<job_id>.eventlog`
    /// object (best effort). Awaits the write.
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

    /// Persist `job_id`'s log a final time and drop its in-memory buffer. Must be
    /// called after the terminal event has been enqueued (e.g. via
    /// `append_final`) so it is ordered after it on the single-consumer FIFO
    /// channel. Best effort: if the channel is closed this logs and returns.
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

async fn run(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    mut rx: mpsc::Receiver<WriterMsg>,
) {
    // The full serialized JSONL log for each still-running job, held in memory
    // until it is persisted whole on flush/finish. An object store has no
    // append, so there is no per-job handle to keep open.
    let mut logs: HashMap<String, Vec<u8>> = HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            WriterMsg::Event { job_id, event } => {
                match event.to_record().and_then(|r| serde_json::to_string(&r)) {
                    Ok(line) => {
                        let buf = logs.entry(job_id).or_default();
                        buf.extend_from_slice(line.as_bytes());
                        buf.push(b'\n');
                    }
                    Err(e) => log::warn!("event-log writer: serialize failed: {e}"),
                }
            }
            WriterMsg::Flush { job_id, done } => {
                if let Some(buf) = logs.get(&job_id) {
                    persist(&*store, &prefix, &job_id, buf).await;
                }
                let _ = done.send(());
            }
            WriterMsg::Finish { job_id, done } => {
                if let Some(buf) = logs.remove(&job_id) {
                    persist(&*store, &prefix, &job_id, &buf).await;
                }
                let _ = done.send(());
            }
        }
    }
}

/// Write a job's accumulated log out as a single object. Best effort: a failure
/// is logged and the buffer is left in place for a later flush/finish to retry.
async fn persist(store: &dyn ObjectStore, prefix: &Path, job_id: &str, buf: &[u8]) {
    let location = prefix.clone().join(format!("{job_id}.eventlog"));
    if let Err(e) = store.put(&location, buf.to_vec().into()).await {
        log::warn!("event-log writer: put failed for {job_id}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{
        HistoryEvent, JobEnd, JobEndStatus, JobIndex, JobStart, StageStart,
    };
    use serde_json::value::RawValue;
    use std::collections::BTreeMap;

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

        let path = dir.path().join("job-1.eventlog");
        let contents = std::fs::read_to_string(&path).unwrap();
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

        let path = dir.path().join("job-1.eventlog");
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"ev\":\"JobStart\""));
        assert!(lines[1].contains("\"ev\":\"StageStart\""));
    }

    #[tokio::test]
    async fn with_object_store_persists_to_a_custom_store() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let writer =
            EventLogWriter::with_object_store(store.clone(), Path::from("logs"), 16);

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

        let bytes = store
            .get(&Path::from("logs/job-1.eventlog"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let contents = String::from_utf8(bytes.to_vec()).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"ev\":\"JobStart\""));
        assert!(lines[1].contains("\"ev\":\"JobEnd\""));
    }
}
