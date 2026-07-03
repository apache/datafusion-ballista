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
//! `<log_dir>/<job_id>.eventlog` as JSONL. Appends are non-blocking; a background
//! task performs the file I/O so the scheduler hot path never waits on disk.

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
}

#[derive(Clone)]
pub struct EventLogWriter {
    tx: mpsc::Sender<WriterMsg>,
}

impl EventLogWriter {
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
            eprintln!(
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
}

async fn run(log_dir: PathBuf, mut rx: mpsc::Receiver<WriterMsg>) {
    if let Err(e) = tokio::fs::create_dir_all(&log_dir).await {
        eprintln!("event-log writer: cannot create {}: {e}", log_dir.display());
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
                match serde_json::to_string(&*event) {
                    Ok(mut line) => {
                        line.push('\n');
                        if let Err(e) = file.write_all(line.as_bytes()).await {
                            eprintln!("event-log writer: write failed for {job_id}: {e}");
                        }
                    }
                    Err(e) => eprintln!("event-log writer: serialize failed: {e}"),
                }
            }
            WriterMsg::Flush { job_id, done } => {
                if let Some(file) = handles.get_mut(&job_id) {
                    let _ = file.flush().await;
                }
                let _ = done.send(());
            }
        }
    }
}

async fn open_for<'a>(
    log_dir: &Path,
    handles: &'a mut HashMap<String, tokio::fs::File>,
    job_id: &str,
) -> Option<&'a mut tokio::fs::File> {
    if !handles.contains_key(job_id) {
        let path = log_dir.join(format!("{job_id}.eventlog"));
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
                eprintln!("event-log writer: cannot open {}: {e}", path.display());
                return None;
            }
        }
    }
    handles.get_mut(job_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{HistoryEvent, SCHEMA_VERSION};

    #[tokio::test]
    async fn append_writes_one_jsonl_line_per_event() {
        let dir = tempfile::tempdir().unwrap();
        let writer = EventLogWriter::new(dir.path().to_path_buf(), 16);
        writer.append(
            "job-1",
            HistoryEvent::JobStart {
                version: SCHEMA_VERSION,
                job_id: "job-1".into(),
                job_name: "q1".into(),
                queued_at: 1,
                submitted_at: 2,
                logical_plan: None,
                physical_plan: None,
            },
        );
        writer.append(
            "job-1",
            HistoryEvent::StageStart {
                stage_id: 1,
                partitions: 4,
            },
        );
        writer.flush_job("job-1").await;

        let path = dir.path().join("job-1.eventlog");
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"ev\":\"JobStart\""));
        assert!(lines[1].contains("\"ev\":\"StageStart\""));
    }
}
