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

#![warn(missing_docs)]

//! Durable event logs for completed Ballista jobs.
//!
//! The scheduler appends one JSONL record per event to
//! `<event_log_dir>/<job_id>.eventlog` while a job runs. The history server
//! reads those files back and serves the same `/api/*` responses the live
//! scheduler does, long after the scheduler has forgotten the job.
//!
//! # Write once, replay verbatim
//!
//! The terminal [`event::JobEnd`] record embeds the finished `/api/*` responses
//! the scheduler built from its live execution graph, stored as raw JSON. Replay
//! relays those bytes unchanged, so the history server never re-derives a
//! response and there is no second implementation to drift.
//!
//! The earlier records ([`event::JobStart`], [`event::StageStart`],
//! [`event::StageEnd`], [`event::TaskEnd`]) form an incremental timeline.
//! Nothing reads them yet; they exist so a future UI can show a job progressing
//! rather than only its final state.
//!
//! # Compatibility
//!
//! Logs outlive the binaries that wrote them, so a newer reader must keep
//! reading older logs indefinitely. [`event::SCHEMA_VERSION`] documents the
//! policy, [`event::LogRecord`] is the self-describing envelope that makes it
//! enforceable, and `testdata/schema-v1.eventlog` is a frozen log that CI
//! replays on every build to catch regressions.
//!
//! # Durability
//!
//! [`writer::EventLogWriter`] does all file I/O on a background task, so the
//! scheduler's event loop never waits on a disk write. Timeline events are
//! dropped rather than allowed to block if the queue backs up, on the grounds
//! that losing a progress record is better than stalling scheduling. The
//! terminal `JobEnd` is the exception: it waits for queue capacity, because a
//! job missing its `JobEnd` is invisible to the history server.

pub mod event;
pub mod reader;
pub mod writer;
