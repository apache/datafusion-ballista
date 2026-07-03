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

//! Job state event notifications for subscribers.
//!
//! This module provides a broadcast-based notification system for job state changes.
//! Consumers can subscribe to receive notifications when jobs change state, avoiding
//! the need to poll for status updates.
//!
//! # Choosing between `JobStateEvent` and `JobStatusSubscriber`
//!
//! Ballista has two mechanisms for observing job progress:
//!
//! - **[`JobStateEvent`] / [`crate::scheduler_server::SchedulerServer::subscribe_job_updates`]**:
//!   a `tokio::sync::broadcast` channel that delivers lifecycle events for *all* jobs
//!   on this scheduler. Suited for cluster-wide observers such as metrics collectors,
//!   audit logs, or HA state replication. Subscribers receive a lightweight event
//!   containing only the job ID and new state; they must query the scheduler separately
//!   for full job details. Slow subscribers may lag and miss events if the channel buffer fills;
//!   see [`crate::config::SchedulerConfig::job_state_channel_capacity`] to tune this.
//!
//! - **`JobStatusSubscriber`** (`tokio::sync::mpsc::Sender<JobStatus>`): a per-job
//!   `mpsc` channel threaded through [`crate::scheduler_server::SchedulerServer::submit_job`].
//!   Suited for a single caller that submitted a job and wants rich status updates
//!   (including partition locations on success) for that specific job. Backpressure
//!   is applied to the scheduler if the subscriber is slow, so this is best used by
//!   in-process callers that consume updates promptly.

use ballista_core::serde::protobuf::job_status;
use std::fmt;

/// Represents the current state of a job in the scheduler.
///
/// Mirrors the variants of the protobuf `job_status::Status` but is
/// lightweight for broadcasting. When adding or renaming variants here,
/// update the `From<job_status::Status>` impl below to keep them in sync.
/// Note that `Cancelled` has no protobuf counterpart; cancelled jobs are
/// represented as `Failed` in the wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    /// Job is queued and waiting to be scheduled.
    Queued,
    /// Job is currently running.
    Running,
    /// Job has completed successfully.
    Completed,
    /// Job has failed with an error message.
    Failed(String),
    /// Job was cancelled.
    Cancelled,
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobState::Queued => write!(f, "Queued"),
            JobState::Running => write!(f, "Running"),
            JobState::Completed => write!(f, "Completed"),
            JobState::Failed(msg) => write!(f, "Failed: {}", msg),
            JobState::Cancelled => write!(f, "Cancelled"),
        }
    }
}

impl From<job_status::Status> for JobState {
    fn from(status: job_status::Status) -> Self {
        match status {
            job_status::Status::Queued(_) => JobState::Queued,
            job_status::Status::Running(_) => JobState::Running,
            job_status::Status::Failed(f) => JobState::Failed(f.error),
            // Protobuf has no Cancelled variant; cancelled jobs surface as Failed on the wire.
            job_status::Status::Successful(_) => JobState::Completed,
        }
    }
}

/// Event emitted when a job's state changes.
///
/// This struct is designed to be cloned and sent through a broadcast channel
/// to notify subscribers about job state changes.
#[derive(Debug, Clone)]
pub struct JobStateEvent {
    /// The unique identifier of the job.
    pub job_id: String,
    /// The new state of the job.
    pub state: JobState,
}

impl JobStateEvent {
    /// Creates a new job state event.
    pub fn new(job_id: impl Into<String>, state: JobState) -> Self {
        Self {
            job_id: job_id.into(),
            state,
        }
    }

    /// Creates a queued event for the given job.
    pub fn queued(job_id: impl Into<String>) -> Self {
        Self::new(job_id, JobState::Queued)
    }

    /// Creates a running event for the given job.
    pub fn running(job_id: impl Into<String>) -> Self {
        Self::new(job_id, JobState::Running)
    }

    /// Creates a completed event for the given job.
    pub fn completed(job_id: impl Into<String>) -> Self {
        Self::new(job_id, JobState::Completed)
    }

    /// Creates a failed event for the given job.
    pub fn failed(job_id: impl Into<String>, error: impl Into<String>) -> Self {
        Self::new(job_id, JobState::Failed(error.into()))
    }

    /// Creates a cancelled event for the given job.
    pub fn cancelled(job_id: impl Into<String>) -> Self {
        Self::new(job_id, JobState::Cancelled)
    }
}

impl fmt::Display for JobStateEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JobStateEvent[job_id={}, state={}]",
            self.job_id, self.state
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_state_event_creation() {
        let event = JobStateEvent::queued("job-123");
        assert_eq!(event.job_id, "job-123");
        assert_eq!(event.state, JobState::Queued);

        let event = JobStateEvent::running("job-123");
        assert_eq!(event.state, JobState::Running);

        let event = JobStateEvent::completed("job-123");
        assert_eq!(event.state, JobState::Completed);

        let event = JobStateEvent::failed("job-123", "Something went wrong");
        assert_eq!(
            event.state,
            JobState::Failed("Something went wrong".to_string())
        );

        let event = JobStateEvent::cancelled("job-123");
        assert_eq!(event.state, JobState::Cancelled);
    }

    #[test]
    fn test_job_state_display() {
        assert_eq!(JobState::Queued.to_string(), "Queued");
        assert_eq!(JobState::Running.to_string(), "Running");
        assert_eq!(JobState::Completed.to_string(), "Completed");
        assert_eq!(
            JobState::Failed("error".to_string()).to_string(),
            "Failed: error"
        );
        assert_eq!(JobState::Cancelled.to_string(), "Cancelled");
    }
}
