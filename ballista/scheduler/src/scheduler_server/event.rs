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

use std::fmt::{Debug, Formatter};

use datafusion::logical_expr::LogicalPlan;

use crate::state::execution_graph::RunningTaskInfo;
use ballista_core::serde::protobuf::TaskStatus;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Events that drive the query stage scheduler state machine.
#[derive(Clone)]
pub enum QueryStageSchedulerEvent {
    /// A new job has been queued for execution.
    JobQueued {
        /// Unique job identifier.
        job_id: String,
        /// Human-readable job name.
        job_name: String,
        /// Session context for the job.
        session_ctx: Arc<SessionContext>,
        /// Logical plan to execute.
        plan: Box<LogicalPlan>,
        /// Timestamp when the job was queued.
        queued_at: u64,
    },
    /// A job has been submitted for execution.
    JobSubmitted {
        /// Unique job identifier.
        job_id: String,
        /// Timestamp when the job was queued.
        queued_at: u64,
        /// Timestamp when the job was submitted.
        submitted_at: u64,
    },
    /// A job failed during the planning phase.
    JobPlanningFailed {
        /// Unique job identifier.
        job_id: String,
        /// Error message describing the failure.
        fail_message: String,
        /// Timestamp when the job was queued.
        queued_at: u64,
        /// Timestamp when the job failed.
        failed_at: u64,
    },
    /// A job has completed successfully.
    JobFinished {
        /// Unique job identifier.
        job_id: String,
        /// Timestamp when the job was queued.
        queued_at: u64,
        /// Timestamp when the job completed.
        completed_at: u64,
    },
    /// A job failed during execution.
    JobRunningFailed {
        /// Unique job identifier.
        job_id: String,
        /// Error message describing the failure.
        fail_message: String,
        /// Timestamp when the job was queued.
        queued_at: u64,
        /// Timestamp when the job failed.
        failed_at: u64,
    },
    /// A job's execution graph has been updated.
    JobUpdated(String),
    /// Request to cancel a job.
    JobCancel(String),
    /// Request to clean up job data.
    JobDataClean(String),
    /// Task status updates received.
    TaskUpdating(String, Vec<TaskStatus>),
    /// Signal to revive task offers.
    ReviveOffers,
    /// An executor has been lost.
    ExecutorLost(String, Option<String>),
    /// Request to cancel specific running tasks.
    CancelTasks(Vec<RunningTaskInfo>),
}

impl Debug for QueryStageSchedulerEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryStageSchedulerEvent::JobQueued {
                job_id, job_name, ..
            } => {
                write!(f, "JobQueued : job_id={job_id}, job_name={job_name}.")
            }
            QueryStageSchedulerEvent::JobSubmitted { job_id, .. } => {
                write!(f, "JobSubmitted : job_id={job_id}.")
            }
            QueryStageSchedulerEvent::JobPlanningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                write!(
                    f,
                    "JobPlanningFailed : job_id={job_id}, fail_message={fail_message}, queued_at={queued_at}, failed_at={failed_at}.",
                )
            }
            QueryStageSchedulerEvent::JobFinished {
                job_id,
                queued_at,
                completed_at,
            } => {
                write!(
                    f,
                    "JobFinished : job_id={job_id}, queued_at={queued_at}, completed_at={completed_at}.",
                )
            }
            QueryStageSchedulerEvent::JobRunningFailed {
                job_id,
                fail_message,
                queued_at,
                failed_at,
            } => {
                write!(
                    f,
                    "JobRunningFailed : job_id={job_id}, fail_message={fail_message}, queued_at={queued_at}, failed_at={failed_at}.",
                )
            }
            QueryStageSchedulerEvent::JobUpdated(job_id) => {
                write!(f, "JobUpdated : job_id={job_id}.")
            }
            QueryStageSchedulerEvent::JobCancel(job_id) => {
                write!(f, "JobCancel : job_id={job_id}.")
            }
            QueryStageSchedulerEvent::JobDataClean(job_id) => {
                write!(f, "JobDataClean : job_id={job_id}.")
            }
            QueryStageSchedulerEvent::TaskUpdating(job_id, status) => {
                write!(f, "TaskUpdating : job_id={job_id}, status:[{status:?}].")
            }
            QueryStageSchedulerEvent::ReviveOffers => {
                write!(f, "ReviveOffers.")
            }
            QueryStageSchedulerEvent::ExecutorLost(executor_id, reason) => {
                write!(
                    f,
                    "ExecutorLost : executor_id={executor_id}, reason:[{reason:?}]."
                )
            }
            QueryStageSchedulerEvent::CancelTasks(status) => {
                write!(f, "CancelTasks : status:[{status:?}].")
            }
        }
    }
}
