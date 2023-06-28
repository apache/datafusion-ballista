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

#[derive(Clone)]
pub enum QueryStageSchedulerEvent {
    JobQueued {
        job_id: String,
        job_name: String,
        session_ctx: Arc<SessionContext>,
        plan: Box<LogicalPlan>,
        queued_at: u64,
    },
    JobSubmitted {
        job_id: String,
        queued_at: u64,
        submitted_at: u64,
    },
    // For a job which failed during planning
    JobPlanningFailed {
        job_id: String,
        fail_message: String,
        queued_at: u64,
        failed_at: u64,
    },
    JobFinished {
        job_id: String,
        queued_at: u64,
        completed_at: u64,
    },
    // For a job fails with its execution graph setting failed
    JobRunningFailed {
        job_id: String,
        fail_message: String,
        queued_at: u64,
        failed_at: u64,
    },
    JobUpdated(String),
    JobCancel(String),
    JobDataClean(String),
    TaskUpdating(String, Vec<TaskStatus>),
    ReviveOffers,
    ExecutorLost(String, Option<String>),
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
            QueryStageSchedulerEvent::ExecutorLost(job_id, reason) => {
                write!(f, "ExecutorLost : job_id={job_id}, reason:[{reason:?}].")
            }
            QueryStageSchedulerEvent::CancelTasks(status) => {
                write!(f, "CancelTasks : status:[{status:?}].")
            }
        }
    }
}
