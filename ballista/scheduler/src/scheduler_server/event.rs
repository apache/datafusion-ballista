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

use crate::state::executor_manager::ExecutorReservation;
use std::fmt::{Debug, Formatter};

use datafusion::logical_expr::LogicalPlan;

use crate::state::execution_graph::RunningTaskInfo;
use ballista_core::{
    error::BallistaError,
    serde::protobuf::{execution_error, TaskStatus},
};
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
        resubmit: bool,
    },
    // For a job which failed during planning
    JobPlanningFailed {
        job_id: String,
        fail_message: String,
        queued_at: u64,
        failed_at: u64,
        error: Arc<BallistaError>,
    },
    JobFinished {
        job_id: String,
        queued_at: u64,
        completed_at: u64,
    },
    // For a job fails with its execution graph setting failed
    JobRunningFailed {
        job_id: String,
        queued_at: u64,
        failed_at: u64,
        error: Arc<execution_error::Error>,
    },
    JobUpdated(String),
    JobCancel(String),
    JobDataClean(String),
    TaskUpdating(String, Vec<TaskStatus>, bool),
    SchedulerLost(String, String, Vec<TaskStatus>),
    ReservationOffering(Vec<ExecutorReservation>),
    ExecutorLost(String, Option<String>),
    CancelTasks(Vec<RunningTaskInfo>),
    Tick,
    CircuitBreakerTripped(String),
}

impl QueryStageSchedulerEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            QueryStageSchedulerEvent::JobQueued { .. } => "JobQueued",
            QueryStageSchedulerEvent::JobSubmitted { .. } => "JobSubmitted",
            QueryStageSchedulerEvent::JobPlanningFailed { .. } => "JobPlanningFailed",
            QueryStageSchedulerEvent::JobFinished { .. } => "JobFinished",
            QueryStageSchedulerEvent::JobRunningFailed { .. } => "JobRunningFailed",
            QueryStageSchedulerEvent::JobUpdated(_) => "JobUpdated",
            QueryStageSchedulerEvent::JobCancel(_) => "JobCancel",
            QueryStageSchedulerEvent::JobDataClean(_) => "JobDataClean",
            QueryStageSchedulerEvent::TaskUpdating(_, _, _) => "TaskUpdating",
            QueryStageSchedulerEvent::SchedulerLost(_, _, _) => "SchedulerLost",
            QueryStageSchedulerEvent::ReservationOffering(_) => "ReservationOffering",
            QueryStageSchedulerEvent::ExecutorLost(_, _) => "ExecutorLost",
            QueryStageSchedulerEvent::CancelTasks(_) => "CancelTasks",
            QueryStageSchedulerEvent::Tick => "Tick",
            QueryStageSchedulerEvent::CircuitBreakerTripped(_) => "CircuitBreakerTripped",
        }
    }
}

impl Debug for QueryStageSchedulerEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryStageSchedulerEvent::JobQueued { .. } => write!(f, "JobQueued"),
            QueryStageSchedulerEvent::JobSubmitted { .. } => write!(f, "JobSubmitted"),
            QueryStageSchedulerEvent::JobPlanningFailed { .. } => {
                write!(f, "JobPlanningFailed")
            }
            QueryStageSchedulerEvent::JobFinished { .. } => write!(f, "JobFinished"),
            QueryStageSchedulerEvent::JobRunningFailed { .. } => {
                write!(f, "JobRunningFailed")
            }
            QueryStageSchedulerEvent::JobUpdated(_) => write!(f, "JobUpdated"),
            QueryStageSchedulerEvent::JobCancel(_) => write!(f, "JobCancel"),
            QueryStageSchedulerEvent::JobDataClean(_) => write!(f, "JobDataClean"),
            QueryStageSchedulerEvent::TaskUpdating(_, _, _) => write!(f, "TaskUpdating"),
            QueryStageSchedulerEvent::SchedulerLost(_, _, _) => {
                write!(f, "SchedulerLost")
            }
            QueryStageSchedulerEvent::ReservationOffering(_) => {
                write!(f, "ReservationOffering")
            }
            QueryStageSchedulerEvent::ExecutorLost(_, _) => write!(f, "ExecutorLost"),
            QueryStageSchedulerEvent::CancelTasks(_) => write!(f, "CancelTasks"),
            QueryStageSchedulerEvent::Tick => write!(f, "Tick"),
            QueryStageSchedulerEvent::CircuitBreakerTripped(_) => {
                write!(f, "CircuitBreakerTripped")
            }
        }
    }
}
