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

use datafusion::logical_plan::LogicalPlan;

use datafusion::prelude::SessionContext;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum SchedulerServerEvent {
    /// Offer a list of executor reservations (representing executor task slots available for scheduling)
    Offer(Vec<ExecutorReservation>),
}

#[derive(Clone)]
pub enum QueryStageSchedulerEvent {
    JobQueued {
        job_id: String,
        session_ctx: Arc<SessionContext>,
        plan: Box<LogicalPlan>,
    },
    JobSubmitted(String),
    JobFinished(String),
    JobFailed(String, String),
}
