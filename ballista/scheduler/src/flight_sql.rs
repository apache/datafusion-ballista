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

//! Binds the [`ballista_flight_sql`] frontend to this scheduler.
//!
//! The frontend does not know about `SchedulerServer`; this is the only place
//! the two meet, which is what keeps the Flight SQL code from re-acquiring the
//! coupling that made the pre-46.0.0 implementation unmaintainable.

use std::sync::Arc;

use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::{JobStatus, SuccessfulJob, job_status};
use ballista_flight_sql::backend::{QueryBackend, QueryResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;

use crate::scheduler_server::SchedulerServer;

/// Buffer for the job status stream. Statuses are consumed as fast as they
/// arrive, so this only has to absorb bursts.
const STATUS_BUFFER: usize = 16;

#[async_trait::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryBackend
    for SchedulerServer<T, U>
{
    async fn session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        let config = self.state.session_manager.produce_config();
        self.state
            .session_manager
            .create_or_update_session(session_id, &config)
            .await
    }

    async fn close_session(&self, session_id: &str) -> Result<()> {
        self.state.session_manager.remove_session(session_id).await
    }

    async fn execute(
        &self,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: LogicalPlan,
    ) -> Result<QueryResult> {
        let schema = Arc::new(plan.schema().as_arrow().clone());

        // Subscribe before submitting so no status can be missed, and follow
        // the status stream rather than polling `get_job_status`.
        let (subscriber, mut statuses) =
            tokio::sync::mpsc::channel::<JobStatus>(STATUS_BUFFER);
        let job_id = self
            .submit_job(job_name, ctx, &plan, Some(subscriber))
            .await?;

        while let Some(status) = statuses.recv().await {
            match status.status {
                Some(job_status::Status::Successful(SuccessfulJob {
                    partition_location,
                    ..
                })) => {
                    return Ok(QueryResult {
                        job_id: job_id.to_string(),
                        schema,
                        partitions: partition_location,
                    });
                }
                Some(job_status::Status::Failed(failed)) => {
                    return Err(BallistaError::General(format!(
                        "job {job_id} failed: {}",
                        failed.error
                    )));
                }
                // Queued and Running are progress reports, not outcomes.
                _ => continue,
            }
        }

        Err(BallistaError::General(format!(
            "job {job_id} ended without reporting a final status"
        )))
    }

    async fn cancel(&self, job_id: &str) -> Result<()> {
        SchedulerServer::cancel_job(self, job_id.into()).await
    }
}
