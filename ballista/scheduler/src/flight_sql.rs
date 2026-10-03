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
//! the two meet.

use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::protobuf::{JobStatus, SuccessfulJob, job_status};
use ballista_flight_sql::backend::{QueryBackend, QueryResult};
use datafusion::arrow::datatypes::SchemaRef;
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
        // Subscribe before submitting so no status can be missed, and follow
        // the status stream rather than polling `get_job_status`.
        let (subscriber, mut statuses) =
            tokio::sync::mpsc::channel::<JobStatus>(STATUS_BUFFER);
        let job_id = self
            .submit_job(job_name, ctx.clone(), &plan, Some(subscriber))
            .await?;

        while let Some(status) = statuses.recv().await {
            match status.status {
                Some(job_status::Status::Successful(SuccessfulJob {
                    partition_location,
                    ..
                })) => {
                    let schema = self.result_schema(&job_id, &ctx, &plan).await?;
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
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerServer<T, U> {
    /// The schema of a finished job's result files.
    ///
    /// This has to be the schema the shuffle files actually carry, which is
    /// the physical plan's rather than the logical plan's: the two disagree
    /// about nullability often enough to matter (`version()` is nullable
    /// logically and not-null in the data; a list literal goes the other way),
    /// and a Flight SQL client that is promised one schema and handed another,
    /// ADBC among them, rejects the result outright.
    ///
    /// The job's final stage is the plan that wrote those files, so its schema
    /// is read from there. Only if the graph is no longer available is the plan
    /// planned again, which for a listing table means listing its files again.
    async fn result_schema(
        &self,
        job_id: &JobId,
        ctx: &SessionContext,
        plan: &LogicalPlan,
    ) -> Result<SchemaRef> {
        let graph = self
            .state
            .task_manager
            .get_job_execution_graph(job_id)
            .await?;
        let final_stage_schema = graph.and_then(|graph| {
            graph
                .stages()
                .values()
                .find(|stage| stage.output_links().is_empty())
                .map(|stage| stage.plan().schema())
        });

        match final_stage_schema {
            Some(schema) => Ok(schema),
            None => Ok(ctx.state().create_physical_plan(plan).await?.schema()),
        }
    }
}
