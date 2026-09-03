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

//! The transport-neutral contract between the Flight SQL frontend and whatever
//! actually runs the query.
//!
//! The frontend never touches scheduler internals. It plans SQL against a
//! [`SessionContext`] the backend hands it, submits the resulting
//! [`LogicalPlan`], and turns the returned partition locations into Flight
//! endpoints. `ballista-scheduler` provides the only implementation today
//! (behind its `flight-sql` feature); embedders can provide their own.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use ballista_core::error::Result;
use ballista_core::serde::protobuf::PartitionLocation;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

/// A distributed query that ran to completion.
///
/// The partitions are shuffle outputs still sitting on the executors that
/// produced them; the frontend turns each one into a `FlightEndpoint` whose
/// ticket the client redeems with `DoGet`.
#[derive(Debug)]
pub struct QueryResult {
    /// Scheduler-assigned id of the job that produced the result. Used to
    /// cancel the query and to release its shuffle data afterwards.
    pub job_id: String,
    /// Schema of the result, taken from the submitted plan.
    pub schema: SchemaRef,
    /// One entry per output partition, in the order the scheduler reported.
    pub partitions: Vec<PartitionLocation>,
}

/// Submits queries on behalf of a protocol frontend.
///
/// Implementations own session lifetime and job submission. They are expected
/// to be cheap to clone or to be held behind an `Arc`.
#[async_trait]
pub trait QueryBackend: Send + Sync + 'static {
    /// Returns the session context for `session_id`, creating it if this is
    /// the first time the frontend has seen that id.
    ///
    /// The context carries the catalog that SQL is planned against, so
    /// whatever tables an embedder registers through its `SessionBuilder` are
    /// visible to Flight SQL clients.
    async fn session(&self, session_id: &str) -> Result<Arc<SessionContext>>;

    /// Discards the session and any state attached to it.
    async fn close_session(&self, session_id: &str) -> Result<()>;

    /// Submits `plan` for distributed execution and resolves once the job
    /// reaches a terminal state.
    ///
    /// Returns `Err` if the job failed or was cancelled.
    async fn execute(
        &self,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: LogicalPlan,
    ) -> Result<QueryResult>;

    /// Requests cancellation of a running job. Cancellation is asynchronous;
    /// returning `Ok` means the request was accepted, not that the job stopped.
    ///
    /// Shuffle data left behind by a completed job is not the frontend's to
    /// release: a client may redeem its tickets at any point before they
    /// expire, so cleanup is left to the backend's own retention policy (in
    /// Ballista, `finished_job_data_clean_up_interval_seconds`).
    async fn cancel(&self, job_id: &str) -> Result<()>;
}
