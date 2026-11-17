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

use chaos_testing::budget::FaultBudget;
use chaos_testing::cluster::TestCluster;
use chaos_testing::fetch::ResultFetch;
use chaos_testing::fixture::{Fixture, query};
use datafusion::prelude::SessionContext;

/// One cluster plus its fixture, wired for a single scenario.
pub struct ChaosRun {
    pub cluster: TestCluster,
    pub fixture: Fixture,
    ctx: SessionContext,
}

impl ChaosRun {
    pub async fn start(aqe: bool, executors: usize) -> Self {
        Self::start_with(aqe, executors, 5).await
    }

    /// `executor_timeout_seconds` selects which HA mechanism a kill surfaces
    /// through: short biases toward ExecutorLost (heartbeat expiry), long biases
    /// toward FetchPartitionError (a downstream fetch from a dead executor).
    pub async fn start_with(
        aqe: bool,
        executors: usize,
        executor_timeout_seconds: u64,
    ) -> Self {
        Self::start_with_concurrent_tasks(aqe, executors, executor_timeout_seconds, 4)
            .await
    }

    pub async fn start_with_concurrent_tasks(
        aqe: bool,
        executors: usize,
        executor_timeout_seconds: u64,
        concurrent_tasks: usize,
    ) -> Self {
        let cluster = TestCluster::builder()
            .executors(executors)
            .executor_timeout_seconds(executor_timeout_seconds)
            .concurrent_tasks(concurrent_tasks)
            .start()
            .await
            .expect("cluster must start");

        Self::with_cluster(cluster, aqe).await
    }

    /// Like [`start`](Self::start), with clients fetching results in `mode`.
    pub async fn start_with_result_fetch(
        aqe: bool,
        executors: usize,
        mode: ResultFetch,
    ) -> Self {
        let cluster = TestCluster::builder()
            .executors(executors)
            .executor_timeout_seconds(5)
            .result_fetch(mode)
            .start()
            .await
            .expect("cluster must start");

        Self::with_cluster(cluster, aqe).await
    }

    async fn with_cluster(cluster: TestCluster, aqe: bool) -> Self {
        let _ = env_logger::builder().is_test(true).try_init();

        let fixture = Fixture::write(cluster.shared_dir())
            .await
            .expect("fixture must be written");
        let ctx = fixture.connect(&cluster.scheduler_url(), aqe).await;

        Self {
            cluster,
            fixture,
            ctx,
        }
    }

    /// A fault budget in the cluster's shared directory, visible to every executor.
    pub fn budget(&self, name: &str, tokens: usize) -> FaultBudget {
        let dir = self.cluster.shared_dir().join(format!("budget-{name}"));
        FaultBudget::create(&dir, tokens).expect("budget must be created")
    }

    /// Run a query on the cluster, returning the formatted result.
    pub async fn sql(&self, sql: &str) -> Result<String, String> {
        query(&self.ctx, sql).await
    }

    /// The expected answer, computed by plain local DataFusion.
    pub async fn local_baseline(&self) -> String {
        self.local_sql(Fixture::baseline_query()).await
    }

    pub async fn local_sql(&self, sql: &str) -> String {
        self.fixture.local_sql(sql).await
    }

    /// A clone of the session context, for running a query concurrently with a
    /// fault: the query is submitted in a spawned task while the main task
    /// polls the scheduler and kills an executor.
    pub fn clone_ctx(&self) -> SessionContext {
        self.ctx.clone()
    }
}
