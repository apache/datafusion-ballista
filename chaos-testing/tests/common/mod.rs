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

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::config::BALLISTA_ADAPTIVE_PLANNER_ENABLED;
use chaos_testing::budget::FaultBudget;
use chaos_testing::cluster::TestCluster;
use chaos_testing::fixture::Fixture;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

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
        let _ = env_logger::builder().is_test(true).try_init();

        let cluster = TestCluster::builder()
            .executors(executors)
            .executor_timeout_seconds(executor_timeout_seconds)
            .start()
            .await
            .expect("cluster must start");

        let fixture = Fixture::write(cluster.shared_dir())
            .await
            .expect("fixture must be written");

        let config = SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, aqe);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_scalar_functions(vec![
                chaos_testing::udf::chaos_fail_udf(),
                chaos_testing::udf::chaos_delay_udf(),
            ])
            .build();

        let ctx = SessionContext::remote_with_state(&cluster.scheduler_url(), state)
            .await
            .expect("client must connect to the scheduler");

        for stmt in fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }

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
        let df = self.ctx.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;
        Ok(pretty_format_batches(&batches)
            .map_err(|e| e.to_string())?
            .to_string())
    }

    /// The expected answer, computed by plain local DataFusion.
    pub async fn local_baseline(&self) -> String {
        let ctx = SessionContext::new();
        for stmt in self.fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }
        let batches = ctx
            .sql(Fixture::baseline_query())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    /// A clone of the session context, for running a query concurrently with a fault.
    pub fn clone_ctx(&self) -> SessionContext {
        self.ctx.clone()
    }
}
