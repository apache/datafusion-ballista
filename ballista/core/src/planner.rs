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

use crate::config::BallistaConfig;
use crate::execution_plans::{DistributedExplainAnalyzeExec, DistributedQueryExec};
use crate::serde::BallistaLogicalExtensionCodec;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use std::marker::PhantomData;
use std::sync::Arc;

/// [BallistaQueryPlanner] planner takes logical plan
/// and executes it remotely on on scheduler.
///
/// Under the hood it will create [DistributedQueryExec]
/// which will establish gprc connection with the scheduler.
///
pub struct BallistaQueryPlanner<T: AsLogicalPlan> {
    scheduler_url: String,
    config: BallistaConfig,
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    local_planner: DefaultPhysicalPlanner,
    _plan_type: PhantomData<T>,
}

impl<T: AsLogicalPlan> std::fmt::Debug for BallistaQueryPlanner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BallistaQueryPlanner")
            .field("scheduler_url", &self.scheduler_url)
            .field("config", &self.config)
            .field("extension_codec", &self.extension_codec)
            .field("_plan_type", &self._plan_type)
            .finish()
    }
}

impl<T: 'static + AsLogicalPlan> BallistaQueryPlanner<T> {
    /// Creates a new Ballista query planner with the specified scheduler URL and configuration.
    pub fn new(scheduler_url: String, config: BallistaConfig) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec: Arc::new(BallistaLogicalExtensionCodec::default()),
            local_planner: DefaultPhysicalPlanner::default(),
            _plan_type: PhantomData,
        }
    }

    /// Creates a new Ballista query planner with a custom extension codec.
    pub fn with_extension(
        scheduler_url: String,
        config: BallistaConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            local_planner: DefaultPhysicalPlanner::default(),
            _plan_type: PhantomData,
        }
    }

    /// Creates a new Ballista query planner with a custom local physical planner.
    pub fn with_local_planner(
        scheduler_url: String,
        config: BallistaConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        local_planner: DefaultPhysicalPlanner,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            _plan_type: PhantomData,
            local_planner,
        }
    }
}

#[async_trait::async_trait]
impl<T: 'static + AsLogicalPlan> QueryPlanner for BallistaQueryPlanner<T> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        log::debug!("create_physical_plan - plan: {:?}", logical_plan);
        // we inspect if plan scans only tables located in information_schema,
        // which describe the catalog of this context,
        // if that is the case, we run that plan
        // on this same context, not on cluster
        if scans_only_information_schema(logical_plan)? {
            log::debug!("create_physical_plan - plan can be executed locally");

            self.local_planner
                .create_physical_plan(logical_plan, session_state)
                .await
        } else {
            match logical_plan {
                LogicalPlan::EmptyRelation(_) => {
                    log::debug!("create_physical_plan - handling empty exec");
                    Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
                }
                LogicalPlan::Analyze(analyze) => {
                    log::debug!(
                        "create_physical_plan - handling explain analyze statement"
                    );
                    let inner_plan = analyze.input.as_ref().clone();
                    let distributed_query_exec =
                        Arc::new(DistributedQueryExec::<T>::with_extension(
                            self.scheduler_url.clone(),
                            self.config.clone(),
                            inner_plan,
                            self.extension_codec.clone(),
                            session_state.session_id().to_string(),
                        ));

                    Ok(Arc::new(DistributedExplainAnalyzeExec::new(
                        distributed_query_exec,
                        self.scheduler_url.clone(),
                        Arc::clone(analyze.schema.inner()),
                        analyze.verbose,
                    )))
                }
                _ => {
                    log::debug!("create_physical_plan - handling general statement");

                    Ok(Arc::new(DistributedQueryExec::<T>::with_extension(
                        self.scheduler_url.clone(),
                        self.config.clone(),
                        logical_plan.clone(),
                        self.extension_codec.clone(),
                        session_state.session_id().to_string(),
                    )))
                }
            }
        }
    }
}

/// Returns `true` if every table `plan` reads, subqueries included, is in
/// `information_schema`, and `false` if none is.
///
/// Those tables describe the catalog of the session that planned `plan`, which
/// no other node shares, and their scans cannot be serialized, so a plan
/// reading only them should run where it was planned. A plan that also reads
/// other tables would have to run there as well, without the cluster, so it is
/// refused with an error instead.
pub fn scans_only_information_schema(
    plan: &LogicalPlan,
) -> Result<bool, DataFusionError> {
    let (mut information_schema, mut other) = (false, false);
    // Subqueries scan tables too, and `apply` does not descend into them.
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(TableScan { table_name, .. }) = node {
            if table_name.schema() == Some("information_schema") {
                information_schema = true;
            } else {
                other = true;
            }
        }
        Ok(if information_schema && other {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;

    if information_schema && other {
        return Err(DataFusionError::NotImplemented(
            "Ballista cannot run a query that reads information_schema together \
             with other tables. Query information_schema on its own."
                .to_string(),
        ));
    }
    Ok(information_schema)
}

#[cfg(test)]
mod test {
    use datafusion::{
        error::Result,
        execution::{
            SessionStateBuilder, context::QueryPlanner, runtime_env::RuntimeEnvBuilder,
        },
        logical_expr::LogicalPlan,
        physical_plan::ExecutionPlan,
        prelude::{SessionConfig, SessionContext},
    };
    use datafusion_proto::protobuf::LogicalPlanNode;

    use super::{BallistaQueryPlanner, scans_only_information_schema};
    use crate::config::BallistaConfig;
    use crate::execution_plans::{DistributedExplainAnalyzeExec, DistributedQueryExec};

    fn context() -> SessionContext {
        let runtime_environment = RuntimeEnvBuilder::new().build().unwrap();

        let session_config = SessionConfig::new().with_information_schema(true);

        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime_environment.into())
            .with_default_features()
            .build();

        SessionContext::new_with_state(state)
    }

    #[tokio::test]
    async fn should_detect_show_table_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SHOW TABLES").await?;

        assert!(scans_only_information_schema(df.logical_plan())?);

        Ok(())
    }

    #[tokio::test]
    async fn should_detect_select_from_information_schema_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SELECT * FROM information_schema.df_settings WHERE NAME LIKE 'ballista%'").await?;

        assert!(scans_only_information_schema(df.logical_plan())?);

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_local_table() -> Result<()> {
        let ctx = context();
        ctx.sql("CREATE TABLE tt (c0 INT, c1 INT)")
            .await?
            .show()
            .await?;
        let df = ctx.sql("SELECT * FROM tt").await?;

        assert!(!scans_only_information_schema(df.logical_plan())?);

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_external_table() -> Result<()> {
        let ctx = context();
        ctx.register_csv("tt", "tests/customer.csv", Default::default())
            .await?;
        let df = ctx.sql("SELECT * FROM tt").await?;

        assert!(!scans_only_information_schema(df.logical_plan())?);

        Ok(())
    }

    #[tokio::test]
    async fn should_reject_information_schema_with_other_tables() -> Result<()> {
        let ctx = context();
        ctx.sql("CREATE TABLE big (name VARCHAR)")
            .await?
            .show()
            .await?;

        for sql in [
            "SELECT table_name FROM information_schema.tables \
             WHERE table_name IN (SELECT name FROM big)",
            "SELECT name FROM big \
             WHERE name IN (SELECT table_name FROM information_schema.tables)",
            "SELECT t.table_name FROM information_schema.tables t \
             JOIN big b ON t.table_name = b.name",
        ] {
            // Unoptimized, so subqueries are still expressions rather than the
            // joins the optimizer would decorrelate them into.
            let plan = ctx.state().create_logical_plan(sql).await?;

            assert!(scans_only_information_schema(&plan).is_err(), "{sql}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_create_distributed_explain_analyze_exec() -> Result<()> {
        let ctx = context();
        ctx.sql("CREATE TABLE tt (c0 INT)").await?.show().await?;
        let analyze_df = ctx.sql("EXPLAIN ANALYZE SELECT * FROM tt").await?;
        let planner = BallistaQueryPlanner::<LogicalPlanNode>::new(
            "http://localhost:50050".to_string(),
            BallistaConfig::default(),
        );
        let plan = planner
            .create_physical_plan(analyze_df.logical_plan(), &ctx.state())
            .await?;

        assert!(matches!(analyze_df.logical_plan(), LogicalPlan::Analyze(_)));
        let explain = plan
            .downcast_ref::<DistributedExplainAnalyzeExec<LogicalPlanNode>>()
            .unwrap();
        assert!(
            explain.children()[0]
                .downcast_ref::<DistributedQueryExec<LogicalPlanNode>>()
                .is_some()
        );
        Ok(())
    }
}
