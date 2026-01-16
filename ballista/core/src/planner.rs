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
use crate::execution_plans::DistributedQueryExec;
use crate::serde::BallistaLogicalExtensionCodec;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::{TreeNode, TreeNodeVisitor};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{QueryPlanner, SessionState};
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

#[async_trait]
impl<T: 'static + AsLogicalPlan> QueryPlanner for BallistaQueryPlanner<T> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        log::debug!("create_physical_plan - plan: {:?}", logical_plan);
        // we inspect if plan scans local tables only,
        // like tables located in information_schema,
        // if that is the case, we run that plan
        // on this same context, not on cluster
        let mut local_run = LocalRun::default();
        let _ = logical_plan.visit(&mut local_run);

        if local_run.can_be_local {
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

/// A Visitor which detect if query is using local tables,
/// such as tables located in `information_schema` and returns true
/// only if all scans are in from local tables
#[derive(Debug, Default)]
struct LocalRun {
    can_be_local: bool,
}

impl<'n> TreeNodeVisitor<'n> for LocalRun {
    type Node = LogicalPlan;

    fn f_down(
        &mut self,
        node: &'n Self::Node,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        match node {
            LogicalPlan::TableScan(TableScan { table_name, .. }) => match table_name {
                datafusion::sql::TableReference::Partial { schema, .. }
                | datafusion::sql::TableReference::Full { schema, .. }
                    if schema.as_ref() == "information_schema" =>
                {
                    self.can_be_local = true;
                    Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
                }
                _ => {
                    self.can_be_local = false;
                    Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop)
                }
            },
            _ => Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue),
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion::{
        common::tree_node::TreeNode,
        error::Result,
        execution::{SessionStateBuilder, runtime_env::RuntimeEnvBuilder},
        prelude::{SessionConfig, SessionContext},
    };

    use super::LocalRun;

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
        let lp = df.logical_plan();
        let mut local_run = LocalRun::default();

        lp.visit(&mut local_run).unwrap();

        assert!(local_run.can_be_local);

        Ok(())
    }

    #[tokio::test]
    async fn should_detect_select_from_information_schema_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SELECT * FROM information_schema.df_settings WHERE NAME LIKE 'ballista%'").await?;
        let lp = df.logical_plan();
        let mut local_run = LocalRun::default();

        lp.visit(&mut local_run).unwrap();

        assert!(local_run.can_be_local);

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
        let lp = df.logical_plan();
        let mut local_run = LocalRun::default();

        lp.visit(&mut local_run).unwrap();

        assert!(!local_run.can_be_local);

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_external_table() -> Result<()> {
        let ctx = context();
        ctx.register_csv("tt", "tests/customer.csv", Default::default())
            .await?;
        let df = ctx.sql("SELECT * FROM tt").await?;
        let lp = df.logical_plan();
        let mut local_run = LocalRun::default();

        lp.visit(&mut local_run).unwrap();

        assert!(!local_run.can_be_local);

        Ok(())
    }
}
