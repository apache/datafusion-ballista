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
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{Expr, LogicalPlan, Subquery, TableScan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use futures::future::BoxFuture;
use std::collections::HashMap;
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

                    // Execute any uncorrelated scalar subqueries first and
                    // substitute their values, so the plan sent to the scheduler
                    // is subquery-free. See #1910.
                    let logical_plan = self
                        .materialize_scalar_subqueries(
                            logical_plan.clone(),
                            session_state,
                        )
                        .await?;

                    Ok(Arc::new(DistributedQueryExec::<T>::with_extension(
                        self.scheduler_url.clone(),
                        self.config.clone(),
                        logical_plan,
                        self.extension_codec.clone(),
                        session_state.session_id().to_string(),
                    )))
                }
            }
        }
    }
}

impl<T: 'static + AsLogicalPlan> BallistaQueryPlanner<T> {
    /// Execute every uncorrelated scalar subquery in `plan` and substitute its
    /// single value as a literal, returning a subquery-free plan.
    ///
    /// DataFusion 54 plans an uncorrelated scalar subquery as a physical
    /// `ScalarSubqueryExec` whose `ScalarSubqueryExpr` cannot be deserialized
    /// once Ballista splits the plan into stages. Rather than decorrelating to a
    /// join (correct but adds join work for what is logically a constant), the
    /// subquery is run first and its value is inlined. See #1910.
    fn materialize_scalar_subqueries<'a>(
        &'a self,
        plan: LogicalPlan,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<LogicalPlan, DataFusionError>> {
        Box::pin(async move {
            let subqueries = collect_uncorrelated_scalar_subqueries(&plan)?;
            if subqueries.is_empty() {
                return Ok(plan);
            }

            // Map each subquery (keyed by the identity of its plan Arc) to its
            // computed value.
            let mut values: HashMap<usize, ScalarValue> = HashMap::new();
            for subquery in subqueries {
                let key = Arc::as_ptr(&subquery.subquery) as usize;
                if values.contains_key(&key) {
                    continue;
                }
                // A subquery may itself contain nested scalar subqueries; inline
                // those before running it.
                let inner = self
                    .materialize_scalar_subqueries(
                        subquery.subquery.as_ref().clone(),
                        session_state,
                    )
                    .await?;
                let value = self.execute_scalar_subquery(inner, session_state).await?;
                values.insert(key, value);
            }

            substitute_scalar_subqueries(plan, &values)
        })
    }

    /// Run a subquery plan as its own distributed query and reduce its result to
    /// a single [`ScalarValue`]: 0 rows -> a typed null, 1 row -> the value,
    /// more than one row -> an error.
    async fn execute_scalar_subquery(
        &self,
        plan: LogicalPlan,
        session_state: &SessionState,
    ) -> Result<ScalarValue, DataFusionError> {
        let data_type = plan.schema().field(0).data_type().clone();

        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(DistributedQueryExec::<T>::with_extension(
                self.scheduler_url.clone(),
                self.config.clone(),
                plan,
                self.extension_codec.clone(),
                session_state.session_id().to_string(),
            ));

        let batches =
            datafusion::physical_plan::collect(exec, session_state.task_ctx()).await?;
        let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if num_rows > 1 {
            return Err(DataFusionError::Execution(
                "scalar subquery returned more than one row".to_string(),
            ));
        }
        match batches.into_iter().find(|b| b.num_rows() == 1) {
            Some(batch) => ScalarValue::try_from_array(batch.column(0), 0),
            None => ScalarValue::try_from(&data_type),
        }
    }
}

/// Collect uncorrelated scalar subqueries from a plan's expressions, without
/// descending into the subquery plans themselves (those are handled by
/// recursion in [`BallistaQueryPlanner::materialize_scalar_subqueries`]).
fn collect_uncorrelated_scalar_subqueries(
    plan: &LogicalPlan,
) -> Result<Vec<Subquery>, DataFusionError> {
    let mut found = Vec::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        for expr in node.expressions() {
            expr.apply(|e| {
                if let Expr::ScalarSubquery(subquery) = e
                    && subquery.outer_ref_columns.is_empty()
                {
                    found.push(subquery.clone());
                    // Do not descend into the subquery's own plan here.
                    return Ok(TreeNodeRecursion::Jump);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        stack.extend(node.inputs());
    }
    Ok(found)
}

/// Replace each uncorrelated scalar subquery (matched by the identity of its
/// plan Arc) with its precomputed literal value.
fn substitute_scalar_subqueries(
    plan: LogicalPlan,
    values: &HashMap<usize, ScalarValue>,
) -> Result<LogicalPlan, DataFusionError> {
    plan.transform_up(|node| {
        node.map_expressions(|expr| {
            expr.transform_up(|e| {
                if let Expr::ScalarSubquery(subquery) = &e {
                    let key = Arc::as_ptr(&subquery.subquery) as usize;
                    if let Some(value) = values.get(&key) {
                        return Ok(Transformed::yes(Expr::Literal(value.clone(), None)));
                    }
                }
                Ok(Transformed::no(e))
            })
        })
    })
    .map(|transformed| transformed.data)
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
        execution::{
            SessionStateBuilder, context::QueryPlanner, runtime_env::RuntimeEnvBuilder,
        },
        logical_expr::LogicalPlan,
        physical_plan::ExecutionPlan,
        prelude::{SessionConfig, SessionContext},
    };
    use datafusion_proto::protobuf::LogicalPlanNode;

    use super::{BallistaQueryPlanner, LocalRun};
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

    fn one_row_subquery() -> std::sync::Arc<LogicalPlan> {
        use datafusion::logical_expr::{LogicalPlanBuilder, lit};
        std::sync::Arc::new(
            LogicalPlanBuilder::empty(true)
                .project(vec![lit(42i64).alias("m")])
                .unwrap()
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn collect_finds_uncorrelated_scalar_subqueries() {
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::logical_expr::expr_fn::scalar_subquery;

        let subquery = one_row_subquery();
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_subquery(std::sync::Arc::clone(&subquery)).alias("x"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let found = super::collect_uncorrelated_scalar_subqueries(&plan).unwrap();
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn collect_skips_correlated_scalar_subqueries() {
        use datafusion::logical_expr::{Expr, LogicalPlanBuilder, Subquery, col};

        let correlated = Expr::ScalarSubquery(Subquery {
            subquery: one_row_subquery(),
            outer_ref_columns: vec![col("y")],
            spans: Default::default(),
        });
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![correlated.alias("x")])
            .unwrap()
            .build()
            .unwrap();

        let found = super::collect_uncorrelated_scalar_subqueries(&plan).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn substitute_replaces_scalar_subqueries_with_literals() {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::logical_expr::expr_fn::scalar_subquery;
        use std::collections::HashMap;

        let subquery = one_row_subquery();
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_subquery(std::sync::Arc::clone(&subquery)).alias("x"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let mut values = HashMap::new();
        values.insert(
            std::sync::Arc::as_ptr(&subquery) as usize,
            ScalarValue::Int64(Some(42)),
        );

        let rewritten = super::substitute_scalar_subqueries(plan, &values).unwrap();
        // No scalar subquery should remain after substitution.
        let remaining =
            super::collect_uncorrelated_scalar_subqueries(&rewritten).unwrap();
        assert!(remaining.is_empty());
    }
}
