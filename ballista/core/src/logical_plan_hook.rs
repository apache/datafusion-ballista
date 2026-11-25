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

//! Hooks that rewrite a query's logical plan each time it's submitted.

use std::fmt::Debug;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;

use crate::extension::SessionConfigExt;

/// Rewrites a query's logical plan each time it's submitted for distributed
/// execution.
///
/// A reused `DataFrame` or Flight SQL prepared statement runs it again on every
/// run, so it is the place to decide what should be decided when a query runs
/// rather than when it's planned, such as the version of a table to read.
/// DataFusion decides that in `TableProvider::scan`, but a Ballista job is
/// physically planned on the scheduler, possibly more than once.
///
/// Register hooks with [`SessionConfigExt::with_ballista_logical_plan_hook`].
/// They run in the order they were added.
///
/// # When it runs
///
/// Once per run, on the node that planned the query, before the plan is
/// encoded or physically planned:
///
/// - On a client, in [`BallistaQueryPlanner`], after DataFusion has optimized
///   the plan.
/// - On the scheduler, for a Flight SQL client, before the scheduler optimizes
///   the plan.
///
/// Hooks don't run for:
///
/// - A plan the client runs itself, such as `SHOW` or a query of
///   `information_schema` alone, or a statement Flight SQL runs on the
///   scheduler itself, such as DDL, `SHOW` or a catalog query.
/// - A session whose query planner is replaced with
///   [`SessionConfigExt::with_ballista_query_planner`], unless that planner
///   delegates to [`BallistaQueryPlanner`].
/// - A physical plan submitted with [`execute_physical_plan`].
///
/// # Contract
///
/// The returned plan must have the same schema as `plan`. A client's plan is
/// already optimized, so a `TableScan` may hold pushed-down filters, a
/// projection and a fetch, and a filter its source accepted as
/// [`TableProviderFilterPushDown::Exact`] is applied nowhere else. A hook that
/// replaces a scan's source must replace it with one that applies them the
/// same way, or the query returns wrong rows.
///
/// [`BallistaQueryPlanner`]: crate::planner::BallistaQueryPlanner
/// [`execute_physical_plan`]: crate::execution_plans::execute_physical_plan
/// [`TableProviderFilterPushDown::Exact`]: datafusion::logical_expr::TableProviderFilterPushDown::Exact
#[async_trait]
pub trait LogicalPlanHook: Debug + Send + Sync {
    /// Returns `plan` rewritten for this run. An error fails the query with it.
    async fn before_submit(
        &self,
        plan: LogicalPlan,
        session: &dyn Session,
    ) -> Result<LogicalPlan>;
}

/// Runs the [`LogicalPlanHook`]s registered in `session`'s config on `plan`, in
/// the order they were added.
pub async fn run_logical_plan_hooks(
    mut plan: LogicalPlan,
    session: &dyn Session,
) -> Result<LogicalPlan> {
    for hook in session.config().ballista_logical_plan_hooks() {
        plan = hook.before_submit(plan, session).await?;
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::SessionConfig;

    use super::*;
    use crate::test_utils::logical_plan_hook::AlwaysFail;

    /// Limits the plan to `self.0` rows, so the plan shows which hooks ran and
    /// in what order.
    #[derive(Debug)]
    struct Limit(usize);

    #[async_trait]
    impl LogicalPlanHook for Limit {
        async fn before_submit(
            &self,
            plan: LogicalPlan,
            _session: &dyn Session,
        ) -> Result<LogicalPlan> {
            LogicalPlanBuilder::from(plan)
                .limit(0, Some(self.0))?
                .build()
        }
    }

    /// Adds `hooks` to a session's config in order, then runs them on an empty
    /// plan and returns the result.
    async fn run_hooks(hooks: Vec<Arc<dyn LogicalPlanHook>>) -> Result<String> {
        let config = hooks
            .into_iter()
            .fold(SessionConfig::new(), |config, hook| {
                config.with_ballista_logical_plan_hook(hook)
            });
        let state = SessionStateBuilder::new().with_config(config).build();
        let plan = LogicalPlanBuilder::empty(true).build()?;
        let plan = run_logical_plan_hooks(plan, &state).await?;
        Ok(plan.display_indent().to_string())
    }

    #[tokio::test]
    async fn should_leave_the_plan_alone_without_hooks() -> Result<()> {
        assert_eq!(run_hooks(vec![]).await?, "EmptyRelation: rows=1");
        Ok(())
    }

    #[tokio::test]
    async fn should_run_hooks_in_the_order_they_were_added() -> Result<()> {
        let hooks: Vec<Arc<dyn LogicalPlanHook>> =
            vec![Arc::new(Limit(2)), Arc::new(Limit(1))];
        assert_eq!(
            run_hooks(hooks).await?,
            "Limit: skip=0, fetch=1\n  \
             Limit: skip=0, fetch=2\n    \
             EmptyRelation: rows=1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn should_fail_with_a_hooks_error() {
        let hooks: Vec<Arc<dyn LogicalPlanHook>> =
            vec![Arc::new(Limit(2)), Arc::new(AlwaysFail), Arc::new(Limit(1))];
        assert_eq!(
            run_hooks(hooks).await.unwrap_err().to_string(),
            "Execution error: rejected by AlwaysFail"
        );
    }
}
