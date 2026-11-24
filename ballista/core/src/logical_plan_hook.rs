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
/// execution, on the node that planned it: the client for a Rust client, and
/// the scheduler for a Flight SQL client.
///
/// It runs exactly once per run, before the plan is encoded or physically
/// planned, so every part of the plan sees the same rewrite. A client's plan is
/// already optimized; a Flight SQL plan is optimized on the scheduler
/// afterwards. A reused `DataFrame` or Flight SQL prepared statement runs it
/// again each time. That makes it the place to decide what should be decided
/// when a query runs rather than when it's planned, such as the version of a
/// table to read. DataFusion decides that in `TableProvider::scan`, but a
/// Ballista job is physically planned on the scheduler, possibly more than
/// once.
///
/// Plans that run on the client, such as `SHOW` or a query of
/// `information_schema` alone, are planned there without running hooks.
///
/// Register hooks with
/// [`SessionConfigExt::with_ballista_logical_plan_hook`]. They run in the order
/// they were added.
#[async_trait]
pub trait LogicalPlanHook: Debug + Send + Sync {
    /// Returns `plan` rewritten for this run. An error fails the query with it.
    async fn before_submit(
        &self,
        plan: LogicalPlan,
        session: &dyn Session,
    ) -> Result<LogicalPlan>;
}

/// Runs the hooks registered in `session`'s config on `plan`, in the order they
/// were added.
pub async fn before_submit(
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

    use datafusion::common::exec_err;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::SessionConfig;

    use super::*;

    /// Limits the plan to `self.0` rows, failing instead if `self.0` is 0.
    #[derive(Debug)]
    struct Limit(usize);

    #[async_trait]
    impl LogicalPlanHook for Limit {
        async fn before_submit(
            &self,
            plan: LogicalPlan,
            _session: &dyn Session,
        ) -> Result<LogicalPlan> {
            if self.0 == 0 {
                return exec_err!("no rows allowed");
            }
            LogicalPlanBuilder::from(plan)
                .limit(0, Some(self.0))?
                .build()
        }
    }

    async fn submit(hooks: &[usize]) -> Result<String> {
        let config = hooks.iter().fold(SessionConfig::new(), |config, fetch| {
            config.with_ballista_logical_plan_hook(Arc::new(Limit(*fetch)))
        });
        let state = SessionStateBuilder::new().with_config(config).build();
        let plan = LogicalPlanBuilder::empty(true).build()?;
        let plan = before_submit(plan, &state).await?;
        Ok(plan.display_indent().to_string())
    }

    #[tokio::test]
    async fn should_leave_the_plan_alone_without_hooks() -> Result<()> {
        assert_eq!(submit(&[]).await?, "EmptyRelation: rows=1");
        Ok(())
    }

    #[tokio::test]
    async fn should_run_hooks_in_the_order_they_were_added() -> Result<()> {
        assert_eq!(
            submit(&[2, 1]).await?,
            "Limit: skip=0, fetch=1\n  \
             Limit: skip=0, fetch=2\n    \
             EmptyRelation: rows=1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn should_fail_with_a_hooks_error() {
        assert_eq!(
            submit(&[2, 0, 1]).await.unwrap_err().to_string(),
            "Execution error: no rows allowed"
        );
    }
}
