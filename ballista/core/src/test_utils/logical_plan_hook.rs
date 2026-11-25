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

//! [`LogicalPlanHook`]s for testing where and how often hooks run.

use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};

use crate::logical_plan_hook::LogicalPlanHook;

/// Replaces every plan with `SELECT <n> AS run`, where `n` is the number of
/// times it has run, starting at 1.
///
/// Run on `SELECT 0 AS run`, a query returns which run of the hook it is, so a
/// result of 1 and then 2 shows the hook ran exactly once per run, and that the
/// plan it returned is the one that ran.
#[derive(Debug, Default)]
pub struct CountRuns(AtomicI64);

#[async_trait]
impl LogicalPlanHook for CountRuns {
    async fn before_submit(
        &self,
        _plan: LogicalPlan,
        _session: &dyn Session,
    ) -> Result<LogicalPlan> {
        let run = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        LogicalPlanBuilder::values(vec![vec![lit(run)]])?
            .project([col("column1").alias("run")])?
            .build()
    }
}

/// Fails every query it runs on, with `Execution error: rejected by AlwaysFail`.
#[derive(Debug)]
pub struct AlwaysFail;

#[async_trait]
impl LogicalPlanHook for AlwaysFail {
    async fn before_submit(
        &self,
        _plan: LogicalPlan,
        _session: &dyn Session,
    ) -> Result<LogicalPlan> {
        exec_err!("rejected by AlwaysFail")
    }
}
