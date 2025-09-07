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

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use ballista_core::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

use crate::state::execution_graph::ExecutionStage;
use crate::{
    planner::{DefaultDistributedPlanner, DistributedPlanner},
    state::execution_graph::ExecutionStageBuilder,
};

pub async fn generate_distributed_explain_plan(
    job_id: &str,
    session_ctx: Arc<SessionContext>,
    plan: Arc<LogicalPlan>,
) -> Result<String> {
    let session_config = Arc::new(session_ctx.copied_config());

    let plan = session_ctx.state().create_physical_plan(&plan).await?;

    let mut planner = DefaultDistributedPlanner::new();
    let shuffle_stages =
        planner.plan_query_stages(job_id, plan, session_config.options())?;
    let builder = ExecutionStageBuilder::new(session_config.clone());
    let stages = builder.build(shuffle_stages)?;

    Ok(render_stages(stages))
}

fn render_stages(stages: HashMap<usize, ExecutionStage>) -> String {
    let mut buf = String::new();
    let mut keys: Vec<_> = stages.keys().cloned().collect();
    keys.sort();
    for k in keys {
        let stage = &stages[&k];
        writeln!(buf, "{:#?}", stage).ok();
    }
    buf
}
