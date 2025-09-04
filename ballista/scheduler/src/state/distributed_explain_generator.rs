use std::collections::HashMap;
use std::sync::Arc;
use std::fmt::Write as _;

use ballista_core::error::Result;
use datafusion::prelude::{SessionContext};
use datafusion::logical_expr::{LogicalPlan};

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

    let plan = session_ctx
        .state()
        .create_physical_plan(&plan)
        .await?;
    
    let mut planner = DefaultDistributedPlanner::new();
    let shuffle_stages = planner.plan_query_stages(job_id, plan)?;
    let builder = ExecutionStageBuilder::new(session_config.clone());
    let stages = builder.build(shuffle_stages)?;

    Ok(render_stages(stages))
}

fn render_stages(stages: HashMap<usize,ExecutionStage>) -> String {
    let mut buf = String::new();
    let mut keys: Vec<_> = stages.keys().cloned().collect();
    keys.sort(); 
    for k in keys {
        let stage = &stages[&k];
        writeln!(buf, "{:#?}", stage).ok();
    }
    buf
}