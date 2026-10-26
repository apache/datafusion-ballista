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
//! A completed stage's per-column output statistics, carried through
//! `AdaptivePlanner::resolve_stage_partitions` onto the stage's exchange so the
//! replan that follows can read them.

use crate::state::aqe::execution_plan::ExchangeExec;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::stats_table::{estimated_statistics, sized_statistics};
use crate::state::aqe::test::{
    MB, ballista_ctx, mock_partitions_with_size, narrow_schema, register_stats_table,
};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs};
use std::sync::Arc;

/// Statistics of the resolved exchange for `stage_id` in `plan`, if any.
fn resolved_exchange_statistics(
    plan: &dyn ExecutionPlan,
    stage_id: usize,
) -> Option<Arc<Statistics>> {
    if let Some(exchange) = plan.downcast_ref::<ExchangeExec>()
        && exchange.stage_id() == Some(stage_id)
        && exchange.shuffle_created()
    {
        return Some(
            StatisticsContext::new()
                .compute(plan, &StatisticsArgs::new())
                .unwrap(),
        );
    }
    plan.children()
        .into_iter()
        .find_map(|child| resolved_exchange_statistics(child.as_ref(), stage_id))
}

#[tokio::test]
async fn completed_stage_column_statistics_reach_its_exchange() {
    // A guessed-large dimension against a fact table, so the first pass stages
    // the dimension side alone as stage 0.
    let ctx = ballista_ctx();
    let schema = narrow_schema();
    register_stats_table(
        &ctx,
        "dim",
        schema.clone(),
        estimated_statistics(&schema, 1_000_000, 200 * MB),
    );
    register_stats_table(
        &ctx,
        "fact",
        schema.clone(),
        sized_statistics(&schema, 1_000_000_000, 40 * 1024 * MB),
    );
    let lp = ctx
        .sql("SELECT dim.val FROM dim JOIN fact ON dim.id = fact.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let mut planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".into())
        .await
        .unwrap();
    let (stages, _) = planner.actionable_stages().unwrap();
    assert_eq!(1, stages.unwrap().len());

    let reported: Vec<ColumnStatistics> = [7, 0]
        .into_iter()
        .map(|nulls| ColumnStatistics {
            null_count: Precision::Exact(nulls),
            ..ColumnStatistics::new_unknown()
        })
        .collect();
    planner
        .resolve_stage_partitions(
            0,
            mock_partitions_with_size(1_000_000, (200 * MB) as u64),
            reported,
        )
        .unwrap();

    let stats = resolved_exchange_statistics(planner.current_plan(), 0)
        .expect("stage 0's exchange stays in the plan, resolved");
    let null_counts: Vec<_> = stats
        .column_statistics
        .iter()
        .map(|column| column.null_count)
        .collect();
    assert_eq!(null_counts, vec![Precision::Exact(7), Precision::Exact(0)]);
}
