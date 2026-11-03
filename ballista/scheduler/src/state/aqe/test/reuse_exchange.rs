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

//! A subplan read more than once runs its stage once (`ReuseExchangeRule`).

use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::aqe::execution_plan::ExchangeExec;
use crate::state::aqe::optimizer_rule::ReuseExchangeRule;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{
    mock_batch, mock_context, mock_partitions_with_statistics,
};
use crate::state::execution_graph::ExecutionGraph;
use crate::state::execution_stage::ExecutionStage;
use crate::test_utils::{
    mock_executor, revive_graph_and_complete_next_stage_with_executor,
};
use ballista_core::config::BallistaConfig;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{Partitioning, PhysicalExpr, expressions::Column};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// A common table expression read twice by a union.
const UNION: &str = "with v as (select c, sum(a) as s from t group by c) \
    select c, s from v union all select c, s from v";
/// A common table expression joined to itself.
const SELF_JOIN: &str = "with v as (select c, sum(a) as s from t group by c) \
    select x.c, x.s, y.s from v x join v y on x.c = y.c";
/// An aggregate compared with its own maximum, as TPC-H q15 does.
const Q15_SHAPE: &str = "with v as (select c, sum(a) as s from t group by c) \
    select c, s from v where s = (select max(s) from v)";

fn context(reuse: bool) -> SessionContext {
    let config = mock_context()
        .copied_config()
        .with_ballista_adaptive_reuse_exchange_enabled(reuse);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_batch("t", mock_batch().unwrap()).unwrap();
    ctx
}

async fn planner(ctx: &SessionContext, sql: &str) -> AdaptivePlanner {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    AdaptivePlanner::try_from_plan(ctx.state().config(), plan, "job".into()).unwrap()
}

async fn graph(ctx: &SessionContext, sql: &str) -> AdaptiveExecutionGraph {
    let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
    AdaptiveExecutionGraph::try_new("scheduler", &"job".into(), "", ctx, &plan, 0, None)
        .await
        .unwrap()
}

/// Runs every stage of `graph` on `executor` and returns how many stages ran.
fn run_to_completion(graph: &mut AdaptiveExecutionGraph, executor: &str) -> usize {
    let executor = mock_executor(executor.to_owned());
    let mut stages = 0;
    while !graph.is_successful() {
        revive_graph_and_complete_next_stage_with_executor(graph, &executor).unwrap();
        stages += 1;
    }
    stages
}

#[tokio::test]
async fn a_repeated_aggregate_runs_once() {
    for sql in [UNION, SELF_JOIN, Q15_SHAPE] {
        let mut planner = planner(&context(true), sql).await;

        let stages = planner.identify_runnable_stages().unwrap().unwrap();
        assert_eq!(stages.len(), 1, "{sql}");
        planner
            .finalise_stage_internal(0, mock_partitions_with_statistics())
            .unwrap();

        // Both readers are resolved by the one stage. What runs next reads
        // the aggregate, as the final stage or a broadcast of its output.
        let stages = planner.identify_runnable_stages().unwrap().unwrap();
        assert!(!stages.iter().any(aggregates_the_table), "{sql}");
    }
}

/// Whether `stage` is a stage of the `v` aggregate, rather than one reading it.
fn aggregates_the_table(stage: &Arc<dyn ExecutionPlan>) -> bool {
    stage
        .downcast_ref::<ExchangeExec>()
        .is_some_and(|exchange| {
            !exchange.input().children().is_empty()
                && exchange.input().children()[0].name() == "DataSourceExec"
        })
}

#[tokio::test]
async fn reuse_can_be_switched_off() {
    for sql in [UNION, SELF_JOIN, Q15_SHAPE] {
        let mut planner = planner(&context(false), sql).await;

        let stages = planner.identify_runnable_stages().unwrap().unwrap();
        assert_eq!(stages.len(), 2, "{sql}");
    }
}

#[tokio::test]
async fn a_volatile_subplan_runs_for_each_reader() {
    // `random()` below the shuffle, where reading one stage would give both
    // readers the same values.
    let sql = "with v as (select c, sum(a * random()) as s from t group by c) \
        select c, s from v union all select c, s from v";
    let mut planner = planner(&context(true), sql).await;

    let stages = planner.identify_runnable_stages().unwrap().unwrap();
    assert_eq!(stages.len(), 2);
}

#[tokio::test]
async fn a_job_reading_an_aggregate_twice_runs_one_stage_fewer() {
    for sql in [UNION, SELF_JOIN, Q15_SHAPE] {
        let without = run_to_completion(&mut graph(&context(false), sql).await, "e");
        let with = run_to_completion(&mut graph(&context(true), sql).await, "e");

        assert_eq!(with, without - 1, "{sql}");
    }
}

// AQE does not yet rerun a finished stage whose output was lost (#1986,
// #2539), with or without reuse. What reuse adds is a stage with more than
// one reader, and losing its output must roll back the one stage and every
// stage reading it.
#[tokio::test]
async fn losing_a_shared_stage_output_rolls_back_the_stage_once() {
    let mut graph = graph(&context(true), UNION).await;
    revive_graph_and_complete_next_stage_with_executor(
        &mut graph,
        &mock_executor("a".into()),
    )
    .unwrap();
    graph.revive();

    let (reset, _) = graph.reset_stages_on_lost_executor("a").unwrap();

    assert_eq!(graph.stages.len(), 2);
    assert!(reset.contains(&0) && reset.contains(&1));
    assert!(matches!(graph.stages[&0], ExecutionStage::Running(_)));
    assert!(matches!(graph.stages[&1], ExecutionStage::UnResolved(_)));
}

fn exchange(plan_id: usize) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
    Arc::new(ExchangeExec::new(
        Arc::new(EmptyExec::new(schema)),
        Some(Partitioning::Hash(vec![key], 2)),
        plan_id,
    ))
}

fn reuse(plan: Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut config = ConfigOptions::default();
    config.extensions.insert(BallistaConfig::default());
    let rule =
        ReuseExchangeRule::new(Arc::new(BallistaPhysicalExtensionCodec::default()));
    rule.optimize(plan, &config)
        .unwrap()
        .children()
        .into_iter()
        .cloned()
        .collect()
}

fn as_exchange(plan: &Arc<dyn ExecutionPlan>) -> &ExchangeExec {
    plan.downcast_ref::<ExchangeExec>().unwrap()
}

#[test]
fn a_repeat_found_after_the_stage_finished_reads_its_output() {
    let finished = exchange(0);
    as_exchange(&finished).set_stage_id(3);
    as_exchange(&finished).resolve_shuffle_partitions(mock_partitions_with_statistics());

    // The finished exchange comes second in the walk, but is the one read.
    let children = reuse(UnionExec::try_new(vec![exchange(1), finished]).unwrap());

    let repeat = as_exchange(&children[0]);
    assert!(repeat.shuffle_created());
    assert_eq!(repeat.stage_id(), Some(3));
    assert_eq!(repeat.plan_id(), 1);
}

#[test]
fn exchanges_already_running_stages_of_their_own_are_left_alone() {
    let first = exchange(0);
    let second = exchange(1);
    as_exchange(&first).set_stage_id(3);
    as_exchange(&second).set_stage_id(4);

    let children = reuse(UnionExec::try_new(vec![first, second]).unwrap());

    assert_eq!(as_exchange(&children[0]).stage_id(), Some(3));
    assert_eq!(as_exchange(&children[1]).stage_id(), Some(4));
}

#[test]
fn readers_of_a_pending_stage_that_diverged_are_split() {
    // Turning one reader of a pending shared stage into a broadcast leaves
    // two subtrees behind one stage. Each must write its own output.
    let hash = exchange(0);
    let shared: Arc<dyn ExecutionPlan> =
        Arc::new(as_exchange(&exchange(1)).share_stage_of(as_exchange(&hash)));
    let broadcast: Arc<dyn ExecutionPlan> =
        Arc::new(as_exchange(&shared).to_broadcast(2));

    let children = reuse(UnionExec::try_new(vec![hash, broadcast]).unwrap());

    assert!(!as_exchange(&children[0]).shares_stage_with(as_exchange(&children[1])));
}
