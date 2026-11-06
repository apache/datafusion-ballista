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

//! A subplan read more than once runs its stage once (see
//! `crate::state::aqe::reuse_exchange`).

use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::reuse_exchange::{Unshareable, reuse_key};
use crate::state::aqe::test::{
    mock_batch, mock_context, mock_partitions_with_statistics,
};
use crate::state::execution_graph::ExecutionGraph;
use crate::state::execution_stage::ExecutionStage;
use crate::test_utils::{
    mock_executor, revive_graph_and_complete_next_stage,
    revive_graph_and_complete_next_stage_with_executor,
};
use ballista_core::extension::SessionConfigExt;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{Partitioning, PhysicalExpr, expressions::Column};
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use parking_lot::Mutex;
use rstest::rstest;
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
/// `random()` below the shuffle, where reading one stage would give both
/// readers the same values.
const VOLATILE: &str = "with v as (select c, sum(a * random()) as s from t group by c) \
    select c, s from v union all select c, s from v";

fn context(reuse: bool) -> SessionContext {
    let config = mock_context()
        .copied_config()
        .with_ballista_reuse_exchange_enabled(reuse);
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

/// Runs every stage of `graph` and returns how many stages ran.
fn run_to_completion(graph: &mut AdaptiveExecutionGraph) -> usize {
    let mut stages = 0;
    while !graph.is_successful() {
        revive_graph_and_complete_next_stage(graph).unwrap();
        stages += 1;
    }
    stages
}

#[rstest]
#[tokio::test]
async fn a_repeated_aggregate_runs_once(
    #[values(UNION, SELF_JOIN, Q15_SHAPE)] sql: &str,
    #[values(true, false)] reuse: bool,
) {
    let mut planner = planner(&context(reuse), sql).await;

    let stages = planner.identify_runnable_stages().unwrap().unwrap();
    assert_eq!(stages.len(), if reuse { 1 } else { 2 });
    planner
        .finalise_stage_internal(0, mock_partitions_with_statistics())
        .unwrap();

    // With reuse, both readers are resolved by the one stage, and what runs
    // next reads the aggregate, as the final stage or a broadcast of its
    // output. Without, the second copy of the aggregate is still running.
    let stages = planner.identify_runnable_stages().unwrap().unwrap();
    assert_eq!(stages.iter().any(aggregates_the_table), !reuse);
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
async fn a_volatile_subplan_runs_for_each_reader() {
    let mut planner = planner(&context(true), VOLATILE).await;

    let stages = planner.identify_runnable_stages().unwrap().unwrap();
    assert_eq!(stages.len(), 2);
}

#[tokio::test]
async fn a_volatile_subplan_is_unshareable_for_its_volatile_expression() {
    let ctx = context(true);
    let planner = planner(&ctx, VOLATILE).await;
    let codec = ctx.state().config().ballista_physical_extension_codec();

    let mut runnable = Vec::new();
    AdaptivePlanner::find_runnable_exchanges(&planner.plan, &mut runnable);
    assert_eq!(runnable.len(), 2);
    for exchange in &runnable {
        let reason = reuse_key(exchange, &codec).unwrap_err();
        assert!(matches!(reason, Unshareable::Volatile(_)), "{reason}");
    }
}

#[rstest]
#[tokio::test]
async fn a_job_reading_an_aggregate_twice_runs_one_stage_fewer(
    #[values(UNION, SELF_JOIN, Q15_SHAPE)] sql: &str,
) {
    let without = run_to_completion(&mut graph(&context(false), sql).await);
    let with = run_to_completion(&mut graph(&context(true), sql).await);

    assert_eq!(with, without - 1);
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

fn exchange(plan_id: usize) -> ExchangeExec {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
    ExchangeExec::new(
        Arc::new(EmptyExec::new(schema)),
        Some(Partitioning::Hash(vec![key], 2)),
        plan_id,
    )
}

/// Counts, on each pass of the optimizer chain, the resolved exchanges it
/// sees, as the rules that react to a resolved exchange, such as join
/// selection, see them.
#[derive(Debug, Default)]
struct CountResolvedExchanges {
    passes: Mutex<Vec<usize>>,
}

impl PhysicalOptimizerRule for CountResolvedExchanges {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut resolved = 0;
        plan.apply(|node| {
            if node
                .downcast_ref::<ExchangeExec>()
                .is_some_and(ExchangeExec::shuffle_created)
            {
                resolved += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        self.passes.lock().push(resolved);
        Ok(plan)
    }

    fn name(&self) -> &str {
        "CountResolvedExchanges"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A planner over two identical exchanges, the second held back, so the
/// first launches on its own and the second becomes runnable later, as a
/// rule releasing a held-back stage does. Its only rule is `counter`.
fn planner_with_a_late_repeat(counter: Arc<CountResolvedExchanges>) -> AdaptivePlanner {
    let union = UnionExec::try_new(vec![
        Arc::new(exchange(0)),
        Arc::new(exchange(1).with_inactive_stage(true)),
    ])
    .unwrap();
    let plan = Arc::new(AdaptiveDatafusionExec::new(2, union));
    let state =
        SessionStateBuilder::new().with_config(SessionConfig::new_with_ballista());
    AdaptivePlanner::try_new_with_optimizers(state, plan, "job".into(), vec![counter])
        .unwrap()
}

/// Releases every held-back exchange of `planner`'s plan.
fn release_held_back_exchanges(planner: &mut AdaptivePlanner) {
    planner.plan = Arc::clone(&planner.plan)
        .transform_up(|node| match node.downcast_ref::<ExchangeExec>() {
            Some(exchange) if exchange.is_inactive_stage() => Ok(Transformed::yes(
                Arc::new(exchange.with_inactive_stage(false)) as Arc<dyn ExecutionPlan>,
            )),
            _ => Ok(Transformed::no(node)),
        })
        .unwrap()
        .data;
}

fn stage_ids(stages: &[Arc<dyn ExecutionPlan>]) -> Vec<Option<usize>> {
    stages
        .iter()
        .map(|stage| match stage.downcast_ref::<ExchangeExec>() {
            Some(exchange) => exchange.stage_id(),
            None => stage
                .downcast_ref::<AdaptiveDatafusionExec>()
                .and_then(AdaptiveDatafusionExec::stage_id),
        })
        .collect()
}

#[test]
fn a_repeat_launched_while_the_stage_runs_reads_it() {
    let mut planner = planner_with_a_late_repeat(Arc::default());
    let stages = planner.identify_runnable_stages().unwrap().unwrap();
    assert_eq!(stage_ids(&stages), vec![Some(0)]);

    release_held_back_exchanges(&mut planner);
    let stages = planner.identify_runnable_stages().unwrap().unwrap();

    // Stage 0 is reported once, for both readers, and no stage 1 launches.
    assert_eq!(stage_ids(&stages), vec![Some(0)]);
}

// A repeat that reads a finished stage is resolved when it would launch,
// after the rules that react to a resolved exchange, such as join selection,
// have run. The plan must be replanned, as for a stage finishing, or a stage
// can launch with a join still undecided above the repeat (TPC-DS q25, q64
// and q74).
#[test]
fn a_repeat_launched_after_the_stage_finished_reads_its_output() {
    let counter = Arc::new(CountResolvedExchanges::default());
    let mut planner = planner_with_a_late_repeat(counter.clone());
    planner.identify_runnable_stages().unwrap().unwrap();
    planner
        .finalise_stage_internal(0, mock_partitions_with_statistics())
        .unwrap();

    release_held_back_exchanges(&mut planner);
    let stages = planner.identify_runnable_stages().unwrap().unwrap();

    // Both readers are resolved, so the final stage runs next, and the rules
    // saw both resolved before it did.
    assert!(stages[0].is::<AdaptiveDatafusionExec>());
    assert_eq!(stage_ids(&stages), vec![Some(1)]);
    assert_eq!(counter.passes.lock().last(), Some(&2));
}

#[test]
fn a_repeat_of_a_dropped_stage_runs_its_own() {
    let mut planner = planner_with_a_late_repeat(Arc::default());
    planner.identify_runnable_stages().unwrap().unwrap();
    // A replan dropped stage 0 before it finished.
    planner.cancel_stage(0).unwrap();
    planner.plan = Arc::new(AdaptiveDatafusionExec::new(
        2,
        Arc::new(exchange(1).with_inactive_stage(true)),
    ));

    release_held_back_exchanges(&mut planner);
    let stages = planner.identify_runnable_stages().unwrap().unwrap();

    assert_eq!(stage_ids(&stages), vec![Some(1)]);
}
