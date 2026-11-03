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

//! Tests for the rules a scheduler embedder adds to adaptive planning.

use crate::state::aqe::adaptive_rules::{AdaptiveRule, AdaptiveRules};
use crate::state::aqe::execution_plan::ExchangeExec;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{mock_batch, mock_context, mock_partitions_with_size};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::StatisticsArgs;
use datafusion::physical_plan::statistics::StatisticsContext;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// What the embedder's rules saw.
#[derive(Debug, Default)]
struct Seen {
    preparation_runs: AtomicUsize,
    replan_runs: AtomicUsize,
    /// The measured row counts of finished stages' exchanges, by stage id.
    measured_rows: Mutex<Vec<(usize, usize)>>,
}

/// Counts its runs, records the measured rows of each finished stage, and
/// optionally holds back every stage that hasn't run yet.
#[derive(Debug)]
struct Recording {
    seen: Arc<Seen>,
    replan: bool,
    hold: bool,
}

impl PhysicalOptimizerRule for Recording {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !self.replan {
            self.seen.preparation_runs.fetch_add(1, Ordering::Relaxed);
            return Ok(plan);
        }
        self.seen.replan_runs.fetch_add(1, Ordering::Relaxed);
        plan.apply(|node| {
            if let Some(exchange) = node.downcast_ref::<ExchangeExec>()
                && exchange.shuffle_created()
            {
                let stats = StatisticsContext::new()
                    .compute(node.as_ref(), &StatisticsArgs::new())?;
                if let (Some(stage_id), Some(rows)) =
                    (exchange.stage_id(), stats.num_rows.get_value())
                {
                    self.seen
                        .measured_rows
                        .lock()
                        .unwrap()
                        .push((stage_id, *rows));
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if !self.hold {
            return Ok(plan);
        }
        Ok(plan
            .transform_up(|node| {
                let Some(exchange) = node.downcast_ref::<ExchangeExec>() else {
                    return Ok(Transformed::no(node));
                };
                if exchange.shuffle_created() || exchange.is_inactive_stage() {
                    return Ok(Transformed::no(node));
                }
                let held: Arc<dyn ExecutionPlan> =
                    Arc::new(exchange.with_inactive_stage(true));
                Ok(Transformed::yes(held))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "Recording"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct Embedder {
    seen: Arc<Seen>,
    hold: bool,
}

impl AdaptiveRules for Embedder {
    fn preparation_rules(&self, _plan_ids: Arc<AtomicUsize>) -> Vec<AdaptiveRule> {
        vec![Arc::new(Recording {
            seen: Arc::clone(&self.seen),
            replan: false,
            hold: false,
        })]
    }

    fn replan_rules(&self, _plan_ids: Arc<AtomicUsize>) -> Vec<AdaptiveRule> {
        vec![Arc::new(Recording {
            seen: Arc::clone(&self.seen),
            replan: true,
            hold: self.hold,
        })]
    }
}

/// Two stages: one shuffling on `(a, c)`, and one reading it and shuffling on
/// `c`, then the final stage.
async fn two_stage_planner(
    embedder: &Embedder,
) -> datafusion::common::Result<AdaptivePlanner> {
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;
    let lp = ctx
        .sql("select c, count(*) from (select a, c from t group by a, c) group by c")
        .await?
        .into_optimized_plan()?;
    AdaptivePlanner::try_new(&ctx, &lp, "test_job".into(), Some(embedder)).await
}

/// An embedder's preparation rules run once, and its replan rules run when
/// the job is planned and again after each stage completes, seeing the
/// finished stage's measured statistics.
#[tokio::test]
async fn should_run_an_embedders_rules_and_show_them_measured_stages()
-> datafusion::common::Result<()> {
    let seen = Arc::new(Seen::default());
    let embedder = Embedder {
        seen: Arc::clone(&seen),
        hold: false,
    };
    let mut planner = two_stage_planner(&embedder).await?;
    assert_eq!(seen.preparation_runs.load(Ordering::Relaxed), 1);
    assert_eq!(seen.replan_runs.load(Ordering::Relaxed), 1);

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(stages.len(), 1);
    planner.finalise_stage_internal(0, mock_partitions_with_size(42, 1024))?;

    assert_eq!(seen.preparation_runs.load(Ordering::Relaxed), 1);
    assert_eq!(seen.replan_runs.load(Ordering::Relaxed), 2);
    assert_eq!(*seen.measured_rows.lock().unwrap(), vec![(0, 42)]);
    Ok(())
}

/// A replan rule sees each stage before it can start, so it can hold one back,
/// and a held stage doesn't run even though its inputs are ready.
#[tokio::test]
async fn should_not_run_a_stage_an_embedders_rule_holds() -> datafusion::common::Result<()>
{
    let embedder = Embedder {
        seen: Arc::new(Seen::default()),
        hold: true,
    };
    let mut planner = two_stage_planner(&embedder).await?;
    let stages = planner.runnable_stages()?.unwrap();
    assert!(stages.is_empty(), "a held stage must not run");
    Ok(())
}
