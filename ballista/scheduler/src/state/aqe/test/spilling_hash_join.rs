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

//! End-to-end coverage that the AQE (adaptive) planner substitutes eligible
//! `HashJoinExec` nodes with `SpillingHashJoinExec` when
//! `ballista.execution.spilling_hash_join.enabled` is set, using the SAME
//! broadcast-first ordering discipline as the static planner: the substitution
//! runs only after AQE's own dynamic-join / broadcast-promotion machinery has
//! resolved a join, so a plain leftover `Inner`/`Partitioned` `HashJoinExec` is
//! the only thing substituted.

use crate::physical_optimizer::spilling_hash_join::SpillingHashJoinRule;
use crate::state::aqe::optimizer_rule::DelayJoinSelectionRule;
use crate::state::aqe::{
    planner::AdaptivePlanner, test::mock_partitions_with_statistics,
};
use ballista_core::config::{BALLISTA_SPILLING_HASH_JOIN_ENABLED, BallistaConfig};
use ballista_core::extension::SessionConfigExt;
use datafusion::{
    arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    },
    common::config::ConfigOptions,
    config::ExtensionOptions,
    datasource::MemTable,
    execution::{SessionStateBuilder, config::SessionConfig, context::SessionContext},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, displayable},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use std::sync::Arc;

fn make_table(schema: Arc<Schema>) -> Arc<MemTable> {
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0])),
            Arc::new(Int32Array::from(vec![
                10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
            ])),
        ],
    )
    .unwrap();

    Arc::new(
        MemTable::try_new(
            schema,
            vec![
                vec![batch.clone()],
                vec![batch.clone()],
                vec![batch.clone()],
                vec![batch],
            ],
        )
        .unwrap(),
    )
}

/// A session that keeps a two-table equijoin as a plain `Inner`/`Partitioned`
/// `HashJoinExec` under AQE: `prefer_hash_join=true` picks hash over sort-merge,
/// and a broadcast threshold of `0` disables `CollectLeft` promotion so the join
/// is repartitioned rather than broadcast. `spilling` toggles the spilling flag.
fn make_ctx(spilling: bool) -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .set_bool("datafusion.optimizer.prefer_hash_join", true)
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            0,
        )
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            0,
        )
        .with_ballista_broadcast_join_threshold_bytes(0)
        .set_str(
            BALLISTA_SPILLING_HASH_JOIN_ENABLED,
            if spilling { "true" } else { "false" },
        )
        .with_target_partitions(4)
        .with_round_robin_repartition(false);

    let state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .build();
    SessionContext::new_with_state(state)
}

fn register_2tables(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]));
    ctx.register_table("t1", make_table(Arc::clone(&schema)))
        .unwrap();
    ctx.register_table("t2", make_table(schema)).unwrap();
}

fn plan_string(plan: &dyn ExecutionPlan) -> String {
    format!("{}", displayable(plan).indent(false))
}

/// Whether the rendered plan contains a bare `HashJoinExec` node. A
/// `SpillingHashJoinExec` line ends with `HashJoinExec`, so a naive
/// `contains("HashJoinExec")` would false-positive; this matches only lines
/// whose (trimmed) node name starts with `HashJoinExec`.
fn has_bare_hash_join(s: &str) -> bool {
    s.lines()
        .any(|line| line.trim_start().starts_with("HashJoinExec"))
}

/// Drives the AQE planner to the point where the two input shuffle stages have
/// finished and the join stage is runnable, then returns the rendered plan of
/// the runnable join stage handed to the executors.
async fn resolved_join_stage_string(spilling: bool) -> String {
    let ctx = make_ctx(spilling);
    register_2tables(&ctx);

    // `SELECT *` keeps every join-output column in natural order, so the
    // physical planner folds no projection into the join — leaving a
    // projection-less `HashJoinExec` that matches the strict eligibility shape.
    let lp = ctx
        .sql("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();

    let mut planner = AdaptivePlanner::try_new(&ctx, &lp, "test_job".to_owned())
        .await
        .unwrap();

    // First round: the two input scan stages become runnable.
    let (stages, _) = planner.actionable_stages().unwrap();
    let stages = stages.unwrap();
    assert_eq!(2, stages.len(), "expected the two input scan stages");

    // Finalise them so the dynamic join resolves to a Partitioned hash join.
    planner
        .finalise_stage_internal(0, mock_partitions_with_statistics())
        .unwrap();
    planner
        .finalise_stage_internal(1, mock_partitions_with_statistics())
        .unwrap();

    // Second round: the join stage is now runnable; its adapted plan is where
    // the spilling substitution (if enabled) must appear.
    let (stages, _) = planner.actionable_stages().unwrap();
    let stages = stages.unwrap();
    assert_eq!(1, stages.len(), "expected the single join stage");

    plan_string(stages.first().unwrap().plan.as_ref())
}

/// Flag ON: a plain `Inner`/`Partitioned` `HashJoinExec` that AQE leaves
/// un-promoted must be substituted with `SpillingHashJoinExec` in the runnable
/// join stage.
#[tokio::test]
async fn aqe_planner_substitutes_when_enabled() {
    let s = resolved_join_stage_string(true).await;
    assert!(s.contains("SpillingHashJoinExec"), "{s}");
    assert!(!has_bare_hash_join(&s), "{s}");
}

/// Flag OFF: the same join is left as a bare `HashJoinExec`; no substitution.
#[tokio::test]
async fn aqe_planner_no_substitution_when_disabled() {
    let s = resolved_join_stage_string(false).await;
    assert!(has_bare_hash_join(&s), "{s}");
    assert!(!s.contains("SpillingHashJoinExec"), "{s}");
}

/// Guards the exact clobbering risk called out for the AQE path: a join that is
/// still wrapped in `DynamicJoinSelectionExec` (i.e. AQE has NOT yet resolved
/// it) must never be substituted, even with the flag on. `DynamicJoinSelectionExec`
/// stores the join parameters as fields and exposes only the join *inputs* as
/// its children — it does not wrap a `HashJoinExec` — so a whole-tree
/// `transform_up` cannot reach an inner join to clobber. This proves the wrapper
/// and its (nonexistent) inner join both survive the rule.
#[tokio::test]
async fn dynamic_join_wrapper_not_substituted() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]));
    let ctx = SessionContext::new();
    ctx.register_table("t1", make_table(Arc::clone(&schema)))
        .unwrap();
    ctx.register_table("t2", make_table(schema)).unwrap();

    let state = SessionStateBuilder::new()
        .with_physical_optimizer_rules(vec![])
        .build();
    let lp = ctx
        .sql("SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let plan = DefaultPhysicalPlanner::default()
        .create_physical_plan(&lp, &state)
        .await
        .unwrap();

    // Wrap the join in a DynamicJoinSelectionExec (adaptive_join defaults on).
    let dynamic = DelayJoinSelectionRule::default()
        .optimize(plan, &ConfigOptions::default())
        .unwrap();
    let before = plan_string(dynamic.as_ref());
    assert!(before.contains("DynamicJoinSelectionExec"), "{before}");

    // Apply the spilling rule with the flag ON over the whole tree.
    let mut bc = BallistaConfig::default();
    bc.set("execution.spilling_hash_join.enabled", "true")
        .unwrap();
    let mut cfg = ConfigOptions::default();
    cfg.extensions.insert(bc);

    let after = SpillingHashJoinRule::default()
        .optimize(dynamic, &cfg)
        .unwrap();
    let s = plan_string(after.as_ref());

    // The dynamic wrapper survives and nothing was substituted: no HashJoinExec
    // is exposed for the rule to reach, so no SpillingHashJoinExec is created.
    assert!(s.contains("DynamicJoinSelectionExec"), "{s}");
    assert!(!s.contains("SpillingHashJoinExec"), "{s}");
    assert!(!has_bare_hash_join(&s), "{s}");
}
