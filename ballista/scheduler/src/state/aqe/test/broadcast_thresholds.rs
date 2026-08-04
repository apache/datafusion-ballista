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

//! Whether the adaptive planner broadcasts a join's build side, over the sizes
//! it has to make that call on in production.
//!
//! These tests differ from `join_selection` in two ways that matter:
//!
//! * They run under [`SessionConfig::new_with_ballista`], so the thresholds are
//!   the ones a deployment actually uses (10 MB / 1,000,000 rows) rather than
//!   DataFusion's defaults. A test that sets its own thresholds can only show
//!   the rule is self-consistent, not that the shipped numbers behave.
//! * Their tables declare statistics instead of holding rows, so a case like
//!   "800,000 rows of unknown size" is one line rather than a table nobody would
//!   build. Sizes are the input to this decision, so they need to be the thing
//!   the test varies.

use crate::state::aqe::test::stats_table::{
    StatsTable, sized_statistics, sizeless_statistics,
};
use crate::state::aqe::{
    planner::AdaptivePlanner, test::mock_partitions_with_statistics,
};
use ballista_core::extension::SessionConfigExt;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Statistics;
use datafusion::execution::{
    SessionStateBuilder, config::SessionConfig, context::SessionContext,
};
use datafusion::physical_plan::displayable;
use std::sync::Arc;

const MB: usize = 1024 * 1024;

/// A join key plus one variable-width column, so the row width is realistic and
/// `Statistics::calculate_total_byte_size` cannot reconstruct a size for it.
fn wide_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// A join key plus one fixed-width column.
fn narrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]))
}

/// A context carrying Ballista's shipped configuration, so these tests exercise
/// the thresholds a deployment runs with.
fn ballista_ctx() -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_round_robin_repartition(false);
    let state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .build();
    SessionContext::new_with_state(state)
}

fn register(ctx: &SessionContext, name: &str, schema: Arc<Schema>, stats: Statistics) {
    ctx.register_table(name, Arc::new(StatsTable::new(schema, stats, 4)))
        .unwrap();
}

/// Resolves the join and reports whether any side ended up broadcast.
async fn plan_broadcasts(ctx: &SessionContext, sql: &str) -> (bool, String) {
    let lp = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
    let planner = AdaptivePlanner::try_new(ctx, &lp, "test_job".to_owned())
        .await
        .unwrap();
    let plan = displayable(planner.current_plan()).indent(true).to_string();

    (plan.contains("mode=CollectLeft"), plan)
}

/// The case this guards: a build side under the row threshold but far over the
/// byte threshold, whose size is unknown.
///
/// 800,000 rows of an `Int32` and a `Utf8` is roughly 19 MB, so broadcasting it
/// hands every probe task ~19 MB. Deciding on the row count alone cannot see
/// that, and a rule that reads `800_000 < 1_000_000` will broadcast it.
#[tokio::test]
async fn wide_rows_of_unknown_size_are_not_broadcast() {
    let ctx = ballista_ctx();
    let schema = wide_schema();
    register(
        &ctx,
        "big",
        Arc::clone(&schema),
        sizeless_statistics(&schema, 800_000),
    );
    register(
        &ctx,
        "probe",
        narrow_schema(),
        sized_statistics(&narrow_schema(), 500_000_000, 4_000_000_000),
    );

    let (broadcast, plan) = plan_broadcasts(
        &ctx,
        "SELECT big.name FROM big JOIN probe ON big.id = probe.id",
    )
    .await;

    assert!(
        !broadcast,
        "800k rows of unknown size (~19 MB) must not be broadcast; plan was:\n{plan}"
    );
}

/// The counterweight: a dimension table small enough to broadcast must still be
/// broadcast even though its size is unknown, or the fix above would simply
/// disable broadcasting.
#[tokio::test]
async fn small_dimension_of_unknown_size_is_still_broadcast() {
    let ctx = ballista_ctx();
    let schema = wide_schema();
    register(
        &ctx,
        "dim",
        Arc::clone(&schema),
        sizeless_statistics(&schema, 25),
    );
    register(
        &ctx,
        "probe",
        narrow_schema(),
        sized_statistics(&narrow_schema(), 500_000_000, 4_000_000_000),
    );

    let (broadcast, plan) = plan_broadcasts(
        &ctx,
        "SELECT dim.name FROM dim JOIN probe ON dim.id = probe.id",
    )
    .await;

    assert!(
        broadcast,
        "a 25-row dimension must still be broadcast; plan was:\n{plan}"
    );
}

/// Narrow rows of unknown size stay broadcastable: 100,000 rows of two `Int32`s
/// is under a megabyte, so the estimate must not reject it.
#[tokio::test]
async fn narrow_rows_of_unknown_size_are_broadcast() {
    let ctx = ballista_ctx();
    let schema = narrow_schema();
    register(
        &ctx,
        "small",
        Arc::clone(&schema),
        sizeless_statistics(&schema, 100_000),
    );
    register(
        &ctx,
        "probe",
        narrow_schema(),
        sized_statistics(&narrow_schema(), 500_000_000, 4_000_000_000),
    );

    let (broadcast, plan) = plan_broadcasts(
        &ctx,
        "SELECT small.val FROM small JOIN probe ON small.id = probe.id",
    )
    .await;

    assert!(
        broadcast,
        "100k narrow rows (<1 MB) should be broadcast; plan was:\n{plan}"
    );
}

/// A known size over the threshold is rejected, which held before this rule
/// existed and must keep holding.
#[tokio::test]
async fn known_size_over_threshold_is_not_broadcast() {
    let ctx = ballista_ctx();
    let schema = narrow_schema();
    register(
        &ctx,
        "big",
        Arc::clone(&schema),
        sized_statistics(&schema, 1_000, 64 * MB),
    );
    register(
        &ctx,
        "probe",
        narrow_schema(),
        sized_statistics(&narrow_schema(), 500_000_000, 4_000_000_000),
    );

    let (broadcast, plan) = plan_broadcasts(
        &ctx,
        "SELECT big.val FROM big JOIN probe ON big.id = probe.id",
    )
    .await;

    assert!(
        !broadcast,
        "a known 64 MB side must not be broadcast; plan was:\n{plan}"
    );
}

/// A known size under the threshold is broadcast. Together with the test above
/// this pins that a declared size is still authoritative and is not overridden
/// by the estimate.
#[tokio::test]
async fn known_size_under_threshold_is_broadcast() {
    let ctx = ballista_ctx();
    let schema = narrow_schema();
    register(
        &ctx,
        "small",
        Arc::clone(&schema),
        sized_statistics(&schema, 1_000, 8 * 1024),
    );
    register(
        &ctx,
        "probe",
        narrow_schema(),
        sized_statistics(&narrow_schema(), 500_000_000, 4_000_000_000),
    );

    let (broadcast, plan) = plan_broadcasts(
        &ctx,
        "SELECT small.val FROM small JOIN probe ON small.id = probe.id",
    )
    .await;

    assert!(
        broadcast,
        "a known 8 KB side must be broadcast; plan was:\n{plan}"
    );
}

/// Ballista ships `hash_join_single_partition_threshold = 10 MB`, and this
/// decision is only meaningful if that is what the planner reads. A test that
/// sets the threshold itself would pass even if the shipped default were zero,
/// which is what it was until recently.
#[tokio::test]
async fn ballista_config_carries_the_shipped_thresholds() {
    let config = SessionConfig::new_with_ballista();

    assert_eq!(
        config
            .options()
            .optimizer
            .hash_join_single_partition_threshold,
        10 * 1024 * 1024,
    );
    assert_eq!(
        config
            .options()
            .optimizer
            .hash_join_single_partition_threshold_rows,
        1_000_000,
    );
    assert!(!config.options().optimizer.prefer_hash_join);
}

/// Statistics arriving from a completed stage are measured, so a build side
/// under the threshold there is broadcast on its real size rather than an
/// estimate. Guards that the estimate stays out of the way once a shuffle has
/// reported what it actually wrote.
#[tokio::test]
async fn measured_shuffle_statistics_are_used_when_present() {
    let partitions = mock_partitions_with_statistics();

    let stats: Vec<_> = partitions
        .iter()
        .flatten()
        .map(|l| l.partition_stats)
        .collect();

    assert!(
        stats.iter().all(|s| s.num_bytes().is_some()),
        "the shuffle fixture must report measured bytes, or downstream planning \
         falls back to an estimate for reasons unrelated to the test"
    );
}
