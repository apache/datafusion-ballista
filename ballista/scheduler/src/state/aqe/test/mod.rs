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

/// Rules a scheduler embedder adds to adaptive planning
mod adaptive_rules;
/// Test if stages can be added or removed
mod alter_stages;
/// Broadcast-threshold decisions over declared statistics
mod broadcast_thresholds;
/// Functional tests for the CoalescePartitionsRule end-to-end through the planner
mod coalesce_rule;
/// Stage column statistics carried onto the stage's exchange
mod column_statistics;
/// Job-failure lifecycle tests for the adaptive graph
mod job_failure;
/// covers join selection tests
mod join_selection;
/// Adaptive planning of a submitted physical plan
mod physical_submission;
/// Tests if plan is going to be split to stages correctly
mod plan_to_stages;
/// Regression tests for range-repartition planning end-to-end
/// through `AdaptivePlanner` (DER → routing park → filter injection).
mod range_repartition;
/// A subplan read more than once runs its stage once
mod reuse_exchange;
/// Functional tests for the OptimizeSkewedJoinRule end-to-end through the planner
mod skew_join_rule;
/// Multi-pass coverage for staging a join's build side before deciding the join
mod stage_build_side;
/// Failed stage transitions leave the stage in the graph
mod stage_transitions;
/// A table whose statistics are declared rather than measured
mod stats_table;

use crate::state::aqe::adaptive_rules::{AdaptiveRule, AdaptiveRules};
use crate::state::aqe::execution_plan::ExchangeExec;
use crate::state::aqe::planner::{AdaptivePlanner, RunnableStages};
use ballista_core::JobId;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::scheduler::{
    ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    PartitionId, PartitionLocation, PartitionStats,
};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs};
use datafusion::prelude::{SessionConfig, SessionContext};
use stats_table::StatsTable;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

impl AdaptivePlanner {
    /// Creates an `AdaptivePlanner` over an already-built physical `plan`, with
    /// the default physical optimizer rules.
    pub(super) fn try_from_plan(
        session_config: &SessionConfig,
        plan: Arc<dyn ExecutionPlan>,
        job_id: JobId,
    ) -> datafusion::common::Result<Self> {
        let plan_id_generator = Arc::new(AtomicUsize::new(0));
        let state_builder = SessionStateBuilder::new_with_default_features()
            .with_config(session_config.clone());
        Self::try_new_with_optimizers(
            state_builder,
            plan,
            job_id,
            Self::default_optimizers(plan_id_generator),
        )
    }

    /// Plans `sql` in `ctx` to a physical plan and wraps it in an
    /// `AdaptivePlanner`, bypassing the preparation rules.
    pub(super) async fn from_physical_sql(
        ctx: &SessionContext,
        sql: &str,
    ) -> datafusion::common::Result<Self> {
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        Self::try_from_plan(ctx.state().config(), plan, "test_job".into())
    }

    /// Plans `sql` in `ctx` to an optimized logical plan and hands it to
    /// [`AdaptivePlanner::try_new`], which runs the preparation rules.
    pub(super) async fn from_logical_sql(
        ctx: &SessionContext,
        sql: &str,
        rules: Option<&dyn AdaptiveRules>,
    ) -> datafusion::common::Result<Self> {
        let plan = ctx.sql(sql).await?.into_optimized_plan()?;
        Self::try_new(ctx, &plan, "test_job".into(), rules).await
    }

    /// The stages that can run now, or `None` if no more stages can run.
    pub(super) fn runnable_stages(
        &mut self,
    ) -> datafusion::common::Result<RunnableStages> {
        let (runnable_stages, _) = self.actionable_stages()?;
        Ok(runnable_stages)
    }

    /// The stage ids of the runnable exchanges in the current plan, without
    /// changing the planner's state.
    pub(super) fn inspect_runnable_stages(
        &self,
    ) -> datafusion::common::Result<HashSet<usize>> {
        let mut runnable_stages = Vec::new();
        Self::find_runnable_exchanges(&self.plan, &mut runnable_stages);

        runnable_stages
            .into_iter()
            .map(|exec| {
                exec.downcast_ref::<ExchangeExec>()
                    .ok_or_else(|| DataFusionError::Plan("ExchangeExec expected".into()))
                    .map(|e| e.stage_id().unwrap())
            })
            .collect()
    }

    /// The current physical plan.
    pub(super) fn current_plan(&self) -> &dyn ExecutionPlan {
        self.plan.as_ref()
    }

    /// Resolves a finished stage with its partitions alone, for tests that
    /// don't exercise column statistics.
    pub(super) fn finalise_stage_internal(
        &mut self,
        stage_id: usize,
        partitions: Vec<Vec<PartitionLocation>>,
    ) -> datafusion::common::Result<()> {
        self.finalise_stage(stage_id, partitions, None)
    }
}

/// One mebibyte, for declaring fixture sizes.
pub(crate) const MB: usize = 1024 * 1024;

/// A join key plus one fixed-width column.
pub(crate) fn narrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]))
}

/// A context carrying Ballista's shipped configuration, so tests exercise the
/// thresholds a deployment runs with rather than DataFusion's defaults.
pub(crate) fn ballista_ctx() -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_round_robin_repartition(false);
    SessionContext::new_with_config(config)
}

/// Registers a [`StatsTable`] declaring `stats` over `schema`.
pub(crate) fn register_stats_table(
    ctx: &SessionContext,
    name: &str,
    schema: Arc<Schema>,
    stats: Statistics,
) {
    ctx.register_table(name, Arc::new(StatsTable::new(schema, stats, 4)))
        .unwrap();
}

pub(crate) fn mock_partitions_with_statistics() -> Vec<Vec<PartitionLocation>> {
    mock_partitions_with_size(42, 10)
}

/// Shuffle output reporting `num_rows` rows over `num_bytes` bytes in a single
/// partition. Tests that turn on the *value* of a measured size, rather than
/// just its presence, pick their own figures.
pub(crate) fn mock_partitions_with_size(
    num_rows: u64,
    num_bytes: u64,
) -> Vec<Vec<PartitionLocation>> {
    vec![vec![mock_partition_location(0, num_rows, num_bytes)]]
}

/// Output partition `partition_id` of stage 0 on a blank executor, reporting
/// `num_rows` rows over `num_bytes` bytes.
pub(crate) fn mock_partition_location(
    partition_id: usize,
    num_rows: u64,
    num_bytes: u64,
) -> PartitionLocation {
    PartitionLocation {
        map_partition_id: 0,
        partition_id: PartitionId {
            job_id: "".into(),
            stage_id: 0,
            partition_id,
        },
        executor_meta: ExecutorMetadata {
            id: "".to_string(),
            host: "".to_string(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification::default().with_vcores(0),
            os_info: ExecutorOperatingSystemSpecification::default(),
        },
        partition_stats: PartitionStats::new(Some(num_rows), None, Some(num_bytes)),
        file_id: None,
        is_sort_shuffle: false,
    }
}

/// Column statistics carrying only the given exact null counts.
pub(crate) fn null_count_statistics(null_counts: &[usize]) -> Vec<ColumnStatistics> {
    null_counts
        .iter()
        .map(|&n| ColumnStatistics::new_unknown().with_null_count(Precision::Exact(n)))
        .collect()
}

pub(crate) fn mock_partitions_with_statistics_no_data() -> Vec<Vec<PartitionLocation>> {
    mock_partitions_with_size(0, 0)
}

/// Returns schema with three columns (a,b,c) all of [DataType::Int32] type
pub(crate) fn mock_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Int32, true),
    ]))
}

/// Returns single batch with schema having three columns (a,b,c)
/// all of [DataType::Int32] type
pub(crate) fn mock_batch() -> datafusion::common::Result<RecordBatch> {
    let batch = RecordBatch::try_new(
        mock_schema(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, None])),
            Arc::new(Int32Array::from(vec![Some(4), None, Some(6), Some(8)])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
        ],
    )?;

    Ok(batch)
}

pub(crate) fn mock_memory_table() -> Arc<dyn TableProvider> {
    let data = vec![vec![mock_batch().unwrap()], vec![mock_batch().unwrap()]];
    Arc::new(MemTable::try_new(mock_schema(), data).unwrap())
}

pub(crate) fn mock_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(2)
        .with_round_robin_repartition(false)
        // Ballista disables dynamic filter pushdown in its session defaults
        // because dynamic filters cannot cross stage boundaries; mirror that
        // here so join-input swaps during AQE re-optimization are legal.
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false);

    SessionContext::new_with_config(config)
}

/// A session context with the Ballista config extension installed and the
/// coalesce-relevant knobs forced to specific values. The rule packs toward
/// `target_partition_bytes` (200, for test scale), so a scenario traces from its
/// inputs alone. Joins plan as sort-merge joins over hash-partitioned inputs.
pub(crate) fn coalesce_context(
    target_partitions: usize,
    enabled: bool,
) -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(target_partitions)
        .with_round_robin_repartition(false)
        .with_ballista_coalesce_enabled(enabled)
        .with_ballista_coalesce_target_partition_bytes(200)
        .set_bool("datafusion.optimizer.prefer_hash_join", false)
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            0,
        )
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            0,
        );

    SessionContext::new_with_config(config)
}

/// Registers a MemTable with `n_partitions` partitions, each holding one copy
/// of `mock_batch`. Multi-partition sources force DataFusion's
/// `EnforceDistribution` to insert a hash repartition before partitioned
/// joins; without that, two 1-partition inputs satisfy `Partitioned` on a
/// single partition and no `ExchangeExec` shows up in the final plan.
pub(crate) fn register_partitioned_table(
    ctx: &SessionContext,
    name: &str,
    n_partitions: usize,
) -> datafusion::error::Result<()> {
    let data = (0..n_partitions)
        .map(|_| Ok(vec![mock_batch()?]))
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    let table = MemTable::try_new(mock_schema(), data)?;
    ctx.register_table(name, Arc::new(table))?;
    Ok(())
}

/// A `Vec<Vec<PartitionLocation>>` of length `per_partition_bytes.len()` where
/// each upstream partition reports the given byte size. The coalesce rule sums
/// `partition_stats.num_bytes` across leaves before bin-packing, which is the
/// only field these tests need to vary.
pub(crate) fn partitions_with_byte_sizes(
    per_partition_bytes: &[u64],
) -> Vec<Vec<PartitionLocation>> {
    per_partition_bytes
        .iter()
        .enumerate()
        .map(|(idx, &bytes)| vec![mock_partition_location(idx, 1, bytes)])
        .collect()
}

/// What an embedder's rules saw.
#[derive(Debug, Default)]
pub(crate) struct Seen {
    pub(crate) preparation_runs: AtomicUsize,
    pub(crate) replan_runs: AtomicUsize,
    /// The measured row counts of finished stages' exchanges, by stage id.
    pub(crate) measured_rows: Mutex<Vec<(usize, usize)>>,
    /// A preparation rule was given a `RepartitionExec`.
    pub(crate) prepared_submitted_shuffle: AtomicBool,
    /// A preparation rule was given an `ExchangeExec`.
    pub(crate) prepared_stage_boundary: AtomicBool,
}

/// Counts its runs, records what a preparation rule is given and the measured
/// rows of each finished stage a replan rule is given, and optionally holds
/// back every stage that hasn't run yet.
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
            plan.apply(|node| {
                if node.is::<RepartitionExec>() {
                    self.seen
                        .prepared_submitted_shuffle
                        .store(true, Ordering::Relaxed);
                }
                if node.is::<ExchangeExec>() {
                    self.seen
                        .prepared_stage_boundary
                        .store(true, Ordering::Relaxed);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
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

/// A scheduler embedder whose preparation and replan rules are [`Recording`]
/// rules. Its replan rule holds back the stages that haven't run when `hold`
/// is set.
#[derive(Debug)]
pub(crate) struct Embedder {
    pub(crate) seen: Arc<Seen>,
    pub(crate) hold: bool,
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
