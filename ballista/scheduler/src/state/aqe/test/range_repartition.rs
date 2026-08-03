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

//! Regression tests for range-repartition planning end-to-end
//! through `AdaptivePlanner` — DER inserts the boundary
//! `ExchangeExec`, `set_repartition_routing` parks the recovered
//! cuts on it, and `cut_partitions` duplicates straddlers so
//! downstream can inject a `PerPartitionFilterExec` to trim them.

use crate::state::aqe::execution_plan::RangeRepartitionRouting;
use crate::state::aqe::planner::AdaptivePlanner;
use ballista_core::execution_plans::{
    RuntimeStatsExec, UnorderedRangeRepartitionExec, cut_partitions,
};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{RuntimeStatsPartitionEntry, RuntimeStatsReport};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    PartitionId, PartitionLocation, PartitionStats,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::expressions::col;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

fn v_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]))
}

/// The canonical shape a range-repartition rule is told to emit,
/// here at the root of the plan.
fn stats_over_urre_root() -> Arc<dyn ExecutionPlan> {
    let schema = v_schema();
    let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![]], schema.clone(), None).unwrap(),
    )));
    let sort_expr = PhysicalSortExpr {
        expr: col("v", schema.as_ref()).unwrap(),
        options: SortOptions {
            descending: false,
            nulls_first: false,
        },
    };
    let urre: Arc<dyn ExecutionPlan> = Arc::new(
        UnorderedRangeRepartitionExec::try_new(source, vec![sort_expr.clone()], 2)
            .unwrap(),
    );
    Arc::new(RuntimeStatsExec::try_new(urre, Some(vec![sort_expr])).unwrap())
}

/// Same shape with an ordinary parent above it. A `FilterExec` survives
/// the optimizer pipeline; a `RoundRobinBatch` `RepartitionExec` and
/// `CoalescePartitionsExec` do not (both get stripped), so the parent
/// choice matters for keeping the range-repartition off the plan root.
fn stats_over_urre_with_parent() -> Arc<dyn ExecutionPlan> {
    let pred: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
        Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            col("v", v_schema().as_ref()).unwrap(),
            datafusion::logical_expr::Operator::Gt,
            Arc::new(datafusion::physical_expr::expressions::Literal::new(
                datafusion::scalar::ScalarValue::Float64(Some(0.0)),
            )),
        ));
    Arc::new(
        datafusion::physical_plan::filter::FilterExec::try_new(
            pred,
            stats_over_urre_root(),
        )
        .unwrap(),
    )
}

fn location(sub_part_id: usize, producer_task_id: usize, rows: u64) -> PartitionLocation {
    PartitionLocation {
        map_partition_id: 0,
        partition_id: PartitionId {
            job_id: "repro-job".into(),
            stage_id: 0,
            partition_id: sub_part_id,
        },
        executor_meta: ExecutorMetadata {
            id: format!("exec-{producer_task_id}"),
            host: "".to_string(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification::default().with_vcores(0),
            os_info: ExecutorOperatingSystemSpecification::default(),
        },
        partition_stats: PartitionStats::new(Some(rows), None, None),
        file_id: Some(producer_task_id as u64),
        is_sort_shuffle: false,
    }
}

/// End-to-end through `AdaptivePlanner`: with the range-repartition
/// at the plan root, `cut_partitions` duplicates a straddler across
/// both downstream partitions — correct pre-filter shape — and
/// `set_repartition_routing` must park the cuts on a boundary
/// `ExchangeExec` so downstream can inject the read-side filter.
#[tokio::test]
async fn routing_parks_when_range_repartition_is_plan_root()
-> datafusion::error::Result<()> {
    let config = SessionConfig::new_with_ballista();
    let mut planner = AdaptivePlanner::try_from_plan(
        &config,
        stats_over_urre_root(),
        "regression-job".into(),
    )?;

    let stages = planner.runnable_stages()?;
    let stage_id = stages
        .as_ref()
        .and_then(|s| s.first())
        .map(|e| e.plan.stage_id())
        .expect("a runnable stage must exist");

    // Producer task 7, sub-part 0, sketched [5, 15, 25], straddles the cut at 15.
    let reports = vec![ballista_core::execution_plans::TaskRuntimeStats {
        producer_task_id: 7,
        report: RuntimeStatsReport {
            order_by: vec![],
            partitions: vec![RuntimeStatsPartitionEntry {
                partition_id: 0,
                row_count: 3,
                sketch: Some(ballista_core::execution_plans::sketch_to_proto(
                    &datafusion_functions_aggregate_common::tdigest::TDigest::new(100)
                        .merge_unsorted_f64(vec![5.0, 15.0, 25.0]),
                )?),
            }],
        },
    }];
    let cuts = vec![15.0];
    let remapped = cut_partitions(vec![vec![location(0, 7, 3)]], &reports, &cuts)?;

    // `cut_partitions` must duplicate the straddler into both partitions —
    // the read-side filter is expected to trim on read.
    assert_eq!(remapped[0].len(), 1, "straddler routed into partition 0");
    assert_eq!(remapped[1].len(), 1, "straddler routed into partition 1");

    let routing = RangeRepartitionRouting {
        cuts: cuts.clone(),
        routing_expr: col("v", v_schema().as_ref()).unwrap(),
    };
    planner.set_repartition_routing(stage_id, routing)?;

    let plan_str = format!(
        "{}",
        datafusion::physical_plan::displayable(planner.current_plan()).indent(true)
    );
    assert!(
        plan_str.contains("range_repartition_cuts=1"),
        "cuts must be parked on the boundary ExchangeExec so downstream \
         gets a PerPartitionFilterExec — actual plan:\n{plan_str}"
    );

    Ok(())
}

/// `set_repartition_routing` is only meaningful when the stage's
/// cached boundary is an `ExchangeExec` — otherwise there's nowhere
/// to park cuts and downstream will never inject the read-side
/// filter. Silently returning `Ok(())` in that case was the outlier
/// in a code path that otherwise errors hard on invariant breaks
/// (Andy's review of PR #2196), so make it fail loud.
#[tokio::test]
async fn set_repartition_routing_errs_when_stage_has_no_exchange()
-> datafusion::error::Result<()> {
    let config = SessionConfig::new_with_ballista();
    // A bare leaf plan has no `ExchangeExec` anywhere; its only
    // runnable stage caches the outer `AdaptiveDatafusionExec` as
    // the final-stage wrapper, which is not a parking slot.
    let plan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![]], v_schema(), None).unwrap(),
    )));
    let mut planner = AdaptivePlanner::try_from_plan(&config, plan, "err-path".into())?;

    let stages = planner.runnable_stages()?;
    let stage_id = stages
        .as_ref()
        .and_then(|s| s.first())
        .map(|e| e.plan.stage_id())
        .expect("a runnable stage must exist");

    let routing = RangeRepartitionRouting {
        cuts: vec![0.0],
        routing_expr: col("v", v_schema().as_ref()).unwrap(),
    };
    let result = planner.set_repartition_routing(stage_id, routing);
    assert!(
        result.is_err(),
        "must fail loud when there's no ExchangeExec to park on; got {result:?}"
    );

    Ok(())
}

/// Sibling of `routing_parks_when_range_repartition_is_plan_root`:
/// with an ordinary parent (`FilterExec`) above the range-repartition,
/// the exchange gets inserted and the cuts get parked. Position in
/// the plan should be the only variable between this and the
/// root-level test.
#[tokio::test]
async fn routing_parks_when_range_repartition_has_a_parent()
-> datafusion::error::Result<()> {
    let config = SessionConfig::new_with_ballista();
    let mut planner = AdaptivePlanner::try_from_plan(
        &config,
        stats_over_urre_with_parent(),
        "control-job".into(),
    )?;

    let stages = planner.runnable_stages()?;
    let stage_id = stages
        .as_ref()
        .and_then(|s| s.first())
        .map(|e| e.plan.stage_id())
        .expect("a runnable stage must exist");

    let routing = RangeRepartitionRouting {
        cuts: vec![15.0],
        routing_expr: col("v", v_schema().as_ref()).unwrap(),
    };
    planner.set_repartition_routing(stage_id, routing)?;

    let plan_str = format!(
        "{}",
        datafusion::physical_plan::displayable(planner.current_plan()).indent(true)
    );
    assert!(
        plan_str.contains("range_repartition_cuts=1"),
        "control: the cuts must be parked on the boundary ExchangeExec"
    );

    Ok(())
}
