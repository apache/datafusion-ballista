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

//! Jobs in which one input finishes with no rows.
//!
//! Neither table declares a row count, so the planner learns that `empty` is
//! empty only when its stage finishes.

use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::stats_table::{StatsExec, StatsTable};
use crate::state::aqe::test::{
    mock_partitions_with_size, mock_partitions_with_statistics_no_data,
};
use ballista_core::assert_plan;
use ballista_core::execution_plans::{ShuffleReaderExec, ShuffleWriter};
use ballista_core::extension::SessionConfigExt;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Statistics;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use rstest::rstest;
use std::collections::HashSet;
use std::sync::Arc;

/// The partition count of `customers`, which tells its scans from those of
/// `empty`.
const CUSTOMERS_PARTITIONS: usize = 3;

/// The most stages a job of these tests may run: one per table, one that a
/// join plans over `empty` after the first `empty` stage finishes, and the
/// final stage.
const STAGE_RUNS_MAX: usize = 4;

const LEFT_JOIN: &str = "SELECT c.id, e.eid AS other FROM customers c LEFT JOIN empty e ON e.eemail = c.email";

/// Each query reads the finished `empty` stage below a different operator that
/// requires its hash partitioning: a join that `PropagateEmptyExecRule` does
/// not remove because of its embedded projection, a mark join, and a window.
#[rstest]
#[case::left_join(LEFT_JOIN)]
#[case::mark_join(
    "SELECT c.id FROM customers c \
     WHERE c.id > 5 OR EXISTS (SELECT 1 FROM empty e WHERE e.eemail = c.email)"
)]
#[case::window(
    "SELECT eemail, row_number() OVER (PARTITION BY eemail ORDER BY eid) AS rn FROM empty"
)]
#[tokio::test]
async fn job_with_an_empty_input_finishes(
    #[case] sql: &str,
) -> datafusion::error::Result<()> {
    assert_job_finishes(sql).await?;
    Ok(())
}

/// The join reads the finished `empty` stage through a shuffle reader, which
/// keeps the hash partitioning the join was planned against.
#[tokio::test]
async fn left_join_reads_the_empty_stage() -> datafusion::error::Result<()> {
    let stages = assert_job_finishes(LEFT_JOIN).await?;
    let last = stages.last().expect("a finished job ran a stage");

    assert_plan!(last.as_ref(), @r"
    ShuffleWriterExec: partitioning: Hash([email@3], 4)
      ProjectionExec: expr=[id@0 as id, eid@1 as other]
        HashJoinExec: mode=Partitioned, join_type=Right, on=[(eemail@1, email@1)], projection=[id@2, eid@0]
          ShuffleReaderExec: upstream_stage: 2, partitioning: Hash([eemail@1], 4)
          ShuffleReaderExec: upstream_stage: 0, partitioning: Hash([email@1], 4)
    ");
    Ok(())
}

/// A context with `customers` and `empty`, two tables of unknown size, that
/// plans every hash join as partitioned.
fn context() -> SessionContext {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_round_robin_repartition(false)
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            0,
        )
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            0,
        );
    let ctx = SessionContext::new_with_config(config);
    for (name, columns, partitions) in [
        ("customers", ["id", "email"], CUSTOMERS_PARTITIONS),
        ("empty", ["eid", "eemail"], 1),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new(columns[0], DataType::Int64, true),
            Field::new(columns[1], DataType::Utf8, true),
        ]));
        let statistics = Statistics::new_unknown(&schema);
        ctx.register_table(
            name,
            Arc::new(StatsTable::new(schema, statistics, partitions)),
        )
        .unwrap();
    }
    ctx
}

/// Plans `sql`, then finishes runnable stages one at a time, in the order the
/// planner lists them, until the job has none left, and returns the finished
/// stage plans in order.
///
/// A stage that reads `customers` reports 100 rows and any other stage none.
/// Panics when the job runs more than [`STAGE_RUNS_MAX`] stages.
async fn assert_job_finishes(
    sql: &str,
) -> datafusion::error::Result<Vec<Arc<dyn ShuffleWriter>>> {
    let mut planner = AdaptivePlanner::from_logical_sql(&context(), sql, None).await?;
    let mut customers_stages = HashSet::new();
    let mut finished = Vec::new();
    while let Some(stages) = planner.runnable_stages()? {
        assert!(
            finished.len() < STAGE_RUNS_MAX,
            "the job ran {STAGE_RUNS_MAX} stages and can run more:\n{}",
            displayable(planner.current_plan()).indent(false)
        );
        let stage = stages
            .into_iter()
            .next()
            .expect("a job that is not done has a runnable stage");
        let stage_id = stage.plan.stage_id();
        let partitions = if reads_customers(stage.plan.as_ref(), &customers_stages) {
            customers_stages.insert(stage_id);
            mock_partitions_with_size(100, 1000)
        } else {
            mock_partitions_with_statistics_no_data()
        };
        planner.finalise_stage_internal(stage_id, partitions)?;
        finished.push(stage.plan);
    }
    Ok(finished)
}

/// Returns true when `plan` scans `customers` or reads one of
/// `customers_stages`.
fn reads_customers(plan: &dyn ExecutionPlan, customers_stages: &HashSet<usize>) -> bool {
    if let Some(scan) = plan.downcast_ref::<StatsExec>() {
        return scan.properties().output_partitioning().partition_count()
            == CUSTOMERS_PARTITIONS;
    }
    if let Some(reader) = plan.downcast_ref::<ShuffleReaderExec>() {
        return customers_stages.contains(&reader.stage_id);
    }
    plan.children()
        .into_iter()
        .any(|child| reads_customers(child.as_ref(), customers_stages))
}
