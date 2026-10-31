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

//! A range `RepartitionExec`: the stages it plans to, and a distributed global
//! sort run end to end.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use ballista_core::assert_plan;
use ballista_core::error::Result;
use ballista_core::execution_plans::{
    ShuffleReaderExec, ShuffleWriter, compute_global_output_partition_ids,
};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::serde::scheduler::PartitionLocation;
use ballista_executor::execution_engine::{DefaultExecutionEngine, ExecutionEngine};
use datafusion::arrow::array::{AsArray, Int64Array, RecordBatch};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    LexOrdering, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::collect_partitioned;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use tempfile::TempDir;

use super::test::roundtrip_operator;
use super::{DefaultDistributedPlanner, DistributedPlanner, remove_unresolved_shuffles};
use crate::state::execution_graph::{
    ExecutionGraph, ExecutionStage, StaticExecutionGraph, partition_to_location,
};
use crate::state::task_builder::restrict_plan_to_partitions;
use crate::test_utils::{mock_executor, revive_graph_and_complete_next_stage};

const JOB: &str = "job";
/// The sort key `k` is split into three ranges by these.
const SPLIT_POINTS: [i64; 2] = [100, 200];

/// `k` ascending, nulls last.
fn key_ordering() -> LexOrdering {
    LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("k", 0)),
        SortOptions::new(false, false),
    )])
    .unwrap()
}

fn range_partitioning() -> Partitioning {
    let split_points = SPLIT_POINTS
        .iter()
        .map(|&v| SplitPoint::new(vec![ScalarValue::Int64(Some(v))]))
        .collect();
    Partitioning::Range(RangePartitioning::try_new(key_ordering(), split_points).unwrap())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("id", DataType::Int64, false),
    ]))
}

/// `(k, id)` rows; `id` makes each row identifiable after a shuffle.
type Rows = Vec<(Option<i64>, i64)>;

/// One input partition per entry.
fn source(partitions: &[Rows]) -> Arc<dyn ExecutionPlan> {
    let batches: Vec<Vec<RecordBatch>> = partitions
        .iter()
        .map(|rows| {
            let keys = Int64Array::from_iter(rows.iter().map(|(k, _)| *k));
            let ids = Int64Array::from_iter_values(rows.iter().map(|(_, id)| *id));
            vec![
                RecordBatch::try_new(schema(), vec![Arc::new(keys), Arc::new(ids)])
                    .unwrap(),
            ]
        })
        .collect();
    MemorySourceConfig::try_new_exec(&batches, schema(), None).unwrap()
}

/// A distributed `ORDER BY k`: range-partition, then sort each partition.
fn global_sort_plan(
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
) -> Arc<dyn ExecutionPlan> {
    let repartitioned = repartition(input, partitioning);
    Arc::new(
        SortExec::new(key_ordering(), repartitioned).with_preserve_partitioning(true),
    )
}

fn repartition(
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, partitioning).unwrap())
}

fn plan_stages(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Vec<Arc<dyn ShuffleWriter>>> {
    let mut options = ConfigOptions::new();
    options.execution.target_partitions = target_partitions;
    DefaultDistributedPlanner::new().plan_query_stages(&JOB.into(), plan, &options)
}

#[test]
fn range_repartition_becomes_a_range_routed_shuffle_stage() -> Result<()> {
    let plan = global_sort_plan(source(&[vec![], vec![], vec![]]), range_partitioning());

    let stages = plan_stages(plan, 3)?;

    assert_eq!(stages.len(), 2, "a boundary at the repartition");
    assert_plan!(stages[0].as_ref(), @r"
    SortShuffleWriterExec: partitioning=Range([k@0 ASC NULLS LAST], [(100), (200)], 3)
      DataSourceExec: partitions=3, partition_sizes=[1, 1, 1]
    ");
    assert_plan!(stages[1].as_ref(), @r"
    ShuffleWriterExec: partitioning: Range([k@0 ASC NULLS LAST], [(100), (200)], 3)
      SortExec: expr=[k@0 ASC NULLS LAST], preserve_partitioning=[true]
        UnresolvedShuffleExec: stage=1, partitioning: Range([k@0 ASC NULLS LAST], [(100), (200)], 3)
    ");
    Ok(())
}

/// A shuffle cannot merge sorted inputs, so this has to be an error rather than
/// partitions that are range-disjoint but no longer sorted.
#[test]
fn range_repartition_that_must_preserve_order_is_refused() {
    let sorted_input = Arc::new(
        SortExec::new(key_ordering(), source(&[vec![], vec![]]))
            .with_preserve_partitioning(true),
    );
    let repartition = RepartitionExec::try_new(sorted_input, range_partitioning())
        .unwrap()
        .with_preserve_order();
    assert!(repartition.preserve_order(), "setup: must preserve order");

    let err = plan_stages(Arc::new(repartition), 2)
        .expect_err("a shuffle cannot preserve the order of its input")
        .to_string();

    assert!(err.contains("preserve"), "actual: {err}");
}

#[test]
fn round_robin_repartition_is_still_dropped() -> Result<()> {
    let plan = repartition(source(&[vec![], vec![]]), Partitioning::RoundRobinBatch(8));

    let stages = plan_stages(plan, 2)?;

    assert_eq!(stages.len(), 1, "round-robin is not a stage boundary");
    assert_plan!(stages[0].as_ref(), @r"
    ShuffleWriterExec: partitioning: UnknownPartitioning(2)
      DataSourceExec: partitions=2, partition_sizes=[1, 1]
    ");
    Ok(())
}

// --- end to end -----------------------------------------------------------

/// How a stage's input partitions are dealt out to its tasks.
#[derive(Clone, Copy)]
enum Tasks {
    PerPartition,
    /// Neighbouring pairs, the last task taking the remainder.
    Pairs,
    /// One task owning every partition, in descending order.
    AllDescending,
}

impl Tasks {
    fn slices(self, partitions: usize) -> Vec<Vec<usize>> {
        let all: Vec<usize> = (0..partitions).collect();
        match self {
            Tasks::PerPartition => all.into_iter().map(|p| vec![p]).collect(),
            Tasks::Pairs => all.chunks(2).map(<[usize]>::to_vec).collect(),
            Tasks::AllDescending => vec![all.into_iter().rev().collect()],
        }
    }
}

/// Runs one task of `stage` as the scheduler and an executor would, short of the
/// network.
async fn run_task(
    stage: &Arc<dyn ExecutionPlan>,
    stage_id: usize,
    task_id: usize,
    slice: &[usize],
    work_dir: &Path,
) -> Result<Vec<ShuffleWritePartition>> {
    let ctx = SessionContext::new();
    let restricted = restrict_plan_to_partitions(Arc::clone(stage), slice)?;
    let output_ids = compute_global_output_partition_ids(stage, slice);
    let plan = roundtrip_operator(&ctx.task_ctx(), restricted)?;
    let task = DefaultExecutionEngine::new().create_query_stage_exec(
        JOB.into(),
        stage_id,
        task_id,
        output_ids,
        plan,
        work_dir.to_str().unwrap(),
        &SessionConfig::new_with_ballista(),
    )?;
    Ok(task.execute_query_stage(task_id, ctx.task_ctx()).await?)
}

/// Runs every task of a stage, dealing its input partitions out as `tasks` says,
/// and returns where each output partition was written.
async fn run_stage(
    stage: &Arc<dyn ExecutionPlan>,
    stage_id: usize,
    tasks: Tasks,
    work_dir: &Path,
) -> Result<HashMap<usize, Vec<PartitionLocation>>> {
    let input_partitions = stage.children()[0]
        .properties()
        .output_partitioning()
        .partition_count();
    let executor = mock_executor("executor-0".to_string());
    let mut locations: HashMap<usize, Vec<PartitionLocation>> = HashMap::new();
    for (task_id, slice) in tasks.slices(input_partitions).iter().enumerate() {
        let written = run_task(stage, stage_id, task_id, slice, work_dir).await?;
        for location in
            partition_to_location(&JOB.into(), task_id, stage_id, &executor, written)
        {
            locations
                .entry(location.partition_id.partition_id)
                .or_default()
                .push(location);
        }
    }
    Ok(locations)
}

/// Runs a job's stages in order and returns the final stage's output partitions.
async fn run_job(
    stages: &[Arc<dyn ShuffleWriter>],
    tasks: Tasks,
    work_dir: &Path,
) -> Result<Vec<Vec<RecordBatch>>> {
    let (last, earlier) = stages.split_last().expect("a job has a stage");
    let mut inputs = HashMap::new();
    for stage in earlier {
        let resolved = remove_unresolved_shuffles(stage.clone(), &inputs)?;
        let locations = run_stage(&resolved, stage.stage_id(), tasks, work_dir).await?;
        inputs.insert(stage.stage_id(), locations);
    }
    let resolved = remove_unresolved_shuffles(last.clone(), &inputs)?;
    let mut locations = run_stage(&resolved, last.stage_id(), tasks, work_dir).await?;

    // Read the final stage back as a client would.
    let partitions = resolved
        .properties()
        .output_partitioning()
        .partition_count();
    let reader = ShuffleReaderExec::try_new(
        last.stage_id(),
        (0..partitions)
            .map(|p| locations.remove(&p).unwrap_or_default())
            .collect(),
        resolved.schema(),
        Partitioning::UnknownPartitioning(partitions),
    )?
    .with_work_dir(work_dir.to_str().unwrap().to_string());
    Ok(collect_partitioned(Arc::new(reader), SessionContext::new().task_ctx()).await?)
}

/// Runs [`global_sort_plan`] over `input` and returns the rows of each output
/// partition, in the order they were read.
async fn global_sort(input: &[Rows], tasks: Tasks) -> Result<Vec<Rows>> {
    let plan = global_sort_plan(source(input), range_partitioning());
    let stages = plan_stages(plan, input.len())?;
    let work_dir = TempDir::new()?;

    let output = run_job(&stages, tasks, work_dir.path()).await?;

    Ok(output
        .iter()
        .map(|partition| {
            partition
                .iter()
                .flat_map(|batch| {
                    let keys = batch.column(0).as_primitive::<Int64Type>();
                    let ids = batch.column(1).as_primitive::<Int64Type>();
                    keys.iter().zip(ids.values().iter().copied())
                })
                .collect()
        })
        .collect())
}

/// Keys spread over `0..300`, with a null every 17th row, so that every input
/// partition holds rows for every range.
fn input_rows(partitions: usize, rows_per_partition: usize) -> Vec<Rows> {
    (0..partitions)
        .map(|p| {
            (0..rows_per_partition)
                .map(|i| {
                    let key = (i % 17 != 16).then_some(((p * 31 + i * 97) % 300) as i64);
                    (key, (p * rows_per_partition + i) as i64)
                })
                .collect()
        })
        .collect()
}

/// Ascending with nulls last.
fn sort_key(key: Option<i64>) -> (bool, i64) {
    (key.is_none(), key.unwrap_or_default())
}

/// Every input row comes back exactly once, in the partition for its key, and
/// the partitions read in order are globally sorted.
async fn assert_global_sort_is_correct(tasks: Tasks) -> Result<()> {
    let input = input_rows(3, 60);

    let output = global_sort(&input, tasks).await?;

    assert_eq!(output.len(), SPLIT_POINTS.len() + 1);
    let mut ids = BTreeSet::new();
    let mut keys = vec![];
    for (partition, rows) in output.iter().enumerate() {
        for &(key, id) in rows {
            // Nulls sort last, so they belong to the final range.
            let expected = key.map_or(SPLIT_POINTS.len(), |k| {
                SPLIT_POINTS.iter().filter(|&&split| k >= split).count()
            });
            assert_eq!(partition, expected, "key {key:?} (row {id})");
            assert!(ids.insert(id), "row {id} came back twice");
            keys.push(key);
        }
    }
    assert_eq!(
        ids.len(),
        input.iter().map(Vec::len).sum::<usize>(),
        "every row must come back"
    );
    assert!(
        keys.windows(2)
            .all(|pair| sort_key(pair[0]) <= sort_key(pair[1])),
        "not globally sorted"
    );
    Ok(())
}

#[tokio::test]
async fn range_repartition_then_sort_is_a_correct_global_sort() -> Result<()> {
    assert_global_sort_is_correct(Tasks::PerPartition).await
}

#[tokio::test]
async fn global_sort_is_correct_when_tasks_own_several_partitions() -> Result<()> {
    assert_global_sort_is_correct(Tasks::Pairs).await
}

/// The range reader has no split points for a slice out of key order, and has to
/// say less rather than fail.
#[tokio::test]
async fn global_sort_is_correct_when_a_tasks_partitions_are_out_of_key_order()
-> Result<()> {
    assert_global_sort_is_correct(Tasks::AllDescending).await
}

#[tokio::test]
async fn global_sort_of_an_empty_input_yields_an_empty_partition_per_range() -> Result<()>
{
    let output = global_sort(&[vec![], vec![], vec![]], Tasks::PerPartition).await?;

    assert_eq!(output, vec![Rows::new(); 3]);
    Ok(())
}

#[tokio::test]
async fn global_sort_of_input_that_falls_in_a_single_range_is_correct() -> Result<()> {
    // Every key is in 231..=250, the last range.
    let input: Vec<Rows> = (0..3)
        .map(|p| (0..20).map(|i| (Some(250 - i), p * 20 + i)).collect())
        .collect();

    let output = global_sort(&input, Tasks::Pairs).await?;

    let sizes: Vec<usize> = output.iter().map(Vec::len).collect();
    assert_eq!(sizes, vec![0, 0, 60]);
    let keys: Vec<_> = output[2].iter().map(|&(key, _)| key).collect();
    assert!(keys.windows(2).all(|pair| pair[0] <= pair[1]), "{keys:?}");
    Ok(())
}

// --- through the execution graph --------------------------------------------

fn find_reader(plan: &dyn ExecutionPlan) -> Option<&ShuffleReaderExec> {
    plan.downcast_ref::<ShuffleReaderExec>().or_else(|| {
        plan.children()
            .into_iter()
            .find_map(|child| find_reader(child.as_ref()))
    })
}

/// Once the producer stage is done, the scheduler resolves its consumer into a
/// reader with one partition per range.
#[tokio::test]
async fn consumer_of_a_range_shuffle_resolves_to_one_partition_per_range() -> Result<()> {
    let partitioning = range_partitioning();
    let plan = global_sort_plan(source(&[vec![], vec![], vec![]]), partitioning.clone());
    let mut graph = StaticExecutionGraph::new(
        "localhost:50050",
        &JOB.into(),
        "",
        "session",
        plan,
        0,
        Arc::new(SessionConfig::new_with_ballista()),
        &mut DefaultDistributedPlanner::new(),
        None,
    )?;

    revive_graph_and_complete_next_stage(&mut graph)?;
    graph.revive();

    let running: Vec<_> = graph
        .stages()
        .values()
        .filter_map(|stage| match stage {
            ExecutionStage::Running(running) => Some(running),
            _ => None,
        })
        .collect();
    assert_eq!(running.len(), 1, "only the consumer is left to run");
    assert_eq!(running[0].partitions, 3, "one per range");
    let reader = find_reader(running[0].plan.as_ref())
        .expect("the consumer's placeholder was resolved to a reader");
    assert_eq!(reader.partition.len(), 3);
    assert_eq!(reader.properties().output_partitioning(), &partitioning);
    Ok(())
}
