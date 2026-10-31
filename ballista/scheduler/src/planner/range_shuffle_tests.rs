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

//! Planning a `RepartitionExec` with `Partitioning::Range`.
//!
//! A range repartition is part of what a plan means: a consumer's partition `i`
//! is expected to hold exactly range `i`. A plan that range-partitions and then
//! sorts each partition is a distributed global sort, and dropping the
//! repartition (which is harmless for a round-robin one) returns partitions
//! that are each sorted but not range-disjoint, with no error.
//!
//! The structural tests pin the stage layout the planner produces. The
//! end-to-end tests run those stages and check the answer, which is the
//! property the layout exists to deliver.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::error::Result;
use ballista_core::execution_plans::{
    ShuffleReaderExec, ShuffleWriter, ShuffleWriterExec, SortShuffleWriterExec,
    compute_global_output_partition_ids,
};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::serde::scheduler::{ExecutorMetadata, PartitionLocation};
use datafusion::arrow::array::{
    Array, AsArray, Int64Array, RecordBatch, StructArray, UInt64Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{
    DataType, Field, Int64Type, Schema, SchemaRef, UInt32Type, UInt64Type,
};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    LexOrdering, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::TryStreamExt;
use tempfile::TempDir;

use super::{
    DefaultDistributedPlanner, DistributedPlanner, find_unresolved_shuffles,
    remove_unresolved_shuffles,
};
use crate::state::execution_graph::{
    ExecutionGraph, ExecutionStage, StaticExecutionGraph, partition_to_location,
};
use crate::state::task_builder::restrict_plan_to_partitions;
use crate::test_utils::revive_graph_and_complete_next_stage;

/// The range key, ascending with nulls last, as Spark's `ORDER BY k` asks for.
fn key_ordering() -> LexOrdering {
    LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("k", 0)),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )])
    .unwrap()
}

fn range_partitioning(split_points: &[i64]) -> Partitioning {
    let split_points = split_points
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

/// `partitions.len()` input partitions, each holding the given `(k, id)` rows.
fn source(partitions: &[Vec<(Option<i64>, i64)>]) -> Arc<dyn ExecutionPlan> {
    let batches: Vec<Vec<RecordBatch>> = partitions
        .iter()
        .map(|rows| {
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            vec![batch]
        })
        .collect();
    let source = MemorySourceConfig::try_new(&batches, schema(), None).unwrap();
    Arc::new(DataSourceExec::new(Arc::new(source)))
}

/// Sorts each partition on its own, the way a range exchange is followed in a
/// distributed `ORDER BY`.
fn sort_each_partition(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortExec::new(key_ordering(), input).with_preserve_partitioning(true))
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
    DefaultDistributedPlanner::new().plan_query_stages(&"job".into(), plan, &options)
}

fn contains_repartition(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<RepartitionExec>() || plan.children().into_iter().any(contains_repartition)
}

#[test]
fn range_repartition_becomes_a_range_routed_shuffle_stage() -> Result<()> {
    let partitioning = range_partitioning(&[100, 200]);
    let plan = sort_each_partition(repartition(
        source(&[vec![], vec![], vec![]]),
        partitioning.clone(),
    ));

    let stages = plan_stages(plan, 3)?;

    assert_eq!(stages.len(), 2, "a boundary at the repartition");

    // The producer routes by the plan's own split points. The repartition is
    // not left behind in it: the writer does the routing, so the rows are not
    // routed twice.
    let producer: Arc<dyn ExecutionPlan> = stages[0].clone();
    let writer = producer
        .downcast_ref::<SortShuffleWriterExec>()
        .expect("the producer stage ends in a sort-shuffle writer");
    assert_eq!(writer.shuffle_output_partitioning(), &partitioning);
    assert!(!contains_repartition(&producer), "{producer:?}");

    // The consumer keeps the operators that sat above the repartition and reads
    // one partition per range, reporting the same range partitioning the
    // repartition did.
    let consumer: Arc<dyn ExecutionPlan> = stages[1].clone();
    assert!(
        consumer.children()[0].is::<SortExec>(),
        "the sort stays in the consumer: {consumer:?}"
    );
    let reads = find_unresolved_shuffles(&consumer)?;
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].stage_id, stages[0].stage_id());
    assert_eq!(reads[0].properties().output_partitioning(), &partitioning);
    assert_eq!(reads[0].output_partition_count, 3);
    Ok(())
}

/// A sort-preserving repartition merges N sorted inputs into each output
/// partition. A shuffle has nowhere to do that, so planning one would return
/// range-disjoint partitions that are no longer sorted. That has to be an error,
/// not a quietly different plan.
#[test]
fn range_repartition_that_must_preserve_order_is_refused() {
    let sorted_input = Arc::new(
        SortExec::new(key_ordering(), source(&[vec![], vec![]]))
            .with_preserve_partitioning(true),
    );
    let repartition = RepartitionExec::try_new(sorted_input, range_partitioning(&[10]))
        .unwrap()
        .with_preserve_order();
    assert!(
        repartition.preserve_order(),
        "the test needs a repartition that really preserves order"
    );

    let err = plan_stages(Arc::new(repartition), 2)
        .expect_err("a shuffle cannot preserve the order of its input")
        .to_string();

    assert!(err.contains("preserve"), "actual: {err}");
}

/// Dropping a round-robin repartition only costs parallelism, so it still goes.
#[test]
fn round_robin_repartition_is_still_dropped() -> Result<()> {
    let plan = repartition(source(&[vec![], vec![]]), Partitioning::RoundRobinBatch(8));

    let stages = plan_stages(plan, 2)?;

    assert_eq!(stages.len(), 1, "round-robin is not a stage boundary");
    let stage: Arc<dyn ExecutionPlan> = stages[0].clone();
    assert!(!contains_repartition(&stage), "{stage:?}");
    Ok(())
}

// --- end to end -----------------------------------------------------------

/// How a stage's input partitions are dealt out to its tasks.
#[derive(Clone, Copy, Debug)]
enum Tasks {
    /// One partition per task.
    PerPartition,
    /// Pairs of neighbouring partitions, the last task taking the remainder.
    Pairs,
    /// One task owning every partition, in descending order: a slice that does
    /// not follow the key order, as a retried partition can leave behind.
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

fn executor() -> ExecutorMetadata {
    ExecutorMetadata {
        id: "executor-0".to_string(),
        host: "localhost".to_string(),
        port: 0,
        grpc_port: 0,
        specification: Default::default(),
        os_info: Default::default(),
    }
}

/// Ship `plan` to an "executor" and back through the real codec.
fn over_the_wire(
    ctx: &TaskContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> =
        BallistaCodec::default();
    let proto =
        PhysicalPlanNode::try_from_physical_plan(plan, codec.physical_extension_codec())?;
    Ok(proto.try_into_physical_plan(ctx, codec.physical_extension_codec())?)
}

/// The shuffle files a writer's summary batches describe.
fn written_partitions(
    batches: &[RecordBatch],
    is_sort_shuffle: bool,
) -> Vec<ShuffleWritePartition> {
    fn counter<'a>(stats: &'a StructArray, name: &str) -> &'a UInt64Array {
        stats
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no `{name}` in the writer's stats"))
            .as_primitive::<UInt64Type>()
    }

    let mut written = vec![];
    for batch in batches {
        let partitions = batch.column(0).as_primitive::<UInt32Type>();
        let file_ids = batch.column(2).as_primitive::<UInt64Type>();
        let stats = batch.column(3).as_struct();
        let (num_rows, num_batches, num_bytes) = (
            counter(stats, "num_rows"),
            counter(stats, "num_batches"),
            counter(stats, "num_bytes"),
        );
        for row in 0..batch.num_rows() {
            written.push(ShuffleWritePartition {
                partition_id: partitions.value(row) as u64,
                num_batches: num_batches.value(row),
                num_rows: num_rows.value(row),
                num_bytes: num_bytes.value(row),
                file_id: file_ids.is_valid(row).then(|| file_ids.value(row)),
                is_sort_shuffle,
            });
        }
    }
    // A slot that produced nothing is a sentinel, not a file to fetch.
    written.retain(|partition| partition.num_rows > 0);
    written
}

/// Runs one task of a stage the way the scheduler and an executor between them
/// would, short of the network: restrict the plan to the task's partitions,
/// send it over the wire, point its readers at the work dir, give its writer the
/// task's identity, and drive every output partition at once.
async fn run_task(
    stage: &Arc<dyn ExecutionPlan>,
    job_id: &JobId,
    stage_id: usize,
    task_id: usize,
    slice: &[usize],
    work_dir: &Path,
) -> Result<Vec<ShuffleWritePartition>> {
    let work_dir_str = work_dir.to_str().unwrap().to_string();
    let ctx = SessionContext::new();

    // Scheduler.
    let restricted = restrict_plan_to_partitions(Arc::clone(stage), slice)?;
    let global_output_partition_ids = compute_global_output_partition_ids(stage, slice);

    // Wire.
    let plan = over_the_wire(&ctx.task_ctx(), restricted)?;

    // Executor.
    let plan = plan
        .transform(|node| {
            Ok(match node.downcast_ref::<ShuffleReaderExec>() {
                Some(reader) => {
                    Transformed::yes(Arc::new(reader.with_work_dir(work_dir_str.clone()))
                        as Arc<dyn ExecutionPlan>)
                }
                None => Transformed::no(node),
            })
        })?
        .data;
    let input = Arc::clone(plan.children()[0]);
    let (writer, is_sort_shuffle): (Arc<dyn ExecutionPlan>, bool) =
        match plan.downcast_ref::<SortShuffleWriterExec>() {
            Some(sort) => (
                Arc::new(
                    SortShuffleWriterExec::try_new(
                        job_id.clone(),
                        stage_id,
                        input,
                        work_dir_str,
                        sort.shuffle_output_partitioning().clone(),
                        sort.config().clone(),
                    )?
                    .with_task_id(task_id)
                    .with_global_output_partition_ids(global_output_partition_ids),
                ),
                true,
            ),
            None => (
                Arc::new(
                    ShuffleWriterExec::try_new(
                        job_id.clone(),
                        stage_id,
                        input,
                        work_dir_str,
                    )?
                    .with_task_id(task_id)
                    .with_global_output_partition_ids(global_output_partition_ids),
                ),
                false,
            ),
        };

    // Every output partition is driven at once: the writers' coordinators only
    // run once each partition's receiver has been taken.
    let output_partitions = writer.properties().output_partitioning().partition_count();
    let drains: Vec<_> = (0..output_partitions)
        .map(|partition| {
            let writer = Arc::clone(&writer);
            let task_ctx = ctx.task_ctx();
            tokio::spawn(async move {
                writer
                    .execute(partition, task_ctx)?
                    .try_collect::<Vec<_>>()
                    .await
            })
        })
        .collect();
    let mut written = vec![];
    for drain in drains {
        let batches = drain.await.expect("drain task panicked")?;
        written.extend(written_partitions(&batches, is_sort_shuffle));
    }
    Ok(written)
}

/// Runs a job's stages in order, dealing each stage's input partitions out to
/// tasks as `tasks` says, and returns the final stage's output partitions.
async fn run_job(
    stages: &[Arc<dyn ShuffleWriter>],
    tasks: Tasks,
    work_dir: &Path,
) -> Result<Vec<Vec<RecordBatch>>> {
    let job_id: JobId = "job".into();
    let mut locations: HashMap<usize, HashMap<usize, Vec<PartitionLocation>>> =
        HashMap::new();
    let mut final_output = vec![];

    for (position, stage) in stages.iter().enumerate() {
        let stage_id = stage.stage_id();
        let upstream: Arc<dyn ExecutionPlan> = stage.clone();
        let resolved = remove_unresolved_shuffles(upstream, &locations)?;
        let input_partitions = resolved.children()[0]
            .properties()
            .output_partitioning()
            .partition_count();

        let mut by_partition: HashMap<usize, Vec<PartitionLocation>> = HashMap::new();
        for (task_id, slice) in tasks.slices(input_partitions).iter().enumerate() {
            let written =
                run_task(&resolved, &job_id, stage_id, task_id, slice, work_dir).await?;
            for location in
                partition_to_location(&job_id, task_id, stage_id, &executor(), written)
            {
                by_partition
                    .entry(location.partition_id.partition_id)
                    .or_default()
                    .push(location);
            }
        }

        if position + 1 == stages.len() {
            // Read the final stage back as a client would.
            let output_partitions = resolved
                .properties()
                .output_partitioning()
                .partition_count();
            let reader = ShuffleReaderExec::try_new(
                stage_id,
                (0..output_partitions)
                    .map(|p| by_partition.remove(&p).unwrap_or_default())
                    .collect(),
                resolved.schema(),
                Partitioning::UnknownPartitioning(output_partitions),
            )?
            .with_work_dir(work_dir.to_str().unwrap().to_string());
            let ctx = SessionContext::new();
            for partition in 0..output_partitions {
                final_output.push(
                    reader
                        .execute(partition, ctx.task_ctx())?
                        .try_collect::<Vec<_>>()
                        .await?,
                );
            }
        } else {
            locations.insert(stage_id, by_partition);
        }
    }
    Ok(final_output)
}

/// `(k, id)` for every row of one output partition, in the order it was read.
fn rows_of(partition: &[RecordBatch]) -> Vec<(Option<i64>, i64)> {
    partition
        .iter()
        .flat_map(|batch| {
            let keys = batch.column(0).as_primitive::<Int64Type>();
            let ids = batch.column(1).as_primitive::<Int64Type>();
            (0..batch.num_rows())
                .map(|row| (keys.is_valid(row).then(|| keys.value(row)), ids.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Deterministic keys over `0..300` with a null every 17th row, spread over
/// `partitions` input partitions so that every one of them holds rows for every
/// range.
fn input_rows(
    partitions: usize,
    rows_per_partition: usize,
) -> Vec<Vec<(Option<i64>, i64)>> {
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    (0..partitions)
        .map(|p| {
            (0..rows_per_partition)
                .map(|i| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let id = (p * rows_per_partition + i) as i64;
                    let key = (i % 17 != 16).then_some(((state >> 33) % 300) as i64);
                    (key, id)
                })
                .collect()
        })
        .collect()
}

/// The distributed global sort the issue describes, run for real: range
/// partition on `k`, sort each partition. Concatenating the output partitions
/// in order must give every input row exactly once, in key order.
async fn assert_global_sort_is_correct(tasks: Tasks) -> Result<()> {
    let split_points = [100, 200];
    let input = input_rows(3, 60);
    let plan = sort_each_partition(repartition(
        source(&input),
        range_partitioning(&split_points),
    ));
    let stages = plan_stages(plan, 3)?;
    let work_dir = TempDir::new()?;

    let output = run_job(&stages, tasks, work_dir.path()).await?;

    assert_eq!(output.len(), split_points.len() + 1, "{tasks:?}");

    let mut all_ids = BTreeSet::new();
    let mut concatenated = vec![];
    for (partition, batches) in output.iter().enumerate() {
        let rows = rows_of(batches);
        for (key, id) in &rows {
            // Range `p` holds the keys from its lower split point up to, but
            // not including, its upper one; nulls sort last.
            let expected = match key {
                None => split_points.len(),
                Some(k) => split_points.iter().filter(|&&s| *k >= s).count(),
            };
            assert_eq!(
                partition, expected,
                "{tasks:?}: key {key:?} (row {id}) is in partition {partition}"
            );
            assert!(all_ids.insert(*id), "{tasks:?}: row {id} came back twice");
        }
        assert!(
            rows.windows(2)
                .all(|pair| sort_key(pair[0].0) <= sort_key(pair[1].0)),
            "{tasks:?}: partition {partition} is not sorted: {rows:?}"
        );
        concatenated.extend(rows);
    }

    assert_eq!(
        all_ids.len(),
        input.iter().map(Vec::len).sum::<usize>(),
        "{tasks:?}: every input row must come back"
    );
    assert!(
        concatenated
            .windows(2)
            .all(|pair| sort_key(pair[0].0) <= sort_key(pair[1].0)),
        "{tasks:?}: the partitions, read in order, are not globally sorted"
    );
    Ok(())
}

/// Ascending with nulls last.
fn sort_key(key: Option<i64>) -> (bool, i64) {
    (key.is_none(), key.unwrap_or_default())
}

#[tokio::test]
async fn range_repartition_then_sort_is_a_correct_global_sort() -> Result<()> {
    assert_global_sort_is_correct(Tasks::PerPartition).await
}

/// Tasks that own several partitions at once, on both sides of the shuffle.
#[tokio::test]
async fn global_sort_is_correct_when_tasks_own_several_partitions() -> Result<()> {
    assert_global_sort_is_correct(Tasks::Pairs).await
}

/// A task whose partitions do not follow the key order: its range reader has no
/// split points that describe it, and has to say less rather than fail.
#[tokio::test]
async fn global_sort_is_correct_when_a_tasks_partitions_are_out_of_key_order()
-> Result<()> {
    assert_global_sort_is_correct(Tasks::AllDescending).await
}

/// Nothing to sort is not an error: every range comes back, empty.
#[tokio::test]
async fn global_sort_of_an_empty_input_yields_an_empty_partition_per_range() -> Result<()>
{
    let plan = sort_each_partition(repartition(
        source(&[vec![], vec![], vec![]]),
        range_partitioning(&[100, 200]),
    ));
    let stages = plan_stages(plan, 3)?;
    let work_dir = TempDir::new()?;

    let output = run_job(&stages, Tasks::PerPartition, work_dir.path()).await?;

    assert_eq!(output.len(), 3);
    assert!(output.iter().all(|partition| rows_of(partition).is_empty()));
    Ok(())
}

/// Every row in one range leaves the others empty, and still correct.
#[tokio::test]
async fn global_sort_of_input_that_falls_in_a_single_range_is_correct() -> Result<()> {
    let rows: Vec<Vec<(Option<i64>, i64)>> = (0..3)
        .map(|p| (0..20).map(|i| (Some(250 - i), p * 20 + i)).collect())
        .collect();
    let plan =
        sort_each_partition(repartition(source(&rows), range_partitioning(&[100, 200])));
    let stages = plan_stages(plan, 3)?;
    let work_dir = TempDir::new()?;

    let output = run_job(&stages, Tasks::Pairs, work_dir.path()).await?;

    let sizes: Vec<usize> = output.iter().map(|p| rows_of(p).len()).collect();
    assert_eq!(sizes, vec![0, 0, 60], "all keys are 231..=250");
    let keys: Vec<_> = rows_of(&output[2]).into_iter().map(|(k, _)| k).collect();
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

/// The scheduler's own state machine, rather than the loop above: once the
/// producer stage is done, its consumer resolves into a reader with one
/// partition per range, and is scheduled as that many partitions.
#[tokio::test]
async fn consumer_of_a_range_shuffle_resolves_to_one_partition_per_range() -> Result<()> {
    let partitioning = range_partitioning(&[100, 200]);
    let plan = sort_each_partition(repartition(
        source(&[vec![], vec![], vec![]]),
        partitioning.clone(),
    ));
    let mut graph = StaticExecutionGraph::new(
        "localhost:50050",
        &"job".into(),
        "",
        "session",
        plan,
        0,
        Arc::new(SessionConfig::new_with_ballista()),
        &mut DefaultDistributedPlanner::new(),
        None,
    )?;

    // The consumer cannot resolve before the producer has finished.
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
