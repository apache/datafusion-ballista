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

//! A partition-id shuffle end to end, without a network: the scheduler's planner
//! splits a plan containing a [`PartitionIdRepartitionExec`] into stages, the
//! executor's engine runs the first, and the second's reader is resolved against
//! what it wrote. Partition `i` must hold exactly the rows whose id was `i`, and
//! nothing of the id column.

use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::execution_plans::{
    PartitionIdRepartitionExec, ShuffleReaderExec, UnresolvedShuffleExec,
};
use ballista_core::serde::protobuf::ShuffleWritePartition;
use ballista_core::serde::scheduler::{
    ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    PartitionId, PartitionLocation, PartitionStats,
};
use ballista_executor::execution_engine::{DefaultExecutionEngine, ExecutionEngine};
use ballista_scheduler::planner::{
    DefaultDistributedPlanner, DistributedPlanner, find_unresolved_shuffles,
    remove_unresolved_shuffles,
};
use datafusion::arrow::array::{AsArray, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use tempfile::TempDir;

const PARTITIONS: usize = 5;
const ROWS: i64 = 600;

/// The client's placement: a residue class, which no hash of the value produces.
fn client_partition(v: i64) -> i32 {
    (v % PARTITIONS as i64) as i32
}

/// `(v: Int64, pid: Int32, w: Utf8)` over three input partitions of several
/// batches each.
fn input() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("pid", DataType::Int32, false),
        Field::new("w", DataType::Utf8, true),
    ]));
    let per_partition = ROWS / 3;
    let partitions: Vec<Vec<RecordBatch>> = (0..3)
        .map(|p| {
            (0..4)
                .map(|b| {
                    let batch_rows = per_partition / 4;
                    let start = p * per_partition + b * batch_rows;
                    let values: Vec<i64> = (start..start + batch_rows).collect();
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(values.clone())),
                            Arc::new(Int32Array::from(
                                values
                                    .iter()
                                    .map(|v| client_partition(*v))
                                    .collect::<Vec<_>>(),
                            )),
                            Arc::new(StringArray::from(
                                values
                                    .iter()
                                    .map(|v| format!("w{v}"))
                                    .collect::<Vec<_>>(),
                            )),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect();
    MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap()
}

fn executor() -> ExecutorMetadata {
    ExecutorMetadata {
        id: "executor-1".to_string(),
        host: "localhost".to_string(),
        port: 0,
        grpc_port: 0,
        specification: ExecutorSpecification::default().with_vcores(1),
        os_info: ExecutorOperatingSystemSpecification::default(),
    }
}

/// The first stage's output locations, keyed for `remove_unresolved_shuffles`:
/// stage, then output partition.
fn locations(
    job_id: &JobId,
    stage_id: usize,
    written: Vec<ShuffleWritePartition>,
) -> HashMap<usize, HashMap<usize, Vec<PartitionLocation>>> {
    let mut by_partition: HashMap<usize, Vec<PartitionLocation>> = HashMap::new();
    for shuffle in written {
        by_partition
            .entry(shuffle.partition_id as usize)
            .or_default()
            .push(PartitionLocation {
                map_partition_id: 0,
                partition_id: PartitionId {
                    job_id: job_id.clone(),
                    stage_id,
                    partition_id: shuffle.partition_id as usize,
                },
                executor_meta: executor(),
                partition_stats: PartitionStats::new(
                    Some(shuffle.num_rows),
                    Some(shuffle.num_batches),
                    Some(shuffle.num_bytes),
                ),
                file_id: shuffle.file_id,
                is_sort_shuffle: shuffle.is_sort_shuffle,
            });
    }
    HashMap::from([(stage_id, by_partition)])
}

#[tokio::test]
async fn partition_id_shuffle_places_every_row_in_the_partition_it_named() {
    let job_id: JobId = "partition-id-e2e".into();
    let work_dir = TempDir::new().unwrap();
    let work_dir = work_dir.path().to_str().unwrap();

    // Scheduler: plan `Projection(v, w) <- PartitionIdRepartition <- input`.
    let marker =
        Arc::new(PartitionIdRepartitionExec::try_new(input(), 1, PARTITIONS).unwrap());
    let schema = marker.schema();
    let consumer: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![
                ProjectionExpr::new(col("v", &schema).unwrap(), "v".to_string()),
                ProjectionExpr::new(col("w", &schema).unwrap(), "w".to_string()),
            ],
            marker,
        )
        .unwrap(),
    );
    let stages = DefaultDistributedPlanner::new()
        .plan_query_stages(&job_id, consumer, &ConfigOptions::default())
        .unwrap();
    assert_eq!(stages.len(), 2);

    // Executor: run the first stage's task over all three input partitions.
    let producer: Arc<dyn ExecutionPlan> = stages[0].clone();
    let stage_id = stages[0].stage_id();
    let task = DefaultExecutionEngine::new()
        .create_query_stage_exec(
            job_id.clone(),
            stage_id,
            0,
            vec![0, 1, 2],
            producer,
            work_dir,
            &SessionConfig::new(),
        )
        .unwrap();
    let written = task
        .execute_query_stage(0, SessionContext::new().task_ctx())
        .await
        .unwrap();
    // One per written column: the id column has none.
    assert_eq!(task.collect_column_stats().len(), 2);

    // Scheduler again: hand the first stage's locations to the second stage.
    let consumer_stage: Arc<dyn ExecutionPlan> = stages[1].clone();
    let unresolved = find_unresolved_shuffles(&consumer_stage).unwrap();
    let [
        UnresolvedShuffleExec {
            stage_id: input_stage,
            ..
        },
    ] = unresolved.as_slice()
    else {
        panic!("the second stage should read exactly one shuffle: {unresolved:?}");
    };
    assert_eq!(*input_stage, stage_id);
    let resolved = remove_unresolved_shuffles(
        consumer_stage,
        &locations(&job_id, stage_id, written),
    )
    .unwrap();

    // Executor: what reads the shuffle reads local files under the work dir.
    let resolved = resolved
        .transform(|plan| {
            Ok(match plan.downcast_ref::<ShuffleReaderExec>() {
                Some(reader) => {
                    Transformed::yes(Arc::new(reader.with_work_dir(work_dir.to_string()))
                        as Arc<dyn ExecutionPlan>)
                }
                None => Transformed::no(plan),
            })
        })
        .unwrap()
        .data;
    // The stage's own writer sits at the top; what it reads is its child.
    let reads = Arc::clone(resolved.children()[0]);
    assert_eq!(reads.output_partitioning().partition_count(), PARTITIONS);

    let ctx = SessionContext::new().task_ctx();
    let mut total = 0;
    for partition in 0..PARTITIONS {
        let batches: Vec<RecordBatch> = reads
            .execute(partition, ctx.clone())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut values: Vec<i64> = Vec::new();
        for batch in &batches {
            let names: Vec<_> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            assert_eq!(names, ["v", "w"], "partition {partition}");
            values.extend(batch.column(0).as_primitive::<Int64Type>().values());
        }
        values.sort();

        let expected: Vec<i64> = (0..ROWS)
            .filter(|v| client_partition(*v) as usize == partition)
            .collect();
        assert_eq!(values, expected, "partition {partition}");
        total += values.len();
    }
    assert_eq!(total as i64, ROWS);
}
