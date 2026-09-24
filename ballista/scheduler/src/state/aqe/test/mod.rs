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

/// Test if stages can be added or removed
mod alter_stages;
/// Broadcast-threshold decisions over declared statistics
mod broadcast_thresholds;
/// Functional tests for the CoalescePartitionsRule end-to-end through the planner
mod coalesce_rule;
/// Job-failure lifecycle tests for the adaptive graph
mod job_failure;
/// covers join selection tests
mod join_selection;
/// Tests if plan is going to be split to stages correctly
mod plan_to_stages;
/// Regression tests for range-repartition planning end-to-end
/// through `AdaptivePlanner` (DER → routing park → filter injection).
mod range_repartition;
/// Multi-pass coverage for staging a join's build side before deciding the join
mod stage_build_side;
/// A table whose statistics are declared rather than measured
mod stats_table;

use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::scheduler::{
    ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
    PartitionId, PartitionLocation, PartitionStats,
};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::Statistics;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use stats_table::StatsTable;
use std::sync::Arc;

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
    let state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .build();
    SessionContext::new_with_state(state)
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
    let location = PartitionLocation {
        // next few properties are generic values
        map_partition_id: 0,
        partition_id: PartitionId {
            job_id: "".into(),
            stage_id: 0,
            partition_id: 0,
        },
        executor_meta: ExecutorMetadata {
            id: "".to_string(),
            host: "".to_string(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification::default().with_vcores(0),
            os_info: ExecutorOperatingSystemSpecification::default(),
        },
        // next few properties are needed
        partition_stats: PartitionStats::new(Some(num_rows), None, Some(num_bytes)),
        file_id: None,
        is_sort_shuffle: false,
    };
    vec![vec![location]]
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

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    SessionContext::new_with_state(state)
}
