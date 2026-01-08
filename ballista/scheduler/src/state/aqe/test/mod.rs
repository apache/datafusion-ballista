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
/// Tests if plan is going to be split to stages correctly
mod plan_to_stages;

use ballista_core::serde::scheduler::{
    ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionLocation,
    PartitionStats,
};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

pub(crate) fn mock_partitions_with_statistics() -> Vec<Vec<PartitionLocation>> {
    let location = PartitionLocation {
        // next few properties are generic values
        map_partition_id: 0,
        partition_id: PartitionId {
            job_id: "".to_string(),
            stage_id: 0,
            partition_id: 0,
        },
        executor_meta: ExecutorMetadata {
            id: "".to_string(),
            host: "".to_string(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification { task_slots: 0 },
        },
        path: "".to_string(),
        // next few properties are needed
        partition_stats: PartitionStats::new(Some(42), None, Some(10)),
    };
    vec![vec![location]]
}

pub(crate) fn mock_partitions_with_statistics_no_data() -> Vec<Vec<PartitionLocation>> {
    let location = PartitionLocation {
        // next few properties are generic values
        map_partition_id: 0,
        partition_id: PartitionId {
            job_id: "".to_string(),
            stage_id: 0,
            partition_id: 0,
        },
        executor_meta: ExecutorMetadata {
            id: "".to_string(),
            host: "".to_string(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification { task_slots: 0 },
        },
        path: "".to_string(),
        // next few properties are needed
        partition_stats: PartitionStats::new(Some(0), None, Some(0)),
    };
    vec![vec![location]]
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
    let config = SessionConfig::new().with_target_partitions(2);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    SessionContext::new_with_state(state)
}
