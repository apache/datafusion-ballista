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

//! Fixtures shared by the unit tests of several execution plans.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{
    Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;

use crate::error::BallistaError;
use crate::serde::scheduler::{
    ExecutorMetadata, ExecutorOperatingSystemSpecification, ExecutorSpecification,
};
use crate::utils;

/// Two partitions of two identical batches of `(a UInt32, b Utf8)`.
pub(crate) fn two_by_two_input() -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![Some(1), Some(3)])),
            Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
        ],
    )?;
    let partition = vec![batch.clone(), batch];
    let partitions = vec![partition.clone(), partition];
    Ok(MemorySourceConfig::try_new_exec(&partitions, schema, None)?)
}

/// Executes every output partition of `plan` on its own task and returns the
/// collected batches of each, in partition order.
pub(crate) async fn drive_partition_results<P: ExecutionPlan + 'static>(
    plan: Arc<P>,
    task_ctx: Arc<TaskContext>,
) -> Vec<crate::error::Result<Vec<RecordBatch>>> {
    let k = plan.properties().output_partitioning().partition_count();
    let mut handles = Vec::with_capacity(k);
    for n in 0..k {
        let plan = plan.clone();
        let ctx = task_ctx.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = plan.execute(n, ctx).map_err(BallistaError::from)?;
            utils::collect_stream(&mut stream).await
        }));
    }
    let mut results = Vec::new();
    for h in handles {
        results.push(
            h.await
                .expect("drive_partition_results task should not panic"),
        );
    }
    results
}

/// A single non-nullable `v Int64` column.
pub(crate) fn one_col_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

/// Memory source with `partitions` partitions over [`one_col_schema`], each
/// carrying a single batch of `[start .. start + rows_per)` where
/// `start = k * rows_per`.
pub(crate) fn partitioned_source(
    partitions: usize,
    rows_per: usize,
) -> Arc<dyn ExecutionPlan> {
    let schema = one_col_schema();
    let mut per_partition: Vec<Vec<RecordBatch>> = Vec::with_capacity(partitions);
    for k in 0..partitions {
        let start = (k * rows_per) as i64;
        let arr = Int64Array::from_iter_values(start..start + rows_per as i64);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        per_partition.push(vec![batch]);
    }
    MemorySourceConfig::try_new_exec(&per_partition, schema, None).unwrap()
}

/// A single non-nullable `a Int32` column.
pub(crate) fn int32_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
}

/// A batch of [`int32_schema`] holding `values`.
pub(crate) fn int32_batch(values: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        int32_schema(),
        vec![Arc::new(Int32Array::from(values.to_vec()))],
    )
    .unwrap()
}

/// A batch of `(k Int32, payload Utf8)` with `payload` `row-<k>`.
pub(crate) fn key_payload_batch(keys: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let payload: Vec<String> = keys.iter().map(|k| format!("row-{k}")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys.to_vec())),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .unwrap()
}

/// A single-vcore executor with the id and host `executor_1`.
pub(crate) fn test_executor() -> ExecutorMetadata {
    ExecutorMetadata {
        id: "executor_1".to_string(),
        host: "executor_1".to_string(),
        port: 7070,
        grpc_port: 8080,
        specification: ExecutorSpecification::default().with_vcores(1),
        os_info: ExecutorOperatingSystemSpecification::default(),
    }
}

/// An Arrow IPC stream holding `batches`, which must not be empty.
pub(crate) fn ipc_stream_bytes(batches: &[RecordBatch]) -> Vec<u8> {
    let mut bytes = vec![];
    let mut writer = StreamWriter::try_new(&mut bytes, &batches[0].schema()).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    bytes
}

/// Writes an Arrow IPC stream holding `batches` to `path`, creating its parent
/// directory.
pub(crate) fn write_ipc_stream(path: &Path, batches: &[RecordBatch]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, ipc_stream_bytes(batches)).unwrap();
}
