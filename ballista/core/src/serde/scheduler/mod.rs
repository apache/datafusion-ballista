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

use std::collections::HashSet;
use std::fmt::Debug;
use std::{collections::HashMap, fmt, sync::Arc};

use datafusion::arrow::array::{
    ArrayBuilder, StructArray, StructBuilder, UInt64Array, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use serde::Serialize;

use crate::error::BallistaError;

pub mod from_proto;
pub mod to_proto;

/// Action that can be sent to an executor
#[derive(Debug, Clone)]
pub enum Action {
    /// Collect a shuffle partition
    FetchPartition {
        job_id: String,
        stage_id: usize,
        partition_id: usize,
        path: String,
        host: String,
        port: u16,
    },
}

/// Unique identifier for the output partition of an operator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId {
    pub job_id: String,
    pub stage_id: usize,
    pub partition_id: usize,
}

impl PartitionId {
    pub fn new(job_id: &str, stage_id: usize, partition_id: usize) -> Self {
        Self {
            job_id: job_id.to_string(),
            stage_id,
            partition_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionLocation {
    pub map_partition_id: usize,
    pub partition_id: PartitionId,
    pub executor_meta: ExecutorMetadata,
    pub partition_stats: PartitionStats,
    pub path: String,
}

/// Meta-data for an executor, used when fetching shuffle partitions from other executors
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorMetadata {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub grpc_port: u16,
    pub specification: ExecutorSpecification,
}

/// Specification of an executor, indicting executor resources, like total task slots
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ExecutorSpecification {
    pub task_slots: u32,
}

/// From Spark, available resources for an executor, like available task slots
#[derive(Debug, Clone, Serialize)]
pub struct ExecutorData {
    pub executor_id: String,
    pub total_task_slots: u32,
    pub available_task_slots: u32,
}

pub struct ExecutorDataChange {
    pub executor_id: String,
    pub task_slots: i32,
}

/// Summary of executed partition
#[derive(Debug, Copy, Clone, Default)]
pub struct PartitionStats {
    pub(crate) num_rows: Option<u64>,
    pub(crate) num_batches: Option<u64>,
    pub(crate) num_bytes: Option<u64>,
}

impl fmt::Display for PartitionStats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "numBatches={:?}, numRows={:?}, numBytes={:?}",
            self.num_batches, self.num_rows, self.num_bytes
        )
    }
}

impl PartitionStats {
    pub fn new(
        num_rows: Option<u64>,
        num_batches: Option<u64>,
        num_bytes: Option<u64>,
    ) -> Self {
        Self {
            num_rows,
            num_batches,
            num_bytes,
        }
    }

    pub fn arrow_struct_repr(self) -> Field {
        Field::new(
            "partition_stats",
            DataType::Struct(self.arrow_struct_fields().into()),
            false,
        )
    }

    pub fn arrow_struct_fields(self) -> Vec<Field> {
        vec![
            Field::new("num_rows", DataType::UInt64, false),
            Field::new("num_batches", DataType::UInt64, false),
            Field::new("num_bytes", DataType::UInt64, false),
        ]
    }

    pub fn to_arrow_arrayref(self) -> Result<Arc<StructArray>, BallistaError> {
        let mut field_builders = Vec::new();

        let mut num_rows_builder = UInt64Builder::with_capacity(1);
        match self.num_rows {
            Some(n) => num_rows_builder.append_value(n),
            None => num_rows_builder.append_null(),
        }
        field_builders.push(Box::new(num_rows_builder) as Box<dyn ArrayBuilder>);

        let mut num_batches_builder = UInt64Builder::with_capacity(1);
        match self.num_batches {
            Some(n) => num_batches_builder.append_value(n),
            None => num_batches_builder.append_null(),
        }
        field_builders.push(Box::new(num_batches_builder) as Box<dyn ArrayBuilder>);

        let mut num_bytes_builder = UInt64Builder::with_capacity(1);
        match self.num_bytes {
            Some(n) => num_bytes_builder.append_value(n),
            None => num_bytes_builder.append_null(),
        }
        field_builders.push(Box::new(num_bytes_builder) as Box<dyn ArrayBuilder>);

        let mut struct_builder =
            StructBuilder::new(self.arrow_struct_fields(), field_builders);
        struct_builder.append(true);
        Ok(Arc::new(struct_builder.finish()))
    }

    pub fn from_arrow_struct_array(struct_array: &StructArray) -> PartitionStats {
        let num_rows = struct_array
            .column_by_name("num_rows")
            .expect("from_arrow_struct_array expected a field num_rows")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("from_arrow_struct_array expected num_rows to be a UInt64Array");
        let num_batches = struct_array
            .column_by_name("num_batches")
            .expect("from_arrow_struct_array expected a field num_batches")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("from_arrow_struct_array expected num_batches to be a UInt64Array");
        let num_bytes = struct_array
            .column_by_name("num_bytes")
            .expect("from_arrow_struct_array expected a field num_bytes")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("from_arrow_struct_array expected num_bytes to be a UInt64Array");
        PartitionStats {
            num_rows: Some(num_rows.value(0).to_owned()),
            num_batches: Some(num_batches.value(0).to_owned()),
            num_bytes: Some(num_bytes.value(0).to_owned()),
        }
    }
}

/// Task that can be sent to an executor to execute one stage of a query and write
/// results out to disk
#[derive(Debug, Clone)]
pub struct ExecutePartition {
    /// Unique ID representing this query execution
    pub job_id: String,
    /// Unique ID representing this query stage within the overall query
    pub stage_id: usize,
    /// The partitions to execute. The same plan could be sent to multiple executors and each
    /// executor will execute a range of partitions per QueryStageTask
    pub partition_id: Vec<usize>,
    /// The physical plan for this query stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// Location of shuffle partitions that this query stage may depend on
    pub shuffle_locations: HashMap<PartitionId, ExecutorMetadata>,
    /// Output partitioning for shuffle writes
    pub output_partitioning: Option<Partitioning>,
}

impl ExecutePartition {
    pub fn new(
        job_id: String,
        stage_id: usize,
        partition_id: Vec<usize>,
        plan: Arc<dyn ExecutionPlan>,
        shuffle_locations: HashMap<PartitionId, ExecutorMetadata>,
        output_partitioning: Option<Partitioning>,
    ) -> Self {
        Self {
            job_id,
            stage_id,
            partition_id,
            plan,
            shuffle_locations,
            output_partitioning,
        }
    }

    pub fn key(&self) -> String {
        format!("{}.{}.{:?}", self.job_id, self.stage_id, self.partition_id)
    }
}

#[derive(Debug)]
pub struct ExecutePartitionResult {
    /// Path containing results for this partition
    path: String,
    stats: PartitionStats,
}

impl ExecutePartitionResult {
    pub fn new(path: &str, stats: PartitionStats) -> Self {
        Self {
            path: path.to_owned(),
            stats,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn statistics(&self) -> &PartitionStats {
        &self.stats
    }
}

#[derive(Clone, Debug)]
pub struct TaskDefinition {
    pub task_id: usize,
    pub task_attempt_num: usize,
    pub job_id: String,
    pub stage_id: usize,
    pub stage_attempt_num: usize,
    pub partition_id: usize,
    pub plan: Arc<dyn ExecutionPlan>,
    pub launch_time: u64,
    pub session_id: String,
    pub props: Arc<HashMap<String, String>>,
    pub function_registry: Arc<SimpleFunctionRegistry>,
}

#[derive(Debug)]
pub struct SimpleFunctionRegistry {
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    pub window_functions: HashMap<String, Arc<WindowUDF>>,
}

impl FunctionRegistry for SimpleFunctionRegistry {
    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }

    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> datafusion::common::Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDF named \"{name}\" in the TaskContext"
            ))
        })
    }

    fn udaf(&self, name: &str) -> datafusion::common::Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDAF named \"{name}\" in the TaskContext"
            ))
        })
    }

    fn udwf(&self, name: &str) -> datafusion::common::Result<Arc<WindowUDF>> {
        let result = self.window_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDWF named \"{name}\" in the TaskContext"
            ))
        })
    }
}
