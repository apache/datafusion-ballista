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

use crate::error::BallistaError;
use crate::execution_plans::create_shuffle_path;
use crate::registry::BallistaFunctionRegistry;
use datafusion::arrow::array::{
    ArrayBuilder, StructArray, StructBuilder, UInt64Array, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::prelude::SessionConfig;
use serde::Serialize;
use std::fmt::Debug;
use std::path::PathBuf;
use std::{collections::HashMap, fmt, sync::Arc};

/// Conversions from protobuf types to Ballista types.
pub mod from_proto;
/// Conversions from Ballista types to protobuf types.
pub mod to_proto;

/// Action that can be sent to an executor
#[derive(Debug, Clone)]
pub enum Action {
    /// Collect a shuffle partition
    FetchPartition {
        /// The job identifier.
        job_id: String,
        /// The stage identifier within the job.
        stage_id: usize,
        /// The partition identifier within the stage.
        partition_id: usize,
        /// Hostname or IP address of the executor hosting this partition.
        host: String,
        /// Port number for data transfer.
        port: u16,
        /// shuffle file block id
        file_id: Option<u64>,
        /// whether this partition uses sort shuffle
        is_sort_shuffle: bool,
    },
}

/// Unique identifier for the output partition of an operator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId {
    /// The job identifier.
    pub job_id: String,
    /// The stage identifier within the job.
    pub stage_id: usize,
    /// The partition identifier within the stage.
    pub partition_id: usize,
}

impl PartitionId {
    /// Creates a new partition ID with the given job, stage, and partition identifiers.
    pub fn new(job_id: &str, stage_id: usize, partition_id: usize) -> Self {
        Self {
            job_id: job_id.to_string(),
            stage_id,
            partition_id,
        }
    }
}

/// Location information for a shuffle partition.
#[derive(Debug, Clone)]
pub struct PartitionLocation {
    /// The source partition ID from the map stage.
    pub map_partition_id: usize,
    /// The partition identifier.
    pub partition_id: PartitionId,
    /// Metadata about the executor hosting this partition.
    pub executor_meta: ExecutorMetadata,
    /// Statistics about the partition data.
    pub partition_stats: PartitionStats,
    /// shuffle file id
    pub file_id: Option<u64>,
    /// whether this partition uses sort shuffle
    pub is_sort_shuffle: bool,
}

impl PartitionLocation {
    /// creates file actual file location
    pub fn path(&self, work_dir: &str) -> datafusion::error::Result<PathBuf> {
        create_shuffle_path(
            work_dir,
            &self.partition_id.job_id,
            self.partition_id.stage_id,
            self.partition_id.partition_id,
            self.file_id,
            self.is_sort_shuffle,
        )
    }
}

/// Meta-data for an executor, used when fetching shuffle partitions from other executors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorMetadata {
    /// Unique executor identifier.
    pub id: String,
    /// Hostname or IP address of the executor.
    pub host: String,
    /// Port number for data transfer.
    pub port: u16,
    /// Port number for gRPC communication.
    pub grpc_port: u16,
    /// Resource specification for this executor.
    pub specification: ExecutorSpecification,
}

/// Peak values for executor (memory)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ExecutorPeaks {
    /// Peak of physical memory usage on this executor
    pub physical_memory: u64,
    /// Peak of virtual memory usage on this executor
    pub virtual_memory: u64,
}

/// Specification of an executor, indicating executor resources, like total task slots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorSpecification {
    /// Number of concurrent task slots available on this executor.
    pub task_slots: u32,
    /// Number of physical cores available on this executor
    pub physical_cores: u32,
    /// Number of physical disks available on this executor
    pub num_disks: u32,
    /// Total disk space on this executor, in bytes
    pub total_disk_space: u64,
    /// Total available disk space on this executor, in bytes
    pub total_available_disk_space: u64,
    /// Open files limit on this executor
    pub open_files_limit: u64,
    /// System name
    pub system_name: String,
    /// Kernel version
    pub kernel_ver: String,
    /// OS version
    pub os_ver: String,
    /// OS version (long)
    pub os_ver_long: String,
}

impl Default for ExecutorSpecification {
    fn default() -> Self {
        Self {
            task_slots: 1,
            physical_cores: 1,
            num_disks: 2,
            total_disk_space: 1024 * 1024 * 8,
            total_available_disk_space: 1024 * 1024 * 4,
            open_files_limit: 1024,
            system_name: String::from("Ubuntu"),
            kernel_ver: String::from("Linux 6.17.0-20-generic"),
            os_ver: String::from("24.04"),
            os_ver_long: String::from("Linux (Ubuntu 24.04)"),
        }
    }
}

impl ExecutorSpecification {
    /// Setting number of task slots (number of tasks that can be handled by this executor)
    pub fn with_task_slots(mut self, task_slots: u32) -> Self {
        self.task_slots = task_slots;
        self
    }

    /// Setting number of physical cores for executor
    pub fn with_physical_cores(mut self, physical_cores: u32) -> Self {
        self.physical_cores = physical_cores;
        self
    }

    /// Setting number of disks for executor
    pub fn with_num_disks(mut self, num_disks: u32) -> Self {
        self.num_disks = num_disks;
        self
    }

    /// Setting total disks space for executor
    pub fn with_total_disk_space(mut self, total_disk_space: u64) -> Self {
        self.total_disk_space = total_disk_space;
        self
    }

    /// Setting total available disk space for executor
    pub fn with_total_available_disk_space(
        mut self,
        total_available_disk_space: u64,
    ) -> Self {
        self.total_available_disk_space = total_available_disk_space;
        self
    }

    /// Setting open files limit for executor
    pub fn with_open_files_limit(mut self, open_files_limit: u64) -> Self {
        self.open_files_limit = open_files_limit;
        self
    }

    /// Setting system name for executor
    pub fn with_system_name(mut self, system_name: String) -> Self {
        self.system_name = system_name;
        self
    }

    /// Setting kernel version for executor
    pub fn with_kernel_ver(mut self, kernel_ver: String) -> Self {
        self.kernel_ver = kernel_ver;
        self
    }

    /// Setting OS version for executor
    pub fn with_os_ver(mut self, os_ver: String) -> Self {
        self.os_ver = os_ver;
        self
    }

    /// Setting OS version (longer variant) for executor
    pub fn with_os_ver_long(mut self, os_ver_long: String) -> Self {
        self.os_ver_long = os_ver_long;
        self
    }
}

/// Available resources for an executor, including total and available task slots.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutorData {
    /// Unique executor identifier.
    pub executor_id: String,
    /// Total number of task slots.
    pub total_task_slots: u32,
    /// Currently available task slots.
    pub available_task_slots: u32,
}

/// Represents a change in executor task slot availability.
pub struct ExecutorDataChange {
    /// Unique executor identifier.
    pub executor_id: String,
    /// Change in available task slots (positive or negative).
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
    /// Creates new partition statistics with the given values.
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

    /// Returns the Arrow struct field representation of these statistics.
    pub fn arrow_struct_repr(self) -> Field {
        Field::new(
            "partition_stats",
            DataType::Struct(self.arrow_struct_fields().into()),
            false,
        )
    }

    /// Returns the Arrow fields for the statistics struct.
    pub fn arrow_struct_fields(self) -> Vec<Field> {
        vec![
            Field::new("num_rows", DataType::UInt64, false),
            Field::new("num_batches", DataType::UInt64, false),
            Field::new("num_bytes", DataType::UInt64, false),
        ]
    }

    /// Converts these statistics to an Arrow struct array.
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

    /// Creates partition statistics from an Arrow struct array.
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
    /// Creates a new execute partition task.
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

    /// Returns a unique key string for this partition task.
    pub fn key(&self) -> String {
        format!("{}.{}.{:?}", self.job_id, self.stage_id, self.partition_id)
    }
}

/// Result of executing a partition, containing the output path and statistics.
#[derive(Debug)]
pub struct ExecutePartitionResult {
    /// Path containing results for this partition.
    path: String,
    /// Statistics about the executed partition.
    stats: PartitionStats,
}

impl ExecutePartitionResult {
    /// Creates a new execution result with the given path and statistics.
    pub fn new(path: &str, stats: PartitionStats) -> Self {
        Self {
            path: path.to_owned(),
            stats,
        }
    }

    /// Returns the output file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the partition statistics.
    pub fn statistics(&self) -> &PartitionStats {
        &self.stats
    }
}

/// Definition of a task to be executed on an executor.
#[derive(Clone, Debug)]
pub struct TaskDefinition {
    /// Unique task identifier.
    pub task_id: usize,
    /// Current attempt number for this task.
    pub task_attempt_num: usize,
    /// Job identifier this task belongs to.
    pub job_id: String,
    /// Stage identifier within the job.
    pub stage_id: usize,
    /// Current attempt number for the stage.
    pub stage_attempt_num: usize,
    /// Partition to process.
    pub partition_id: usize,
    /// Physical execution plan for this task.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Timestamp when the task was launched.
    pub launch_time: u64,
    /// Session identifier.
    pub session_id: String,
    /// Session configuration.
    pub session_config: SessionConfig,
    /// Function registry for UDFs.
    pub function_registry: Arc<BallistaFunctionRegistry>,
}
