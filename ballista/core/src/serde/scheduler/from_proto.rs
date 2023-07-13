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

use chrono::{TimeZone, Utc};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion::physical_plan::metrics::{
    Count, Gauge, MetricValue, MetricsSet, Time, Timestamp,
};
use datafusion::physical_plan::{ExecutionPlan, Metric};
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use crate::error::BallistaError;
use crate::serde::scheduler::{
    Action, ExecutorData, ExecutorMetadata, ExecutorSpecification, PartitionId,
    PartitionLocation, PartitionStats, SimpleFunctionRegistry, TaskDefinition,
};

use crate::serde::{protobuf, BallistaCodec};
use protobuf::{operator_metric, NamedCount, NamedGauge, NamedTime};

impl TryInto<Action> for protobuf::Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<Action, Self::Error> {
        match self.action_type {
            Some(protobuf::action::ActionType::FetchPartition(fetch)) => {
                Ok(Action::FetchPartition {
                    job_id: fetch.job_id,
                    stage_id: fetch.stage_id as usize,
                    partition_id: fetch.partition_id as usize,
                    path: fetch.path,
                    host: fetch.host,
                    port: fetch.port as u16,
                })
            }
            _ => Err(BallistaError::General(
                "scheduler::from_proto(Action) invalid or missing action".to_owned(),
            )),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<PartitionId> for protobuf::PartitionId {
    fn into(self) -> PartitionId {
        PartitionId::new(
            &self.job_id,
            self.stage_id as usize,
            self.partition_id as usize,
        )
    }
}

#[allow(clippy::from_over_into)]
impl Into<PartitionStats> for protobuf::PartitionStats {
    fn into(self) -> PartitionStats {
        PartitionStats::new(
            foo(self.num_rows),
            foo(self.num_batches),
            foo(self.num_bytes),
        )
    }
}

fn foo(n: i64) -> Option<u64> {
    if n < 0 {
        None
    } else {
        Some(n as u64)
    }
}

impl TryInto<PartitionLocation> for protobuf::PartitionLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<PartitionLocation, Self::Error> {
        Ok(PartitionLocation {
            map_partition_id: self.map_partition_id as usize,
            partition_id: self
                .partition_id
                .ok_or_else(|| {
                    BallistaError::General(
                        "partition_id in PartitionLocation is missing.".to_owned(),
                    )
                })?
                .into(),
            executor_meta: self
                .executor_meta
                .ok_or_else(|| {
                    BallistaError::General(
                        "executor_meta in PartitionLocation is missing".to_owned(),
                    )
                })?
                .into(),
            partition_stats: self
                .partition_stats
                .ok_or_else(|| {
                    BallistaError::General(
                        "partition_stats in PartitionLocation is missing".to_owned(),
                    )
                })?
                .into(),
            path: self.path,
        })
    }
}

impl TryInto<MetricValue> for protobuf::OperatorMetric {
    type Error = BallistaError;

    fn try_into(self) -> Result<MetricValue, Self::Error> {
        match self.metric {
            Some(operator_metric::Metric::OutputRows(value)) => {
                let count = Count::new();
                count.add(value as usize);
                Ok(MetricValue::OutputRows(count))
            }
            Some(operator_metric::Metric::ElapseTime(value)) => {
                let time = Time::new();
                time.add_duration(Duration::from_nanos(value));
                Ok(MetricValue::ElapsedCompute(time))
            }
            Some(operator_metric::Metric::SpillCount(value)) => {
                let count = Count::new();
                count.add(value as usize);
                Ok(MetricValue::SpillCount(count))
            }
            Some(operator_metric::Metric::SpilledBytes(value)) => {
                let count = Count::new();
                count.add(value as usize);
                Ok(MetricValue::SpilledBytes(count))
            }
            Some(operator_metric::Metric::CurrentMemoryUsage(value)) => {
                let gauge = Gauge::new();
                gauge.add(value as usize);
                Ok(MetricValue::CurrentMemoryUsage(gauge))
            }
            Some(operator_metric::Metric::Count(NamedCount { name, value })) => {
                let count = Count::new();
                count.add(value as usize);
                Ok(MetricValue::Count {
                    name: name.into(),
                    count,
                })
            }
            Some(operator_metric::Metric::Gauge(NamedGauge { name, value })) => {
                let gauge = Gauge::new();
                gauge.add(value as usize);
                Ok(MetricValue::Gauge {
                    name: name.into(),
                    gauge,
                })
            }
            Some(operator_metric::Metric::Time(NamedTime { name, value })) => {
                let time = Time::new();
                time.add_duration(Duration::from_nanos(value));
                Ok(MetricValue::Time {
                    name: name.into(),
                    time,
                })
            }
            Some(operator_metric::Metric::StartTimestamp(value)) => {
                let timestamp = Timestamp::new();
                timestamp.set(Utc.timestamp_nanos(value));
                Ok(MetricValue::StartTimestamp(timestamp))
            }
            Some(operator_metric::Metric::EndTimestamp(value)) => {
                let timestamp = Timestamp::new();
                timestamp.set(Utc.timestamp_nanos(value));
                Ok(MetricValue::EndTimestamp(timestamp))
            }
            None => Err(BallistaError::General(
                "scheduler::from_proto(OperatorMetric) metric is None.".to_owned(),
            )),
        }
    }
}

impl TryInto<MetricsSet> for protobuf::OperatorMetricsSet {
    type Error = BallistaError;

    fn try_into(self) -> Result<MetricsSet, Self::Error> {
        let mut ms = MetricsSet::new();
        let metrics = self
            .metrics
            .into_iter()
            .map(|m| m.try_into())
            .collect::<Result<Vec<_>, BallistaError>>()?;

        for value in metrics {
            let new_metric = Arc::new(Metric::new(value, None));
            ms.push(new_metric)
        }
        Ok(ms)
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorMetadata> for protobuf::ExecutorMetadata {
    fn into(self) -> ExecutorMetadata {
        ExecutorMetadata {
            id: self.id,
            host: self.host,
            port: self.port as u16,
            grpc_port: self.grpc_port as u16,
            specification: self.specification.unwrap().into(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorSpecification> for protobuf::ExecutorSpecification {
    fn into(self) -> ExecutorSpecification {
        let mut ret = ExecutorSpecification { task_slots: 0 };
        for resource in self.resources {
            if let Some(protobuf::executor_resource::Resource::TaskSlots(task_slots)) =
                resource.resource
            {
                ret.task_slots = task_slots
            }
        }
        ret
    }
}

#[allow(clippy::from_over_into)]
impl Into<ExecutorData> for protobuf::ExecutorData {
    fn into(self) -> ExecutorData {
        let mut ret = ExecutorData {
            executor_id: self.executor_id,
            total_task_slots: 0,
            available_task_slots: 0,
        };
        for resource in self.resources {
            if let Some(task_slots) = resource.total {
                if let Some(protobuf::executor_resource::Resource::TaskSlots(
                    task_slots,
                )) = task_slots.resource
                {
                    ret.total_task_slots = task_slots
                }
            };
            if let Some(task_slots) = resource.available {
                if let Some(protobuf::executor_resource::Resource::TaskSlots(
                    task_slots,
                )) = task_slots.resource
                {
                    ret.available_task_slots = task_slots
                }
            };
        }
        ret
    }
}

pub fn get_task_definition<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan>(
    task: protobuf::TaskDefinition,
    runtime: Arc<RuntimeEnv>,
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    codec: BallistaCodec<T, U>,
) -> Result<TaskDefinition, BallistaError> {
    let mut props = HashMap::new();
    for kv_pair in task.props {
        props.insert(kv_pair.key, kv_pair.value);
    }
    let props = Arc::new(props);

    let mut task_scalar_functions = HashMap::new();
    let mut task_aggregate_functions = HashMap::new();
    let mut task_window_functions = HashMap::new();
    // TODO combine the functions from Executor's functions and TaskDefinition's function resources
    for scalar_func in scalar_functions {
        task_scalar_functions.insert(scalar_func.0, scalar_func.1);
    }
    for agg_func in aggregate_functions {
        task_aggregate_functions.insert(agg_func.0, agg_func.1);
    }
    for agg_func in window_functions {
        task_window_functions.insert(agg_func.0, agg_func.1);
    }
    let function_registry = Arc::new(SimpleFunctionRegistry {
        scalar_functions: task_scalar_functions,
        aggregate_functions: task_aggregate_functions,
        window_functions: task_window_functions,
    });

    let encoded_plan = task.plan.as_slice();
    let plan: Arc<dyn ExecutionPlan> = U::try_decode(encoded_plan).and_then(|proto| {
        proto.try_into_physical_plan(
            function_registry.as_ref(),
            runtime.as_ref(),
            codec.physical_extension_codec(),
        )
    })?;

    let job_id = task.job_id;
    let stage_id = task.stage_id as usize;
    let partition_id = task.partition_id as usize;
    let task_attempt_num = task.task_attempt_num as usize;
    let stage_attempt_num = task.stage_attempt_num as usize;
    let launch_time = task.launch_time;
    let task_id = task.task_id as usize;
    let session_id = task.session_id;

    Ok(TaskDefinition {
        task_id,
        task_attempt_num,
        job_id,
        stage_id,
        stage_attempt_num,
        partition_id,
        plan,
        launch_time,
        session_id,
        props,
        function_registry,
    })
}

pub fn get_task_definition_vec<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    multi_task: protobuf::MultiTaskDefinition,
    runtime: Arc<RuntimeEnv>,
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    codec: BallistaCodec<T, U>,
) -> Result<Vec<TaskDefinition>, BallistaError> {
    let mut props = HashMap::new();
    for kv_pair in multi_task.props {
        props.insert(kv_pair.key, kv_pair.value);
    }
    let props = Arc::new(props);

    let mut task_scalar_functions = HashMap::new();
    let mut task_aggregate_functions = HashMap::new();
    let mut task_window_functions = HashMap::new();
    // TODO combine the functions from Executor's functions and TaskDefinition's function resources
    for scalar_func in scalar_functions {
        task_scalar_functions.insert(scalar_func.0, scalar_func.1);
    }
    for agg_func in aggregate_functions {
        task_aggregate_functions.insert(agg_func.0, agg_func.1);
    }
    for agg_func in window_functions {
        task_window_functions.insert(agg_func.0, agg_func.1);
    }
    let function_registry = Arc::new(SimpleFunctionRegistry {
        scalar_functions: task_scalar_functions,
        aggregate_functions: task_aggregate_functions,
        window_functions: task_window_functions,
    });

    let encoded_plan = multi_task.plan.as_slice();
    let plan: Arc<dyn ExecutionPlan> = U::try_decode(encoded_plan).and_then(|proto| {
        proto.try_into_physical_plan(
            function_registry.as_ref(),
            runtime.as_ref(),
            codec.physical_extension_codec(),
        )
    })?;

    let job_id = multi_task.job_id;
    let stage_id = multi_task.stage_id as usize;
    let stage_attempt_num = multi_task.stage_attempt_num as usize;
    let launch_time = multi_task.launch_time;
    let task_ids = multi_task.task_ids;
    let session_id = multi_task.session_id;

    task_ids
        .iter()
        .map(|task_id| {
            Ok(TaskDefinition {
                task_id: task_id.task_id as usize,
                task_attempt_num: task_id.task_attempt_num as usize,
                job_id: job_id.clone(),
                stage_id,
                stage_attempt_num,
                partition_id: task_id.partition_id as usize,
                plan: reset_metrics_for_execution_plan(plan.clone())?,
                launch_time,
                session_id: session_id.clone(),
                props: props.clone(),
                function_registry: function_registry.clone(),
            })
        })
        .collect()
}

fn reset_metrics_for_execution_plan(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
    plan.transform(&|plan| {
        let children = plan.children().clone();
        plan.with_new_children(children).map(Transformed::Yes)
    })
    .map_err(BallistaError::DataFusionError)
}
