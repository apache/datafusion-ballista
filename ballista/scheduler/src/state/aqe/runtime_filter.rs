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

//! Job-scoped runtime filter hub. Keeping it in the execution graph gives
//! `(job_id, filter_id)` isolation and ties cleanup to the job's existing lifetime.
//! Reports stay in TaskInfo: task/stage retry and executor-loss handling already
//! invalidate those statuses, so a separate partial-report cache cannot go stale.

use crate::state::execution_stage::ExecutionStage;
use ballista_core::execution_plans::runtime_filter::RuntimeFilter;
use ballista_core::execution_plans::{RuntimeFilterApplyExec, RuntimeFilterBuildExec};
use ballista_core::serde::protobuf::{RuntimeFilterPayload, task_status};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(super) struct RuntimeFilterRegistry {
    producers: HashMap<u64, usize>,
    prepared: HashSet<usize>,
}

impl RuntimeFilterRegistry {
    pub fn register(&mut self, stage_id: usize, plan: &dyn ExecutionPlan) {
        if let Some(build) = plan.downcast_ref::<RuntimeFilterBuildExec>() {
            self.producers.insert(build.filter_id, stage_id);
        }
        for child in plan.children() {
            self.register(stage_id, child.as_ref());
        }
    }

    /// Freeze available complete filters before this stage's first dispatch.
    /// Missing or unfinished producers leave their consumers as pass-throughs.
    pub fn prepare(
        &mut self,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        stages: &HashMap<usize, ExecutionStage>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Binding is attempted once, including when the caller falls back
        // after an invalid report. Later tasks must keep the same stage plan.
        if !self.prepared.insert(stage_id) {
            return Ok(plan);
        }
        let plan = plan
            .transform_up(|node| {
                let Some(apply) = node.downcast_ref::<RuntimeFilterApplyExec>() else {
                    return Ok(Transformed::no(node));
                };
                let Some(payload) = completed_filter(
                    apply.filter_id,
                    self.producers
                        .get(&apply.filter_id)
                        .and_then(|id| stages.get(id)),
                )?
                else {
                    return Ok(Transformed::yes(apply.children()[0].clone()));
                };
                Ok(Transformed::yes(Arc::new(apply.with_payload(payload)?)))
            })?
            .data;
        Ok(plan)
    }
}

fn completed_filter(
    id: u64,
    stage: Option<&ExecutionStage>,
) -> Result<Option<RuntimeFilterPayload>> {
    let tasks = match stage {
        Some(ExecutionStage::Successful(stage)) => &stage.task_infos,
        Some(ExecutionStage::Running(stage)) if stage.is_successful() => {
            &stage.task_infos
        }
        _ => return Ok(None),
    };
    let mut merged = RuntimeFilter::default();
    for task in tasks {
        // Existing retry/loss handling invalidates old successful slices.
        if let task_status::Status::Successful(success) = &task.task_status {
            let Some(report) = success.runtime_filters.iter().find(|p| p.filter_id == id)
            else {
                return Ok(None);
            };
            merged.merge(&RuntimeFilter::from_proto(report)?);
        }
    }
    merged.to_proto(id).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::execution_graph::create_task_info;
    use crate::state::execution_stage::{RunningStage, TaskInfo};
    use ballista_core::execution_plans::runtime_filter::collect_reports;
    use ballista_core::serde::protobuf::SuccessfulTask;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn source(values: Vec<i64>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    async fn report(value: i64) -> RuntimeFilterPayload {
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            source(vec![value]),
            3,
            Arc::new(Column::new("k", 0)),
        ));
        collect(build.clone(), SessionContext::new().task_ctx())
            .await
            .unwrap();
        collect_reports(&build).unwrap().pop().unwrap()
    }

    fn task(
        id: usize,
        partition: usize,
        payload: Option<RuntimeFilterPayload>,
    ) -> TaskInfo {
        let mut task = create_task_info("e".into(), id);
        task.global_input_partition_ids = vec![partition];
        if let Some(payload) = payload {
            task.task_status = task_status::Status::Successful(SuccessfulTask {
                runtime_filters: vec![payload],
                ..Default::default()
            });
        }
        task
    }

    async fn keys(plan: Arc<dyn ExecutionPlan>) -> Vec<i64> {
        let batches = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    #[tokio::test]
    async fn runtime_filter_hub_uses_only_complete_current_attempts() {
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            source(vec![]),
            3,
            Arc::new(Column::new("k", 0)),
        ));
        let mut stage = RunningStage::new(
            1,
            0,
            build,
            2,
            vec![],
            HashMap::new(),
            Arc::new(SessionConfig::new()),
        );
        stage.pending.next_slice(2);
        stage.task_infos = vec![task(0, 0, Some(report(1).await)), task(1, 1, None)];
        let mut stages = HashMap::from([(1, ExecutionStage::Running(stage))]);
        let probe: Arc<dyn ExecutionPlan> = Arc::new(
            RuntimeFilterApplyExec::try_new(
                source(vec![1, 2, 3, 4]),
                3,
                Arc::new(Column::new("k", 0)),
                None,
            )
            .unwrap(),
        );
        let mut hub = RuntimeFilterRegistry::default();
        hub.register(1, stages[&1].plan());
        let bypass = hub.prepare(2, probe.clone(), &stages).unwrap();
        assert_eq!(keys(bypass.clone()).await, [1, 2, 3, 4]);
        let ExecutionStage::Running(stage) = stages.get_mut(&1).unwrap() else {
            unreachable!()
        };
        // Drop the lost attempt's key 1; only replacement slices may contribute.
        stage.reset_task_info(0);
        stage.task_infos[1] = task(1, 1, Some(report(2).await));
        stage.pending.next_slice(1);
        stage.task_infos.push(task(2, 0, Some(report(3).await)));
        // A stage already dispatched without a filter stays unchanged.
        let frozen = hub.prepare(2, bypass, &stages).unwrap();
        assert_eq!(keys(frozen).await, [1, 2, 3, 4]);
        let plan = hub.prepare(3, probe, &stages).unwrap();
        assert_eq!(keys(plan).await, [2, 3]);
    }
}
