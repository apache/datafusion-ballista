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

use crate::planner::create_shuffle_writer_with_config;
use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::planner::AdaptiveStageInfo;
use ballista_core::execution_plans::ShuffleReaderExec;
use datafusion::common::exec_err;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning};
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    physical_plan::ExecutionPlan,
};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(crate) struct BallistaAdapter {
    inputs: Vec<usize>,
}

///
/// Used to transform plan nodes used in adaptive planning
/// to ballista specific nodes such as
/// ShuffleWriterExec/SortShuffleWriterExec and [ShuffleReaderExec]
///
impl BallistaAdapter {
    fn transform_children(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>() {
            let schema = exchange.schema().clone();
            let partitions = exchange.shuffle_partitions().ok_or_else(|| {
                DataFusionError::Execution(
                    "partitions have to be resolved at this point".to_string(),
                )
            })?;

            let stage_id = exchange.stage_id().ok_or_else(|| {
                DataFusionError::Execution(
                    "stage ID has to be generated at this point".to_string(),
                )
            })?;
            self.inputs.push(stage_id);
            let partitioning = exchange.properties().partitioning.clone();

            let reader = match (exchange.coalesce(), exchange.broadcast) {
                (Some(cp), false) => {
                    // Concatenate M-shape locations into K-shape per CoalescePlan.groups.
                    let k_shape: Vec<Vec<_>> = cp
                        .groups
                        .iter()
                        .map(|pg| {
                            let mut concat = Vec::new();
                            for &idx in &pg.upstream_indices {
                                if let Some(inner) = partitions.get(idx as usize) {
                                    concat.extend_from_slice(inner);
                                }
                            }
                            concat
                        })
                        .collect();
                    let new_partitioning = match &partitioning {
                        Partitioning::Hash(keys, _m) => {
                            Partitioning::Hash(keys.clone(), cp.groups.len())
                        }
                        _ => Partitioning::UnknownPartitioning(cp.groups.len()),
                    };
                    ShuffleReaderExec::try_new_coalesced(
                        stage_id,
                        k_shape,
                        (*cp).clone(),
                        schema,
                        new_partitioning,
                    )?
                }
                (None, false) => ShuffleReaderExec::try_new(
                    stage_id,
                    partitions,
                    schema,
                    partitioning,
                )?,
                (_, true) => ShuffleReaderExec::try_new_broadcast(
                    stage_id,
                    exchange.shuffle_partitions_flattened(),
                    schema,
                    exchange.input().output_partitioning().partition_count(),
                )?,
            };

            Ok(Transformed::yes(Arc::new(reader)))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    /// Converts Adaptive plan to plan which ballista expects
    /// This is to be used to convert [ExchangeExec] to
    /// ShuffleWriterExec/SortShuffleWriterExec and [ShuffleReaderExec]
    pub fn adapt_to_ballista(
        plan: Arc<dyn ExecutionPlan>,
        job_id: &JobId,
        config: &ConfigOptions,
    ) -> datafusion::error::Result<AdaptiveStageInfo> {
        if let Some(root) = plan.as_any().downcast_ref::<ExchangeExec>() {
            let mut adapter = BallistaAdapter::default();
            let plan = root
                .input()
                .clone()
                .transform_down(|e| adapter.transform_children(e))?
                .data;
            let stage_id = root.stage_id().ok_or_else(|| {
                DataFusionError::Execution(
                    "shuffle partitions have to be resolved at this point".to_string(),
                )
            })?;
            let partitioning = root.partitioning.clone();

            let writer = create_shuffle_writer_with_config(
                job_id,
                stage_id,
                plan,
                partitioning,
                config,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(AdaptiveStageInfo {
                plan: writer,
                inputs: adapter.inputs,
            })
        } else if let Some(root) = plan.as_any().downcast_ref::<AdaptiveDatafusionExec>()
        {
            let mut adapter = BallistaAdapter::default();
            let plan = root
                .input()
                .clone()
                .transform_down(|e| adapter.transform_children(e))?
                .data;
            let stage_id = root.stage_id().ok_or_else(|| {
                DataFusionError::Execution(
                    "shuffle partitions have to be resolved at this point".to_string(),
                )
            })?;

            let writer =
                create_shuffle_writer_with_config(job_id, stage_id, plan, None, config)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(AdaptiveStageInfo {
                plan: writer,
                inputs: adapter.inputs,
            })
        } else {
            exec_err!(
                "Root exec expected to be either ExchangeExec or AdaptiveDatafusionExec"
            )
        }
    }
}
