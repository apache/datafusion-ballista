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

use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::planner::AdaptiveStageInfo;
use ballista_core::execution_plans::{ShuffleReaderExec, ShuffleWriterExec};
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
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
/// [ShuffleWriterExec] or [ShuffleReaderExec]
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
            let shuffle_read =
                ShuffleReaderExec::try_new(stage_id, partitions, schema, partitioning)?;

            Ok(Transformed::yes(Arc::new(shuffle_read)))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    /// Converts Adaptive plan to plan which ballista expects
    /// This is to be used to convert [ExchangeExec] to
    /// [ShuffleWriterExec] and [ShuffleReaderExec]
    pub fn adapt_to_ballista(
        plan: Arc<dyn ExecutionPlan>,
        job_id: &str,
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
            let work_dir = "".to_string();

            Ok(AdaptiveStageInfo {
                plan: Arc::new(ShuffleWriterExec::try_new(
                    job_id.to_string(),
                    stage_id,
                    plan,
                    work_dir,
                    partitioning,
                )?),
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

            let work_dir = "".to_string();

            Ok(AdaptiveStageInfo {
                plan: Arc::new(ShuffleWriterExec::try_new(
                    job_id.to_string(),
                    stage_id,
                    plan,
                    work_dir,
                    None,
                )?),
                inputs: adapter.inputs,
            })
        } else {
            exec_err!(
                "Root exec expected to be either ExchangeExec or AdaptiveDatafusionExec"
            )
        }
    }
}
