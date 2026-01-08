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
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, execution_plan};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[derive(Debug, Clone, Default)]
pub struct DistributedExchangeRule {
    plan_id_generator: Arc<AtomicUsize>,
}

impl DistributedExchangeRule {
    pub fn plan_invalid(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<bool> {
        execution_plan
            .transform_up(|p| self.transform(p))
            .transformed()
    }

    pub(crate) fn transform(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(coalesce) = execution_plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            && coalesce
                .input()
                .as_any()
                .downcast_ref::<ExchangeExec>()
                .is_none()
        {
            // FIXME do we use input or current node partitioning

            let exchange_exec = ExchangeExec::new(
                coalesce.input().clone(),
                None,
                self.plan_id_generator
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            );
            Ok(Transformed::yes(
                execution_plan.with_new_children(vec![Arc::new(exchange_exec)])?,
            ))
        } else if let Some(sort_preserving_merge) = execution_plan
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>(
        ) && sort_preserving_merge
            .input()
            .as_any()
            .downcast_ref::<ExchangeExec>()
            .is_none()
        {
            // FIXME do we use input or current node partitioning
            let exchange_exec = ExchangeExec::new(
                sort_preserving_merge.input().clone(),
                None,
                self.plan_id_generator
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            );
            Ok(Transformed::yes(
                execution_plan.with_new_children(vec![Arc::new(exchange_exec)])?,
            ))
        } else if let Some(repartition) =
            execution_plan.as_any().downcast_ref::<RepartitionExec>()
        {
            match repartition.partitioning() {
                execution_plan::Partitioning::Hash(_, _) => {
                    let exchange_exec = ExchangeExec::new(
                        repartition.input().clone(),
                        Some(repartition.partitioning().clone()),
                        self.plan_id_generator
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    );
                    Ok(Transformed::yes(Arc::new(exchange_exec)))
                }
                execution_plan::Partitioning::RoundRobinBatch(_)
                | execution_plan::Partitioning::UnknownPartitioning(_) => {
                    Ok(Transformed::no(execution_plan))
                }
            }
        } else {
            Ok(Transformed::no(execution_plan))
        }
    }
}

impl PhysicalOptimizerRule for DistributedExchangeRule {
    fn optimize(
        &self,
        execution_plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<
        std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    > {
        //execution_plan.rewrite(rewriter)
        let result = execution_plan.transform_up(|p| self.transform(p))?;
        //datafusion::physical_optimizer::enforce_sorting::EnforceSorting

        if result
            .data
            .as_any()
            .downcast_ref::<AdaptiveDatafusionExec>()
            .is_some()
        {
            Ok(result.data)
        } else {
            let plan_id = self
                .plan_id_generator
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            Ok(Arc::new(AdaptiveDatafusionExec::new(plan_id, result.data)))
        }
    }

    fn name(&self) -> &str {
        "DistributedExchangeRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
