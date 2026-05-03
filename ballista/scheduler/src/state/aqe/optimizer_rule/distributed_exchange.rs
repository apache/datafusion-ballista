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
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, execution_plan};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

enum ExchangeStatus {
    None,
    Resolved,
    Unresolved,
}

#[derive(Debug, Clone, Default)]
pub struct DistributedExchangeRule {
    plan_id_generator: Arc<AtomicUsize>,
}

impl DistributedExchangeRule {
    // TODO: remove this once we're sure we do not need multi pass optimization
    // check if plan is going to be transformed if this
    // rule executed
    // pub(crate) fn is_plan_transformed(
    //     &self,
    //     execution_plan: Arc<dyn ExecutionPlan>,
    // ) -> datafusion::error::Result<bool> {
    //     execution_plan
    //         .transform_up(|p| self.transform(p))
    //         .transformed()
    // }

    pub(crate) fn transform(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(coalesce) = execution_plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
        {
            let input = coalesce.input();
            if input.as_any().downcast_ref::<ExchangeExec>().is_none()
                && !matches!(find_exchange_status(input), ExchangeStatus::Unresolved)
            {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    None,
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(
                    execution_plan.with_new_children(vec![Arc::new(exchange_exec)])?,
                ));
            }
        } else if let Some(sort_preserving_merge) = execution_plan
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>(
        ) {
            let input = sort_preserving_merge.input();
            if input.as_any().downcast_ref::<ExchangeExec>().is_none()
                && !matches!(find_exchange_status(input), ExchangeStatus::Unresolved)
            {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    None,
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(
                    execution_plan.with_new_children(vec![Arc::new(exchange_exec)])?,
                ));
            }
        } else if let Some(repartition) =
            execution_plan.as_any().downcast_ref::<RepartitionExec>()
            && let execution_plan::Partitioning::Hash(_, _) = repartition.partitioning()
        {
            let input = repartition.input();
            if !matches!(find_exchange_status(input), ExchangeStatus::Unresolved) {
                let exchange_exec = ExchangeExec::new(
                    input.clone(),
                    Some(repartition.partitioning().clone()),
                    self.plan_id_generator
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                return Ok(Transformed::yes(Arc::new(exchange_exec)));
            }
        }
        Ok(Transformed::no(execution_plan))
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
        let result = execution_plan.transform_up(|p| self.transform(p))?;

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

/// Scans the subtree for the nearest `ExchangeExec` in each path and returns the
/// aggregate status. Stops recursing at `ExchangeExec` boundaries so that only the
/// shallowest exchange in each branch is considered.
///
/// Returns `Unresolved` as soon as any branch contains an unresolved exchange
/// (short-circuits), `Resolved` if every branch that has an exchange has a resolved
/// one, and `None` if no exchange is found anywhere.
fn find_exchange_status(plan: &Arc<dyn ExecutionPlan>) -> ExchangeStatus {
    if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>() {
        if exchange.shuffle_created() {
            ExchangeStatus::Resolved
        } else {
            ExchangeStatus::Unresolved
        }
    } else {
        let mut found_resolved = false;
        for child in plan.children() {
            match find_exchange_status(child) {
                ExchangeStatus::Unresolved => return ExchangeStatus::Unresolved,
                ExchangeStatus::Resolved => found_resolved = true,
                ExchangeStatus::None => {}
            }
        }
        if found_resolved {
            ExchangeStatus::Resolved
        } else {
            ExchangeStatus::None
        }
    }
}
