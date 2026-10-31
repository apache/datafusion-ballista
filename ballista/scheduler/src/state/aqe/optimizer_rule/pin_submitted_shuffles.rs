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

//! Keeps the shuffles of a submitted physical plan.
//!
//! A plan planned from a logical plan only has the repartitions DataFusion
//! derived from operator requirements, so `EnforceDistribution` can remove
//! them all and plant them again on every adaptive replan. A client that
//! submits a physical plan placed its hash repartitions on purpose, and the
//! operators around them may be extension nodes that declare no requirement
//! DataFusion could derive the repartition back from. [`PinSubmittedShufflesRule`]
//! runs once, before the first replan, and turns every hash or range
//! `RepartitionExec` into an [`ExchangeExec`]: a stage boundary the rest of the
//! chain keeps. A range repartition matters even more than a hash one: nothing
//! derives it back, and without it a consumer that sorts each partition no
//! longer sorts the whole result. One that must preserve the order of its
//! input is left alone, since a shuffle cannot do that.
//!
//! A `CoalescePartitionsExec` is not pinned. DataFusion re-derives it from the
//! consumer's `required_input_distribution`, so an extension operator that
//! needs a single partition has to say so there.

use crate::state::aqe::execution_plan::ExchangeExec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Turns the hash and range repartitions of a submitted physical plan into
/// stage boundaries before DataFusion's optimizers can remove them.
#[derive(Debug, Default)]
pub struct PinSubmittedShufflesRule {
    plan_id_generator: Arc<AtomicUsize>,
}

impl PinSubmittedShufflesRule {
    pub(crate) fn new(plan_id_generator: Arc<AtomicUsize>) -> Self {
        Self { plan_id_generator }
    }
}

impl PhysicalOptimizerRule for PinSubmittedShufflesRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_up(|node| {
                let Some(repartition) = node.downcast_ref::<RepartitionExec>() else {
                    return Ok(Transformed::no(node));
                };
                let partitioning = repartition.partitioning();
                let pinned = match partitioning {
                    Partitioning::Hash(_, _) => true,
                    Partitioning::Range(_) => !repartition.preserve_order(),
                    _ => false,
                };
                if !pinned {
                    return Ok(Transformed::no(node));
                }
                Ok(Transformed::yes(Arc::new(ExchangeExec::new(
                    repartition.input().clone(),
                    Some(partitioning.clone()),
                    self.plan_id_generator.fetch_add(1, Ordering::Relaxed),
                ))))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "PinSubmittedShufflesRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
