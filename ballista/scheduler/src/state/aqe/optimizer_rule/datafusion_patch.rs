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

//! Some datafusion rules are not idempotent, thus set of
//! physical plan rules needed to fix them.
//!
//! Datafusion needs to be patched, until then
//! we keep the rules.
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, displayable, execution_plan};
use log::warn;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct EliminateRoundRobbinRule {}
impl EliminateRoundRobbinRule {
    fn transform(
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(repartition) =
            execution_plan.as_any().downcast_ref::<RepartitionExec>()
        {
            match repartition.partitioning() {
                execution_plan::Partitioning::RoundRobinBatch(_) => {
                    Ok(Transformed::yes(repartition.input().clone()))
                }
                _ => Ok(Transformed::no(execution_plan)),
            }
        } else {
            Ok(Transformed::no(execution_plan))
        }
    }
}

impl PhysicalOptimizerRule for EliminateRoundRobbinRule {
    fn optimize(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(execution_plan.transform_up(Self::transform)?.data)
    }

    fn name(&self) -> &str {
        "DistributedEliminateRoundRobbinRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// In some cases double [CooperativeExec] will be inserted.
/// Until we get chance to investigate the issue this rule will
/// eliminate doubles
//
// TODO: this has been fixed in datafusion 53, should be removed when updated
//
#[derive(Debug, Clone, Default)]
pub struct EliminateCooperativeExecRule {}
/// for some reason double CoalesceBatchesExec
/// show
impl EliminateCooperativeExecRule {
    fn transform(
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(coalesce) = execution_plan.as_any().downcast_ref::<CooperativeExec>()
        {
            if coalesce
                .input()
                .as_any()
                .downcast_ref::<CooperativeExec>()
                .is_some()
            {
                Ok(Transformed::yes(coalesce.input().clone()))
            } else {
                Ok(Transformed::no(execution_plan))
            }
        } else {
            Ok(Transformed::no(execution_plan))
        }
    }
}

impl PhysicalOptimizerRule for EliminateCooperativeExecRule {
    fn optimize(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let result = execution_plan.transform_up(Self::transform)?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "EliminateCooperativeExecRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
/// This rule is just for DEBUG purposes as some
/// physical rules are not idempotent.
#[derive(Debug, Clone, Default)]
pub struct WarnOnDuplicateExecRule {}

impl WarnOnDuplicateExecRule {
    fn transform(
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let exec_name = execution_plan.name();
        if execution_plan
            .children()
            .iter()
            .any(|child| child.name() == exec_name)
        {
            warn!(
                "there might be a duplicated exec with name {},\n {}",
                exec_name,
                displayable(execution_plan.as_ref()).indent(false)
            );
        }

        Ok(Transformed::no(execution_plan))
    }
}

impl PhysicalOptimizerRule for WarnOnDuplicateExecRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(Self::transform).data()
    }

    fn name(&self) -> &str {
        "WarnOnDuplicateExecRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
