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

use crate::state::aqe::execution_plan::ExchangeExec;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct EliminateEmptyExchangeRule {}

impl EliminateEmptyExchangeRule {
    fn transform(
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exchange) = plan.as_any().downcast_ref::<ExchangeExec>() {
            let stats = exchange.partition_statistics(None)?;
            match stats.num_rows {
                Precision::Exact(0) => Ok(Transformed::yes(Arc::new(EmptyExec::new(
                    plan.schema().clone(),
                )))),
                _ => Ok(Transformed::no(plan)),
            }
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
impl PhysicalOptimizerRule for EliminateEmptyExchangeRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan.transform_up(Self::transform)?.data)
    }

    fn name(&self) -> &str {
        "EliminateEmptyExchangeRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}
