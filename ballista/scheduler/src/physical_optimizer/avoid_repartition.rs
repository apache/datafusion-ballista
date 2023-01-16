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

//! Rule to push hash partitioning down and replace round-robin partitioning

use std::sync::Arc;

use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, with_new_children_if_necessary};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::prelude::SessionConfig;
use datafusion::common::Result;

/// This rule attempts to push hash repartitions (usually introduced to facilitate hash partitioned
/// joins) down to the table scan and replace any round-robin partitioning that exists. It is
/// wasteful to perform round-robin partition first only to repartition later for the join. It is
/// more efficient to just partition by the join key(s) right away.
#[derive(Default)]
pub struct AvoidRepartition {}

impl PhysicalOptimizerRule for AvoidRepartition {

    fn name(&self) -> &str {
        "avoid_repartition"
    }

    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &SessionConfig) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        remove_redundant_repartitioning(plan, None)
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn remove_redundant_repartitioning(
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(repart) = plan.as_any().downcast_ref::<RepartitionExec>() {
        match repart.partitioning() {
            Partitioning::RoundRobinBatch(_) => {
                match partitioning {
                    Some(Partitioning::Hash(expr, n)) => {
                        // check that all partition columns are resolvable
                        if expr.iter().all(|e| {
                            if let Some(x) = e.as_any().downcast_ref::<Column>() {
                                repart.schema().column_with_name(x.name()).is_some()
                            } else {
                                false
                            }
                        }) {
                            // drop the round-robin repartition and replace with the hash partitioning
                            Ok(Arc::new(RepartitionExec::try_new(
                                plan.children()[0].clone(),
                                Partitioning::Hash(expr.clone(), n),
                            )?))
                        } else {
                            Ok(plan.clone())
                        }
                    }
                    _ => Ok(plan.clone())
                }
            }
            Partitioning::Hash(expr, _) => {
                // we only attempt to push hash-partitioning down if the hash expressions
                // are simple column references
                if expr.iter().all(|e| e.as_any().downcast_ref::<Column>().is_some()) {
                    let p = repart.partitioning().clone();
                    let new_plan = optimize_children(plan, Some(p.clone()))?;
                    if new_plan.children()[0].output_partitioning() == p {
                        // drop this hash partitioning if we pushed it down
                        Ok(new_plan.children()[0].clone())
                    } else {
                        Ok(new_plan)
                    }
                } else {
                    Ok(plan.clone())
                }
            }
            _ => optimize_children(plan, partitioning),
        }
    } else if plan.as_any().downcast_ref::<AggregateExec>().is_some()
        || plan.as_any().downcast_ref::<WindowAggExec>().is_some() /*
        || plan.as_any().downcast_ref::<BoundedWindowAggExec>().is_some()*/ {
        // skip these
        Ok(plan.clone())
    } else {
        optimize_children(plan, partitioning)
    }
}

fn optimize_children(
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let inputs = plan.children();
    if inputs.is_empty() {
        return Ok(plan.clone());
    }
    let mut new_inputs = vec![];
    for input in &inputs {
        let new_input =
            remove_redundant_repartitioning(input.clone(), partitioning.clone())?;
        new_inputs.push(new_input);
    }
    with_new_children_if_necessary(plan, new_inputs).map_err(|e| e.into())
}
