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

//! Rules a scheduler embedder adds to adaptive query planning.

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// A physical optimizer rule, as adaptive planning runs it.
pub type AdaptiveRule = Arc<dyn PhysicalOptimizerRule + Send + Sync>;

/// Physical optimizer rules that a scheduler embedder adds to adaptive query
/// planning, for plans with `ExecutionPlan` nodes of its own that Ballista's
/// rules don't know, such as nodes an extension planner creates.
///
/// A rule that places an exchange of its own takes its plan id from
/// `plan_ids`, the generator the job's other rules share.
pub trait AdaptiveRules: Debug + Send + Sync {
    /// Rules that run once, while the job's physical plan is created, after
    /// Ballista's own preparation rules and before its stage boundaries are
    /// placed.
    fn preparation_rules(&self, _plan_ids: Arc<AtomicUsize>) -> Vec<AdaptiveRule> {
        vec![]
    }

    /// Rules that run each time the plan is optimized: when the job is
    /// planned, and again after each stage completes, when the finished
    /// stage's [`ExchangeExec`](super::execution_plan::ExchangeExec) reports
    /// measured statistics.
    ///
    /// They run last, after Ballista has placed the stage boundaries, so they
    /// see every stage before it can start. A rule can hold back a stage that
    /// hasn't started by marking its exchange inactive, and release it on a
    /// later replan. A rule must leave the stage boundaries valid: no other
    /// rule runs after it to place an exchange that a change needs.
    fn replan_rules(&self, _plan_ids: Arc<AtomicUsize>) -> Vec<AdaptiveRule> {
        vec![]
    }
}
