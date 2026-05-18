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

//! Plan-time analyzer that identifies eligible `GlobalLimitExec` operators
//! and the producer stages whose row counts feed them.
//!
//! Runs once at the end of [`AdaptiveExecutionGraph::try_new`]. Walks the
//! optimized plan top-down, finds the outermost eligible LIMIT, and traces
//! through pass-through operators (LocalLimit, Coalesce*, Projection) to
//! the immediate `ExchangeExec`s that produce its input. Returns one
//! [`JobLimitContext`] per eligible LIMIT, naming the producer stage IDs
//! the runtime tracker should observe.
//!
//! Eligibility (false negatives are safe — the query simply runs without
//! early-stop, matching today's behavior):
//!   - `skip == 0` (no OFFSET; v2)
//!   - `fetch.is_some()` (no `LIMIT ALL`)
//!   - no `SortExec` / `SortPreservingMergeExec` / ordered aggregate /
//!     ordered window in the subtree (Top-K is a separate optimization;
//!     ordered cancellation would be wrong without sort awareness)
//!   - the LIMIT is connected to its producer Exchange(s) only through
//!     pass-through operators (LocalLimit, CoalesceBatches,
//!     CoalescePartitions, Projection, Union)
//!   - each producer Exchange already has a `stage_id` assigned
//!
//! Nested LIMITs: only the outermost is tagged.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use log::debug;

use crate::state::aqe::execution_plan::ExchangeExec;

/// Per-job tagging produced by [`LimitEarlyStopAnalyzer`].
///
/// `fetch` is the LIMIT's row count. `producer_stage_ids` is the set of
/// stages whose `ShuffleWritePartition.num_rows` should be summed by the
/// runtime tracker; once the sum crosses `fetch * safety_factor` the
/// scheduler fires an early-stop cancellation for the job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobLimitContext {
    pub fetch: u64,
    pub producer_stage_ids: HashSet<usize>,
}

/// Walks the post-stage-resolution plan and collects [`JobLimitContext`]
/// entries for eligible LIMIT operators.
pub struct LimitEarlyStopAnalyzer<'a> {
    plan: &'a Arc<dyn ExecutionPlan>,
}

impl<'a> LimitEarlyStopAnalyzer<'a> {
    pub fn new(plan: &'a Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }

    /// Top-down scan: emit one context per outermost eligible LIMIT.
    ///
    /// Returns an empty vec for plans with no eligible LIMIT, including
    /// plans whose producer Exchange has no stage_id yet (early-stop can
    /// still be added in a later AQE iteration; v1 simply skips).
    pub fn analyze(&self) -> Vec<JobLimitContext> {
        let mut contexts = Vec::new();
        Self::visit(self.plan, &mut contexts);
        contexts
    }

    fn visit(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<JobLimitContext>) {
        if let Some(limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
            if let Some(ctx) = Self::try_build_context(limit) {
                out.push(ctx);
                // Outermost-only: do not recurse below an emitted LIMIT.
                return;
            }
            // Ineligible — still recurse to find a nested eligible LIMIT.
        }
        for child in plan.children() {
            Self::visit(child, out);
        }
    }

    fn try_build_context(limit: &GlobalLimitExec) -> Option<JobLimitContext> {
        if limit.skip() != 0 {
            return None;
        }
        let fetch = limit.fetch()?;
        if fetch == 0 {
            return None;
        }
        let child = limit.children().into_iter().next()?.clone();
        if subtree_has_ordering(&child) {
            return None;
        }
        let mut producer_stage_ids = HashSet::new();
        if !collect_producer_stage_ids(&child, &mut producer_stage_ids) {
            return None;
        }
        if producer_stage_ids.is_empty() {
            return None;
        }
        Some(JobLimitContext {
            fetch: fetch as u64,
            producer_stage_ids,
        })
    }
}

/// Walk from the LIMIT's input until reaching producer Exchanges,
/// pushing each Exchange's `stage_id` into `out`. Returns false if any
/// branch leads to an Exchange without a `stage_id` set, or hits a node
/// outside the pass-through allowlist before finding one.
///
/// Allowlist (operators that do not invalidate row-count tracking when
/// placed between the LIMIT and its producer Exchange):
///   - `LocalLimitExec` (slices but the writer above counts pre-slice)
///   - `CoalesceBatchesExec`, `CoalescePartitionsExec`
///   - `ProjectionExec` (pure expression evaluation)
///   - `UnionExec` (recurse into each branch)
fn collect_producer_stage_ids(
    node: &Arc<dyn ExecutionPlan>,
    out: &mut HashSet<usize>,
) -> bool {
    let any = node.as_any();
    if let Some(exchange) = any.downcast_ref::<ExchangeExec>() {
        match exchange.stage_id() {
            Some(id) => {
                out.insert(id);
                true
            }
            None => {
                debug!(
                    "LimitEarlyStopAnalyzer: producer ExchangeExec has no \
                     stage_id yet; skipping early-stop for this LIMIT"
                );
                false
            }
        }
    } else if any.downcast_ref::<LocalLimitExec>().is_some()
        || is_coalesce_batches(any)
        || any.downcast_ref::<CoalescePartitionsExec>().is_some()
        || any.downcast_ref::<ProjectionExec>().is_some()
        || any.downcast_ref::<UnionExec>().is_some()
    {
        // Pass-through: recurse through all children.
        for child in node.children() {
            if !collect_producer_stage_ids(child, out) {
                return false;
            }
        }
        true
    } else {
        // Anything else (joins, aggregates, scans, sorts, etc.) breaks
        // the simple producer-row counting model used in v1.
        false
    }
}

#[allow(deprecated)]
fn is_coalesce_batches(any: &dyn std::any::Any) -> bool {
    any.downcast_ref::<CoalesceBatchesExec>().is_some()
}

/// Conservative ordering detection. Any of these in the LIMIT's subtree
/// means we bail — early-stop must not silently reorder results.
fn subtree_has_ordering(node: &Arc<dyn ExecutionPlan>) -> bool {
    let any = node.as_any();
    if any.downcast_ref::<SortExec>().is_some()
        || any.downcast_ref::<SortPreservingMergeExec>().is_some()
        || any.downcast_ref::<WindowAggExec>().is_some()
        || any.downcast_ref::<BoundedWindowAggExec>().is_some()
    {
        return true;
    }
    if let Some(agg) = any.downcast_ref::<AggregateExec>() {
        if agg.input_order_mode() != &datafusion::physical_plan::InputOrderMode::Linear
        {
            return true;
        }
    }
    node.children()
        .iter()
        .any(|c| subtree_has_ordering(c))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::execution_plan::AdaptiveDatafusionExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{ExecutionPlan, expressions::Column};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, true)]))
    }

    fn leaf() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    fn exchange_with_stage(
        input: Arc<dyn ExecutionPlan>,
        stage_id: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let exec = ExchangeExec::new(input, None, 0);
        if let Some(id) = stage_id {
            exec.set_stage_id(id);
        }
        Arc::new(exec)
    }

    fn limit(
        skip: usize,
        fetch: Option<usize>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(input, skip, fetch))
    }

    fn analyze(plan: Arc<dyn ExecutionPlan>) -> Vec<JobLimitContext> {
        let plan = Arc::new(AdaptiveDatafusionExec::new(99, plan));
        let plan: Arc<dyn ExecutionPlan> = plan;
        LimitEarlyStopAnalyzer::new(&plan).analyze()
    }

    #[test]
    fn eligible_simple_limit() {
        // root -> GlobalLimit(100) -> Exchange(stage=7) -> leaf
        let plan = limit(0, Some(100), exchange_with_stage(leaf(), Some(7)));
        let ctx = analyze(plan);
        assert_eq!(ctx.len(), 1);
        assert_eq!(ctx[0].fetch, 100);
        assert_eq!(ctx[0].producer_stage_ids, HashSet::from([7]));
    }

    #[test]
    fn eligible_with_passthrough_operators() {
        // GlobalLimit -> CoalescePartitions -> LocalLimit -> Exchange
        let exchange = exchange_with_stage(leaf(), Some(3));
        let local = Arc::new(LocalLimitExec::new(exchange, 100));
        let coalesce = Arc::new(CoalescePartitionsExec::new(local));
        let plan = limit(0, Some(100), coalesce);
        let ctx = analyze(plan);
        assert_eq!(ctx.len(), 1);
        assert_eq!(ctx[0].producer_stage_ids, HashSet::from([3]));
    }

    #[test]
    fn eligible_union_multi_producer() {
        // GlobalLimit -> Union(Exchange(2), Exchange(5))
        let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![
            exchange_with_stage(leaf(), Some(2)),
            exchange_with_stage(leaf(), Some(5)),
        ])
        .unwrap();
        let plan = limit(0, Some(50), union);
        let ctx = analyze(plan);
        assert_eq!(ctx.len(), 1);
        assert_eq!(ctx[0].producer_stage_ids, HashSet::from([2, 5]));
    }

    #[test]
    fn ineligible_offset() {
        let plan = limit(5, Some(100), exchange_with_stage(leaf(), Some(7)));
        assert!(analyze(plan).is_empty());
    }

    #[test]
    fn ineligible_no_fetch() {
        let plan = limit(0, None, exchange_with_stage(leaf(), Some(7)));
        assert!(analyze(plan).is_empty());
    }

    #[test]
    fn ineligible_sort_below() {
        let sort_expr =
            datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(
                Column::new("c", 0),
            ));
        let sorted = Arc::new(SortExec::new(
            datafusion::physical_expr::LexOrdering::new(vec![sort_expr]).unwrap(),
            leaf(),
        ));
        let plan = limit(0, Some(100), exchange_with_stage(sorted, Some(7)));
        assert!(analyze(plan).is_empty());
    }

    #[test]
    fn ineligible_producer_exchange_without_stage_id() {
        let plan = limit(0, Some(100), exchange_with_stage(leaf(), None));
        assert!(analyze(plan).is_empty());
    }

    #[test]
    fn ineligible_non_passthrough_between_limit_and_exchange() {
        // GlobalLimit -> Projection(empty list) -> Exchange : Projection IS
        // in the allowlist, so this should still be eligible. Pick something
        // that's not allow-listed instead: Sort with a single sort expr.
        let sort_expr =
            datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(
                Column::new("c", 0),
            ));
        let sorted = Arc::new(SortExec::new(
            datafusion::physical_expr::LexOrdering::new(vec![sort_expr]).unwrap(),
            exchange_with_stage(leaf(), Some(7)),
        ));
        let plan = limit(0, Some(100), sorted);
        // Subtree contains SortExec → bails out via ordering check before
        // producer collection. Either way: ineligible.
        assert!(analyze(plan).is_empty());
    }

    #[test]
    fn projection_between_limit_and_exchange_is_eligible() {
        use datafusion::physical_plan::PhysicalExpr;
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c", 0));
        let proj: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![(col, "c".to_string())],
                exchange_with_stage(leaf(), Some(11)),
            )
            .unwrap(),
        );
        let plan = limit(0, Some(100), proj);
        let ctx = analyze(plan);
        assert_eq!(ctx.len(), 1);
        assert_eq!(ctx[0].producer_stage_ids, HashSet::from([11]));
    }

    #[test]
    fn nested_limits_outermost_only() {
        // Outer Limit(50) -> Exchange(stage 4) -> inner Limit(10) -> Exchange(stage 9)
        let inner = limit(0, Some(10), exchange_with_stage(leaf(), Some(9)));
        let middle_exchange = exchange_with_stage(inner, Some(4));
        let plan = limit(0, Some(50), middle_exchange);
        let ctx = analyze(plan);
        assert_eq!(ctx.len(), 1);
        assert_eq!(ctx[0].fetch, 50);
        assert_eq!(ctx[0].producer_stage_ids, HashSet::from([4]));
    }
}
