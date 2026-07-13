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

//! The circuit-breaker plan node.
//!
//! [`MemoryGuardExec`] is the last line of defence in the executor's OOM protection.
//! The cooperative gate in `RealUsagePool` makes DataFusion spill *before* the process
//! runs out of memory; this node handles the case where that is not enough -- memory
//! the pool never sees, growing past the limit anyway -- by failing the single offending
//! task with a retriable `ResourcesExhausted` instead of letting the OS OOM-kill the
//! whole executor and every task on it.
//!
//! Enforcement happens here, at a `poll_next` boundary, rather than in the allocator:
//! unwinding out of a `GlobalAlloc` is undefined behaviour, and DataFusion runs parts of
//! a plan on spawned tokio sub-tasks, where a panic would merely surface as a `JoinError`
//! and never reach a catch site. A poll boundary is a safe point, needs no unwinding
//! machinery, and costs a handful of relaxed atomic loads per batch.
//!
//! The quantity it checks is the allocator's *live-bytes* counter, which decrements on
//! every `dealloc`. So when the cooperative gate does its job and a consumer spills, the
//! guard stops tripping immediately: it can never latch into failing every batch of every
//! task on the executor. See `oom_guard` for why enforcing on RSS would do exactly that.

use crate::memory_pools::oom_guard;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use std::fmt::Formatter;
use std::sync::Arc;

/// A pass-through [`ExecutionPlan`] that fails its task when the executor's real memory
/// usage is over budget.
///
/// The node is deliberately transparent: it forwards its input's schema, properties and
/// statistics untouched, and its stream yields the input's batches in order, unbuffered
/// and unmodified. Its only added behaviour is a call to [`oom_guard::check_budget`]
/// before each item the input yields; when that reports the process is over the limit,
/// the stream yields `ResourcesExhausted` instead of the batch, failing this one task.
///
/// Metrics are the one exception to "forwards untouched": `metrics()` reports `None`
/// rather than delegating to the input, because Ballista's metrics collection already
/// visits the child separately (see the `metrics()` override below for why delegating
/// would double-count it).
///
/// Transparency is a correctness requirement, not a nicety. Because
/// [`properties`](ExecutionPlan::properties) returns the input's `PlanProperties`
/// unchanged, inserting this node cannot alter output partitioning, ordering, emission
/// type or boundedness -- so it cannot change query results or perturb the scheduler's
/// stage planning.
#[derive(Debug)]
pub struct MemoryGuardExec {
    /// The wrapped input; this node adds nothing to its output but the budget check.
    input: Arc<dyn ExecutionPlan>,
}

impl MemoryGuardExec {
    /// Wrap `input` so that every batch it produces is gated on the memory budget.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }

    /// The wrapped input plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for MemoryGuardExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => write!(f, "MemoryGuardExec"),
        }
    }
}

impl ExecutionPlan for MemoryGuardExec {
    fn name(&self) -> &str {
        "MemoryGuardExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// The input's properties, verbatim. Returning anything else would let this node
    /// change the plan's partitioning or ordering -- a silent correctness bug.
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "MemoryGuardExec expected one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(children.pop().unwrap())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.input.schema();
        // Check before passing each item on. The check sums the balance shards with
        // relaxed loads, so per batch is the right granularity: cheap enough to be free
        // against the cost of producing a batch, frequent enough to stop a runaway task
        // well before the process is killed -- and, because the balance falls as soon as
        // memory is freed, to *stop* failing the moment a spill has done its job.
        //
        // Only gate the success path: if the child itself yielded an error, that error
        // must win over an over-budget verdict. Using `Result::and` here would let a
        // budget check that fails on the same item silently replace the child's real
        // error (e.g. a corrupt-file or serde failure) with `ResourcesExhausted` --
        // misreporting a non-retriable failure as retriable and burning the task's
        // retry budget on a doomed retry.
        let guarded = self
            .input
            .execute(partition, context)?
            .map(|item| item.and_then(|batch| oom_guard::check_budget().map(|_| batch)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, guarded)))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    // Deliberately no `metrics()` override here: this node reports `None` (DataFusion's
    // default), not the child's `MetricsSet`. `collect_plan_metrics`
    // (`ballista/core/src/utils.rs`) pushes `plan.metrics()` for a node and then
    // recurses into its children -- so delegating to `self.input.metrics()` would push
    // the child's metrics twice (once for this node, once for the child itself). That
    // desyncs the executor's flattened metrics list from the scheduler's metrics-free
    // view of the same plan, which zips the two by position
    // (`merge_stage_metrics` in `ballista/scheduler/src/display.rs`) -- a length
    // mismatch makes it bail out, silently dropping stage metrics for every stage of
    // every job. This node has no metrics of its own; the child's are already visited
    // by the recursion. Do not re-add this delegation.

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pools::oom_guard;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

    fn test_input() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    /// The schema shared by the test doubles below: a single non-null `Int32` column.
    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn int_batch(schema: &SchemaRef, values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    /// A test-only input that streams three distinct batches on a single partition and,
    /// as a side effect of producing the *second* one, drives the shared balance over
    /// whatever limit is currently armed. It sets the balance directly (the production
    /// path moves it only through the allocator's deltas) so the step is deterministic.
    ///
    /// This pins the "checks before every batch" contract from `MemoryGuardExec`: with
    /// the guard's per-batch check, the first batch must still arrive intact (the budget
    /// was fine when it was produced), and the stream must then fail once the second
    /// batch's production has pushed the balance over the limit. A guard that instead
    /// checked the budget once, eagerly, at `execute()` time -- before any batch has been
    /// pulled from this input, and therefore before the side effect below ever runs --
    /// would let all three batches through, because the check would already be behind it
    /// by the time the balance moves. That eager, one-shot behaviour is exactly what this
    /// test must catch.
    #[derive(Debug)]
    struct StepBudgetExec {
        properties: Arc<PlanProperties>,
        schema: SchemaRef,
        /// The balance value written once the second batch is produced.
        raised_balance: usize,
    }

    impl StepBudgetExec {
        fn new(raised_balance: usize) -> Self {
            let schema = test_schema();
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self {
                properties,
                schema,
                raised_balance,
            }
        }
    }

    impl DisplayAs for StepBudgetExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => write!(f, "StepBudgetExec"),
            }
        }
    }

    impl ExecutionPlan for StepBudgetExec {
        fn name(&self) -> &str {
            "StepBudgetExec"
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return internal_err!(
                    "StepBudgetExec expected no children, got {}",
                    children.len()
                );
            }
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let schema = self.schema();
            let batches = vec![
                int_batch(&schema, &[1, 2, 3]),
                int_batch(&schema, &[4, 5, 6]),
                int_batch(&schema, &[7, 8, 9]),
            ];
            let raised_balance = self.raised_balance;
            let stream = futures::stream::iter(batches.into_iter().enumerate()).map(
                move |(i, batch)| {
                    if i == 1 {
                        // Fires while producing the *second* batch -- after the first
                        // has already been handed to the guard, never before `execute`
                        // was called.
                        oom_guard::test_support::set_balance_for_test(
                            raised_balance as isize,
                        );
                    }
                    Ok(batch)
                },
            );
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }

        fn partition_statistics(
            &self,
            _partition: Option<usize>,
        ) -> Result<Arc<Statistics>> {
            Ok(Arc::new(Statistics::new_unknown(&self.schema)))
        }
    }

    /// A test-only input whose single batch is an `Err`, for proving that the guard does
    /// not let an over-budget verdict mask a real error from the child.
    #[derive(Debug)]
    struct AlwaysErrExec {
        properties: Arc<PlanProperties>,
        schema: SchemaRef,
    }

    impl AlwaysErrExec {
        fn new() -> Self {
            let schema = test_schema();
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self { properties, schema }
        }
    }

    impl DisplayAs for AlwaysErrExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => write!(f, "AlwaysErrExec"),
            }
        }
    }

    impl ExecutionPlan for AlwaysErrExec {
        fn name(&self) -> &str {
            "AlwaysErrExec"
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return internal_err!(
                    "AlwaysErrExec expected no children, got {}",
                    children.len()
                );
            }
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let schema = self.schema();
            let stream = futures::stream::once(async {
                Err(DataFusionError::Execution("boom".to_string()))
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }

        fn partition_statistics(
            &self,
            _partition: Option<usize>,
        ) -> Result<Arc<Statistics>> {
            Ok(Arc::new(Statistics::new_unknown(&self.schema)))
        }
    }

    #[tokio::test]
    async fn passes_batches_through_when_under_budget() {
        let _g = oom_guard::test_support::ArmedGuard::acquire();
        oom_guard::arm(0); // unset limit: never trips

        let input = test_input();
        let guard = Arc::new(MemoryGuardExec::new(Arc::clone(&input)));

        assert_eq!(guard.schema(), input.schema(), "schema must pass through");

        let stream = guard.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = collect(stream).await.expect("under budget: must succeed");

        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].num_rows(),
            3,
            "batches must pass through unchanged"
        );
    }

    #[tokio::test]
    async fn fails_with_resources_exhausted_when_over_budget() {
        // `ArmedGuard` restores the limit and the balance on drop, including on an
        // early return from a failed assertion below.
        let _g = oom_guard::test_support::ArmedGuard::acquire();
        // The tracking allocator is not installed in this (unit) test binary, so the
        // real balance is always zero here. Drive it explicitly to a value far above
        // the limit, making the over-budget condition deterministic.
        oom_guard::test_support::set_balance_for_test(64 * 1024 * 1024);
        oom_guard::arm(1);

        let guard = Arc::new(MemoryGuardExec::new(test_input()));
        let stream = guard.execute(0, Arc::new(TaskContext::default())).unwrap();
        let result = collect(stream).await;

        let err = result.expect_err("over budget: the stream must fail");
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "must be ResourcesExhausted so the task failure is retriable, got: {err}"
        );
    }

    #[tokio::test]
    async fn checks_the_budget_before_every_batch_not_just_once() {
        // `ArmedGuard` restores the limit and the balance on drop, including on an
        // early return from a failed assertion below.
        let _g = oom_guard::test_support::ArmedGuard::acquire();
        // Start comfortably under a 1 MiB limit; `StepBudgetExec` raises the balance to
        // 64 MiB of its own accord, part-way through the stream.
        oom_guard::test_support::set_balance_for_test(0);
        oom_guard::arm(1024 * 1024);

        let input: Arc<dyn ExecutionPlan> =
            Arc::new(StepBudgetExec::new(64 * 1024 * 1024));
        let guard = Arc::new(MemoryGuardExec::new(input));
        let mut stream = guard.execute(0, Arc::new(TaskContext::default())).unwrap();

        // The first batch was produced while the balance was still under the limit, so
        // it must arrive, and arrive with its own exact contents -- not some other
        // batch's, and not merely "a" batch. A one-shot guard that checked only once at
        // `execute()` time would also pass this assertion, which is why the next one
        // matters.
        let first = stream
            .next()
            .await
            .expect("stream ended before yielding any batch")
            .expect("first batch: budget was under the limit when it was produced");
        let values = first
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            values.values(),
            &[1, 2, 3],
            "the first batch must arrive with its own exact contents, in order"
        );

        // Producing the batch above already pushed the balance to 64 MiB, over the
        // 1 MiB limit. A per-batch check must catch this on the very next item instead
        // of letting the remaining two batches through. A one-shot guard -- which
        // checked the budget once before the stream ever started, and never again --
        // would instead let all three batches pass, and this is the assertion that
        // catches that.
        let second = stream
            .next()
            .await
            .expect("stream ended instead of surfacing the over-budget error");
        let err = second.expect_err(
            "the balance is now over the limit: the guard must fail, not keep streaming",
        );
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "must be ResourcesExhausted so the task failure is retriable, got: {err}"
        );
    }

    #[tokio::test]
    async fn a_child_error_is_not_masked_by_an_over_budget_check() {
        let _g = oom_guard::test_support::ArmedGuard::acquire();
        // Over budget for the entire test: this is what previously made
        // `check_budget().and(item)` discard the child's own error.
        oom_guard::test_support::set_balance_for_test(64 * 1024 * 1024);
        oom_guard::arm(1);

        let input: Arc<dyn ExecutionPlan> = Arc::new(AlwaysErrExec::new());
        let guard = Arc::new(MemoryGuardExec::new(input));
        let stream = guard.execute(0, Arc::new(TaskContext::default())).unwrap();
        let result = collect(stream).await;

        let err = result.expect_err("the child's error must still surface");
        assert!(
            matches!(&err, DataFusionError::Execution(msg) if msg == "boom"),
            "an over-budget verdict must not mask the child's own error \
             (a non-retriable failure must not be misreported as retriable), got: {err}"
        );
    }

    #[test]
    fn preserves_input_plan_properties() {
        let input = test_input();
        let guard = MemoryGuardExec::new(Arc::clone(&input));

        // Inserting the guard must not perturb the plan: same partitioning, same
        // ordering, same emission type, same boundedness. If it did, it would change
        // query semantics or the scheduler's stage planning.
        //
        // Identity, not equality, is the assertion: `properties()` must hand back the
        // input's `PlanProperties` object itself. That is both the strongest possible
        // statement of transparency (every field is the same field, including ones a
        // structural comparison would miss) and the only sound one here --
        // `PartialEq for Partitioning` in DataFusion models "satisfies", not structural
        // equality, so `UnknownPartitioning` (which this input has) never compares equal
        // even to itself, and `assert_eq!` on it would fail for a perfectly transparent
        // node.
        assert!(
            Arc::ptr_eq(guard.properties(), input.properties()),
            "the guard must return the input's PlanProperties unchanged"
        );

        // Belt and braces: the fields the plan's correctness actually rests on, compared
        // with the operators that do have meaningful equality here.
        assert_eq!(
            guard.properties().output_partitioning().partition_count(),
            input.properties().output_partitioning().partition_count()
        );
        assert_eq!(
            guard.properties().output_ordering(),
            input.properties().output_ordering()
        );
        assert_eq!(
            guard.properties().emission_type,
            input.properties().emission_type
        );
        assert_eq!(
            guard.properties().boundedness,
            input.properties().boundedness
        );
    }
}
