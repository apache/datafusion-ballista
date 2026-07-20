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

// SpillingHashJoinExec is a physical execution plan node for a hash join
// whose build side spills to disk instead of requiring the entire hash table
// to fit in memory. `execute` runs in bounded memory: build sub-partitions
// ("buckets") are kept resident while the runtime `MemoryPool` grants space
// and spilled to disk under pressure, and spilled buckets are drained one at a
// time after the probe side ends (see `stream.rs`).
//
// v1 invariants (enforced by the constructor, which always builds an
// inner join with no residual filter and no output projection):
//   - join_type is always JoinType::Inner
//   - filter is always None
//   - projection is always None
//   - partition_mode must be PartitionMode::Partitioned (execute() always
//     runs Partitioned semantics; CollectLeft is rejected)
//   - num_sub_partitions must be >= 1 (0 would divide by zero when bucketing
//     the build side)

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{JoinType, NullEquality, Result, internal_err, not_impl_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

use super::stream::execute_join;

/// Physical execution plan node for a hash join whose build side spills
/// sub-partitions to disk rather than requiring the whole build side to fit in
/// memory at once.
///
/// `execute` runs in bounded memory, spilling buckets under `MemoryPool`
/// pressure and draining them one at a time (see `stream.rs`).
#[derive(Debug)]
pub struct SpillingHashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    partition_mode: PartitionMode,
    num_sub_partitions: usize,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl SpillingHashJoinExec {
    /// Creates a new `SpillingHashJoinExec` joining `left` and `right` on the
    /// equijoin pairs in `on`.
    ///
    /// This is a v1 scaffold: the join is always `JoinType::Inner`, with no
    /// residual filter and no output projection. A throwaway `HashJoinExec`
    /// is constructed purely to borrow its output `schema()` and
    /// `properties()` (partitioning/equivalence info) — it is never
    /// executed.
    ///
    /// Returns an error if `partition_mode` is not `PartitionMode::Partitioned`
    /// (v1 `execute()` always runs Partitioned semantics — `CollectLeft` is
    /// accepted by the proto codec but not yet implemented here) or if
    /// `num_sub_partitions` is `0` (the build-side hash bucketing divides by
    /// this count and would panic).
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        partition_mode: PartitionMode,
        num_sub_partitions: usize,
    ) -> Result<Self> {
        if partition_mode != PartitionMode::Partitioned {
            return not_impl_err!(
                "SpillingHashJoinExec only supports PartitionMode::Partitioned in v1, got {partition_mode:?}"
            );
        }
        if num_sub_partitions == 0 {
            return internal_err!(
                "SpillingHashJoinExec requires num_sub_partitions >= 1, got 0"
            );
        }

        let hj = HashJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            None,
            &JoinType::Inner,
            None,
            partition_mode,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let schema = hj.schema();
        let properties = hj.properties().clone();

        Ok(Self {
            left,
            right,
            on,
            partition_mode,
            num_sub_partitions,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Returns the equijoin column pairs `(left_expr, right_expr)`.
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    /// Returns the configured partition mode.
    pub fn partition_mode(&self) -> PartitionMode {
        self.partition_mode
    }

    /// Returns the number of sub-partitions the build side is split into
    /// for spilling.
    pub fn num_sub_partitions(&self) -> usize {
        self.num_sub_partitions
    }

    /// Returns the left (build) child.
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// Returns the right (probe) child.
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
}

impl ExecutionPlan for SpillingHashJoinExec {
    fn name(&self) -> &str {
        "SpillingHashJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!(
                "SpillingHashJoinExec expected two children, got {}",
                children.len()
            );
        }
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        Ok(Arc::new(Self::try_new(
            left,
            right,
            self.on.clone(),
            self.partition_mode,
            self.num_sub_partitions,
        )?))
    }

    /// Executes partition `partition` of both children (the Ballista shuffle
    /// contract guarantees rows with equal join keys are co-located in the
    /// same partition number on both sides — see `PartitionMode::Partitioned`
    /// semantics) and returns a stream that drains the left (build) side fully
    /// into per-sub-partition hash tables before probing the right (probe)
    /// side, spilling buckets to disk under memory pressure and draining any
    /// spilled buckets one at a time after the probe side ends.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left = self.left.execute(partition, Arc::clone(&context))?;
        let right = self.right.execute(partition, Arc::clone(&context))?;

        let (left_keys, right_keys): (Vec<PhysicalExprRef>, Vec<PhysicalExprRef>) =
            self.on.iter().cloned().unzip();

        Ok(execute_join(
            self.schema(),
            left,
            right,
            left_keys,
            right_keys,
            self.num_sub_partitions,
            context,
            &self.metrics,
            partition,
        ))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for SpillingHashJoinExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on = self
                    .on
                    .iter()
                    .map(|(l, r)| format!("({l}, {r})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "SpillingHashJoinExec: on=[{on}], mode={:?}",
                    self.partition_mode
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "mode={:?}", self.partition_mode)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::expressions::Column;

    /// Returns `(left, right, on)`: two single-partition-schema `DataSourceExec`
    /// plans with schema `(k Int32, v Int32)`, each split into 4 partitions,
    /// joined on `on = [(k_left, k_right)]`.
    #[allow(clippy::type_complexity)]
    fn two_col_inputs() -> (
        Arc<dyn ExecutionPlan>,
        Arc<dyn ExecutionPlan>,
        Vec<(PhysicalExprRef, PhysicalExprRef)>,
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let partitions: Vec<Vec<_>> = (0..4).map(|_| vec![]).collect();

        let left_source =
            MemorySourceConfig::try_new(&partitions, Arc::clone(&schema), None)
                .expect("left MemorySourceConfig");
        let right_source =
            MemorySourceConfig::try_new(&partitions, Arc::clone(&schema), None)
                .expect("right MemorySourceConfig");

        let left: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(left_source)));
        let right: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(right_source)));

        let on: Vec<(PhysicalExprRef, PhysicalExprRef)> =
            vec![(Arc::new(Column::new("k", 0)), Arc::new(Column::new("k", 0)))];

        (left, right, on)
    }

    #[test]
    fn rejects_collect_left_partition_mode() {
        let (left, right, on) = two_col_inputs();
        let err = SpillingHashJoinExec::try_new(
            left,
            right,
            on,
            PartitionMode::CollectLeft,
            16,
        )
        .expect_err("CollectLeft is not implemented in v1 and must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("Partitioned"),
            "error should name the unsupported mode, got: {msg}"
        );
    }

    #[test]
    fn rejects_zero_sub_partitions() {
        let (left, right, on) = two_col_inputs();
        let err =
            SpillingHashJoinExec::try_new(left, right, on, PartitionMode::Partitioned, 0)
                .expect_err("num_sub_partitions == 0 must be rejected, not panic later");
        let msg = err.to_string();
        assert!(
            msg.contains("num_sub_partitions"),
            "error should name the bad argument, got: {msg}"
        );
    }

    #[test]
    fn scaffold_schema_and_name() {
        let (left, right, on) = two_col_inputs();
        let exec = SpillingHashJoinExec::try_new(
            left,
            right,
            on,
            PartitionMode::Partitioned,
            16,
        )
        .unwrap();
        assert_eq!(exec.name(), "SpillingHashJoinExec");
        // Output schema is left ++ right columns.
        assert_eq!(exec.schema().fields().len(), 4);
        let rendered =
            format!("{}", displayable(&exec as &dyn ExecutionPlan).indent(false));
        assert!(rendered.contains("SpillingHashJoinExec"), "{rendered}");
    }

    // --- oracle test: matches DataFusion's own Partitioned HashJoinExec ---
    //
    // `PartitionMode::Partitioned` carries a contract that is easy to break
    // in a test without noticing: it assumes rows with equal join keys are
    // already co-located in the same partition number on both sides (the
    // Ballista shuffle contract). `HashJoinExec` (DataFusion's own
    // Partitioned join) makes the exact same assumption — feed either join
    // un-hash-partitioned input and both produce silently wrong results in
    // the same way, so a naive comparison could pass for the wrong reason.
    //
    // To keep the comparison meaningful, both sides below are driven from
    // data that has actually been hash-partitioned on the join key via
    // `RepartitionExec`, so equal keys really are co-located per partition
    // for both plans under test.
    use datafusion::arrow::array::Int32Array;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::repartition::RepartitionExec;

    /// Builds a single-input-partition `DataSourceExec` with schema
    /// `(key_name Int32, val_name Int32)`, split into several batches of
    /// `batch_size` rows (to exercise multi-batch accumulation on both the
    /// build and probe sides), `num_rows` rows total. Row `i` gets key
    /// `i % key_modulus` (so keys repeat, producing multi-row matches) and
    /// value `i as i32 + val_offset`.
    fn make_source(
        key_name: &str,
        val_name: &str,
        num_rows: usize,
        key_modulus: i32,
        val_offset: i32,
        batch_size: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(key_name, DataType::Int32, false),
            Field::new(val_name, DataType::Int32, false),
        ]));

        let mut batches = Vec::new();
        let mut start = 0usize;
        while start < num_rows {
            let end = (start + batch_size).min(num_rows);
            let keys: Vec<i32> = (start..end).map(|r| (r as i32) % key_modulus).collect();
            let vals: Vec<i32> = (start..end).map(|r| r as i32 + val_offset).collect();
            batches.push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(keys)),
                        Arc::new(Int32Array::from(vals)),
                    ],
                )
                .unwrap(),
            );
            start = end;
        }

        let partitions = vec![batches];
        let source = MemorySourceConfig::try_new(&partitions, schema, None)
            .expect("MemorySourceConfig");
        Arc::new(DataSourceExec::new(Arc::new(source)))
    }

    /// Hash-repartitions `base` (a single-partition source) into `n`
    /// partitions on the column at `key_index`, using a fresh
    /// `RepartitionExec`.
    ///
    /// Deliberately builds a *new* `RepartitionExec` per call rather than
    /// sharing one between the oracle and the exec under test: each output
    /// partition of a `RepartitionExec` is single-consumer (its receiver is
    /// taken on first `execute`), so one shared instance could not feed both
    /// joins' full partition sweep. Hash bucketing is a pure, deterministic
    /// function of the input batches, expressions, and partition count, so
    /// two independent `RepartitionExec`s built over the same `base` data
    /// reproduce byte-identical per-partition bucketing — which is all the
    /// "same repartitioned input" fairness requirement actually needs.
    fn hash_repartition(
        base: Arc<dyn ExecutionPlan>,
        key_index: usize,
        n: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let field = base.schema().field(key_index).clone();
        let expr: PhysicalExprRef = Arc::new(Column::new(field.name(), key_index));
        Arc::new(
            RepartitionExec::try_new(base, Partitioning::Hash(vec![expr], n))
                .expect("RepartitionExec::try_new"),
        )
    }

    /// Runs DataFusion's own `HashJoinExec` (Inner, `PartitionMode::Partitioned`)
    /// over `left`/`right` and collects every output row across all output
    /// partitions — the oracle `SpillingHashJoinExec` must match.
    async fn oracle_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        ctx: Arc<TaskContext>,
    ) -> Vec<RecordBatch> {
        let hj = HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap();
        collect(Arc::new(hj), ctx).await.unwrap()
    }

    /// Flattens output batches with schema `(lk, lv, rk, rv)` into a sorted
    /// vector of row tuples, so two result sets can be compared independent
    /// of batch boundaries or row order (both are unspecified across
    /// partitions and within matched key groups).
    fn sorted_rows(batches: &[RecordBatch]) -> Vec<(i32, i32, i32, i32)> {
        let mut out = Vec::new();
        for batch in batches {
            let col = |i: usize| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
            };
            let (lk, lv, rk, rv) = (col(0), col(1), col(2), col(3));
            for i in 0..batch.num_rows() {
                out.push((lk.value(i), lv.value(i), rk.value(i), rv.value(i)));
            }
        }
        out.sort_unstable();
        out
    }

    #[tokio::test]
    async fn matches_datafusion_inner_partitioned_in_memory() {
        let ctx = Arc::new(TaskContext::default()); // default = generous memory, nothing spills
        let num_partitions = 4;

        // A few thousand rows per side; overlapping, repeating integer keys
        // so both single- and multi-row matches occur on both sides.
        let base_left = make_source("lk", "lv", 3_000, 500, 0, 777);
        let base_right = make_source("rk", "rv", 3_500, 450, 1_000_000, 900);

        let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = vec![(
            Arc::new(Column::new("lk", 0)),
            Arc::new(Column::new("rk", 0)),
        )];

        // Independent (but content-identical, see `hash_repartition`)
        // hash-partitioned inputs for the oracle vs. the exec under test.
        let oracle_left = hash_repartition(Arc::clone(&base_left), 0, num_partitions);
        let oracle_right = hash_repartition(Arc::clone(&base_right), 0, num_partitions);
        let ours_left = hash_repartition(base_left, 0, num_partitions);
        let ours_right = hash_repartition(base_right, 0, num_partitions);

        let expected =
            oracle_join(oracle_left, oracle_right, on.clone(), Arc::clone(&ctx)).await;

        let exec = SpillingHashJoinExec::try_new(
            ours_left,
            ours_right,
            on,
            PartitionMode::Partitioned,
            16,
        )
        .unwrap();
        let actual = collect(Arc::new(exec), ctx).await.unwrap();

        let expected_rows = sorted_rows(&expected);
        let actual_rows = sorted_rows(&actual);
        assert!(
            !expected_rows.is_empty(),
            "test data should produce matching rows"
        );
        assert_eq!(actual_rows, expected_rows);
    }

    // --- spill oracle tests ---
    //
    // These reuse the same "match DataFusion's own Partitioned HashJoinExec"
    // harness, but run `SpillingHashJoinExec` under a tiny `MemoryPool` so the
    // build side cannot stay fully resident. Correctness under a tiny pool
    // requires BOTH build-side spilling and probe-side routing plus a
    // one-at-a-time drain of spilled buckets, so the two tests below gate the
    // whole spilling path, not just a slice of it.
    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::FairSpillPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    /// Builds a `TaskContext` whose `RuntimeEnv` has a `FairSpillPool` of
    /// `pool_bytes` and a real (OS-temp) `DiskManager`, so `try_grow` fails
    /// once the pool is exhausted and spilled batches have somewhere to go.
    fn small_pool_ctx(pool_bytes: usize) -> Arc<TaskContext> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(pool_bytes)))
            .build()
            .expect("build RuntimeEnv");
        Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
    }

    /// Sums the operator's `spill_count` metric across all partitions.
    fn total_spill_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        plan.metrics()
            .and_then(|m| m.sum_by_name("spill_count"))
            .map(|v| v.as_usize())
            .unwrap_or(0)
    }

    /// Sums the operator's `repartition_count` metric (drain-phase recursive
    /// re-partition events) across all partitions.
    fn total_repartition_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        plan.metrics()
            .and_then(|m| m.sum_by_name("repartition_count"))
            .map(|v| v.as_usize())
            .unwrap_or(0)
    }

    /// Shared body for the spill oracle tests: run the oracle under a generous
    /// context and `SpillingHashJoinExec` under a `pool_bytes` pool, assert the
    /// sorted outputs match and that at least `min_spills` spill events fired.
    async fn assert_matches_under_pool(pool_bytes: usize, min_spills: usize) {
        let generous = Arc::new(TaskContext::default());
        let small = small_pool_ctx(pool_bytes);
        let num_partitions = 4;

        let base_left = make_source("lk", "lv", 3_000, 500, 0, 777);
        let base_right = make_source("rk", "rv", 3_500, 450, 1_000_000, 900);

        let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = vec![(
            Arc::new(Column::new("lk", 0)),
            Arc::new(Column::new("rk", 0)),
        )];

        let oracle_left = hash_repartition(Arc::clone(&base_left), 0, num_partitions);
        let oracle_right = hash_repartition(Arc::clone(&base_right), 0, num_partitions);
        let ours_left = hash_repartition(base_left, 0, num_partitions);
        let ours_right = hash_repartition(base_right, 0, num_partitions);

        let expected = oracle_join(oracle_left, oracle_right, on.clone(), generous).await;

        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            SpillingHashJoinExec::try_new(
                ours_left,
                ours_right,
                on,
                PartitionMode::Partitioned,
                16,
            )
            .unwrap(),
        );
        let actual = collect(Arc::clone(&exec), small).await.unwrap();

        let expected_rows = sorted_rows(&expected);
        let actual_rows = sorted_rows(&actual);
        assert!(
            !expected_rows.is_empty(),
            "test data should produce matching rows"
        );
        assert_eq!(actual_rows, expected_rows);

        let spills = total_spill_count(&exec);
        assert!(
            spills >= min_spills,
            "expected at least {min_spills} spill events, got {spills}"
        );
    }

    #[tokio::test]
    async fn matches_datafusion_with_forced_build_spill() {
        // Pool small enough to force build-side spilling of the larger buckets
        // while leaving room for some to stay resident.
        assert_matches_under_pool(4 * 1024, 1).await;
    }

    #[tokio::test]
    async fn matches_datafusion_with_forced_two_sided_spill() {
        // Pool tiny enough that many buckets spill, so their probe rows must be
        // routed to disk and joined in the drain phase. Kept at (just) one
        // minimal Arrow batch so a spilled bucket's build side can still be held
        // resident while it is drained.
        assert_matches_under_pool(1024, 8).await;
    }

    // --- recursive re-partition (skew fallback) tests ---
    //
    // When a single spilled bucket's build side still will not fit in memory
    // during drain, the join re-partitions that bucket (build and probe) into
    // finer sub-buckets with a depth-varied hash seed and recurses, keeping at
    // most one sub-bucket's build table resident. If a bucket holds only one
    // distinct join key it cannot be split, so the join returns a clean error
    // rather than an OOM/panic/hang.

    #[tokio::test]
    async fn matches_datafusion_with_forced_recursion() {
        // Pool so small that even one spilled bucket's build side must be
        // re-partitioned at least once during drain. Driven from a single
        // partition so the tiny pool is exercised by one draining task, not
        // shared across concurrent partitions.
        let generous = Arc::new(TaskContext::default());
        let small = small_pool_ctx(2048);
        let num_partitions = 1;

        // All-distinct keys, so any oversized bucket is guaranteed to hold more
        // than one distinct join key and can therefore always be split. A small
        // sub-partition count (2) packs many keys into each first-level bucket.
        let base_left = make_source("lk", "lv", 600, 600, 0, 128);
        let base_right = make_source("rk", "rv", 600, 600, 1_000_000, 128);

        let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = vec![(
            Arc::new(Column::new("lk", 0)),
            Arc::new(Column::new("rk", 0)),
        )];

        let oracle_left = hash_repartition(Arc::clone(&base_left), 0, num_partitions);
        let oracle_right = hash_repartition(Arc::clone(&base_right), 0, num_partitions);
        let ours_left = hash_repartition(base_left, 0, num_partitions);
        let ours_right = hash_repartition(base_right, 0, num_partitions);

        let expected = oracle_join(oracle_left, oracle_right, on.clone(), generous).await;

        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            SpillingHashJoinExec::try_new(
                ours_left,
                ours_right,
                on,
                PartitionMode::Partitioned,
                2,
            )
            .unwrap(),
        );
        let actual = collect(Arc::clone(&exec), small).await.unwrap();

        let expected_rows = sorted_rows(&expected);
        let actual_rows = sorted_rows(&actual);
        assert!(
            !expected_rows.is_empty(),
            "test data should produce matching rows"
        );
        assert_eq!(actual_rows, expected_rows);
        assert!(
            total_repartition_count(&exec) > 0,
            "the recursive re-partition path must actually be taken"
        );
    }

    #[tokio::test]
    async fn single_key_too_large_errors_cleanly() {
        // Every build row shares ONE join key, so the whole build side lands in
        // a single bucket that cannot be split; under a tiny pool it cannot fit
        // either, so draining it must return a clean, named error.
        let ctx = small_pool_ctx(2048);
        let num_partitions = 1;

        let base_left = make_source("lk", "lv", 2_000, 1, 0, 256);
        let base_right = make_source("rk", "rv", 2_000, 1, 1_000_000, 256);

        let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = vec![(
            Arc::new(Column::new("lk", 0)),
            Arc::new(Column::new("rk", 0)),
        )];

        let ours_left = hash_repartition(base_left, 0, num_partitions);
        let ours_right = hash_repartition(base_right, 0, num_partitions);

        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            SpillingHashJoinExec::try_new(
                ours_left,
                ours_right,
                on,
                PartitionMode::Partitioned,
                2,
            )
            .unwrap(),
        );

        let result = collect(exec, ctx).await;
        let err = result.expect_err("single oversized join key must error, not hang/OOM");
        let msg = err.to_string();
        assert!(
            msg.contains("SpillingHashJoinExec"),
            "error should name the operator, got: {msg}"
        );
        assert!(
            msg.contains("single join key"),
            "error should identify single-join-key skew, got: {msg}"
        );
    }
}
