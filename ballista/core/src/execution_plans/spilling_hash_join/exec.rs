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
// whose build side is designed to spill to disk instead of requiring the
// entire hash table to fit in memory. `execute` currently keeps all build
// sub-partitions ("buckets") resident in memory — spilling any of them to
// disk under memory pressure is not yet implemented (see `stream.rs`).
//
// v1 invariants (enforced by the constructor, which always builds an
// inner join with no residual filter and no output projection):
//   - join_type is always JoinType::Inner
//   - filter is always None
//   - projection is always None

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{JoinType, NullEquality, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

use super::stream::execute_join;

/// Physical execution plan node for a hash join whose build side is designed
/// to spill sub-partitions to disk rather than requiring the whole build
/// side to fit in memory at once.
///
/// `execute` currently keeps every build-side sub-partition resident (no
/// spilling yet — see `stream.rs`).
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
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        partition_mode: PartitionMode,
        num_sub_partitions: usize,
    ) -> Result<Self> {
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
    /// semantics) and returns a stream that drains the left (build) side
    /// fully into resident per-sub-partition hash tables before probing the
    /// right (probe) side against them. No spilling yet: all build buckets
    /// stay in memory for the lifetime of the stream.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left = self.left.execute(partition, Arc::clone(&context))?;
        let right = self.right.execute(partition, context)?;

        let (left_keys, right_keys): (Vec<PhysicalExprRef>, Vec<PhysicalExprRef>) =
            self.on.iter().cloned().unzip();

        Ok(execute_join(
            self.schema(),
            left,
            right,
            left_keys,
            right_keys,
            self.num_sub_partitions,
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
}
