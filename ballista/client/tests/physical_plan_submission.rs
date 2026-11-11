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

//! End-to-end test for submitting an already-built physical plan directly to a
//! Ballista scheduler, bypassing logical plan creation on the scheduler side.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod physical_plan_submission_tests {
    use crate::common::setup_test_cluster;
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_PLANNER_ENABLED, BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK,
        BallistaConfig,
    };
    use ballista_core::execution_plans::execute_physical_plan;
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::array::{AsArray, Int32Array, Int64Array};
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::common::test_util::batches_to_sort_string;
    use datafusion::datasource::MemTable;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{
        LexOrdering, PhysicalSortExpr, RangePartitioning, SplitPoint,
    };
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{self, ExecutionPlan, Partitioning};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use std::sync::Arc;

    /// Builds a two-partition `MemTable`-backed scan wrapped in a hash
    /// `RepartitionExec`, i.e. a physical plan with a shuffle boundary that
    /// `plan_query_stages` will split into two stages.
    async fn build_physical_plan() -> (Arc<dyn ExecutionPlan>, SessionContext) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(Int32Array::from(vec![40, 50])),
            ],
        )
        .unwrap();

        let table = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(table)).unwrap();

        let scan = ctx
            .table("t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let repartitioned: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(
                scan,
                Partitioning::Hash(vec![Arc::new(Column::new("id", 0))], 4),
            )
            .unwrap(),
        );

        (repartitioned, ctx)
    }

    /// Submits a hand-built physical plan (scan -> hash repartition) directly to
    /// a standalone Ballista cluster via the new physical-plan submission path,
    /// and checks that the rows returned match what executing the same plan
    /// locally produces. The plan's `RepartitionExec(Hash)` boundary means the
    /// scheduler must have split it into two shuffle stages for this to work.
    #[tokio::test]
    async fn should_execute_submitted_physical_plan_across_shuffle_stages() {
        let (host, port) = setup_test_cluster().await;
        let scheduler_url = format!("http://{host}:{port}");

        let (plan, ctx) = build_physical_plan().await;

        let expected = physical_plan::collect(plan.clone(), ctx.task_ctx())
            .await
            .unwrap();

        let session_config = SessionConfig::new_with_ballista();
        let codec = session_config.ballista_physical_extension_codec();
        let session_id = uuid::Uuid::new_v4().to_string();

        let stream = execute_physical_plan::<PhysicalPlanNode>(
            scheduler_url,
            &BallistaConfig::default(),
            plan,
            codec.as_ref(),
            session_id,
            session_config,
        )
        .await
        .expect("physical plan job should be submitted and executed");

        let actual = physical_plan::common::collect(stream)
            .await
            .expect("collecting results from the submitted physical plan");

        assert_eq!(
            batches_to_sort_string(&expected),
            batches_to_sort_string(&actual)
        );
    }

    /// Unique keys `0..300`, in an order that interleaves every range, split
    /// over two input partitions.
    fn range_input() -> (Arc<dyn ExecutionPlan>, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let keys: Vec<i64> = (0..300).map(|i| (i * 7) % 300).collect();
        let (a, b) = keys.split_at(150);
        let batch = |keys: &[i64]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(keys.to_vec()))],
            )
            .unwrap()
        };
        let scan = MemorySourceConfig::try_new_exec(
            &[vec![batch(a)], vec![batch(b)]],
            schema.clone(),
            None,
        )
        .unwrap();
        (scan, schema)
    }

    /// `scan -> RepartitionExec(range on k, split at 100 and 200) -> SortExec(k)`
    /// per partition: a distributed global sort.
    fn range_sort_plan() -> Arc<dyn ExecutionPlan> {
        let (scan, _) = range_input();
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("k", 0)),
            SortOptions::new(false, false),
        )])
        .unwrap();
        let split = |v| SplitPoint::new(vec![ScalarValue::Int64(Some(v))]);
        let range =
            RangePartitioning::try_new(ordering.clone(), vec![split(100), split(200)])
                .unwrap();
        let repartition =
            Arc::new(RepartitionExec::try_new(scan, Partitioning::Range(range)).unwrap());
        Arc::new(SortExec::new(ordering, repartition).with_preserve_partitioning(true))
    }

    /// Submits a range-partition-then-sort plan to a standalone cluster with
    /// one task per partition, so the range edge is a real shuffle between
    /// separately scheduled tasks. The job's output partitions come back in
    /// the order their tasks finished, so rather than checking that the
    /// concatenated output is sorted, it checks what makes the result a global
    /// sort: every key exactly once, each partition sorted, and no two
    /// partitions' key ranges overlapping. Keys are unique, so a partition's
    /// batches are the only ones whose ranges may touch.
    async fn assert_range_sort_is_global(adaptive: bool) {
        let (host, port) = setup_test_cluster().await;
        let scheduler_url = format!("http://{host}:{port}");
        let session_config = SessionConfig::new_with_ballista()
            .set_str(BALLISTA_SCHEDULER_MAX_PARTITIONS_PER_TASK, "1")
            .set_bool(BALLISTA_ADAPTIVE_PLANNER_ENABLED, adaptive);
        let codec = session_config.ballista_physical_extension_codec();

        let stream = execute_physical_plan::<PhysicalPlanNode>(
            scheduler_url,
            &BallistaConfig::default(),
            range_sort_plan(),
            codec.as_ref(),
            uuid::Uuid::new_v4().to_string(),
            session_config,
        )
        .await
        .expect("range sort job should be submitted and executed");
        let batches = physical_plan::common::collect(stream)
            .await
            .expect("collecting results from the range sort job");

        let mut keys: Vec<i64> = vec![];
        let mut ranges: Vec<(i64, i64)> = vec![];
        for batch in batches.iter().filter(|b| b.num_rows() > 0) {
            let values = batch.column(0).as_primitive::<Int64Type>().values();
            assert!(
                values.windows(2).all(|w| w[0] < w[1]),
                "adaptive={adaptive}: a partition's batch is not sorted: {values:?}"
            );
            ranges.push((values[0], values[values.len() - 1]));
            keys.extend(values.iter());
        }
        keys.sort_unstable();
        assert_eq!(keys, (0..300).collect::<Vec<_>>(), "adaptive={adaptive}");

        // Batches of one partition follow each other in key order, so after
        // sorting by first key every batch must start after the previous one
        // ends, and the boundaries of the three ranges must not be crossed.
        ranges.sort_unstable();
        for pair in ranges.windows(2) {
            assert!(
                pair[0].1 < pair[1].0,
                "adaptive={adaptive}: partitions overlap: {ranges:?}"
            );
        }
        for (first, last) in &ranges {
            assert_eq!(
                first / 100,
                last / 100,
                "adaptive={adaptive}: a partition spans a split point: {ranges:?}"
            );
        }
    }

    #[tokio::test]
    async fn should_globally_sort_a_submitted_range_repartition() {
        assert_range_sort_is_global(false).await;
    }

    #[tokio::test]
    async fn should_globally_sort_a_submitted_range_repartition_adaptive() {
        assert_range_sort_is_global(true).await;
    }
}
