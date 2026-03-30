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

use crate::assert_plan;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{
    mock_batch, mock_context, mock_partitions_with_statistics_no_data,
};
use ballista_core::serde::scheduler::{
    ExecutorMetadata, ExecutorSpecification, PartitionId, PartitionLocation,
    PartitionStats,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, DataFusionError, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::joins::CrossJoinExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::test::exec::StatisticsExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::Arc;

#[tokio::test]
async fn should_propagate_empty_stage() -> datafusion::error::Result<()> {
    // as stage 0 will produce no data, stage 1
    // will be altered, and it will just execute empty exec
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;

    let q = r#"
            select min(a) as c0, max(b) as c1, c as c2 from t group by c
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages.first().unwrap().plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: Hash([c@0], 2)
      AggregateExec: mode=Partial, gby=[c@2 as c], aggr=[min(t.a), max(t.b)]
        DataSourceExec: partitions=1, partition_sizes=[1]
    ");
    // resolve this stage with no data to shuffle
    // this should trigger rewriting next stage to produce
    // empty output
    planner.finalise_stage_internal(0, mock_partitions_with_statistics_no_data())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages.first().unwrap().plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: None
      EmptyExec
    ");
    planner.finalise_stage_internal(1, mock_partitions_with_statistics_no_data())?;

    let stages = planner.runnable_stages()?;
    assert!(stages.is_none());

    Ok(())
}

#[tokio::test]
async fn should_propagate_empty_stage_and_remove() -> datafusion::error::Result<()> {
    //
    // [stage 2] --------------------------------------------------------------------------------------------------
    // AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=
    //   ProjectionExec: expr=[c0@0 as c0, count(Int64(1))@1 as count(*)]
    //     AggregateExec: mode=FinalPartitioned, gby=[c0@0 as c0], aggr=[count(Int64(1))]
    //       CoalesceBatchesExec: target_batch_size=8192
    // [stage 1] --------------------------------------------------------------------------------------------------
    //         ExchangeExec: partitioning=Hash([c0@0], 2), plan_id=1, stage_id=None, stage_resolved=false
    //           AggregateExec: mode=Partial, gby=[c0@0 as c0], aggr=[count(Int64(1))]
    //             ProjectionExec: expr=[min(t.a)@1 as c0]
    //               AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a)]
    //                 CoalesceBatchesExec: target_batch_size=8192
    // [stage 0] --------------------------------------------------------------------------------------------------
    //                    ExchangeExec: partitioning=Hash([c@0], 2), plan_id=0, stage_id=None, stage_resolved=false
    //                      AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
    //                        DataSourceExec: partitions=1, partition_sizes=[1]
    // ------------------------------------------------------------------------------------------------------------
    //
    // test scenario:
    //
    // - stage 0 will produce empty shuffle files (no data)
    // - this should trigger plan re-write, and stage 1 should be totally removed
    // - result plan will produce empty output

    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;

    let q = r#"
            select c0, count(*) from (select min(a) as c0, max(b) as c1, c as c2 from t where a = 42 group by c) group by c0
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending
      ProjectionExec: expr=[c0@0 as c0, count(Int64(1))@1 as count(*)]
        AggregateExec: mode=FinalPartitioned, gby=[c0@0 as c0], aggr=[count(Int64(1))]
          ExchangeExec: partitioning=Hash([c0@0], 2), plan_id=1, stage_id=pending, stage_resolved=false
            AggregateExec: mode=Partial, gby=[c0@0 as c0], aggr=[count(Int64(1))]
              ProjectionExec: expr=[min(t.a)@1 as c0]
                AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a)]
                  ExchangeExec: partitioning=Hash([c@0], 2), plan_id=0, stage_id=pending, stage_resolved=false
                    AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
                      FilterExec: a@0 = 42
                        DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages.first().unwrap().plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: Hash([c@0], 2)
      AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
        FilterExec: a@0 = 42
          DataSourceExec: partitions=1, partition_sizes=[1]
    ");
    // resolve this stage with no data to shuffle
    // this should trigger rewriting next stage to produce
    // empty output
    planner.finalise_stage_internal(0, mock_partitions_with_statistics_no_data())?;
    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages.first().unwrap().plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: None
      EmptyExec
    ");
    planner.finalise_stage_internal(1, mock_partitions_with_statistics_no_data())?;

    let stages = planner.runnable_stages()?;
    assert!(stages.is_none());

    Ok(())
}

#[tokio::test]
async fn should_insert_new_stage() -> datafusion::error::Result<()> {
    let ctx = mock_context();

    let join = create_plan_with_scan()?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), join, "test_job".to_string())?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending
      CrossJoinExec
        CoalescePartitionsExec
          ExchangeExec: partitioning=Hash([big_col@0], 2), plan_id=0, stage_id=pending, stage_resolved=false
            StatisticsExec: col_count=1, row_count=Inexact(262144)
        CooperativeExec
          MockPartitionedScan: num_partitions=2, statistics=[Rows=Inexact(1024), Bytes=Inexact(8192), [(Col[0]:)]]
    ");

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());

    assert_plan!(stages[0].plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: Hash([big_col@0], 2)
      StatisticsExec: col_count=1, row_count=Inexact(262144)
    ");

    planner.finalise_stage_internal(0, big_statistics_exchange())?;
    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending
      ProjectionExec: expr=[big_col@1 as big_col, big_col@0 as big_col]
        CrossJoinExec
          CoalescePartitionsExec
            ExchangeExec: partitioning=None, plan_id=2, stage_id=pending, stage_resolved=false
              CooperativeExec
                MockPartitionedScan: num_partitions=2, statistics=[Rows=Inexact(1024), Bytes=Inexact(8192), [(Col[0]:)]]
          ExchangeExec: partitioning=Hash([big_col@0], 2), plan_id=0, stage_id=0, stage_resolved=true
            CooperativeExec
              StatisticsExec: col_count=1, row_count=Inexact(262144)
    ");
    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());

    assert_plan!(stages[0].plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: None
      CooperativeExec
        MockPartitionedScan: num_partitions=2, statistics=[Rows=Inexact(1024), Bytes=Inexact(8192), [(Col[0]:)]]
    ");

    planner.finalise_stage_internal(1, small_statistics_exchange())?;
    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());

    assert_plan!(stages[0].plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: None
      ProjectionExec: expr=[big_col@1 as big_col, big_col@0 as big_col]
        CrossJoinExec
          CoalescePartitionsExec
            ShuffleReaderExec: partitioning: UnknownPartitioning(2)
          ShuffleReaderExec: partitioning: Hash([big_col@0], 2)
    ");

    planner.finalise_stage_internal(2, small_statistics_exchange())?;
    let stages = planner.runnable_stages()?;
    assert!(stages.is_none());

    Ok(())
}
// this test demonstrates situation where we have a hash join
// which has two runnable stages. in theory two different stages could be run
// at the same time.
// in this test, build side of join will produce empty shuffles which will then
// cancel whole execution as empty side of inner join will produce no result
//
// as build stage is empty, we need to cancel running probe stage.
#[tokio::test]
async fn should_cancel_the_stage() -> datafusion::error::Result<()> {
    let ctx = mock_context();

    ctx.register_batch("t1", mock_batch()?)?;
    ctx.register_batch("t2", mock_batch()?)?;

    ctx.sql("SET datafusion.optimizer.hash_join_single_partition_threshold = 0")
        .await?
        .show()
        .await?;

    let join = ctx
        .sql("select t1.* from t1 join t2 on t1.a = t2.a where t2.b = 42")
        .await?
        .create_physical_plan()
        .await?;

    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), join, "test_job".to_string())?;

    let (stages, cancellable) = planner.actionable_stages()?;
    assert_eq!(2, stages.unwrap().len());
    assert_eq!(0, cancellable.len());

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0)], projection=[a@1, b@2, c@3]
        ExchangeExec: partitioning=Hash([a@0], 2), plan_id=0, stage_id=0, stage_resolved=false
          FilterExec: b@1 = 42, projection=[a@0]
            DataSourceExec: partitions=1, partition_sizes=[1]
        ExchangeExec: partitioning=Hash([a@0], 2), plan_id=1, stage_id=1, stage_resolved=false
          DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    planner.finalise_stage_internal(0, mock_partitions_with_statistics_no_data())?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=2, stage_id=pending
      EmptyExec
    ");

    let (stages, cancellable) = planner.actionable_stages()?;
    assert_eq!(1, stages.unwrap().len());
    assert_eq!(1, cancellable.len());
    assert_eq!(HashSet::from_iter(vec![1]), cancellable);

    Ok(())
}

fn small_scan(num_partitions: usize) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "big_col",
        DataType::Int32,
        false,
    )]));
    let statistics = small_statistics();
    Arc::new(MockPartitionedScan::new(schema, num_partitions, statistics))
}

fn create_plan_with_scan() -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let (big, _) = create_big_and_small_statistic_scan();

    let small_scan = small_scan(2);

    let column_big = Arc::new(Column::new_with_schema(
        "big_col",
        &Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
    )?);

    let exchange_big = Arc::new(RepartitionExec::try_new(
        big,
        Partitioning::Hash(vec![column_big], 2),
    )?);
    let join =
        Arc::new(CrossJoinExec::new(exchange_big, small_scan)) as Arc<dyn ExecutionPlan>;
    Ok(join)
}

fn create_big_and_small_statistic_scan()
-> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
    let big = Arc::new(StatisticsExec::new(
        big_statistics(),
        Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
    ));

    let small = Arc::new(StatisticsExec::new(
        small_statistics(),
        Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
    ));
    (big, small)
}

/// Return statistics for small table
fn small_statistics_exchange() -> Vec<Vec<PartitionLocation>> {
    let (threshold_num_rows, threshold_byte_size) = get_thresholds();

    let location = PartitionLocation {
        // next few properties are generic values
        map_partition_id: 0,
        partition_id: PartitionId {
            job_id: "".to_string(),
            stage_id: 0,
            partition_id: 0,
        },
        executor_meta: ExecutorMetadata {
            id: "".to_string(),
            host: "".to_string(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification { task_slots: 0 },
        },
        path: "".to_string(),
        // next few properties are needed
        partition_stats: PartitionStats::new(
            Some(threshold_num_rows as u64 / 128),
            None,
            Some(threshold_byte_size as u64 / 128),
        ),
    };
    vec![vec![location]]
}

/// Return statistics for big table
fn big_statistics_exchange() -> Vec<Vec<PartitionLocation>> {
    let (threshold_num_rows, threshold_byte_size) = get_thresholds();

    let location = PartitionLocation {
        // next few properties are generic values
        map_partition_id: 0,
        partition_id: PartitionId {
            job_id: "".to_string(),
            stage_id: 0,
            partition_id: 0,
        },
        executor_meta: ExecutorMetadata {
            id: "".to_string(),
            host: "".to_string(),
            port: 0,
            grpc_port: 0,
            specification: ExecutorSpecification { task_slots: 0 },
        },
        path: "".to_string(),
        // next few properties are needed
        partition_stats: PartitionStats::new(
            Some(threshold_num_rows as u64 * 2),
            None,
            Some(threshold_byte_size as u64 * 2),
        ),
    };
    vec![vec![location]]
}

/// Return statistics for small table
fn small_statistics() -> Statistics {
    let (threshold_num_rows, threshold_byte_size) = get_thresholds();
    Statistics {
        num_rows: Precision::Inexact(threshold_num_rows / 128),
        total_byte_size: Precision::Inexact(threshold_byte_size / 128),
        column_statistics: vec![ColumnStatistics::new_unknown()],
    }
}

/// Return statistics for big table
fn big_statistics() -> Statistics {
    let (threshold_num_rows, threshold_byte_size) = get_thresholds();
    Statistics {
        num_rows: Precision::Inexact(threshold_num_rows * 2),
        total_byte_size: Precision::Inexact(threshold_byte_size * 2),
        column_statistics: vec![ColumnStatistics::new_unknown()],
    }
}
/// Get table thresholds: (num_rows, byte_size)
fn get_thresholds() -> (usize, usize) {
    let optimizer_options = ConfigOptions::new().optimizer;
    (
        optimizer_options.hash_join_single_partition_threshold_rows,
        optimizer_options.hash_join_single_partition_threshold,
    )
}

#[derive(Debug, Clone)]
struct MockPartitionedScan {
    num_partitions: usize,
    statistics: Statistics,
    plan_properties: Arc<PlanProperties>,
}

impl MockPartitionedScan {
    pub fn new(
        schema: Arc<Schema>,
        num_partitions: usize,
        statistics: Statistics,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            num_partitions,
            statistics,
            plan_properties,
        }
    }
}

impl DisplayAs for MockPartitionedScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MockPartitionedScan: num_partitions={}, statistics=[{}]",
                    self.num_partitions, self.statistics
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "num_partitions={}", self.num_partitions)?;
                writeln!(f, "statistics=[{}]", self.statistics)
            }
        }
    }
}
impl ExecutionPlan for MockPartitionedScan {
    fn name(&self) -> &str {
        "MockPartitionedScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        unimplemented!("should not be called")
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        Ok(self.statistics.clone())
    }
}
