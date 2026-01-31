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
use crate::state::aqe::execution_plan::ExchangeExec;
use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{
    mock_batch, mock_context, mock_memory_table, mock_partitions_with_statistics,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ColumnStatistics;
use datafusion::physical_plan::Statistics;
use datafusion::physical_plan::test::exec::StatisticsExec;
use std::collections::HashSet;
use std::sync::Arc;

#[tokio::test]
async fn should_add_exchanges() -> datafusion::error::Result<()> {
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;

    let q = r#"
            select min(a) as c0, max(b) as c1, c as c2 from t group by c
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;
    let planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending
      ProjectionExec: expr=[min(t.a)@1 as c0, max(t.b)@2 as c1, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a), max(t.b)]
          ExchangeExec: partitioning=Hash([c@0], 2), plan_id=0, stage_id=pending, stage_resolved=false
            AggregateExec: mode=Partial, gby=[c@2 as c], aggr=[min(t.a), max(t.b)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

#[tokio::test]
async fn should_split_plan_into_runnable_stages_internal() -> datafusion::error::Result<()>
{
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;

    let q = r#"
            select min(a) as c0, max(b) as c1, c as c2 from t group by c
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending
      ProjectionExec: expr=[min(t.a)@1 as c0, max(t.b)@2 as c1, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a), max(t.b)]
          ExchangeExec: partitioning=Hash([c@0], 2), plan_id=0, stage_id=pending, stage_resolved=false
            AggregateExec: mode=Partial, gby=[c@2 as c], aggr=[min(t.a), max(t.b)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    let stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages.first().unwrap().as_ref(),  @ r"
    ExchangeExec: partitioning=Hash([c@0], 2), plan_id=0, stage_id=0, stage_resolved=false
      AggregateExec: mode=Partial, gby=[c@2 as c], aggr=[min(t.a), max(t.b)]
        DataSourceExec: partitions=1, partition_sizes=[1]
    ");
    planner.finalise_stage_internal(0, mock_partitions_with_statistics())?;

    let stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages.first().unwrap().as_ref(),  @ r"
    AdaptiveDatafusionExec: is_final=true, plan_id=1, stage_id=1
      ProjectionExec: expr=[min(t.a)@1 as c0, max(t.b)@2 as c1, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a), max(t.b)]
          ExchangeExec: partitioning=Hash([c@0], 2), plan_id=0, stage_id=0, stage_resolved=true
            AggregateExec: mode=Partial, gby=[c@2 as c], aggr=[min(t.a), max(t.b)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");
    planner.finalise_stage_internal(1, mock_partitions_with_statistics())?;

    let stages = planner.identify_runnable_stages()?;
    assert!(stages.is_none());

    Ok(())
}

#[tokio::test]
async fn should_split_plan_into_stages() -> datafusion::error::Result<()> {
    let ctx = mock_context();
    ctx.register_batch("t", mock_batch()?)?;

    let q = r#"
            select min(a) as c0, max(b) as c1, c as c2 from t group by c
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;
    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    assert_plan!(planner.current_plan(),  @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=1, stage_id=pending
      ProjectionExec: expr=[min(t.a)@1 as c0, max(t.b)@2 as c1, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a), max(t.b)]
          ExchangeExec: partitioning=Hash([c@0], 2), plan_id=0, stage_id=pending, stage_resolved=false
            AggregateExec: mode=Partial, gby=[c@2 as c], aggr=[min(t.a), max(t.b)]
              DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages.first().unwrap().plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: Hash([c@0], 2)
      AggregateExec: mode=Partial, gby=[c@2 as c], aggr=[min(t.a), max(t.b)]
        DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    planner.finalise_stage_internal(0, mock_partitions_with_statistics())?;

    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(1, stages.len());
    assert_plan!(stages.first().unwrap().plan.as_ref(),  @ r"
    ShuffleWriterExec: partitioning: None
      ProjectionExec: expr=[min(t.a)@1 as c0, max(t.b)@2 as c1, c@0 as c2]
        AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a), max(t.b)]
          ShuffleReaderExec: partitioning: Hash([c@0], 2)
    ");
    planner.finalise_stage_internal(1, mock_partitions_with_statistics())?;

    let stages = planner.runnable_stages()?;
    assert!(stages.is_none());

    Ok(())
}

#[tokio::test]
async fn should_create_initial_plan() -> datafusion::error::Result<()> {
    let ctx = mock_context();

    ctx.register_batch("t", mock_batch()?)?;

    let q = r#"
            select sum(t0.c0)
            from
                ( select min(a) as c0, max(b) as c1, c as c2 from t group by c) as t0
                JOIN
                ( select min(a) as p0, max(b) as p1, c as p2 from t group by c) as t2
                ON c2 = p2
            group by t0.c0
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;

    let planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    assert_plan!(planner.current_plan(), @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=4, stage_id=pending
      ProjectionExec: expr=[sum(t0.c0)@1 as sum(t0.c0)]
        AggregateExec: mode=FinalPartitioned, gby=[c0@0 as c0], aggr=[sum(t0.c0)]
          ExchangeExec: partitioning=Hash([c0@0], 2), plan_id=3, stage_id=pending, stage_resolved=false
            AggregateExec: mode=Partial, gby=[c0@0 as c0], aggr=[sum(t0.c0)]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p2@0, c2@1)], projection=[c0@1]
                CoalescePartitionsExec
                  ExchangeExec: partitioning=None, plan_id=1, stage_id=pending, stage_resolved=false
                    ProjectionExec: expr=[c@0 as p2]
                      AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[]
                        ExchangeExec: partitioning=Hash([c@0], 2), plan_id=0, stage_id=pending, stage_resolved=false
                          AggregateExec: mode=Partial, gby=[c@0 as c], aggr=[]
                            DataSourceExec: partitions=1, partition_sizes=[1]
                ProjectionExec: expr=[min(t.a)@1 as c0, c@0 as c2]
                  AggregateExec: mode=FinalPartitioned, gby=[c@0 as c], aggr=[min(t.a)]
                    ExchangeExec: partitioning=Hash([c@0], 2), plan_id=2, stage_id=pending, stage_resolved=false
                      AggregateExec: mode=Partial, gby=[c@1 as c], aggr=[min(t.a)]
                        DataSourceExec: partitions=1, partition_sizes=[1]
    ");

    Ok(())
}

#[tokio::test]
async fn should_split_stages_resolve_right_branch() -> datafusion::error::Result<()> {
    let partition_locations = mock_partitions_with_statistics();
    let ctx = mock_context();

    ctx.register_table("t", mock_memory_table())?;

    let q = r#"
            select sum(t0.c0)
            from
                ( select min(a) as c0, max(b) as c1, c as c2 from t group by c) as t0
                JOIN
                ( select min(a) as p0, max(b) as p1, c as p2 from t group by c) as t2
                ON c2 = p2
            group by t0.c0
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;

    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(2, runnable_stages.len());

    assert_eq!(
        HashSet::from_iter(vec![0, 1].into_iter()),
        planner.inspect_runnable_stages()?
    );

    planner.finalise_stage_internal(1, partition_locations.clone())?;

    //
    // we finish one of them
    //
    let runnable_stages = planner.identify_runnable_stages()?.unwrap();

    assert_eq!(1, runnable_stages.len());

    planner.finalise_stage_internal(0, partition_locations.clone())?;
    //
    // we should find the number 2
    //

    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(1, runnable_stages.len());

    planner.finalise_stage_internal(2, partition_locations.clone())?;

    //
    // the other one should be returned again
    //

    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(1, runnable_stages.len());
    planner.finalise_stage_internal(3, partition_locations.clone())?;

    let runnable_stages = planner.identify_runnable_stages()?;
    assert_eq!(1, runnable_stages.unwrap().len());

    planner.finalise_stage_internal(4, partition_locations.clone())?;
    let runnable_stages = planner.identify_runnable_stages()?;
    assert!(runnable_stages.is_none());

    Ok(())
}

#[tokio::test]
async fn should_split_stages_resolve_left_branch() -> datafusion::error::Result<()> {
    let partition_locations = mock_partitions_with_statistics();
    let ctx = mock_context();

    ctx.register_table("t", mock_memory_table())?;

    let q = r#"
        select sum(t0.c0)
        from
            ( select min(a) as c0, max(b) as c1, c as c2 from t group by c) as t0
            JOIN
            ( select min(a) as p0, max(b) as p1, c as p2 from t group by c) as t2
            ON c2 = p2
        group by t0.c0
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;

    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(2, runnable_stages.len());

    // we run one of them setting partition location

    assert_eq!(
        HashSet::from_iter(vec![0, 1].into_iter()),
        planner.inspect_runnable_stages()?
    );

    planner.finalise_stage_internal(0, partition_locations.clone())?;
    //
    // we finish one of them
    //
    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(2, runnable_stages.len());
    assert_eq!(
        HashSet::from_iter(vec![2, 1].into_iter()),
        planner.inspect_runnable_stages()?
    );

    planner.finalise_stage_internal(1, partition_locations.clone())?;
    //
    // we should find the number 2
    //

    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(1, runnable_stages.len());

    planner.finalise_stage_internal(2, partition_locations.clone())?;
    //
    // the other one should be returned again
    //

    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(1, runnable_stages.len());
    planner.finalise_stage_internal(3, partition_locations.clone())?;

    let runnable_stages = planner.identify_runnable_stages()?;
    assert_eq!(1, runnable_stages.unwrap().len());

    planner.finalise_stage_internal(4, partition_locations.clone())?;
    let runnable_stages = planner.identify_runnable_stages()?;
    assert!(runnable_stages.is_none());

    Ok(())
}

#[tokio::test]
async fn should_split_stages_resolve_both() -> datafusion::error::Result<()> {
    let partition_locations = mock_partitions_with_statistics();
    let ctx = mock_context();

    ctx.register_table("t", mock_memory_table())?;

    //let q = "select sum(c0) as s0, sum(c1) as s2 from (select min(a) c0, max(b) c1, c c2 from t group by c) group by c2";

    let q = r#"
        select sum(t0.c0)
        from
            ( select min(a) as c0, max(b) as c1, c as c2 from t group by c) as t0
            JOIN
            ( select min(a) as p0, max(b) as p1, c as p2 from t group by c) as t2
            ON c2 = p2
        group by t0.c0
        "#;

    let plan = ctx.sql(q).await?.create_physical_plan().await?;

    let mut planner =
        AdaptivePlanner::try_new(ctx.state().config(), plan, "test_job".to_string())?;

    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(2, runnable_stages.len());

    assert_eq!(
        HashSet::from_iter(vec![0, 1].into_iter()),
        planner.inspect_runnable_stages()?
    );

    // resolve both of them
    planner.finalise_stage_internal(0, partition_locations.clone())?;
    planner.finalise_stage_internal(1, partition_locations.clone())?;
    //
    // we finish one of them
    //
    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(1, runnable_stages.len());

    planner.finalise_stage_internal(2, partition_locations.clone())?;

    let runnable_stages = planner.identify_runnable_stages()?.unwrap();
    assert_eq!(1, runnable_stages.len());
    planner.finalise_stage_internal(3, partition_locations.clone())?;
    //
    // the other one should be returned again
    //

    let runnable_stages = planner.identify_runnable_stages()?;
    assert_eq!(1, runnable_stages.unwrap().len());

    planner.finalise_stage_internal(4, partition_locations.clone())?;
    let runnable_stages = planner.identify_runnable_stages()?;
    assert!(runnable_stages.is_none());

    Ok(())
}

#[tokio::test]
async fn should_ignore_inactive_stages() -> datafusion::error::Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let statistics = Statistics {
        num_rows: Default::default(),
        total_byte_size: Default::default(),
        column_statistics: vec![ColumnStatistics::new_unknown()],
    };
    let empty_exec = Arc::new(StatisticsExec::new(statistics, schema.clone()));

    let mut exchange_exec = ExchangeExec::new(empty_exec, None, 0);
    // we're testing that making stage inactive
    // it won't show up in the runnable stages
    exchange_exec.inactive_stage = true;

    let exchange_exec = Arc::new(exchange_exec);
    let ctx = mock_context();

    let mut planner = AdaptivePlanner::try_new(
        ctx.state().config(),
        exchange_exec,
        "test_job".to_string(),
    )?;

    assert_plan!(planner.current_plan(), @ r"
    AdaptiveDatafusionExec: is_final=false, plan_id=0, stage_id=pending
      ExchangeExec: partitioning=None, plan_id=0, stage_id=pending, stage_resolved=false
        CooperativeExec
          StatisticsExec: col_count=1, row_count=Absent
    ");

    let runnable_stages = planner.runnable_stages()?.unwrap();
    assert_eq!(0, runnable_stages.len());

    Ok(())
}
