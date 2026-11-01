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

//! Exercise skew rewriting through stage planning, adaptation and completion.

use crate::state::aqe::planner::{AdaptivePlanner, AdaptiveStageInfo};
use crate::state::aqe::test::{mock_batch, mock_partitions_with_size, mock_schema};
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::arrow::array::{ArrayRef, AsArray, Int32Array};
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::MemTable;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

fn config() -> SessionConfig {
    SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_round_robin_repartition(false)
        .with_ballista_skew_join_enabled(true)
        .with_ballista_skew_join_partition_threshold_bytes(100)
        .with_ballista_skew_join_advisory_partition_bytes(50)
        .with_ballista_broadcast_join_threshold_bytes(0)
        .with_ballista_hash_join_max_build_partition_bytes(1)
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false)
        .set_bool("datafusion.optimizer.prefer_hash_join", false)
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            0,
        )
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            0,
        )
}

fn partitions(sizes: &[&[u64]]) -> Vec<Vec<PartitionLocation>> {
    sizes
        .iter()
        .enumerate()
        .map(|(partition, sizes)| {
            // Mapper completion order must not determine shard windows.
            sizes
                .iter()
                .enumerate()
                .rev()
                .map(|(mapper, bytes)| {
                    let mut location =
                        mock_partitions_with_size(1, *bytes).remove(0).remove(0);
                    location.partition_id.partition_id = partition;
                    location.map_partition_id = mapper;
                    location
                })
                .collect()
        })
        .collect()
}

async fn planner(
    config: SessionConfig,
    sql: &str,
) -> datafusion::common::Result<AdaptivePlanner> {
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new_with_default_features()
            .with_config(config)
            .build(),
    );
    for name in ["t1", "t2"] {
        let data = vec![vec![mock_batch()?]; 4];
        ctx.register_table(name, Arc::new(MemTable::try_new(mock_schema(), data)?))?;
    }
    let plan = ctx.sql(sql).await?.into_optimized_plan()?;
    AdaptivePlanner::try_new(&ctx, &plan, "test_job".into()).await
}

async fn join_stage(
    config: SessionConfig,
    left_sizes: &[&[u64]],
) -> datafusion::common::Result<(AdaptivePlanner, AdaptiveStageInfo)> {
    let mut planner =
        planner(config, "select t1.a, t2.b from t1 join t2 on t1.c = t2.c").await?;
    finish_join_inputs(&mut planner, left_sizes, &[&[10][..]; 4])?;
    let mut stages = planner.runnable_stages()?.unwrap();
    assert_eq!(stages.len(), 1);
    Ok((planner, stages.remove(0)))
}

fn finish_join_inputs(
    planner: &mut AdaptivePlanner,
    left_sizes: &[&[u64]],
    right_sizes: &[&[u64]],
) -> datafusion::common::Result<()> {
    let stages = planner.runnable_stages()?.unwrap();
    assert_eq!(stages.len(), 2);
    planner.finalise_stage_internal(stages[0].plan.stage_id(), partitions(left_sizes))?;
    planner.finalise_stage_internal(stages[1].plan.stage_id(), partitions(right_sizes))
}

fn readers(plan: Arc<dyn ExecutionPlan>) -> Vec<ShuffleReaderExec> {
    let mut readers = vec![];
    plan.apply(|node| {
        if let Some(reader) = node.downcast_ref::<ShuffleReaderExec>() {
            readers.push(reader.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    readers
}

// Replace assigned shuffle files with fixture rows, then execute the real join.
// The bucket is the join key and the mapper ID is the projected row value.
async fn collect_join_rows(
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Vec<(Option<i32>, Option<i32>)>> {
    let executable = plan
        .transform_up(|node| {
            let Some(reader) = node.downcast_ref::<ShuffleReaderExec>() else {
                return Ok(Transformed::no(node));
            };
            let schema = reader.schema();
            let data = reader
                .partition
                .iter()
                .map(|locations| {
                    let columns = schema
                        .fields()
                        .iter()
                        .map(|field| {
                            let values = locations.iter().map(|location| {
                                if field.name() == "c" {
                                    location.partition_id.partition_id as i32
                                } else {
                                    location.map_partition_id as i32
                                }
                            });
                            Arc::new(Int32Array::from_iter_values(values)) as ArrayRef
                        })
                        .collect();
                    Ok(vec![RecordBatch::try_new(schema.clone(), columns)?])
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            Ok(Transformed::yes(MemorySourceConfig::try_new_exec(
                &data, schema, None,
            )?))
        })?
        .data;
    let batches =
        datafusion::physical_plan::collect(executable, SessionContext::new().task_ctx())
            .await?;
    let mut rows = Vec::new();
    for batch in batches {
        let left = batch.column(0).as_primitive::<Int32Type>();
        let right = batch.column(1).as_primitive::<Int32Type>();
        rows.extend(left.iter().zip(right.iter()));
    }
    rows.sort_unstable();
    Ok(rows)
}

#[tokio::test]
async fn skew_join_preserves_shards_through_stage_completion()
-> datafusion::common::Result<()> {
    // Exercise both supported join implementations through their actual planners.
    for (max_build_bytes, join_name) in [(1, "SortMergeJoinExec"), (1000, "HashJoinExec")]
    {
        let config =
            config().with_ballista_hash_join_max_build_partition_bytes(max_build_bytes);
        let (mut planner, stage) =
            join_stage(config, &[&[25, 25, 150, 150, 150], &[10], &[10], &[10]]).await?;
        let rendered = datafusion::physical_plan::displayable(stage.plan.as_ref())
            .indent(false)
            .to_string();
        assert!(rendered.contains(join_name), "{rendered}");
        let inputs = readers(stage.plan.clone());
        assert_eq!(inputs.len(), 2);
        for reader in &inputs {
            assert!(matches!(
                reader.properties().output_partitioning(),
                Partitioning::UnknownPartitioning(7)
            ));
        }
        let assignments: Vec<Vec<Vec<usize>>> = inputs
            .iter()
            .map(|reader| {
                reader
                    .partition
                    .iter()
                    .map(|locations| {
                        locations.iter().map(|l| l.map_partition_id).collect()
                    })
                    .collect()
            })
            .collect();
        assert!(assignments.contains(&vec![
            vec![0, 1],
            vec![2],
            vec![3],
            vec![4],
            vec![0],
            vec![0],
            vec![0]
        ]));
        assert!(assignments.contains(&vec![vec![0]; 7]));

        let rows = collect_join_rows(stage.plan.children()[0].clone()).await?;
        let mut expected = vec![(Some(0), Some(0)); 4];
        expected.extend((1..5).map(|value| (Some(value), Some(0))));
        assert_eq!(rows, expected);

        let stage_id = stage.plan.stage_id();
        planner.update_exchange_locations(
            stage_id,
            partitions(&[&[1][..]; 7]).into_iter().flatten().collect(),
        )?;
        let output = planner.take_stage_output_partitions(stage_id)?;
        assert_eq!(output.len(), 7);
        assert_eq!(output.iter().map(Vec::len).sum::<usize>(), 7);
        planner.resolve_stage_partitions(stage_id, output)?;
        assert!(planner.runnable_stages()?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn skew_join_keeps_left_join_matches_and_unmatched_rows()
-> datafusion::common::Result<()> {
    let mut planner = planner(
        config().with_ballista_hash_join_max_build_partition_bytes(1000),
        "select t1.a, t2.b from t1 left join t2 on t1.c = t2.c and t1.a != 4",
    )
    .await?;
    // Both inputs are skewed, but only the preserved (left) side may split.
    // Splitting the right would emit the unmatched left row once per replica.
    finish_join_inputs(
        &mut planner,
        &[&[25, 25, 150, 150, 150], &[10], &[10], &[10]],
        &[&[150; 4], &[10], &[10], &[10]],
    )?;
    let stage = planner.runnable_stages()?.unwrap().remove(0);
    assert_eq!(stage.plan.input_partition_count(), 7);
    let rows = collect_join_rows(stage.plan.children()[0].clone()).await?;
    let mut expected: Vec<_> = (0..4)
        .flat_map(|left| (0..4).map(move |right| (Some(left), Some(right))))
        .collect();
    expected.extend([(Some(0), Some(0)); 3]);
    expected.push((Some(4), None));
    expected.sort_unstable();
    assert_eq!(rows, expected);
    Ok(())
}

#[tokio::test]
async fn skew_join_respects_opt_in_and_dynamic_filters() -> datafusion::common::Result<()>
{
    for config in [
        config().with_ballista_skew_join_enabled(false),
        config().set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true),
    ] {
        let (_, stage) = join_stage(config, &[&[150; 4], &[10], &[10], &[10]]).await?;
        assert!(
            readers(stage.plan)
                .iter()
                .all(|reader| reader.skew_join.is_none())
        );
    }
    Ok(())
}

#[tokio::test]
async fn skew_join_requires_strict_threshold_and_relative_skew()
-> datafusion::common::Result<()> {
    // Exactly at the absolute threshold is not skewed; uniform large buckets
    // are not skewed either, even though each could be split into four shards.
    for sizes in [
        vec![vec![25; 4], vec![1], vec![1], vec![1]],
        vec![vec![150; 4]; 4],
    ] {
        let sizes: Vec<_> = sizes.iter().map(Vec::as_slice).collect();
        let (_, stage) = join_stage(config(), &sizes).await?;
        assert!(
            readers(stage.plan)
                .iter()
                .all(|reader| reader.skew_join.is_none())
        );
    }
    // With four partitions the median is (10 + 100) / 2, not 100.
    // The 400-byte bucket exceeds 5 * 55 and must split into four shards.
    let (_, stage) = join_stage(config(), &[&[1], &[10], &[100], &[100; 4]]).await?;
    assert_eq!(stage.plan.input_partition_count(), 7);
    Ok(())
}

#[tokio::test]
async fn skew_join_preserves_grouping_and_stage_boundaries()
-> datafusion::common::Result<()> {
    // The join key already supplies the aggregate's distribution, so the
    // optimizer can put the aggregate and join in the same stage. ORDER BY
    // also exercises a passthrough stage boundary before the final merge.
    for sql in [
        "select t1.c, count(*) from t1 join t2 on t1.c = t2.c group by t1.c",
        "select t1.c, t2.b from (select c, count(*) as n from t1 group by c) t1 join t2 on t1.c = t2.c",
        "select t1.a, t2.b from t1 join t2 on t1.c = t2.c order by t1.a",
    ] {
        let mut planner = planner(config(), sql).await?;
        let stages = planner.runnable_stages()?.unwrap();
        for stage in stages {
            planner.finalise_stage_internal(
                stage.plan.stage_id(),
                partitions(&[&[150; 4], &[10], &[10], &[10]]),
            )?;
        }
        let stages = planner.runnable_stages()?.unwrap();
        for stage in stages {
            assert!(
                readers(stage.plan)
                    .iter()
                    .all(|reader| reader.skew_join.is_none())
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn skew_join_preserves_pushed_down_limits() -> datafusion::common::Result<()> {
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::HashJoinExec;

    for limit_join in [false, true] {
        let mut planner = planner(
            config().with_ballista_hash_join_max_build_partition_bytes(1000),
            "select t1.a, t2.b from t1 join t2 on t1.c = t2.c",
        )
        .await?;
        for stage in planner.runnable_stages()?.unwrap() {
            planner.finalise_stage_internal(
                stage.plan.stage_id(),
                partitions(&[&[150; 4], &[10], &[10], &[10]]),
            )?;
        }
        let mut limited_joins = 0;
        planner.plan = planner
            .plan
            .clone()
            .transform_up(|node| {
                if !node.is::<HashJoinExec>() {
                    return Ok(Transformed::no(node));
                }
                limited_joins += 1;
                let limited = if limit_join {
                    node.with_fetch(Some(1)).unwrap()
                } else {
                    let children = node.children();
                    let left = FilterExec::try_new(lit(true), children[0].clone())?
                        .with_fetch(Some(1))
                        .unwrap();
                    let right = children[1].clone();
                    datafusion::physical_plan::replace_children_if_necessary(
                        node,
                        vec![left, right],
                    )?
                };
                Ok(Transformed::yes(limited))
            })?
            .data;
        assert_eq!(limited_joins, 1);
        let stage = planner.runnable_stages()?.unwrap().remove(0);
        assert!(
            readers(stage.plan)
                .iter()
                .all(|reader| reader.skew_join.is_none())
        );
    }
    Ok(())
}

#[tokio::test]
async fn skew_join_repartitions_partial_aggregates_for_the_next_stage()
-> datafusion::common::Result<()> {
    let mut planner = planner(
        config(),
        "select t2.b, count(*) from t1 join t2 on t1.c = t2.c group by t2.b",
    )
    .await?;
    finish_join_inputs(
        &mut planner,
        &[&[150; 4], &[10], &[10], &[10]],
        &[&[10][..]; 4],
    )?;
    let stage = planner.runnable_stages()?.unwrap().remove(0);
    assert!(
        readers(stage.plan.clone())
            .iter()
            .all(|reader| reader.skew_join.is_some())
    );
    assert_eq!(stage.plan.input_partition_count(), 7);
    assert_eq!(
        stage
            .plan
            .shuffle_output_partitioning()
            .unwrap()
            .partition_count(),
        4
    );
    let stage_id = stage.plan.stage_id();
    planner.update_exchange_locations(
        stage_id,
        partitions(&[&[1][..]; 4]).into_iter().flatten().collect(),
    )?;
    let output = planner.take_stage_output_partitions(stage_id)?;
    assert_eq!(output.len(), 4);
    planner.resolve_stage_partitions(stage_id, output)?;
    let final_stage = planner.runnable_stages()?.unwrap().remove(0);
    let inputs = readers(final_stage.plan);
    assert_eq!(inputs.len(), 1);
    assert_eq!(
        inputs[0]
            .properties()
            .output_partitioning()
            .partition_count(),
        4
    );
    assert_eq!(inputs[0].partition.iter().map(Vec::len).sum::<usize>(), 4);
    Ok(())
}
