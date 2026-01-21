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

//! Distributed query execution

use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::error::{BallistaError, Result};
use ballista_core::execution_plans::ShuffleWriter;
use ballista_core::execution_plans::sort_shuffle::SortShuffleConfig;
use ballista_core::{
    execution_plans::{
        ShuffleReaderExec, ShuffleWriterExec, SortShuffleWriterExec,
        UnresolvedShuffleExec,
    },
    serde::scheduler::PartitionLocation,
};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, with_new_children_if_necessary,
};

use log::{debug, info};

type PartialQueryStageResult = (Arc<dyn ExecutionPlan>, Vec<Arc<dyn ShuffleWriter>>);

/// Trait for breaking an execution plan into distributed query stages.
///
/// The planner creates a DAG of stages where each stage can be executed
/// independently once its input stages are complete.
pub trait DistributedPlanner {
    /// Returns a vector of ExecutionPlans, where the root node is a [`ShuffleWriter`].
    ///
    /// Plans that depend on the input of other plans will have leaf nodes of type
    /// [`UnresolvedShuffleExec`]. A shuffle writer is created whenever the
    /// partitioning changes.
    fn plan_query_stages<'a>(
        &'a mut self,
        job_id: &'a str,
        execution_plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Vec<Arc<dyn ShuffleWriter>>>;
}

/// Default implementation of [`DistributedPlanner`].
///
/// Breaks execution plans into stages at shuffle boundaries (repartition, coalesce).
pub struct DefaultDistributedPlanner {
    /// Counter for generating unique stage IDs.
    next_stage_id: usize,
    /// Optimizer rule for enforcing sort requirements after stage splitting.
    optimizer_enforce_sorting: EnforceSorting,
}

impl DefaultDistributedPlanner {
    /// Creates a new `DefaultDistributedPlanner`.
    pub fn new() -> Self {
        Self {
            next_stage_id: 0,
            // when plan is broken into stages some sorting information may get lost in the process
            // thus stage re-optimisation is needed to adjust sort information
            optimizer_enforce_sorting:
                datafusion::physical_optimizer::enforce_sorting::EnforceSorting::default(),
        }
    }
}

impl Default for DefaultDistributedPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl DistributedPlanner for DefaultDistributedPlanner {
    /// Returns a vector of ExecutionPlans, where the root node is a shuffle writer.
    /// Plans that depend on the input of other plans will have leaf nodes of type [UnresolvedShuffleExec].
    /// A shuffle writer is created whenever the partitioning changes.
    fn plan_query_stages<'a>(
        &'a mut self,
        job_id: &'a str,
        execution_plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Vec<Arc<dyn ShuffleWriter>>> {
        info!("planning query stages for job {job_id}");
        let (new_plan, mut stages) =
            self.plan_query_stages_internal(job_id, execution_plan, config)?;
        stages.push(create_shuffle_writer_with_config(
            job_id,
            self.next_stage_id(),
            new_plan,
            None,
            config,
        )?);
        Ok(stages)
    }
}

impl DefaultDistributedPlanner {
    /// Returns a potentially modified version of the input execution_plan along with the resulting query stages.
    /// This function is needed because the input execution_plan might need to be modified, but it might not hold a
    /// complete query stage (its parent might also belong to the same stage)
    fn plan_query_stages_internal<'a>(
        &'a mut self,
        job_id: &'a str,
        execution_plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<PartialQueryStageResult> {
        // recurse down and replace children
        if execution_plan.children().is_empty() {
            return Ok((execution_plan, vec![]));
        }

        let mut stages = vec![];
        let mut children = vec![];
        for child in execution_plan.children() {
            let (new_child, mut child_stages) =
                self.plan_query_stages_internal(job_id, child.clone(), config)?;
            children.push(new_child);
            stages.append(&mut child_stages);
        }

        if let Some(_coalesce) = execution_plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
        {
            let input = children[0].clone();
            let input = self.optimizer_enforce_sorting.optimize(input, config)?;
            let shuffle_writer = create_shuffle_writer_with_config(
                job_id,
                self.next_stage_id(),
                input,
                None,
                config,
            )?;
            let unresolved_shuffle = create_unresolved_shuffle(shuffle_writer.as_ref());

            stages.push(shuffle_writer);
            Ok((
                with_new_children_if_necessary(execution_plan, vec![unresolved_shuffle])?,
                stages,
            ))
        } else if let Some(_sort_preserving_merge) = execution_plan
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>(
        ) {
            let shuffle_writer = create_shuffle_writer_with_config(
                job_id,
                self.next_stage_id(),
                children[0].clone(),
                None,
                config,
            )?;
            let unresolved_shuffle = create_unresolved_shuffle(shuffle_writer.as_ref());
            stages.push(shuffle_writer);
            Ok((
                with_new_children_if_necessary(execution_plan, vec![unresolved_shuffle])?,
                stages,
            ))
        } else if let Some(repart) =
            execution_plan.as_any().downcast_ref::<RepartitionExec>()
        {
            match repart.properties().output_partitioning() {
                Partitioning::Hash(_, _) => {
                    let input = children[0].clone();
                    let input = self.optimizer_enforce_sorting.optimize(input, config)?;

                    let shuffle_writer = create_shuffle_writer_with_config(
                        job_id,
                        self.next_stage_id(),
                        input,
                        Some(repart.partitioning().to_owned()),
                        config,
                    )?;
                    let unresolved_shuffle =
                        create_unresolved_shuffle(shuffle_writer.as_ref());

                    stages.push(shuffle_writer);
                    Ok((unresolved_shuffle, stages))
                }
                _ => {
                    // remove any non-hash repartition from the distributed plan
                    Ok((children[0].clone(), stages))
                }
            }
        } else {
            Ok((
                with_new_children_if_necessary(execution_plan, children)?,
                stages,
            ))
        }
    }

    /// Generate a new stage ID
    fn next_stage_id(&mut self) -> usize {
        self.next_stage_id += 1;
        self.next_stage_id
    }
}

fn create_unresolved_shuffle(
    shuffle_writer: &dyn ShuffleWriter,
) -> Arc<UnresolvedShuffleExec> {
    Arc::new(UnresolvedShuffleExec::new(
        shuffle_writer.stage_id(),
        shuffle_writer.schema(),
        shuffle_writer.properties().output_partitioning().clone(),
    ))
}

/// Returns all unresolved shuffle nodes in the execution plan.
///
/// Used to identify which input stages a plan depends on.
pub fn find_unresolved_shuffles(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<UnresolvedShuffleExec>> {
    if let Some(unresolved_shuffle) =
        plan.as_any().downcast_ref::<UnresolvedShuffleExec>()
    {
        Ok(vec![unresolved_shuffle.clone()])
    } else {
        Ok(plan
            .children()
            .into_iter()
            .map(find_unresolved_shuffles)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect())
    }
}

/// Replaces [`UnresolvedShuffleExec`] nodes with [`ShuffleReaderExec`] nodes.
///
/// Called after input stages complete to connect stages with their actual partition locations.
pub fn remove_unresolved_shuffles(
    stage: Arc<dyn ExecutionPlan>,
    partition_locations: &HashMap<usize, HashMap<usize, Vec<PartitionLocation>>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for child in stage.children() {
        if let Some(unresolved_shuffle) =
            child.as_any().downcast_ref::<UnresolvedShuffleExec>()
        {
            let mut relevant_locations = vec![];
            let p = partition_locations
                .get(&unresolved_shuffle.stage_id)
                .ok_or_else(|| {
                    BallistaError::General(
                        "Missing partition location. Could not remove unresolved shuffles"
                            .to_owned(),
                    )
                })?
                .clone();

            for i in 0..unresolved_shuffle.output_partition_count {
                if let Some(x) = p.get(&i) {
                    relevant_locations.push(x.to_owned());
                } else {
                    relevant_locations.push(vec![]);
                }
            }
            debug!(
                "Creating shuffle reader: {}",
                relevant_locations
                    .iter()
                    .map(|c| c
                        .iter()
                        .filter(|l| !l.path.is_empty())
                        .map(|l| l.path.clone())
                        .collect::<Vec<_>>()
                        .join(", "))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
            new_children.push(Arc::new(ShuffleReaderExec::try_new(
                unresolved_shuffle.stage_id,
                relevant_locations,
                unresolved_shuffle.schema().clone(),
                unresolved_shuffle
                    .properties()
                    .output_partitioning()
                    .clone(),
            )?))
        } else {
            new_children.push(remove_unresolved_shuffles(
                child.clone(),
                partition_locations,
            )?);
        }
    }
    Ok(with_new_children_if_necessary(stage, new_children)?)
}

/// Rollback the ShuffleReaderExec to UnresolvedShuffleExec.
/// Used when the input stages are finished but some partitions are missing due to executor lost.
/// The entire stage need to be rolled back and rescheduled.
pub fn rollback_resolved_shuffles(
    stage: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for child in stage.children() {
        if let Some(shuffle_reader) = child.as_any().downcast_ref::<ShuffleReaderExec>() {
            let stage_id = shuffle_reader.stage_id;

            let unresolved_shuffle = Arc::new(UnresolvedShuffleExec::new(
                stage_id,
                shuffle_reader.schema(),
                shuffle_reader.properties().partitioning.clone(),
            ));
            new_children.push(unresolved_shuffle);
        } else {
            new_children.push(rollback_resolved_shuffles(child.clone())?);
        }
    }
    Ok(with_new_children_if_necessary(stage, new_children)?)
}

fn create_shuffle_writer_with_config(
    job_id: &str,
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ShuffleWriter>> {
    // Check if sort-based shuffle is enabled
    let ballista_config = config
        .extensions
        .get::<BallistaConfig>()
        .cloned()
        .unwrap_or_default();

    if ballista_config.shuffle_sort_based_enabled() {
        // Sort shuffle requires hash partitioning
        if let Some(Partitioning::Hash(exprs, partition_count)) = partitioning {
            let sort_config = SortShuffleConfig::new(
                true,
                ballista_config.shuffle_sort_based_buffer_size(),
                ballista_config.shuffle_sort_based_memory_limit(),
                ballista_config.shuffle_sort_based_spill_threshold(),
                datafusion::arrow::ipc::CompressionType::LZ4_FRAME,
            );

            return Ok(Arc::new(SortShuffleWriterExec::try_new(
                job_id.to_owned(),
                stage_id,
                plan,
                "".to_owned(),
                Partitioning::Hash(exprs, partition_count),
                sort_config,
            )?));
        }
    }

    // Fall back to standard shuffle writer
    Ok(Arc::new(ShuffleWriterExec::try_new(
        job_id.to_owned(),
        stage_id,
        plan,
        "".to_owned(),
        partitioning,
    )?))
}

#[cfg(test)]
mod test {
    use crate::planner::{DefaultDistributedPlanner, DistributedPlanner};
    use crate::test_utils::datafusion_test_context;
    use ballista_core::error::BallistaError;
    use ballista_core::execution_plans::{ShuffleWriterExec, UnresolvedShuffleExec};
    use ballista_core::serde::BallistaCodec;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::windows::BoundedWindowAggExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion::physical_plan::{InputOrderMode, Partitioning};
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf::LogicalPlanNode;
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use std::sync::Arc;
    use uuid::Uuid;

    macro_rules! downcast_exec {
        ($exec: expr, $ty: ty) => {
            $exec.as_any().downcast_ref::<$ty>().expect(&format!(
                "Downcast to {} failed. Got {:?}",
                stringify!($ty),
                $exec
            ))
        };
    }

    #[tokio::test]
    async fn distributed_aggregate_plan() -> Result<(), BallistaError> {
        let ctx = datafusion_test_context("testdata").await?;
        let session_state = ctx.state();

        // simplified form of TPC-H query 1
        let df = ctx
            .sql(
                "select l_returnflag, sum(l_extendedprice * 1) as sum_disc_price
            from lineitem
            group by l_returnflag
            order by l_returnflag",
            )
            .await?;

        let plan = df.into_optimized_plan()?;
        let plan = session_state.optimize(&plan)?;
        let plan = session_state.create_physical_plan(&plan).await?;

        let mut planner = DefaultDistributedPlanner::new();
        let job_uuid = Uuid::new_v4();
        let stages = planner.plan_query_stages(
            &job_uuid.to_string(),
            plan,
            ctx.state().config().options(),
        )?;
        for (i, stage) in stages.iter().enumerate() {
            println!("Stage {i}:\n{}", displayable(stage.as_ref()).indent(false));
        }

        /* Expected result:

        ShuffleWriterExec: Some(Hash([Column { name: "l_returnflag", index: 0 }], 2))
          AggregateExec: mode=Partial, gby=[l_returnflag@1 as l_returnflag], aggr=[SUM(lineitem.l_extendedprice * Int64(1))]
            CsvExec: files={2 groups: [[ballista/scheduler/testdata/lineitem/partition1.tbl], [ballista/scheduler/testdata/lineitem/partition0.tbl]]}, has_header=false, limit=None, projection=[l_extendedprice, l_returnflag]

        ShuffleWriterExec: None
          SortExec: [l_returnflag@0 ASC NULLS LAST]
            ProjectionExec: expr=[l_returnflag@0 as l_returnflag, SUM(lineitem.l_extendedprice * Int64(1))@1 as sum_disc_price]
              AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag], aggr=[SUM(lineitem.l_extendedprice * Int64(1))]
                CoalesceBatchesExec: target_batch_size=8192
                  UnresolvedShuffleExec

        ShuffleWriterExec: None
          SortPreservingMergeExec: [l_returnflag@0 ASC NULLS LAST]
            UnresolvedShuffleExec
        */

        assert_eq!(3, stages.len());

        // verify stage 0
        let stage0 = stages[0].children()[0].clone();
        let partial_hash = downcast_exec!(stage0, AggregateExec);
        assert!(*partial_hash.mode() == AggregateMode::Partial);

        // verify stage 1
        let stage1 = stages[1].children()[0].clone();
        let sort = downcast_exec!(stage1, SortExec);
        let projection = sort.children()[0].clone();
        let projection = downcast_exec!(projection, ProjectionExec);
        let final_hash = projection.children()[0].clone();
        let final_hash = downcast_exec!(final_hash, AggregateExec);
        assert!(*final_hash.mode() == AggregateMode::FinalPartitioned);
        let coalesce = final_hash.children()[0].clone();
        let coalesce = downcast_exec!(coalesce, CoalesceBatchesExec);
        let unresolved_shuffle = coalesce.children()[0].clone();
        let unresolved_shuffle =
            downcast_exec!(unresolved_shuffle, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle.stage_id, 1);
        assert_eq!(unresolved_shuffle.output_partition_count, 2);
        assert_eq!(
            unresolved_shuffle.properties().partitioning,
            Partitioning::Hash(vec![Arc::new(Column::new("l_returnflag", 0))], 2)
        );

        // verify stage 2
        let stage2 = stages[2].children()[0].clone();
        let merge = downcast_exec!(stage2, SortPreservingMergeExec);
        let unresolved_shuffle = merge.children()[0].clone();
        let unresolved_shuffle =
            downcast_exec!(unresolved_shuffle, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle.stage_id, 2);
        assert_eq!(unresolved_shuffle.output_partition_count, 2);
        assert_eq!(
            unresolved_shuffle.properties().partitioning,
            Partitioning::Hash(vec![Arc::new(Column::new("l_returnflag", 0))], 2)
        );

        Ok(())
    }

    #[tokio::test]
    async fn distributed_join_plan() -> Result<(), BallistaError> {
        let ctx = datafusion_test_context("testdata").await?;
        let session_state = ctx.state();

        // simplified form of TPC-H query 12
        let df = ctx
            .sql(
                "select
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
from
    lineitem
        join
    orders
    on
            l_orderkey = o_orderkey
where
        l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1995-01-01'
group by
    l_shipmode
order by
    l_shipmode;
",
            )
            .await?;

        let plan = df.into_optimized_plan()?;
        let plan = session_state.optimize(&plan)?;
        let plan = session_state.create_physical_plan(&plan).await?;

        let mut planner = DefaultDistributedPlanner::new();
        let job_uuid = Uuid::new_v4();
        let stages = planner.plan_query_stages(
            &job_uuid.to_string(),
            plan,
            ctx.state().config().options(),
        )?;
        for (i, stage) in stages.iter().enumerate() {
            println!("Stage {i}:\n{}", displayable(stage.as_ref()).indent(false));
        }

        /* Expected result:

        ShuffleWriterExec: Some(Hash([Column { name: "l_orderkey", index: 0 }], 2))
          ProjectionExec: expr=[l_orderkey@0 as l_orderkey, l_shipmode@4 as l_shipmode]
            CoalesceBatchesExec: target_batch_size=8192
              FilterExec: (l_shipmode@4 = SHIP OR l_shipmode@4 = MAIL) AND l_commitdate@2 < l_receiptdate@3 AND l_shipdate@1 < l_commitdate@2 AND l_receiptdate@3 >= 8766 AND l_receiptdate@3 < 9131
                CsvExec: files={2 groups: [[testdata/lineitem/partition0.tbl], [testdata/lineitem/partition1.tbl]]}, has_header=false, limit=None, projection=[l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode]

        ShuffleWriterExec: Some(Hash([Column { name: "o_orderkey", index: 0 }], 2))
          CsvExec: files={1 group: [[testdata/orders/orders.tbl]]}, has_header=false, limit=None, projection=[o_orderkey, o_orderpriority]

        ShuffleWriterExec: Some(Hash([Column { name: "l_shipmode", index: 0 }], 2))
          AggregateExec: mode=Partial, gby=[l_shipmode@0 as l_shipmode], aggr=[SUM(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), SUM(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
            CoalesceBatchesExec: target_batch_size=8192
              HashJoinExec: mode=Partitioned, join_type=Inner, on=[(l_orderkey@0, o_orderkey@0)], projection=[l_shipmode@1, o_orderpriority@3]
                CoalesceBatchesExec: target_batch_size=8192
                  UnresolvedShuffleExec
                CoalesceBatchesExec: target_batch_size=8192
                  UnresolvedShuffleExec

        ShuffleWriterExec: None
          SortExec: expr=[l_shipmode@0 ASC NULLS LAST]
            ProjectionExec: expr=[l_shipmode@0 as l_shipmode, SUM(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@1 as high_line_count, SUM(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@2 as low_line_count]
              AggregateExec: mode=FinalPartitioned, gby=[l_shipmode@0 as l_shipmode], aggr=[SUM(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), SUM(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
                CoalesceBatchesExec: target_batch_size=8192
                  UnresolvedShuffleExec

        ShuffleWriterExec: None
          SortPreservingMergeExec: [l_shipmode@0 ASC NULLS LAST]
            UnresolvedShuffleExec
        */

        assert_eq!(5, stages.len());

        // verify partitioning for each stage

        // csv "lineitem" (2 files)
        assert_eq!(
            2,
            stages[0].children()[0]
                .properties()
                .output_partitioning()
                .partition_count()
        );
        assert_eq!(
            2,
            stages[0]
                .shuffle_output_partitioning()
                .expect("stage 0")
                .partition_count()
        );

        // csv "orders" (1 file)
        assert_eq!(
            1,
            stages[1].children()[0]
                .properties()
                .output_partitioning()
                .partition_count()
        );
        assert_eq!(
            2,
            stages[1]
                .shuffle_output_partitioning()
                .expect("stage 1")
                .partition_count()
        );

        // join and partial hash aggregate
        let input = stages[2].children()[0].clone();
        assert_eq!(
            2,
            input.properties().output_partitioning().partition_count()
        );
        assert_eq!(
            2,
            stages[2]
                .shuffle_output_partitioning()
                .expect("stage 2")
                .partition_count()
        );

        let hash_agg = downcast_exec!(input, AggregateExec);

        let coalesce_batches = hash_agg.children()[0].clone();
        let coalesce_batches = downcast_exec!(coalesce_batches, CoalesceBatchesExec);

        let join = coalesce_batches.children()[0].clone();
        let join = downcast_exec!(join, HashJoinExec);
        assert!(join.contains_projection());

        let join_input_1 = join.children()[0].clone();
        // skip CoalesceBatches
        let join_input_1 = join_input_1.children()[0].clone();
        let unresolved_shuffle_reader_1 =
            downcast_exec!(join_input_1, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle_reader_1.output_partition_count, 2);
        assert_eq!(
            unresolved_shuffle_reader_1.properties().partitioning,
            Partitioning::Hash(vec![Arc::new(Column::new("l_orderkey", 0))], 2)
        );

        let join_input_2 = join.children()[1].clone();
        // skip CoalesceBatches
        let join_input_2 = join_input_2.children()[0].clone();
        let unresolved_shuffle_reader_2 =
            downcast_exec!(join_input_2, UnresolvedShuffleExec);
        assert_eq!(unresolved_shuffle_reader_2.output_partition_count, 2);
        assert_eq!(
            unresolved_shuffle_reader_2.properties().partitioning,
            Partitioning::Hash(vec![Arc::new(Column::new("o_orderkey", 0))], 2)
        );

        // final partitioned hash aggregate
        assert_eq!(
            2,
            stages[3].children()[0]
                .properties()
                .output_partitioning()
                .partition_count()
        );
        assert!(stages[3].shuffle_output_partitioning().is_none());

        // coalesce partitions and sort
        assert_eq!(
            1,
            stages[4].children()[0]
                .properties()
                .output_partitioning()
                .partition_count()
        );
        assert!(stages[4].shuffle_output_partitioning().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn distributed_window_plan() -> Result<(), BallistaError> {
        let ctx = datafusion_test_context("testdata").await?;
        let session_state = ctx.state();

        // simplified form of TPC-DS query 67
        let df = ctx
            .sql(
                "
                 select * from (
                     select
                         l_shipmode,
                         l_shipdate,
                         rank() over (partition by l_shipmode order by l_shipdate desc) rk
                     from lineitem
                 ) alias1
                 where rk <= 100 order by l_shipdate, rk;
                ",
            )
            .await?;

        let plan = df.into_optimized_plan()?;
        let plan = session_state.optimize(&plan)?;
        let plan = session_state.create_physical_plan(&plan).await?;

        let mut planner = DefaultDistributedPlanner::new();
        let job_uuid = Uuid::new_v4();
        let stages = planner.plan_query_stages(
            &job_uuid.to_string(),
            plan,
            ctx.state().config().options(),
        )?;
        for (i, stage) in stages.iter().enumerate() {
            println!("Stage {i}:\n{}", displayable(stage.as_ref()).indent(false));
        }
        /*
            expected result:
            Stage 0:
            ShuffleWriterExec: Some(Hash([Column { name: "l_shipmode", index: 1 }], 2))
              CsvExec: file_groups={2 groups: [[testdata/lineitem/partition0.tbl], [testdata/lineitem/partition1.tbl]]}, projection=[l_shipdate, l_shipmode], has_header=false

            Stage 1:
            ShuffleWriterExec: None
              SortExec: expr=[l_shipdate@1 ASC NULLS LAST,rk@2 ASC NULLS LAST], preserve_partitioning=[true]
                ProjectionExec: expr=[l_shipmode@1 as l_shipmode, l_shipdate@0 as l_shipdate, RANK() PARTITION BY [lineitem.l_shipmode] ORDER BY [lineitem.l_shipdate DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as rk]
                  CoalesceBatchesExec: target_batch_size=8192
                    FilterExec: RANK() PARTITION BY [lineitem.l_shipmode] ORDER BY [lineitem.l_shipdate DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 <= 100
                      BoundedWindowAggExec: wdw=[RANK() PARTITION BY [lineitem.l_shipmode] ORDER BY [lineitem.l_shipdate DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Ok(Field { name: "RANK() PARTITION BY [lineitem.l_shipmode] ORDER BY [lineitem.l_shipdate DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(IntervalMonthDayNano("NULL")), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]
                        SortExec: expr=[l_shipmode@1 ASC NULLS LAST,l_shipdate@0 DESC], preserve_partitioning=[true]
                          CoalesceBatchesExec: target_batch_size=8192
                            UnresolvedShuffleExec

            Stage 2:
            ShuffleWriterExec: None
              SortPreservingMergeExec: [l_shipdate@1 ASC NULLS LAST,rk@2 ASC NULLS LAST]
                UnresolvedShuffleExec

        */

        assert_eq!(3, stages.len());

        // stage0
        let stage0 = stages[0].clone();
        let shuffle_write = downcast_exec!(stage0, ShuffleWriterExec);
        let partitioning = shuffle_write.shuffle_output_partitioning().expect("stage0");
        assert_eq!(2, partitioning.partition_count());
        let partition_col = match partitioning {
            Partitioning::Hash(exprs, 2) => match exprs.as_slice() {
                [col] => col.as_any().downcast_ref::<Column>(),
                _ => None,
            },
            _ => None,
        };
        assert_eq!(Some(&Column::new("l_shipmode", 1)), partition_col);

        // stage1
        let sort = downcast_exec!(stages[1].children()[0], SortExec);
        let projection = downcast_exec!(sort.children()[0], ProjectionExec);
        let coalesce = downcast_exec!(projection.children()[0], CoalesceBatchesExec);
        let filter = downcast_exec!(coalesce.children()[0], FilterExec);
        let window = downcast_exec!(filter.children()[0], BoundedWindowAggExec);
        let partition_by = window.partition_keys();
        let partition_by = match partition_by[..] {
            [ref col] => col.as_any().downcast_ref::<Column>(),
            _ => None,
        };
        assert_eq!(Some(&Column::new("l_shipmode", 1)), partition_by);
        assert_eq!(InputOrderMode::Sorted, window.input_order_mode);
        let sort = downcast_exec!(window.children()[0], SortExec);
        match &sort.expr().iter().collect::<Vec<_>>()[..] {
            [expr1, expr2] => {
                assert_eq!(
                    SortOptions {
                        descending: false,
                        nulls_first: false
                    },
                    expr1.options
                );
                assert_eq!(
                    Some(&Column::new("l_shipmode", 1)),
                    expr1.expr.as_any().downcast_ref()
                );
                assert_eq!(
                    SortOptions {
                        descending: true,
                        nulls_first: true
                    },
                    expr2.options
                );
                assert_eq!(
                    Some(&Column::new("l_shipdate", 0)),
                    expr2.expr.as_any().downcast_ref()
                );
            }
            _ => panic!("invalid sort {sort:?}"),
        };

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_serde_aggregate() -> Result<(), BallistaError> {
        let ctx = datafusion_test_context("testdata").await?;
        let session_state = ctx.state();

        // simplified form of TPC-H query 1
        let df = ctx
            .sql(
                "select l_returnflag, sum(l_extendedprice * 1) as sum_disc_price
            from lineitem
            group by l_returnflag
            order by l_returnflag",
            )
            .await?;

        let plan = df.into_optimized_plan()?;
        let plan = session_state.optimize(&plan)?;
        let plan = session_state.create_physical_plan(&plan).await?;

        let mut planner = DefaultDistributedPlanner::new();
        let job_uuid = Uuid::new_v4();
        let stages = planner.plan_query_stages(
            &job_uuid.to_string(),
            plan,
            ctx.state().config().options(),
        )?;

        let partial_hash = stages[0].children()[0].clone();
        let partial_hash_serde =
            roundtrip_operator(&ctx.task_ctx(), partial_hash.clone())?;

        let partial_hash = downcast_exec!(partial_hash, AggregateExec);
        let partial_hash_serde = downcast_exec!(partial_hash_serde, AggregateExec);

        assert_eq!(
            format!("{partial_hash:?}"),
            format!("{partial_hash_serde:?}")
        );

        Ok(())
    }

    fn roundtrip_operator(
        ctx: &TaskContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
        let codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> =
            BallistaCodec::default();
        let proto: datafusion_proto::protobuf::PhysicalPlanNode =
            datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(
                plan.clone(),
                codec.physical_extension_codec(),
            )?;
        let result_exec_plan: Arc<dyn ExecutionPlan> =
            (proto).try_into_physical_plan(ctx, codec.physical_extension_codec())?;
        Ok(result_exec_plan)
    }
}
