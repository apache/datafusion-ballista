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

use ballista_core::execution_plans::{
    CoalescePlan, stats_for_partition, stats_for_partitions,
};
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::Statistics;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
        Partitioning, PlanProperties,
    },
};
use log::trace;
use parking_lot::Mutex;
use std::ops::Deref;
use std::sync::{Arc, atomic::AtomicI64};

/// Range-partition boundaries recovered from an
/// `UnorderedRangeRepartitionExec` / `OrderedRangeRepartitionExec` upstream
/// of this exchange. Written after the range-repartition-producing stage
/// completes and its runtime-stats sketches are merged; read at
/// task-specialization time to build per-downstream-partition range filters
/// (see `PerPartitionFilterExec`).
///
/// `cuts` are `K - 1` monotone `f64` boundaries expressed in the value space
/// of `routing_expr`; downstream partition `k` owns `[cuts[k-1], cuts[k])`
/// with virtual `-∞`/`+∞` sentinels on the ends (matching the range
/// repartition's write-side convention). `routing_expr` is the same
/// expression the range repartition routes on so the filter is symmetric
/// with the writer's placement decision.
#[derive(Clone, Debug)]
pub struct RangeRepartitionRouting {
    pub cuts: Vec<f64>,
    pub routing_expr: Arc<dyn PhysicalExpr>,
}

/// Execution plan representing an exchange/shuffle boundary used by the
/// scheduler during adaptive query execution (AQE).
///
/// `ExchangeExec` acts as a placeholder for a shuffle: it holds the child
/// `input` plan and, when available, the resolved shuffle metadata in
/// `shuffle_partitions`. The scheduler uses the information stored here to
/// decide stage execution and to compute partition statistics without
/// executing the plan directly.
///
/// Note: this type implements DataFusion's `ExecutionPlan` trait but returns
/// an error from `execute` because it is not directly runnable.
#[derive(Debug)]
pub struct ExchangeExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    pub(crate) partitioning: Option<Partitioning>,
    pub(crate) plan_id: usize,
    stage_id: Arc<AtomicI64>,

    /// first vector is target representing target partitioning
    /// (to be called on shuffle read side,  fn execute( partition: usize ...)
    /// will be used as key.
    /// second vector represents exchange files, their locations,
    ///
    /// the so the len of `shuffle_partitions` vector is equal to number
    /// partitions after partitioning, the len of each vector item
    /// can not be assumed.
    shuffle_partitions: Arc<Mutex<Option<Vec<Vec<PartitionLocation>>>>>,

    /// Per-stage coalesce decision attached to this Exchange by
    /// `CoalescePartitionsRule` before adapter conversion.
    ///
    /// `None` means: build the SR with `try_new` (M-partition, no coalesce).
    /// `Some(cp)` means: build the SR with `try_new_coalesced(cp)` so the
    /// reader exposes K = `cp.groups.len()` partitions, each backed by the
    /// upstream-index range described by the corresponding `PartitionGroup`.
    ///
    /// Wrapped in `Arc<Mutex<…>>` so `with_new_children` can clone the slot
    /// alongside the Exchange, keeping rule decisions in sync across
    /// transform-rebuilt parent chains. Same pattern as `shuffle_partitions`.
    coalesce: Arc<Mutex<Option<Arc<CoalescePlan>>>>,

    /// Range-partition boundaries recovered at runtime from an upstream
    /// range-repartition op (URRE or ORRE). Stored when
    /// the range-repartition-producing stage completes and its per-sub-part
    /// quantile sketches have been merged. Read at task-specialization time
    /// to build `PerPartitionFilterExec` predicates for downstream stage `N+1`.
    ///
    /// `None` on any exchange that isn't downstream of a range repartition
    range_repartition_routing: Arc<Mutex<Option<RangeRepartitionRouting>>>,

    /// this disables stage from running even it would be suitable to run.
    ///
    /// the main reason for this property this is to allow rules to override
    /// stage execution logic, and to support making more complex
    /// stage run decisions.
    pub(crate) inactive_stage: bool,

    /// Indicates that this exchange is broadcast exchange,
    /// usually used in broadcast joins.
    /// CollectLeft HashJoin equivalent in datafusion
    pub(crate) broadcast: bool,
}

impl ExchangeExec {
    /// Creates a new `ExchangeExec` with default stage ID (-1) and empty
    /// partition set. The stage ID and partitions should be resolved
    /// before the exchange participates in AQE rules.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
        plan_id: usize,
    ) -> Self {
        Self::new_with_details(
            input,
            partitioning,
            plan_id,
            Arc::new(AtomicI64::new(-1)),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(None)),
            false,
            false,
        )
    }
    /// new broadcast exchange
    pub fn new_broadcast(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
        plan_id: usize,
    ) -> Self {
        Self::new_with_details(
            input,
            partitioning,
            plan_id,
            Arc::new(AtomicI64::new(-1)),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(None)),
            true,
            false,
        )
    }

    pub fn to_broadcast(&self, plan_id: usize) -> Self {
        Self::new_with_details(
            self.input.clone(),
            None,
            plan_id,
            self.stage_id.clone(),
            self.shuffle_partitions.clone(),
            self.coalesce.clone(),
            self.range_repartition_routing.clone(),
            true,
            self.inactive_stage,
        )
    }

    /// Creates a new `ExchangeExec` with explicitly-provided stage ID and
    /// partition storage. Used by the AQE rule infrastructure to construct
    /// exchanges that share atomic state with the enclosing `AdaptivePlanner`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_details(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
        plan_id: usize,
        stage_id: Arc<AtomicI64>,
        stage_partitions: Arc<Mutex<Option<Vec<Vec<PartitionLocation>>>>>,
        coalesce: Arc<Mutex<Option<Arc<CoalescePlan>>>>,
        range_repartition_routing: Arc<Mutex<Option<RangeRepartitionRouting>>>,
        broadcast: bool,
        inactive_stage: bool,
    ) -> Self {
        let plan_partitioning = match (partitioning.as_ref(), broadcast) {
            (Some(partitioning), false) => partitioning.clone(),
            (None, false) => input.output_partitioning().clone(),
            (_, true) => Partitioning::UnknownPartitioning(1),
        };
        let eq_properties = input.properties().eq_properties.clone();
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            plan_partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));

        Self {
            input,
            properties,
            plan_id,
            stage_id,
            shuffle_partitions: stage_partitions,
            partitioning,
            coalesce,
            range_repartition_routing,
            inactive_stage,
            broadcast,
        }
    }

    /// Indicates that partitions have been resolved
    ///
    /// If partitions has been resolved, current stage has
    /// finished and new one could be started.
    /// Unresolved shuffle could be  replaced with shuffle read.
    pub fn shuffle_created(&self) -> bool {
        self.shuffle_partitions.lock().is_some()
    }

    /// Resolves and stores the shuffle partitions for this exchange operation.
    ///
    /// This method should be called once the partitions for the shuffle have been determined.
    /// After calling this method, `shuffle_created()` will return `true` and the stored
    /// partitions can be retrieved via `shuffle_partitions()`.
    ///
    /// # Arguments
    ///
    /// * `partitions` - A vector of partition vectors, where each inner vector contains
    ///   the `PartitionLocation`s for a shuffle partition.
    pub fn resolve_shuffle_partitions(&self, partitions: Vec<Vec<PartitionLocation>>) {
        self.shuffle_partitions.lock().replace(partitions);
    }

    /// Checks whether the shuffle partitions have been resolved.
    ///
    /// Returns `true` if partitions have been resolved, indicating that the current stage
    /// has finished and a new stage can be started. An unresolved shuffle can be replaced
    /// with a shuffle read operation.
    ///
    /// # Returns
    ///
    /// `true` if `shuffle_partitions` contains a value, `false` otherwise.
    pub fn shuffle_partitions(&self) -> Option<Vec<Vec<PartitionLocation>>> {
        self.shuffle_partitions.lock().clone()
    }

    /// Flattens partition locations into single vector,
    /// this method is usually used when we want to collect partitions
    /// to form a broadcast join
    pub(crate) fn shuffle_partitions_flattened(&self) -> Vec<PartitionLocation> {
        let partitions = self.shuffle_partitions.lock().clone().unwrap_or_default();
        partitions.into_iter().flatten().collect()
    }

    /// sets the stage id running this exchange
    pub fn set_stage_id(&self, id: usize) {
        self.stage_id
            .store(id as i64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns the stage ID assigned to this exchange, or `None` if the
    /// stage has not yet been resolved (initial value -1).
    pub fn stage_id(&self) -> Option<usize> {
        let stage_id = self.stage_id.load(std::sync::atomic::Ordering::Relaxed);

        if stage_id >= 0 {
            Some(stage_id as usize)
        } else {
            None
        }
    }

    /// Returns a reference to the input (child) execution plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Attaches a `CoalescePlan` to this Exchange. The adapter consumes the
    /// plan when converting Exchange → ShuffleReader: a Some value triggers
    /// `try_new_coalesced` (K-partition reader); None uses `try_new`
    /// (M-partition reader). Idempotent overwrite.
    pub fn set_coalesce(&self, cp: Arc<CoalescePlan>) {
        self.coalesce.lock().replace(cp);
    }

    /// Returns the attached `CoalescePlan`, if `set_coalesce` was called.
    pub fn coalesce(&self) -> Option<Arc<CoalescePlan>> {
        self.coalesce.lock().clone()
    }

    /// Publishes range-repartition-recovered range boundaries on this
    /// exchange. Called from
    /// `AdaptiveExecutionGraph::maybe_range_repartition_overlap_remap` once
    /// the upstream range-repartition stage completes and its per-sub-part
    /// quantile sketches have been merged into `K - 1` monotone cuts.
    /// Idempotent overwrite matches the `set_coalesce` pattern.
    pub fn resolve_range_repartition_routing(&self, routing: RangeRepartitionRouting) {
        self.range_repartition_routing.lock().replace(routing);
    }

    /// Returns the range-repartition routing info if
    /// `resolve_range_repartition_routing` has fired. Consumers use
    /// `Some(_)` as the signal that this exchange is downstream of a range
    /// repartition and its tasks need per-partition range filters.
    pub fn range_repartition_routing(&self) -> Option<RangeRepartitionRouting> {
        self.range_repartition_routing.lock().clone()
    }
}

impl DisplayAs for ExchangeExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ExchangeExec: partitioning={}, plan_id={}, stage_id={}, stage_resolved={}",
                    self.partitioning
                        .as_ref()
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "None".to_string()),
                    self.plan_id,
                    self.stage_id()
                        .map(|stage_id| format!("{}", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                    self.shuffle_created(),
                )?;
                if let Some(cp) = self.coalesce.lock().as_ref() {
                    write!(
                        f,
                        ", coalesce={} of {}",
                        cp.groups.len(),
                        cp.upstream_partition_count,
                    )?;
                }
                if let Some(r) = self.range_repartition_routing.lock().as_ref() {
                    write!(f, ", range_repartition_cuts={}", r.cuts.len())?;
                }
                if self.broadcast {
                    write!(f, ", broadcast=true",)?
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                writeln!(
                    f,
                    "partitioning={}",
                    self.partitioning
                        .as_ref()
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "None".to_string()),
                )?;
                writeln!(f, "plan_id={}", self.plan_id)?;
                writeln!(
                    f,
                    "stage_id={}",
                    self.stage_id()
                        .map(|stage_id| format!("({})", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                )?;
                writeln!(f, "stage_resolved={}", self.shuffle_created())?;
                if self.broadcast {
                    writeln!(f, "broadcast=true")?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for ExchangeExec {
    fn name(&self) -> &str {
        "ExchangeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        match self.partitioning {
            Some(_) => vec![false; self.children().len()],
            None => vec![true; self.children().len()],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            let new_exec = Self::new_with_details(
                children[0].clone(),
                self.partitioning.clone(),
                self.plan_id,
                self.stage_id.clone(),
                // Carry the coalesce slot so a transform-rebuilt parent chain
                // doesn't lose the rule's decision.
                self.shuffle_partitions.clone(),
                self.coalesce.clone(),
                self.range_repartition_routing.clone(),
                self.broadcast,
                self.inactive_stage,
            );

            Ok(Arc::new(new_exec))
        } else {
            Err(DataFusionError::Plan(
                "ExchangeExec expects single child".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        Err(DataFusionError::Plan(
            "ExchangeExec does not support execution".to_owned(),
        ))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let schema = self.input.schema();
        match self.shuffle_partitions.lock().deref() {
            //
            Some(partition_locations) => {
                if let Some(idx) = partition {
                    let partition_count =
                        self.properties().partitioning.partition_count();
                    if idx >= partition_count {
                        return datafusion::common::internal_err!(
                            "Invalid partition index: {}, the partition count is {}",
                            idx,
                            partition_count
                        );
                    }
                    let stat_for_partition = stats_for_partition(
                        idx,
                        schema.fields().len(),
                        partition_locations,
                    );

                    trace!(
                        "shuffle reader at stage: {:?} and partition {} returned statistics: {:?}",
                        self.stage_id, idx, stat_for_partition
                    );
                    stat_for_partition.map(Arc::new)
                } else {
                    let stats_for_partitions = stats_for_partitions(
                        schema.fields().len(),
                        partition_locations
                            .iter()
                            .flatten()
                            .map(|loc| loc.partition_stats),
                    );
                    trace!(
                        "shuffle reader at stage: {:?} returned statistics for all partitions: {:?}",
                        self.stage_id, stats_for_partitions
                    );
                    Ok(Arc::new(stats_for_partitions))
                }
            }
            None => Ok(Arc::new(Statistics::new_unknown(&schema))),
        }
    }
}

#[cfg(test)]
mod range_repartition_routing_tests {
    //! `RangeRepartitionRouting` parking on `ExchangeExec`. The AQE hook
    //! writes here at range-repartition-stage completion; task
    //! specialization reads it back at
    //! `BallistaAdapter::transform_children` time to wrap the
    //! ShuffleReader in a `PerPartitionFilterExec`. Neither side is
    //! exercised end-to-end without the URRE-inserting rule (a follow-up
    //! PR), so tests here cover the slot itself: roundtrip through
    //! `resolve_range_repartition_routing` → `range_repartition_routing()`,
    //! and preservation across `with_new_children`.
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::expressions::col;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};

    fn v_source() -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let memory =
            Arc::new(MemorySourceConfig::try_new(&[vec![]], schema, None).unwrap());
        Arc::new(DataSourceExec::new(memory))
    }

    fn v_routing_expr() -> Arc<dyn PhysicalExpr> {
        let schema = v_source().schema();
        col("v", schema.as_ref()).unwrap()
    }

    fn sample_routing() -> RangeRepartitionRouting {
        RangeRepartitionRouting {
            cuts: vec![10.0, 20.0, 30.0],
            routing_expr: v_routing_expr(),
        }
    }

    #[test]
    fn range_repartition_routing_unresolved_returns_none() {
        let exchange = ExchangeExec::new(v_source(), None, 42);
        assert!(exchange.range_repartition_routing().is_none());
    }

    #[test]
    fn resolve_range_repartition_routing_roundtrips() {
        let exchange = ExchangeExec::new(v_source(), None, 42);
        exchange.resolve_range_repartition_routing(sample_routing());
        let recovered = exchange
            .range_repartition_routing()
            .expect("routing must be Some after resolve");
        assert_eq!(recovered.cuts, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn resolve_range_repartition_routing_overwrites_prior_value() {
        // Idempotent-overwrite semantics match `set_coalesce`.
        let exchange = ExchangeExec::new(v_source(), None, 42);
        exchange.resolve_range_repartition_routing(sample_routing());
        exchange.resolve_range_repartition_routing(RangeRepartitionRouting {
            cuts: vec![100.0],
            routing_expr: v_routing_expr(),
        });
        let recovered = exchange.range_repartition_routing().unwrap();
        assert_eq!(recovered.cuts, vec![100.0], "second resolve wins");
    }

    /// `with_new_children` must carry the routing slot through: transform
    /// passes that rebuild the parent chain would otherwise silently drop
    /// range boundaries the scheduler already parked here.
    #[test]
    fn with_new_children_preserves_range_repartition_routing() {
        let partitioning = Some(Partitioning::UnknownPartitioning(4));
        let exchange = Arc::new(ExchangeExec::new(v_source(), partitioning, 42));
        exchange.resolve_range_repartition_routing(sample_routing());

        // Rebuild with a fresh (equivalent-schema) child.
        let rebuilt = exchange
            .clone()
            .with_new_children(vec![v_source()])
            .unwrap();
        let rebuilt_exchange = rebuilt
            .downcast_ref::<ExchangeExec>()
            .expect("with_new_children must return an ExchangeExec");
        let recovered = rebuilt_exchange
            .range_repartition_routing()
            .expect("routing must survive with_new_children");
        assert_eq!(recovered.cuts, vec![10.0, 20.0, 30.0]);
    }
}
