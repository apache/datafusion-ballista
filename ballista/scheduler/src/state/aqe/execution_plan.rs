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

//! Adaptive query execution (AQE) execution plan wrappers used by the
//! scheduler.
//!
//! This module provides lightweight ExecutionPlan implementations used by the
//! scheduler's adaptive query execution logic. They do not perform actual
//! execution themselves; instead they act as placeholders/markers for
//! shuffle/exchange boundaries and carry metadata the scheduler uses to
//! resolve shuffle locations, stage ids, and finalization state.
//!
//! Types:
//! - `ExchangeExec`: Represents an unresolved/resolved shuffle exchange. It
//!   stores the child plan, optional target partitioning, and (when
//!   available) the resolved `shuffle_partitions` describing where each
//!   partition's data lives.
//! - `AdaptiveDatafusionExec`: Wrapper used by AQE to mark a plan as
//!   adaptive and to carry mutable state such as `is_final` and resolved
//!   shuffle metadata.

use ballista_core::execution_plans::{stats_for_partition, stats_for_partitions};
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
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
use std::any::Any;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic::AtomicI64};

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
pub(crate) struct ExchangeExec {
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

    /// this disables stage from running even it would be suitable to run.
    ///
    /// the main reason for this property this is to allow rules to override
    /// stage execution logic, and to support making more complex
    /// stage run decisions.
    pub(crate) inactive_stage: bool,
}

impl ExchangeExec {
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
        )
    }

    pub fn new_with_details(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
        plan_id: usize,
        stage_id: Arc<AtomicI64>,
        stage_partitions: Arc<Mutex<Option<Vec<Vec<PartitionLocation>>>>>,
    ) -> Self {
        let plan_partitioning = match partitioning.as_ref() {
            Some(partitioning) => partitioning.clone(),
            None => input.output_partitioning().clone(),
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
            inactive_stage: false,
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

    /// sets the stage id running this exchange
    pub fn set_stage_id(&self, id: usize) {
        self.stage_id
            .store(id as i64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn stage_id(&self) -> Option<usize> {
        let stage_id = self.stage_id.load(std::sync::atomic::Ordering::Relaxed);

        if stage_id >= 0 {
            Some(stage_id as usize)
        } else {
            None
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
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
                    self.shuffle_partitions.lock().is_some()
                )
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
                writeln!(
                    f,
                    "stage_resolved={}",
                    self.shuffle_partitions.lock().is_some()
                )
            }
        }
    }
}

impl ExecutionPlan for ExchangeExec {
    fn name(&self) -> &str {
        "ExchangeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
            let mut new_exec = Self::new_with_details(
                children[0].clone(),
                self.partitioning.clone(),
                self.plan_id,
                self.stage_id.clone(),
                self.shuffle_partitions.clone(),
            );
            new_exec.inactive_stage = self.inactive_stage;

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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
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
                    stat_for_partition
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
                    Ok(stats_for_partitions)
                }
            }
            None => Ok(Statistics::new_unknown(&schema)),
        }
    }
}

/// Wrapper execution plan used by the scheduler to represent an adaptive
/// DataFusion plan.
///
/// `AdaptiveDatafusionExec` is a lightweight wrapper that carries AQE-specific
/// mutable state.  Like `ExchangeExec`, this type implements `ExecutionPlan` for
/// integration but does not support direct execution.
#[derive(Debug)]
pub(crate) struct AdaptiveDatafusionExec {
    input: Arc<dyn ExecutionPlan>,
    shuffle_partitions: Arc<Mutex<Option<Vec<Vec<PartitionLocation>>>>>,
    stage_id: Arc<AtomicI64>,
    plan_id: usize,
    pub(crate) is_final: Arc<AtomicBool>,
}

impl AdaptiveDatafusionExec {
    pub fn new(plan_id: usize, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            is_final: AtomicBool::new(false).into(),
            plan_id,
            input,
            stage_id: Arc::new(AtomicI64::new(-1)),
            shuffle_partitions: Arc::new(Mutex::new(None)),
        }
    }

    pub fn shuffle_created(&self) -> bool {
        self.shuffle_partitions.lock().is_some()
    }

    /// Changes shuffle from unresolved to resolved
    /// providing list of available partitions
    ///
    pub fn resolve_shuffle_partitions(&self, partitions: Vec<Vec<PartitionLocation>>) {
        self.shuffle_partitions.lock().replace(partitions);
    }

    /// sets the stage id running this exchange
    pub fn set_stage_id(&self, id: usize) {
        self.stage_id
            .store(id as i64, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn stage_id(&self) -> Option<usize> {
        let stage_id = self.stage_id.load(std::sync::atomic::Ordering::Relaxed);

        if stage_id >= 0 {
            Some(stage_id as usize)
        } else {
            None
        }
    }

    pub fn set_final_plan(&self) {
        self.is_final
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for AdaptiveDatafusionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "AdaptiveDatafusionExec: is_final={:?}, plan_id={}, stage_id={}",
                    self.is_final,
                    self.plan_id,
                    self.stage_id()
                        .map(|stage_id| format!("{}", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "is_final={:?}", self.is_final)?;
                writeln!(f, "plan_id={}", self.plan_id)?;
                writeln!(
                    f,
                    "stage_id={}",
                    self.stage_id()
                        .map(|stage_id| format!("({})", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                )
            }
        }
    }
}

impl ExecutionPlan for AdaptiveDatafusionExec {
    fn name(&self) -> &str {
        "AdaptiveDatafusionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            let new_exec = Self {
                is_final: self.is_final.clone(),
                plan_id: self.plan_id,
                input: children[0].clone(),
                stage_id: self.stage_id.clone(),
                shuffle_partitions: Arc::clone(&self.shuffle_partitions),
            };

            Ok(Arc::new(new_exec))
        } else {
            Err(DataFusionError::Plan(
                "AdaptiveDatafusionExec expects single child".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Plan(
            "AdaptiveDatafusionExec does not support execution".to_owned(),
        ))
    }
}
