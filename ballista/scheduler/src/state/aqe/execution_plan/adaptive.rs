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

use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic::AtomicI64};

/// Wrapper execution plan used by the scheduler to represent an adaptive
/// DataFusion plan.
///
/// `AdaptiveDatafusionExec` is a lightweight wrapper that carries AQE-specific
/// mutable state.  Like `ExchangeExec`, this type implements `ExecutionPlan` for
/// integration but does not support direct execution.
#[derive(Debug)]
pub struct AdaptiveDatafusionExec {
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
                    "AdaptiveDatafusionExec: is_final={:?}, plan_id={}, stage_id={}, stage_resolved={}",
                    self.is_final,
                    self.plan_id,
                    self.stage_id()
                        .map(|stage_id| format!("{}", stage_id))
                        .unwrap_or_else(|| "pending".to_string()),
                    self.shuffle_created()
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
                )?;
                writeln!(f, "stage_resolved={}", self.shuffle_created())
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
