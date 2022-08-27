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

//! Implementation of ballista physical plan display with metrics. See
//! [`crate::physical_plan::displayable`] for examples of how to
//! format

use ballista_core::utils::collect_plan_metrics;
use datafusion::logical_plan::{StringifiedPlan, ToStringifiedPlan};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    accept, DisplayFormatType, ExecutionPlan, ExecutionPlanVisitor,
};
use log::{error, info};
use std::fmt;

pub fn print_stage_metrics(
    job_id: &str,
    stage_id: usize,
    plan: &dyn ExecutionPlan,
    stage_metrics: &[MetricsSet],
) {
    // The plan_metrics collected here is a snapshot clone from the plan metrics.
    // They are all empty now and need to combine with the stage metrics in the ExecutionStages
    let mut plan_metrics = collect_plan_metrics(plan);
    if plan_metrics.len() == stage_metrics.len() {
        plan_metrics.iter_mut().zip(stage_metrics).for_each(
            |(plan_metric, stage_metric)| {
                stage_metric
                    .iter()
                    .for_each(|s| plan_metric.push(s.clone()));
            },
        );

        info!(
            "=== [{}/{}] Stage finished, physical plan with metrics ===\n{}\n",
            job_id,
            stage_id,
            DisplayableBallistaExecutionPlan::new(plan, &plan_metrics).indent()
        );
    } else {
        error!("Fail to combine stage metrics to plan for stage [{}/{}],  plan metrics array size {} does not equal
                to the stage metrics array size {}", job_id, stage_id, plan_metrics.len(), stage_metrics.len());
    }
}

/// Wraps an `ExecutionPlan` to display this plan with metrics collected/aggregated.
/// The metrics must be collected in the same order as how we visit and display the plan.
pub struct DisplayableBallistaExecutionPlan<'a> {
    inner: &'a dyn ExecutionPlan,
    metrics: &'a Vec<MetricsSet>,
}

impl<'a> DisplayableBallistaExecutionPlan<'a> {
    /// Create a wrapper around an [`'ExecutionPlan'] which can be
    /// pretty printed with aggregated metrics.
    pub fn new(inner: &'a dyn ExecutionPlan, metrics: &'a Vec<MetricsSet>) -> Self {
        Self { inner, metrics }
    }

    /// Return a `format`able structure that produces a single line
    /// per node.
    ///
    /// ```text
    /// ProjectionExec: expr=[a]
    ///   CoalesceBatchesExec: target_batch_size=4096
    ///     FilterExec: a < 5
    ///       RepartitionExec: partitioning=RoundRobinBatch(16)
    ///         CsvExec: source=...",
    /// ```
    pub fn indent(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            plan: &'a dyn ExecutionPlan,
            metrics: &'a Vec<MetricsSet>,
        }
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let t = DisplayFormatType::Default;
                let mut visitor = IndentVisitor {
                    t,
                    f,
                    indent: 0,
                    metrics: self.metrics,
                    metric_index: 0,
                };
                accept(self.plan, &mut visitor)
            }
        }
        Wrapper {
            plan: self.inner,
            metrics: self.metrics,
        }
    }
}

/// Formats plans with a single line per node.
struct IndentVisitor<'a, 'b> {
    /// How to format each node
    t: DisplayFormatType,
    /// Write to this formatter
    f: &'a mut fmt::Formatter<'b>,
    /// Indent size
    indent: usize,
    /// The metrics along with the plan
    metrics: &'a Vec<MetricsSet>,
    /// The metric index
    metric_index: usize,
}

impl<'a, 'b> ExecutionPlanVisitor for IndentVisitor<'a, 'b> {
    type Error = fmt::Error;
    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        plan.fmt_as(self.t, self.f)?;
        if let Some(metrics) = self.metrics.get(self.metric_index) {
            let metrics = metrics
                .aggregate_by_partition()
                .sorted_for_display()
                .timestamps_removed();
            write!(self.f, ", metrics=[{}]", metrics)?;
        } else {
            write!(self.f, ", metrics=[]")?;
        }
        writeln!(self.f)?;
        self.indent += 1;
        self.metric_index += 1;
        Ok(true)
    }

    fn post_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        self.indent -= 1;
        Ok(true)
    }
}

impl<'a> ToStringifiedPlan for DisplayableBallistaExecutionPlan<'a> {
    fn to_stringified(
        &self,
        plan_type: datafusion::logical_plan::PlanType,
    ) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.indent().to_string())
    }
}
