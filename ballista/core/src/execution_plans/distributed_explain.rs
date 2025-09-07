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

use std::any::Any;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use arrow::{array::StringBuilder, datatypes::SchemaRef, record_batch::RecordBatch};

use datafusion::common::display::{PlanType, StringifiedPlan};
use datafusion::common::{internal_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

use log::trace;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum BallistaPlanType {
    DataFusionPlanType(PlanType),
    DistributedPlan,
}

impl Display for BallistaPlanType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            BallistaPlanType::DataFusionPlanType(plan_type) => Display::fmt(plan_type, f),
            BallistaPlanType::DistributedPlan => write!(f, "distributed_plan"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct BallistaStringifiedPlan {
    pub plan_type: BallistaPlanType,
    pub plan: Arc<String>,
}

impl BallistaStringifiedPlan {
    pub fn new(plan_type: BallistaPlanType, plan: impl Into<String>) -> Self {
        BallistaStringifiedPlan {
            plan_type,
            plan: Arc::new(plan.into()),
        }
    }

    pub fn should_display(&self, verbose_mode: bool) -> bool {
        match &self.plan_type {
            BallistaPlanType::DataFusionPlanType(plan_type) => match plan_type {
                PlanType::FinalLogicalPlan
                | PlanType::FinalPhysicalPlan
                | PlanType::PhysicalPlanError => true,
                _ => verbose_mode,
            },
            BallistaPlanType::DistributedPlan => true,
        }
    }
}

impl From<&StringifiedPlan> for BallistaStringifiedPlan {
    fn from(df_plan: &StringifiedPlan) -> Self {
        Self {
            plan_type: BallistaPlanType::DataFusionPlanType(df_plan.plan_type.clone()),
            plan: Arc::clone(&df_plan.plan),
        }
    }
}

impl From<StringifiedPlan> for BallistaStringifiedPlan {
    fn from(df_plan: StringifiedPlan) -> Self {
        (&df_plan).into()
    }
}

#[derive(Debug, Clone)]
pub struct BallistaExplainExec {
    schema: SchemaRef,
    stringified_plans: Vec<BallistaStringifiedPlan>,
    verbose: bool,
    cache: PlanProperties,
}

impl BallistaExplainExec {
    pub fn new(
        schema: SchemaRef,
        df_stringified_plans: Vec<StringifiedPlan>,
        distributed_plan: impl Into<String>,
        verbose: bool,
    ) -> Self {
        let mut rows: Vec<BallistaStringifiedPlan> =
            df_stringified_plans.iter().map(Into::into).collect();

        rows.push(BallistaStringifiedPlan::new(
            BallistaPlanType::DistributedPlan,
            distributed_plan,
        ));

        let cache = Self::compute_properties(schema.clone());
        Self {
            schema,
            stringified_plans: rows,
            verbose,
            cache,
        }
    }

    pub fn from_stringified_plans(
        schema: SchemaRef,
        stringified_plans: Vec<BallistaStringifiedPlan>,
        verbose: bool,
    ) -> Self {
        let cache = Self::compute_properties(schema.clone());
        Self {
            schema,
            stringified_plans,
            verbose,
            cache,
        }
    }

    pub fn stringified_plans(&self) -> &[BallistaStringifiedPlan] {
        &self.stringified_plans
    }

    pub fn verbose(&self) -> bool {
        self.verbose
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for BallistaExplainExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "BallistaExplainExec")
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for BallistaExplainExec {
    fn name(&self) -> &'static str {
        "BallistaExplainExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start BallistaExplainExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        if 0 != partition {
            return internal_err!("BallistaExplainExec invalid partition {partition}");
        }

        let mut type_builder =
            StringBuilder::with_capacity(self.stringified_plans.len(), 1024);
        let mut plan_builder =
            StringBuilder::with_capacity(self.stringified_plans.len(), 1024);

        let plans_to_display = self
            .stringified_plans
            .iter()
            .filter(|r| r.should_display(self.verbose));
        for p in plans_to_display {
            type_builder.append_value(p.plan_type.to_string());
            plan_builder.append_value(p.plan.as_str());
        }

        let record_batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(type_builder.finish()),
                Arc::new(plan_builder.finish()),
            ],
        )?;

        trace!(
            "Before returning RecordBatchStream in BallistaExplainExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(vec![Ok(record_batch)]),
        )))
    }
}
