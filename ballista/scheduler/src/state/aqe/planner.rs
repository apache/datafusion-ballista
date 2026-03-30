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
use crate::state::aqe::adapter::BallistaAdapter;
use crate::state::aqe::execution_plan::{AdaptiveDatafusionExec, ExchangeExec};
use crate::state::aqe::optimizer_rule::{
    DistributedExchangeRule, EliminateCooperativeExecRule, EliminateEmptyExchangeRule,
    PropagateEmptyExecRule, WarnOnDuplicateExecRule,
};

use crate::state::execution_stage::StageOutput;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::common;
use datafusion::common::{HashMap, exec_err};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::SessionConfig;
use log::{debug, warn};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::time::Instant;

type PhysicalOptimizerRuleRef =
    Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>;

type RunnableStages = Option<Vec<AdaptiveStageInfo>>;
type CancellableStageIds = HashSet<usize>;

/// The `AdaptivePlanner` struct is responsible for managing the execution of distributed
/// physical plans in an adaptive manner. It handles the optimization, selection, and
/// cancellation of stages within a distributed query execution plan.
#[derive(Clone)]
pub struct AdaptivePlanner {
    /// Generates the next stage ID.
    /// Stage IDs are incremental and unique for each job.
    stage_id_generator: usize,
    /// The session state needed for optimizer calls.
    /// This also freezes the session configuration for a given job.
    session_state: SessionState,
    /// Physical planner used for this job
    physical_planner: Arc<DefaultPhysicalPlanner>,
    /// physical plan representing given job
    plan: Arc<dyn ExecutionPlan>,
    /// caches current runnable stages
    runnable_stage_cache: HashMap<usize, Arc<dyn ExecutionPlan>>,
    /// Optimizer max passes before it gives up
    // to be configured from a configuration
    max_passes: usize,
    /// job name
    job_name: String,

    runnable_stage_output: HashMap<usize, StageOutput>,
}

impl Debug for AdaptivePlanner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptivePlanner")
            .field("job_name", &self.job_name)
            .finish()
    }
}

impl AdaptivePlanner {
    /// Creates a new `AdaptivePlanner` with the specified physical optimizer rules.
    ///
    /// # Arguments
    /// * `plan` - The physical execution plan for the job.
    /// * `session_config` - The session configuration for the job.
    /// * `physical_optimizer_rules` - A list of physical optimizer rules to apply.
    /// * `job_name` - The name of the job.
    ///
    /// # Returns
    /// A new instance of `AdaptivePlanner` or an error if the initialization fails.
    pub fn try_new_with_optimizers(
        plan: Arc<dyn ExecutionPlan>,
        session_config: &SessionConfig,
        physical_optimizer_rules: Vec<PhysicalOptimizerRuleRef>,
        job_name: String,
    ) -> common::Result<Self> {
        let max_passes = session_config.adaptive_query_planner_max_passes();

        let session_state =
            Self::create_session_state(session_config, physical_optimizer_rules);
        let planner = DefaultPhysicalPlanner::default();
        let plan = planner.optimize_physical_plan(plan, &session_state, |_, _| {})?;

        Ok(Self {
            stage_id_generator: 0,
            session_state,
            physical_planner: planner.into(),
            plan,
            runnable_stage_cache: HashMap::new(),
            max_passes,
            job_name,
            runnable_stage_output: HashMap::new(),
        })
    }
    /// Creates a new `AdaptivePlanner` with default physical optimizer rules.
    ///
    /// # Arguments
    /// * `session_config` - The session configuration for the job.
    /// * `plan` - The physical execution plan for the job.
    /// * `job_name` - The name of the job.
    ///
    /// # Returns
    /// A new instance of `AdaptivePlanner` or an error if the initialization fails.
    pub fn try_new(
        session_config: &SessionConfig,
        plan: Arc<dyn ExecutionPlan>,
        job_name: String,
    ) -> common::Result<Self> {
        Self::try_new_with_optimizers(
            plan,
            session_config,
            Self::default_optimizers(),
            job_name,
        )
    }
    /// Cancels a stage by its ID.
    ///
    /// # Arguments
    /// * `stage_id` - The ID of the stage to cancel.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub fn cancel_stage(&mut self, stage_id: usize) -> common::Result<()> {
        // TODO consider marking cancelled stage
        let _ = self.runnable_stage_cache.remove(&stage_id);
        Ok(())
    }
    /// Resolves a stage by its ID and updates its partitions.
    ///
    /// # Arguments
    /// * `stage_id` - The ID of the stage to resolve.
    /// * `partitions` - The resolved partitions for the stage.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    pub(super) fn finalise_stage_internal(
        &mut self,
        stage_id: usize,
        partitions: Vec<Vec<PartitionLocation>>,
    ) -> common::Result<()> {
        // stage has finished, stage partitions should be resolved,
        // and it has to be removed from active stages cache.
        // Removing stage is important as this will help us to
        // identify stages to cancel.

        match self
            .runnable_stage_cache
            .remove(&stage_id)
            .as_ref()
            .map(|stage| {
                (
                    stage.as_any().downcast_ref::<ExchangeExec>(),
                    stage.as_any().downcast_ref::<AdaptiveDatafusionExec>(),
                )
            }) {
            Some((Some(stage), None)) => {
                stage.resolve_shuffle_partitions(partitions);
                self.replan_stages()?;
                Ok(())
            }
            Some((None, Some(stage))) => {
                stage.resolve_shuffle_partitions(partitions);
                self.replan_stages()?;
                Ok(())
            }
            _ => exec_err!(
                "cant find stage_id {} or stage root node is incorrect ",
                stage_id
            ),
        }
    }
    /// update stage outputs for a given stage id.
    /// this method to be called for each task completed,
    /// in order to track exchange location
    pub fn update_exchange_locations(
        &mut self,
        stage_id: usize,
        partitions: Vec<PartitionLocation>,
    ) -> common::Result<()> {
        if let Some(stage_output) = self.runnable_stage_output.get_mut(&stage_id) {
            for partition in partitions {
                stage_output.add_partition(partition)
            }
            Ok(())
        } else {
            exec_err!("Can't find active stage to update stage outputs")
        }
    }

    /// Once all tasks has been completed marks stage as resolved
    /// and returns partition allocations
    pub fn finalise_stage(
        &mut self,
        stage_id: usize,
    ) -> common::Result<Vec<Vec<PartitionLocation>>> {
        let output_partition_count = self
            .runnable_stage_cache
            .get(&stage_id)
            .ok_or(datafusion::error::DataFusionError::Execution(
                "Can't find active cache resolve".into(),
            ))?
            .output_partitioning()
            .partition_count();

        let stage_output = self
            .runnable_stage_output
            .remove(&stage_id)
            .ok_or(datafusion::error::DataFusionError::Execution(
                "Can't find active stage to update resolve".into(),
            ))?
            .partition_locations(output_partition_count);

        self.finalise_stage_internal(stage_id, stage_output.clone())?;

        Ok(stage_output)
    }

    /// Replans the stages by applying physical optimizations.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    fn replan_stages(&mut self) -> common::Result<()> {
        let interval = Instant::now();
        let mut plan = self.plan.clone();

        debug!(
            "Distributed physical plan (before optimization):\n{}\n",
            displayable(plan.as_ref()).indent(false)
        );

        for pass in 0..self.max_passes {
            plan = self.physical_planner.optimize_physical_plan(
                plan.clone(),
                &self.session_state,
                |_, _| {},
            )?;

            if DistributedExchangeRule::default().plan_invalid(plan.clone())? {
                if pass >= self.max_passes - 1 {
                    warn!("plan needs another distributed optimizer pass");
                    exec_err!("plan needs another distributed optimizer pass")?
                }
                debug!("plan does not look correct correct");
            } else {
                debug!("plan looks correct correct");
                break;
            }
        }

        debug!(
            "Distributed physical plan (after optimization):\n{}\n",
            displayable(plan.as_ref()).indent(false)
        );

        self.plan = plan;

        debug!(
            "Total time taken for stage replan: {:?}",
            interval.elapsed()
        );

        Ok(())
    }
    /// Retrieves the runnable stages from the current plan.
    ///
    /// # Returns
    /// A `Result` containing the runnable stages or `None` if no stages are runnable.
    #[cfg(test)]
    pub(crate) fn runnable_stages(&mut self) -> common::Result<RunnableStages> {
        let (runnable_stages, _) = self.actionable_stages()?;

        Ok(runnable_stages)
    }

    /// Returns a tuple of `(RunnableStages, CancellableStageIds)`:
    /// - `RunnableStages`: A list of executable stages, or `None` if no more stages can run
    /// - `CancellableStageIds`: Stage IDs that should be canceled
    ///
    /// Stages are derived from the current physical plan on each call.
    pub fn actionable_stages(
        &mut self,
    ) -> common::Result<(RunnableStages, CancellableStageIds)> {
        match self.identify_runnable_stages()? {
            None => {
                let stages_to_cancel = self
                    .runnable_stage_cache
                    .keys()
                    .cloned()
                    .collect::<HashSet<_>>();
                Ok((None, stages_to_cancel))
            }
            Some(stages) => {
                let (stage_ids, shuffle_writers) = stages
                    .into_iter()
                    .map(|plan| {
                        // TODO: we need to find input stages for given stage
                        //       thus result should change
                        BallistaAdapter::adapt_to_ballista(plan, self.job_name.as_str())
                            .map(|w| (w.plan.stage_id(), w))
                    })
                    .collect::<common::Result<(HashSet<_>, Vec<_>)>>()?;

                let stages_to_cancel = self
                    .runnable_stage_cache
                    .keys()
                    .filter(|s| !stage_ids.contains(s))
                    .cloned()
                    .collect::<HashSet<_>>();

                Ok((Some(shuffle_writers), stages_to_cancel))
            }
        }
    }
    /// Finds and returns the runnable stages from the current plan.
    ///
    /// # Returns
    /// A `Result` containing the runnable stages or `None` if no stages are runnable.
    pub(crate) fn identify_runnable_stages(
        &mut self,
    ) -> common::Result<Option<Vec<Arc<dyn ExecutionPlan>>>> {
        let mut runnable_stages = Vec::new();

        Self::find_runnable_exchanges(&self.plan, &mut runnable_stages);
        if !runnable_stages.is_empty() {
            let mut runnable = Vec::new();
            for exec in runnable_stages.into_iter() {
                match exec.as_any().downcast_ref::<ExchangeExec>() {
                    Some(exchange) if exchange.inactive_stage => continue,
                    Some(exchange) if exchange.stage_id().is_none() => {
                        exchange.set_stage_id(self.stage_id_generator);
                        // keeping stage in stage cache
                        // so we can process updates
                        self.runnable_stage_cache
                            .insert(self.stage_id_generator, exec.clone());
                        // keeping track of stage outputs
                        self.runnable_stage_output
                            .insert(self.stage_id_generator, Default::default());
                        // incrementing stage id
                        self.stage_id_generator += 1;
                        runnable.push(exec);
                    }
                    Some(_) => {
                        runnable.push(exec);
                    }
                    None => exec_err!("It is not a exchange")?,
                }
            }

            Ok(Some(runnable))
        } else if let Some(root) =
            self.plan.as_any().downcast_ref::<AdaptiveDatafusionExec>()
        {
            // shuffle writer has finished
            // there is no more runnable stages
            if root.shuffle_created() {
                Ok(None)
            } else if root.stage_id().is_some() {
                // root schema has stage id attached
                // previously
                Ok(Some(vec![self.plan.clone()]))
            } else {
                root.set_stage_id(self.stage_id_generator);
                root.set_final_plan();
                // keeping stage in stage cache
                // so we can process updates
                self.runnable_stage_cache
                    .insert(self.stage_id_generator, self.plan.clone());
                // keeping track of stage outputs
                self.runnable_stage_output
                    .insert(self.stage_id_generator, Default::default());
                // incrementing stage id
                self.stage_id_generator += 1;

                Ok(Some(vec![self.plan.clone()]))
            }
        } else {
            exec_err!("Plan root is not a AdaptiveDatafusionExec")
        }
    }
    /// Inspects and retrieves the IDs of runnable stages for testing purposes.
    ///
    /// # Returns
    /// A `Result` containing a set of runnable stage IDs.
    #[cfg(test)]
    pub(crate) fn inspect_runnable_stages(
        &self,
    ) -> common::Result<std::collections::HashSet<usize>> {
        let mut runnable_stages = Vec::new();
        Self::find_runnable_exchanges(&self.plan, &mut runnable_stages);

        runnable_stages
            .into_iter()
            .map(|exec| {
                exec.as_any()
                    .downcast_ref::<ExchangeExec>()
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Plan(
                            "ExchangeExec expected".into(),
                        )
                    })
                    .map(|e| e.stage_id().unwrap())
            })
            .collect()
    }
    /// Retrieves the current physical plan.
    ///
    /// # Returns
    /// A reference to the current physical plan.
    #[cfg(test)]
    pub fn current_plan(&self) -> &dyn ExecutionPlan {
        self.plan.as_ref()
    }
    /// Returns the default set of physical optimizer rules.
    ///
    /// # Returns
    /// A vector of default physical optimizer rules.
    fn default_optimizers() -> Vec<PhysicalOptimizerRuleRef> {
        let mut physical_optimizers: Vec<PhysicalOptimizerRuleRef> = vec![
            // TODO: do we keep it here or make it last
            Arc::new(DistributedExchangeRule::default()),
            Arc::new(EliminateEmptyExchangeRule::default()),
        ];

        let default_optimizers = PhysicalOptimizer::new();
        physical_optimizers.extend(default_optimizers.rules.iter().cloned());
        physical_optimizers.push(Arc::new(PropagateEmptyExecRule::default()));
        // this rule is not required anymore
        // physical_optimizers.push(Arc::new(EliminateRoundRobbinRule::default()));
        physical_optimizers.push(Arc::new(EliminateCooperativeExecRule::default()));
        // we should remove it at the later stage
        // this is just temporary to detect possible duplicate
        // execs
        physical_optimizers.push(Arc::new(WarnOnDuplicateExecRule::default()));
        // physical_optimizers.push(Arc::new(DistributedExchangeRule::default()));

        physical_optimizers
    }
    /// Creates a session state with the given configuration and optimizer rules.
    ///
    /// # Arguments
    /// * `session_config` - The session configuration.
    /// * `physical_optimizers` - A list of physical optimizer rules.
    ///
    /// # Returns
    /// A new `SessionState` instance.
    fn create_session_state(
        session_config: &SessionConfig,
        physical_optimizers: Vec<PhysicalOptimizerRuleRef>,
    ) -> SessionState {
        SessionStateBuilder::new_with_default_features()
            .with_physical_optimizer_rules(physical_optimizers)
            .with_config(session_config.clone())
            .build()
    }
    /// Recursively finds runnable exchanges in the execution plan.
    ///
    /// # Arguments
    /// * `node` - The current execution plan node.
    /// * `runnable_stages` - A mutable reference to the list of runnable stages.
    ///
    /// # Returns
    /// A boolean indicating whether the current node or its children are runnable.
    fn find_runnable_exchanges(
        node: &Arc<dyn ExecutionPlan>,
        runnable_stages: &mut Vec<Arc<dyn ExecutionPlan>>,
    ) -> bool {
        if let Some(exchange) = node.as_any().downcast_ref::<ExchangeExec>()
            && exchange.shuffle_created()
        {
            // we found exchange which has partitions resolved or this stage is not
            // there is no need to progress any further as
            // all runnable children has been run

            false
        } else if let Some(exchange) = node.as_any().downcast_ref::<ExchangeExec>()
            && !exchange.shuffle_created()
        {
            // we found exchange which has not been resolved (run)
            // it is candidate to run if none of children is runnable

            let has_runnable_children = node
                .children()
                .into_iter()
                .any(|child| Self::find_runnable_exchanges(child, runnable_stages));

            // this exchange has NO runnable children
            // thus it can RUN
            if !has_runnable_children {
                runnable_stages.push(node.clone());
            }

            // if it had no runnable children now it is
            // runnable and has to indicate to parent
            true
        } else {
            #[allow(clippy::unnecessary_fold)]
            node.children()
                .into_iter()
                .map(|child| Self::find_runnable_exchanges(child, runnable_stages))
                .fold(false, |acc, s| acc || s) // NOTE: this one can not be changed to `any(|s| s)`
            // it has to iterate through all of them
        }
    }
}

/// Wraps stage plan with addition of references to previous stages
pub(crate) struct AdaptiveStageInfo {
    pub(crate) plan: Arc<ShuffleWriterExec>,
    #[allow(dead_code)] // TODO: still not sure if this is needed
    pub(crate) inputs: Vec<usize>,
}
