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

//! Reading a built `DataFrame::cache()` back into a query.
//!
//! On a cache hit the scheduler replaces the `BallistaCacheNode` with a
//! [`CacheReadNode`](crate::cache::CacheReadNode) leaf, which
//! [`CacheReadPlanner`](crate::cache::CacheReadPlanner) lowers to a
//! [`ShuffleReaderExec`](crate::execution_plans::ShuffleReaderExec) over the
//! cache's shuffle output. The node is scheduler-internal and never serialized.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::TreeNode;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};

use crate::execution_plans::ShuffleReaderExec;
use crate::serde::scheduler::PartitionLocation;

/// First stage id reserved for inputs produced outside a job, such as a cache
/// read, so they never collide with the job's own stages (both distributed
/// planners assert they stay below it). It keys the input in the reading job
/// only; executors fetch by its locations.
pub const EXTERNAL_STAGE_ID_BASE: usize = 1 << 30;

/// Whether `stage_id` is an input produced outside the job.
pub fn is_external_stage_id(stage_id: usize) -> bool {
    stage_id >= EXTERNAL_STAGE_ID_BASE
}

/// A logical leaf reading a built cache's shuffle output, under external stage
/// id `stage_id` (see [`EXTERNAL_STAGE_ID_BASE`]). It keeps the cached plan's
/// schema exactly. Equality, ordering and hashing use `cache_id` and
/// `stage_id` only.
#[derive(Clone, Debug)]
pub struct CacheReadNode {
    cache_id: String,
    stage_id: usize,
    schema: DFSchemaRef,
    /// Shuffle locations by output partition.
    partitions: Vec<Vec<PartitionLocation>>,
}

impl CacheReadNode {
    /// A read of `partitions` with the cached `schema`.
    pub fn new(
        cache_id: impl Into<String>,
        stage_id: usize,
        schema: DFSchemaRef,
        partitions: Vec<Vec<PartitionLocation>>,
    ) -> Self {
        Self {
            cache_id: cache_id.into(),
            stage_id,
            schema,
            partitions,
        }
    }

    fn identity(&self) -> (&str, usize) {
        (self.cache_id.as_str(), self.stage_id)
    }
}

impl PartialEq for CacheReadNode {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}
impl Eq for CacheReadNode {}
impl PartialOrd for CacheReadNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.identity().cmp(&other.identity()))
    }
}
impl Hash for CacheReadNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cache_id.hash(state);
        self.stage_id.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for CacheReadNode {
    fn name(&self) -> &str {
        "CacheReadNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CacheRead: cache_id={}, stage_id={}, partitions={}",
            self.cache_id,
            self.stage_id,
            self.partitions.len()
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        // Leaf node: no inputs or expressions to swap.
        Ok(self.clone())
    }
}

/// Lowers [`CacheReadNode`] to a [`ShuffleReaderExec`]; `None` for any other
/// node, so it composes with other extension planners.
#[derive(Debug, Default)]
pub struct CacheReadPlanner;

#[async_trait]
impl ExtensionPlanner for CacheReadPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<CacheReadNode>() else {
            return Ok(None);
        };
        let partitioning = Partitioning::UnknownPartitioning(node.partitions.len());
        let reader = ShuffleReaderExec::try_new(
            node.stage_id,
            node.partitions.clone(),
            node.schema.inner().clone(),
            partitioning,
        )?;
        Ok(Some(Arc::new(reader)))
    }
}

/// DataFusion's default physical planner plus [`CacheReadPlanner`]; a plan
/// without a [`CacheReadNode`] is planned exactly as by the default.
pub struct CacheReadQueryPlanner {
    inner: DefaultPhysicalPlanner,
}

impl CacheReadQueryPlanner {
    /// A planner whose only extension is the cache read.
    pub fn new() -> Self {
        Self::with_extension_planners(vec![])
    }

    /// A planner with the cache read alongside the caller's extension planners.
    pub fn with_extension_planners(
        extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        let mut planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(CacheReadPlanner)];
        planners.extend(extension_planners);
        Self {
            inner: DefaultPhysicalPlanner::with_extension_planners(planners),
        }
    }
}

impl Default for CacheReadQueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CacheReadQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CacheReadQueryPlanner")
    }
}

#[async_trait]
impl QueryPlanner for CacheReadQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Whether `plan` holds an extension node of type `T`. Walks inputs only, as
/// plans reach the scheduler optimized, with subqueries turned into joins.
pub fn contains_extension<T: 'static>(plan: &LogicalPlan) -> bool {
    plan.exists(|node| {
        Ok(matches!(node, LogicalPlan::Extension(ext) if ext.node.as_any().is::<T>()))
    })
    .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extension::SessionStateBuilderExt;
    use datafusion::execution::SessionStateBuilder;

    /// Stands in for a query planner a user installed.
    #[derive(Debug)]
    struct UserPlanner;

    #[async_trait]
    impl QueryPlanner for UserPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            session_state: &dyn Session,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            DefaultPhysicalPlanner::default()
                .create_physical_plan(logical_plan, session_state)
                .await
        }
    }

    /// A planner the builder's author set is kept.
    #[test]
    fn a_planner_already_set_is_kept() {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(UserPlanner))
            .with_cache_read_planner()
            .build();
        assert!(format!("{:?}", state.query_planner()).starts_with("UserPlanner"));
    }
}
