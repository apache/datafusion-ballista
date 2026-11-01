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

//! Scheduler-side `DataFrame::cache()` plan rewriting: cache nodes become reads
//! of their pinned shuffle output (`resolve_cache_reads`). Claiming builds is
//! the cache linker's.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use ballista_core::JobId;
use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::extension::BallistaCacheNode;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableType, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::future::BoxFuture;

use crate::cluster::CacheState;
use crate::state::cache_registry::{CacheKey, MaterializedCache};
use crate::state::execution_stage::StageOutput;

/// First stage id reserved for inputs produced outside a job, such as a cache
/// read, so they never collide with the job's own stages (debug builds assert
/// that both distributed planners stay below it). It keys the input in the
/// reading job only; executors fetch by its locations.
pub(crate) const EXTERNAL_STAGE_ID_BASE: usize = 1 << 30;

/// Whether `stage_id` is an input produced outside the job.
pub(crate) fn is_external_stage_id(stage_id: usize) -> bool {
    stage_id >= EXTERNAL_STAGE_ID_BASE
}

/// Whether `plan` holds a `DataFrame::cache()` node. Walks inputs only, as plans
/// reach the scheduler optimized, with subqueries turned into joins.
pub(crate) fn contains_cache(plan: &LogicalPlan) -> bool {
    plan.exists(|node| Ok(cache_node(node).is_some()))
        .unwrap_or(false)
}

/// In the failure of a job that lost cache data mid-query. Rebuild-and-resume
/// is future work (see the design doc); the caches the job read are dropped,
/// so a retry rebuilds them.
pub(crate) const CACHE_DATA_LOST: &str = "lost DataFrame::cache() data";

/// Replaces every `BallistaCacheNode` with its subplan, for when an operator
/// disables caching.
pub(crate) fn strip_caches(plan: &LogicalPlan) -> BallistaResult<LogicalPlan> {
    let stripped = plan.clone().transform_up(|node| {
        Ok(match cache_node(&node) {
            Some(cache) => Transformed::yes(cache.inputs()[0].clone()),
            None => Transformed::no(node),
        })
    })?;
    Ok(stripped.data)
}

/// Groups a job's output locations by partition, as `ShuffleReaderExec` and
/// stage inputs expect. Trailing partitions without data are left out, as
/// reading them would only add empty tasks.
pub(crate) fn shape_locations(
    flat: Vec<PartitionLocation>,
) -> Vec<Vec<PartitionLocation>> {
    let mut output = StageOutput::new();
    let mut partitions = 0;
    for location in flat {
        partitions = partitions.max(location.partition_id.partition_id + 1);
        output.add_partition(location);
    }
    output.partition_locations(partitions)
}

/// `plan` as a `DataFrame::cache()` node, if it is one.
pub(crate) fn cache_node(plan: &LogicalPlan) -> Option<&BallistaCacheNode> {
    match plan {
        LogicalPlan::Extension(ext) => ext.node.as_any().downcast_ref(),
        _ => None,
    }
}

/// Whether every location is on a live executor.
pub(crate) fn all_locations_live(
    locations: &[Vec<PartitionLocation>],
    alive_executors: &HashSet<String>,
) -> bool {
    locations
        .iter()
        .flatten()
        .all(|loc| alive_executors.contains(&loc.executor_meta.id))
}

/// A cache read in a job's plan: the cache, and the build job whose output was
/// read, to drop that build if the job loses its data.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedRead {
    pub(crate) key: CacheKey,
    pub(crate) materializing_job: JobId,
}

/// What [`resolve_cache_reads`] does with a cache not built on live executors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnMiss {
    /// Fail: a released job's caches were built, so it was lost since.
    Fail,
    /// Keep its subplan, for `EXPLAIN`.
    Subplan,
}

/// Rewrites each `BallistaCacheNode` in `plan` into a read of its build output,
/// appending it to `reads`; the n-th read gets external stage id
/// `EXTERNAL_STAGE_ID_BASE + n`. Builds nothing; an unreadable cache is handled
/// as `on_miss` says. Pre-order, so a read hides its subplan.
pub(crate) async fn resolve_cache_reads(
    plan: &LogicalPlan,
    cache_state: &dyn CacheState,
    alive_executors: &HashSet<String>,
    reads: &mut Vec<ResolvedRead>,
    on_miss: OnMiss,
) -> BallistaResult<LogicalPlan> {
    Ok(resolve(plan, cache_state, alive_executors, reads, on_miss)
        .await?
        .unwrap_or_else(|| plan.clone()))
}

/// `plan` resolved, or `None` if unchanged, so only nodes above a cache are
/// rebuilt.
fn resolve<'a>(
    plan: &'a LogicalPlan,
    cache_state: &'a dyn CacheState,
    alive_executors: &'a HashSet<String>,
    reads: &'a mut Vec<ResolvedRead>,
    on_miss: OnMiss,
) -> BoxFuture<'a, BallistaResult<Option<LogicalPlan>>> {
    Box::pin(async move {
        if let Some(node) = cache_node(plan) {
            let key = CacheKey::new(node.session_id(), node.cache_id());
            if let Some(cache) = cache_state.lookup(&key).await?
                && all_locations_live(&cache.locations, alive_executors)
            {
                let stage_id = EXTERNAL_STAGE_ID_BASE + reads.len();
                reads.push(ResolvedRead {
                    key,
                    materializing_job: cache.materializing_job.clone(),
                });
                return Ok(Some(cache_read(node, stage_id, cache)?));
            }
            return match on_miss {
                OnMiss::Fail => Err(BallistaError::General(format!(
                    "cache {} was invalidated before it could be read; retry the query",
                    node.cache_id()
                ))),
                OnMiss::Subplan => {
                    let input = node.inputs()[0];
                    let resolved =
                        resolve(input, cache_state, alive_executors, reads, on_miss)
                            .await?;
                    Ok(Some(resolved.unwrap_or_else(|| input.clone())))
                }
            };
        }

        let inputs = plan.inputs();
        let mut resolved = Vec::with_capacity(inputs.len());
        let mut changed = false;
        for input in inputs {
            let input_resolved =
                resolve(input, cache_state, alive_executors, reads, on_miss).await?;
            changed |= input_resolved.is_some();
            resolved.push(input_resolved.unwrap_or_else(|| input.clone()));
        }
        if !changed {
            return Ok(None);
        }
        Ok(Some(plan.with_new_exprs(plan.expressions(), resolved)?))
    })
}

/// `cache` read back as `node`'s schema: a scan of its shuffle output under
/// positional names, then a projection back to the cached qualifiers and names,
/// typed as the build job wrote them.
fn cache_read(
    node: &BallistaCacheNode,
    stage_id: usize,
    cache: MaterializedCache,
) -> BallistaResult<LogicalPlan> {
    let cached = node.schema();
    if cached.fields().len() != cache.schema.fields().len() {
        return Err(BallistaError::General(format!(
            "cache {} was written with {} columns, but its plan has {}",
            node.cache_id(),
            cache.schema.fields().len(),
            cached.fields().len()
        )));
    }
    let schema: SchemaRef = Arc::new(Schema::new(
        cache
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| field.as_ref().clone().with_name(format!("c{i}")))
            .collect::<Vec<_>>(),
    ));
    let table = format!("ballista_cache_read_{}", stage_id - EXTERNAL_STAGE_ID_BASE);
    let columns = cached
        .iter()
        .zip(schema.fields())
        .map(|((qualifier, field), positional)| {
            Expr::Column(Column::new(
                Some(TableReference::bare(table.as_str())),
                positional.name(),
            ))
            .alias_qualified(qualifier.cloned(), field.name())
        })
        .collect::<Vec<_>>();
    let read = CacheReadTable {
        stage_id,
        written: cache.schema,
        schema,
        locations: cache.locations,
    };
    Ok(LogicalPlanBuilder::scan(
        table.as_str(),
        provider_as_source(Arc::new(read)),
        None,
    )?
    .project(columns)?
    .build()?)
}

/// A built cache's shuffle output as a table. Any query planner plans a table
/// scan, so a session needs nothing installed to read caches.
#[derive(Debug)]
struct CacheReadTable {
    /// The read's external stage id (see [`EXTERNAL_STAGE_ID_BASE`]).
    stage_id: usize,
    /// As the build job wrote it, which the shuffle batches match.
    written: SchemaRef,
    /// `written` with the names `c0`, `c1`, …: a table has one qualifier and
    /// unique names, which a cached frame's columns need not have.
    schema: SchemaRef,
    /// Shuffle locations by output partition.
    locations: Vec<Vec<PartitionLocation>>,
}

#[async_trait]
impl TableProvider for CacheReadTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let reader = ShuffleReaderExec::try_new(
            self.stage_id,
            self.locations.clone(),
            self.written.clone(),
            Partitioning::UnknownPartitioning(self.locations.len()),
        )?;
        let columns = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let renamed = columns.into_iter().map(|i| ProjectionExpr {
            expr: Arc::new(PhysicalColumn::new(self.written.field(i).name(), i)),
            alias: self.schema.field(i).name().clone(),
        });
        Ok(Arc::new(ProjectionExec::try_new(
            renamed,
            Arc::new(reader),
        )?))
    }
}
