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

use ballista_core::JobId;
use ballista_core::cache::{CacheReadNode, EXTERNAL_STAGE_ID_BASE, contains_extension};
use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_core::extension::BallistaCacheNode;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use futures::future::BoxFuture;

use crate::cluster::CacheState;
use crate::state::cache_registry::CacheKey;
use crate::state::execution_stage::StageOutput;

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

/// DataFusion's error when no query planner can plan an extension node.
const NO_INSTALLED_PLANNER: &str =
    "No installed planner was able to convert the custom node";

/// Names the fix when planning `plan` failed because the session's query
/// planner cannot plan its cache reads, e.g. a custom session builder that did
/// not add it.
pub(crate) fn name_missing_cache_read_planner(
    plan: &LogicalPlan,
    error: BallistaError,
) -> BallistaError {
    if error.to_string().contains(NO_INSTALLED_PLANNER)
        && contains_extension::<CacheReadNode>(plan)
    {
        BallistaError::General(format!(
            "this scheduler's session cannot plan DataFrame::cache() reads: add \
             CacheReadPlanner to its query planner, e.g. with \
             SessionStateBuilderExt::with_cache_read_planner ({error})"
        ))
    } else {
        error
    }
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
                let input_id = EXTERNAL_STAGE_ID_BASE + reads.len();
                let read = CacheReadNode::new(
                    node.cache_id(),
                    input_id,
                    read_schema(node.schema(), &cache.schema),
                    cache.locations,
                );
                reads.push(ResolvedRead {
                    key,
                    materializing_job: cache.materializing_job,
                });
                return Ok(Some(LogicalPlan::Extension(Extension {
                    node: Arc::new(read),
                })));
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

/// A cache read's schema: the cache node's qualified columns, typed as the
/// build job wrote them, so it matches the shuffle batches read.
fn read_schema(cached: &DFSchemaRef, written: &SchemaRef) -> DFSchemaRef {
    if cached.fields().len() != written.fields().len() {
        return cached.clone();
    }
    let qualifiers = cached
        .iter()
        .map(|(qualifier, _)| qualifier.cloned())
        .collect();
    DFSchema::from_field_specific_qualified_schema(qualifiers, written)
        .map(Arc::new)
        .unwrap_or_else(|_| cached.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    /// A session without the cache planner (a custom `override_session_builder`
    /// that does not add it) fails a cache read with a message naming the fix.
    #[tokio::test]
    async fn a_missing_cache_read_planner_is_named() {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        );
        let schema = Arc::new(DFSchema::try_from(Schema::empty()).unwrap());
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(CacheReadNode::new(
                "c1",
                EXTERNAL_STAGE_ID_BASE,
                schema,
                vec![],
            )),
        });
        let error = ctx.state().create_physical_plan(&plan).await.unwrap_err();
        let err = name_missing_cache_read_planner(&plan, error.into()).to_string();
        assert!(err.contains("with_cache_read_planner"), "{err}");
    }
}
