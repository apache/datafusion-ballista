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

//! Resolution of lazy checkpoints into separate jobs.
//!
//! A `BallistaCheckpointNode` in a submitted logical plan means "materialise
//! everything below me before evaluating anything above me". This module turns
//! that into two jobs at planning time: a `CopyTo` job that writes the subtree
//! to the checkpoint location, and the caller's job, rewritten to scan it.
//!
//! Nothing here reaches physical planning: by the time the plan is handed to
//! `create_physical_plan` every checkpoint node has been replaced by a scan.

use async_trait::async_trait;
use ballista_core::error::{BallistaError, Result};
use ballista_core::extension::BallistaCheckpointNode;
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::file_format::DefaultFileType;
use datafusion::datasource::file_format::parquet::ParquetFormatFactory;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, UserDefinedLogicalNodeCore};
use datafusion::logical_expr::{LogicalPlan, dml::CopyTo};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::StreamExt;
use futures::future::BoxFuture;
use std::sync::Arc;

/// Runs a plan to completion as its own job.
///
/// Implemented by `SchedulerServer`; the indirection keeps this module free of
/// the event loop and lets tests substitute a local runner.
#[async_trait]
pub(crate) trait CheckpointMaterializer: Send + Sync {
    async fn materialize(
        &self,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
    ) -> Result<()>;
}

/// Cheap guard so plans without checkpoints skip the traversal entirely.
pub(crate) fn contains_checkpoint(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Extension(ext) = node
            && ext.node.as_any().is::<BallistaCheckpointNode>()
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Materialises pending checkpoints and returns a checkpoint free plan.
pub(crate) fn resolve_checkpoints<'a>(
    ctx: &'a Arc<SessionContext>,
    plan: &'a LogicalPlan,
    materializer: &'a dyn CheckpointMaterializer,
) -> BoxFuture<'a, Result<LogicalPlan>> {
    Box::pin(async move {
        // Bottom up, so anything we submit is itself already checkpoint free.
        let mut inputs = Vec::with_capacity(plan.inputs().len());
        for input in plan.inputs() {
            inputs.push(resolve_checkpoints(ctx, input, materializer).await?);
        }
        let plan = if inputs.is_empty() {
            plan.clone()
        } else {
            plan.with_new_exprs(plan.expressions(), inputs)?
        };

        let LogicalPlan::Extension(ext) = &plan else {
            return Ok(plan);
        };
        let Some(node) = ext.node.as_any().downcast_ref::<BallistaCheckpointNode>()
        else {
            return Ok(plan);
        };
        let location = node.location().to_string();
        // The schema the rest of the plan was built against.
        let original_schema = node.schema().clone();

        // The location is fixed when checkpoint_lazy() builds the node, so this
        // is idempotent: the first action materialises, later ones fall through.
        if !checkpoint_exists(ctx, &location).await? {
            let write_plan = LogicalPlan::Copy(CopyTo {
                input: Arc::new(node.inputs()[0].clone()),
                output_url: location.clone(),
                partition_by: vec![],
                file_type: Arc::new(DefaultFileType::new(Arc::new(
                    ParquetFormatFactory::new(),
                ))),
                options: Default::default(),
                output_schema: Arc::new(DFSchema::empty()),
            });

            materializer
                .materialize(
                    &format!("checkpoint {}", node.checkpoint_id()),
                    ctx.clone(),
                    &write_plan,
                )
                .await?;
        }

        // Swapping the subtree for a bare scan is the lineage break. read_parquet
        // only builds a ListingTable scan, it does not execute.
        let scan = ctx
            .read_parquet(&location, ParquetReadOptions::default())
            .await
            .map_err(|e| {
                BallistaError::General(format!(
                    "failed to read checkpoint at {location}: {e}"
                ))
            })?
            .into_unoptimized_plan();

        let scan_columns = scan.schema().columns();
        if scan_columns.len() != original_schema.fields().len() {
            return Err(BallistaError::Internal(format!(
                "checkpoint at {location} has {} columns, expected {}",
                scan_columns.len(),
                original_schema.fields().len()
            )));
        }

        // read_parquet scans under UNNAMED_TABLE, so the scan drops the
        // qualifiers the rest of the plan still references. Restore them column
        // by column, which also covers subtrees carrying several qualifiers.
        let projection = original_schema
            .iter()
            .zip(scan_columns)
            .map(|((qualifier, field), scan_column)| {
                Expr::Column(scan_column)
                    .alias_qualified(qualifier.cloned(), field.name().to_owned())
            })
            .collect::<Vec<_>>();

        Ok(LogicalPlanBuilder::from(scan)
            .project(projection)?
            .build()?)
    })
}

async fn checkpoint_exists(ctx: &SessionContext, location: &str) -> Result<bool> {
    let url = ListingTableUrl::parse(location)?;
    let store = ctx.runtime_env().object_store(&url)?;
    let mut listing = store.list(Some(url.prefix()));

    let first = listing.next().await.transpose().map_err(|e| {
        BallistaError::General(format!(
            "failed to list checkpoint location {location}: {e}"
        ))
    })?;

    Ok(first.is_some())
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::MemTable;
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::logical_expr::{Extension, LogicalPlanBuilder};
    use datafusion::prelude::{Expr, col, lit};
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// Materialises locally and records every call, so tests can assert both how
    /// many jobs were split out and in what order.
    #[derive(Default)]
    struct RecordingMaterializer {
        calls: Mutex<Vec<(String, LogicalPlan)>>,
    }

    impl RecordingMaterializer {
        fn locations(&self) -> Vec<String> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(l, _)| l.clone())
                .collect()
        }

        /// The plan each split out job was given, in submission order.
        fn plans(&self) -> Vec<LogicalPlan> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(_, p)| p.clone())
                .collect()
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl CheckpointMaterializer for RecordingMaterializer {
        async fn materialize(
            &self,
            _job_name: &str,
            ctx: Arc<SessionContext>,
            plan: &LogicalPlan,
        ) -> Result<()> {
            let LogicalPlan::Copy(copy) = plan else {
                panic!("expected a CopyTo plan, got {plan:?}");
            };
            self.calls
                .lock()
                .unwrap()
                .push((copy.output_url.clone(), copy.input.as_ref().clone()));

            ctx.execute_logical_plan(plan.clone())
                .await?
                .collect()
                .await?;

            Ok(())
        }
    }

    fn scans_table(plan: &LogicalPlan, table: &str) -> bool {
        let mut found = false;
        let _ = plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && scan.table_name.table() == table
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        found
    }

    struct FailingMaterializer;

    #[async_trait]
    impl CheckpointMaterializer for FailingMaterializer {
        async fn materialize(
            &self,
            _: &str,
            _: Arc<SessionContext>,
            _: &LogicalPlan,
        ) -> Result<()> {
            Err(BallistaError::General("checkpoint job failed".to_string()))
        }
    }

    fn test_context() -> Arc<SessionContext> {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "test",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();

        // Second table so we can checkpoint a subtree carrying more than one
        // qualifier.
        let other_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let other_batch = RecordBatch::try_new(
            other_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["w", "x", "y", "z"])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "other",
            Arc::new(MemTable::try_new(other_schema, vec![vec![other_batch]]).unwrap()),
        )
        .unwrap();

        Arc::new(ctx)
    }

    fn checkpoint(location: &str, input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(BallistaCheckpointNode::new(
                "checkpoint-id".to_string(),
                "session-id".to_string(),
                location.to_string(),
                input,
            )),
        })
    }

    async fn plan_for(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        ctx.sql(sql).await.unwrap().into_unoptimized_plan()
    }

    #[tokio::test]
    async fn should_detect_checkpoint_nodes() {
        let ctx = test_context();
        let plan = plan_for(&ctx, "select * from test").await;

        assert!(!contains_checkpoint(&plan));
        assert!(contains_checkpoint(&checkpoint("/tmp/cp", plan)));
    }

    #[tokio::test]
    async fn should_leave_plan_without_checkpoint_untouched() {
        let ctx = test_context();
        let materializer = RecordingMaterializer::default();

        let plan = plan_for(&ctx, "select * from test").await;
        let resolved = resolve_checkpoints(&ctx, &plan, &materializer)
            .await
            .unwrap();

        assert_eq!(
            format!("{}", resolved.display_indent()),
            format!("{}", plan.display_indent())
        );
        assert_eq!(materializer.call_count(), 0);
    }

    #[tokio::test]
    async fn should_split_plan_at_checkpoint_boundary() {
        let dir = TempDir::new().unwrap();
        let location = format!("{}/cp", dir.path().to_str().unwrap());

        let ctx = test_context();
        let materializer = RecordingMaterializer::default();

        // An aggregate sits above the checkpoint, so the boundary falls in the
        // middle of the plan rather than at its root.
        let inner = checkpoint(
            &location,
            plan_for(&ctx, "select id, name from test where id > 2").await,
        );
        let plan = LogicalPlanBuilder::from(inner)
            .aggregate(Vec::<Expr>::new(), vec![count(col("test.id")).alias("n")])
            .unwrap()
            .build()
            .unwrap();

        let resolved = resolve_checkpoints(&ctx, &plan, &materializer)
            .await
            .unwrap();

        // Exactly one job was split out, writing to the checkpoint location.
        assert_eq!(materializer.locations(), vec![location]);

        // Job 1 is the checkpointed subtree and nothing more: it reads the
        // source, and the aggregate sitting above the checkpoint is not in it.
        let job1 = &materializer.plans()[0];
        assert!(scans_table(job1, "test"));
        assert!(
            !matches!(job1, LogicalPlan::Aggregate(_)),
            "job 1 must stop at the checkpoint, got {}",
            job1.display_indent()
        );

        // Job 2 is disjoint from job 1: the source is gone and the checkpoint
        // node with it. This is the lineage break.
        assert!(
            !scans_table(&resolved, "test"),
            "job 2 must not re-read the source, got {}",
            resolved.display_indent()
        );
        assert!(!contains_checkpoint(&resolved));

        // Job 2 has no path back to the source, so producing the right answer
        // means it read the materialised checkpoint.
        let result = ctx
            .execute_logical_plan(resolved)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(["+---+", "| n |", "+---+", "| 2 |", "+---+"], &result);
    }

    /// Regression: read_parquet scans under `?table?`, so without re-qualifying
    /// the replacement, anything above the checkpoint that references a
    /// qualified column fails to resolve during type coercion.
    #[tokio::test]
    async fn should_preserve_qualifiers_for_operators_above_checkpoint() {
        let dir = TempDir::new().unwrap();
        let location = format!("{}/cp", dir.path().to_str().unwrap());

        let ctx = test_context();
        let materializer = RecordingMaterializer::default();

        let inner =
            checkpoint(&location, plan_for(&ctx, "select id, name from test").await);
        let plan = LogicalPlanBuilder::from(inner)
            .filter(col("test.id").gt(lit(2)))
            .unwrap()
            .build()
            .unwrap();

        let resolved = resolve_checkpoints(&ctx, &plan, &materializer)
            .await
            .unwrap();

        // Would previously fail here with FieldNotFound on test.id.
        let result = ctx
            .execute_logical_plan(resolved)
            .await
            .unwrap()
            .sort_by(vec![col("id")])
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+----+------+",
                "| id | name |",
                "+----+------+",
                "| 3  | c    |",
                "| 4  | d    |",
                "+----+------+",
            ],
            &result
        );
    }

    /// A checkpointed join carries two qualifiers, which is why the replacement
    /// is a projection rather than a single SubqueryAlias.
    #[tokio::test]
    async fn should_preserve_multiple_qualifiers_from_a_join() {
        let dir = TempDir::new().unwrap();
        let location = format!("{}/cp", dir.path().to_str().unwrap());

        let ctx = test_context();
        let materializer = RecordingMaterializer::default();

        let joined = plan_for(
            &ctx,
            "select l.id, r.label from test l join other r on l.id = r.id",
        )
        .await;
        let expected_columns = joined.schema().columns();

        let inner = checkpoint(&location, joined);
        let plan = LogicalPlanBuilder::from(inner)
            .filter(col("r.label").eq(lit("y")))
            .unwrap()
            .build()
            .unwrap();

        let resolved = resolve_checkpoints(&ctx, &plan, &materializer)
            .await
            .unwrap();

        // Both qualifiers survive the round trip, not just the first.
        let scan = resolved.inputs()[0];
        assert_eq!(scan.schema().columns(), expected_columns);

        let result = ctx
            .execute_logical_plan(resolved)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+----+-------+",
                "| id | label |",
                "+----+-------+",
                "| 3  | y     |",
                "+----+-------+",
            ],
            &result
        );
    }

    #[tokio::test]
    async fn should_resolve_nested_checkpoints_inner_first() {
        let dir = TempDir::new().unwrap();
        let base = dir.path().to_str().unwrap();
        let inner_location = format!("{base}/inner");
        let outer_location = format!("{base}/outer");

        let ctx = test_context();
        let materializer = RecordingMaterializer::default();

        let inner = checkpoint(
            &inner_location,
            plan_for(&ctx, "select id, name from test").await,
        );
        let filtered = LogicalPlanBuilder::from(inner)
            .filter(col("test.id").gt(lit(1)))
            .unwrap()
            .build()
            .unwrap();
        let plan = checkpoint(&outer_location, filtered);

        let resolved = resolve_checkpoints(&ctx, &plan, &materializer)
            .await
            .unwrap();

        // Ordering is the point: resolving top down would submit the outer job
        // with an unresolved checkpoint still inside it.
        assert_eq!(
            materializer.locations(),
            vec![inner_location, outer_location]
        );
        assert!(!contains_checkpoint(&resolved));
    }

    #[tokio::test]
    async fn should_not_rematerialize_existing_checkpoint() {
        let dir = TempDir::new().unwrap();
        let location = format!("{}/cp", dir.path().to_str().unwrap());

        let ctx = test_context();
        let materializer = RecordingMaterializer::default();

        let inner = plan_for(&ctx, "select id, name from test").await;

        // First action materialises.
        resolve_checkpoints(&ctx, &checkpoint(&location, inner.clone()), &materializer)
            .await
            .unwrap();
        assert_eq!(materializer.call_count(), 1);

        // Second action reuses it: this is what makes the checkpoint a
        // checkpoint rather than a barrier.
        let plan = checkpoint(&location, inner);
        let expected_columns = plan.schema().columns();
        let resolved = resolve_checkpoints(&ctx, &plan, &materializer)
            .await
            .unwrap();

        assert_eq!(materializer.call_count(), 1);
        assert_eq!(resolved.schema().columns(), expected_columns);
    }

    #[tokio::test]
    async fn should_propagate_materialization_failure() {
        let dir = TempDir::new().unwrap();
        let location = format!("{}/cp", dir.path().to_str().unwrap());

        let ctx = test_context();
        let plan = checkpoint(&location, plan_for(&ctx, "select * from test").await);

        let err = resolve_checkpoints(&ctx, &plan, &FailingMaterializer)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("checkpoint job failed"));
    }
}
