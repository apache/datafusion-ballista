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

use crate::config::BallistaConfig;
use crate::execution_plans::{DistributedExplainAnalyzeExec, DistributedQueryExec};
use crate::serde::BallistaLogicalExtensionCodec;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableScan, lit};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use std::marker::PhantomData;
use std::sync::Arc;

/// [BallistaQueryPlanner] planner takes logical plan
/// and executes it remotely on on scheduler.
///
/// Under the hood it will create [DistributedQueryExec]
/// which will establish gprc connection with the scheduler.
///
pub struct BallistaQueryPlanner<T: AsLogicalPlan> {
    scheduler_url: String,
    config: BallistaConfig,
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    local_planner: DefaultPhysicalPlanner,
    _plan_type: PhantomData<T>,
}

impl<T: AsLogicalPlan> std::fmt::Debug for BallistaQueryPlanner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BallistaQueryPlanner")
            .field("scheduler_url", &self.scheduler_url)
            .field("config", &self.config)
            .field("extension_codec", &self.extension_codec)
            .field("_plan_type", &self._plan_type)
            .finish()
    }
}

impl<T: 'static + AsLogicalPlan> BallistaQueryPlanner<T> {
    /// Creates a new Ballista query planner with the specified scheduler URL and configuration.
    pub fn new(scheduler_url: String, config: BallistaConfig) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec: Arc::new(BallistaLogicalExtensionCodec::default()),
            local_planner: DefaultPhysicalPlanner::default(),
            _plan_type: PhantomData,
        }
    }

    /// Creates a new Ballista query planner with a custom extension codec.
    pub fn with_extension(
        scheduler_url: String,
        config: BallistaConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            local_planner: DefaultPhysicalPlanner::default(),
            _plan_type: PhantomData,
        }
    }

    /// Creates a new Ballista query planner with a custom local physical planner.
    pub fn with_local_planner(
        scheduler_url: String,
        config: BallistaConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        local_planner: DefaultPhysicalPlanner,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            _plan_type: PhantomData,
            local_planner,
        }
    }
}

#[async_trait::async_trait]
impl<T: 'static + AsLogicalPlan> QueryPlanner for BallistaQueryPlanner<T> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        log::debug!("create_physical_plan - plan: {:?}", logical_plan);
        // we inspect if plan scans only tables located in information_schema,
        // which describe the catalog of this context,
        // if that is the case, we run that plan
        // on this same context, not on cluster
        if scans_only_information_schema(logical_plan)? {
            log::debug!("create_physical_plan - plan can be executed locally");

            self.local_planner
                .create_physical_plan(logical_plan, session_state)
                .await
        } else {
            match logical_plan {
                LogicalPlan::EmptyRelation(_) => {
                    log::debug!("create_physical_plan - handling empty exec");
                    Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
                }
                LogicalPlan::Analyze(analyze) => {
                    log::debug!(
                        "create_physical_plan - handling explain analyze statement"
                    );
                    let inner_plan = analyze.input.as_ref().clone();
                    let distributed_query_exec =
                        Arc::new(DistributedQueryExec::<T>::with_extension(
                            self.scheduler_url.clone(),
                            self.config.clone(),
                            inner_plan,
                            self.extension_codec.clone(),
                            session_state.session_id().to_string(),
                        ));

                    Ok(Arc::new(DistributedExplainAnalyzeExec::new(
                        distributed_query_exec,
                        self.scheduler_url.clone(),
                        Arc::clone(analyze.schema.inner()),
                        analyze.verbose,
                    )))
                }
                _ => {
                    log::debug!("create_physical_plan - handling general statement");

                    Ok(Arc::new(DistributedQueryExec::<T>::with_extension(
                        self.scheduler_url.clone(),
                        self.config.clone(),
                        logical_plan.clone(),
                        self.extension_codec.clone(),
                        session_state.session_id().to_string(),
                    )))
                }
            }
        }
    }
}

/// Returns `true` if every table `plan` reads, subqueries included, is in
/// `information_schema`, and `false` if none is.
///
/// Those tables describe the catalog of the session that planned `plan`, which
/// no other node shares, and their scans cannot be serialized, so a plan
/// reading only them should run where it was planned. A plan that also reads
/// other tables would have to run there as well, without the cluster, so it is
/// refused with an error instead.
pub fn scans_only_information_schema(
    plan: &LogicalPlan,
) -> Result<bool, DataFusionError> {
    let (mut information_schema, mut other) = (false, false);
    // Subqueries scan tables too, and `apply` does not descend into them.
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(TableScan { table_name, .. }) = node {
            if table_name.schema() == Some("information_schema") {
                information_schema = true;
            } else {
                other = true;
            }
        }
        Ok(if information_schema && other {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;

    if information_schema && other {
        return Err(DataFusionError::NotImplemented(
            "Ballista cannot run a query that reads information_schema together \
             with other tables. Query information_schema on its own."
                .to_string(),
        ));
    }
    Ok(information_schema)
}

/// Builds a plan that produces `batches` with exactly `schema`, from nodes
/// that serialize without a custom codec.
///
/// The rows become a `Values` node, and a projection gives each column back
/// the qualifier and name it has in `schema`. The plan can therefore replace
/// any subtree that produced `batches`, whichever tables its columns came from.
fn constant_relation(
    schema: &DFSchemaRef,
    batches: &[RecordBatch],
) -> Result<LogicalPlan, DataFusionError> {
    // `Values` needs at least one column, but rows without columns still
    // count, so they get a placeholder column that the projection drops.
    let no_columns = schema.fields().is_empty();
    let values_schema = if no_columns {
        Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
            "placeholder",
            DataType::Boolean,
            false,
        )]))?)
    } else {
        Arc::clone(schema)
    };

    let mut rows = vec![];
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(if no_columns {
                vec![lit(true)]
            } else {
                batch
                    .columns()
                    .iter()
                    .map(|column| ScalarValue::try_from_array(column, row).map(lit))
                    .collect::<Result<Vec<_>, _>>()?
            });
        }
    }

    // `Values` cannot be empty, and neither an empty `Values` nor an
    // `EmptyRelation` keeps its schema through serialization. So no rows is
    // one row of placeholders under `LIMIT 0`. The placeholders are not null,
    // so they fit columns that are not nullable.
    let no_rows = rows.is_empty();
    if no_rows {
        rows.push(
            values_schema
                .fields()
                .iter()
                .map(|field| ScalarValue::new_default(field.data_type()).map(lit))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }

    // `Values` names its columns `column1`, `column2` and so on.
    let columns = schema.iter().enumerate().map(|(i, (qualifier, field))| {
        Expr::Column(Column::new_unqualified(format!("column{}", i + 1)))
            .alias_qualified(qualifier.cloned(), field.name())
    });

    let plan =
        LogicalPlanBuilder::values_with_schema(rows, &values_schema)?.project(columns)?;
    let plan = if no_rows {
        plan.limit(0, Some(0))?
    } else {
        plan
    };
    plan.build()
}

#[cfg(test)]
mod test {
    use datafusion::{
        arrow::{
            array::{StringArray, UInt64Array},
            datatypes::{DataType, Field, Schema},
            record_batch::{RecordBatch, RecordBatchOptions},
            util::pretty::pretty_format_batches,
        },
        common::{DFSchema, DFSchemaRef, TableReference},
        error::Result,
        execution::{
            SessionStateBuilder, context::QueryPlanner, runtime_env::RuntimeEnvBuilder,
        },
        logical_expr::LogicalPlan,
        physical_plan::ExecutionPlan,
        prelude::{SessionConfig, SessionContext},
    };
    use datafusion_proto::logical_plan::AsLogicalPlan;
    use datafusion_proto::protobuf::LogicalPlanNode;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::{BallistaQueryPlanner, constant_relation, scans_only_information_schema};
    use crate::config::BallistaConfig;
    use crate::execution_plans::{DistributedExplainAnalyzeExec, DistributedQueryExec};
    use crate::serde::BallistaLogicalExtensionCodec;

    fn context() -> SessionContext {
        let runtime_environment = RuntimeEnvBuilder::new().build().unwrap();

        let session_config = SessionConfig::new().with_information_schema(true);

        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime_environment.into())
            .with_default_features()
            .build();

        SessionContext::new_with_state(state)
    }

    /// The qualified fields of `schema`, which a replacement has to reproduce
    /// exactly.
    fn qualified_fields(schema: &DFSchema) -> Vec<(Option<TableReference>, Field)> {
        schema
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), field.as_ref().clone()))
            .collect()
    }

    /// Runs `plan` in `ctx` and counts the rows it returns.
    async fn row_count(ctx: &SessionContext, plan: LogicalPlan) -> Result<usize> {
        let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
        Ok(batches.iter().map(|batch| batch.num_rows()).sum())
    }

    /// Runs `plan` in `ctx` and returns its rows as sorted text, so results
    /// that differ only in row order or batching compare equal.
    async fn rows(ctx: &SessionContext, plan: LogicalPlan) -> Result<Vec<String>> {
        let batches: Vec<RecordBatch> = ctx
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();
        let mut lines: Vec<String> = pretty_format_batches(&batches)?
            .to_string()
            .lines()
            .map(String::from)
            .collect();
        lines.sort();
        Ok(lines)
    }

    /// Encodes and decodes `plan` the way it travels to the scheduler.
    fn round_trip(ctx: &SessionContext, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let codec = BallistaLogicalExtensionCodec::default();
        let node = LogicalPlanNode::try_from_logical_plan(plan, &codec)?;
        node.try_into_logical_plan(&ctx.task_ctx(), &codec)
    }

    /// A schema like a join's output: two tables share a column name, and one
    /// column is nullable.
    fn joined_schema() -> Result<DFSchemaRef> {
        Ok(Arc::new(DFSchema::new_with_metadata(
            vec![
                (
                    Some(TableReference::bare("t")),
                    Arc::new(Field::new("table_name", DataType::Utf8, false)),
                ),
                (
                    Some(TableReference::bare("c")),
                    Arc::new(Field::new("table_name", DataType::Utf8, false)),
                ),
                (
                    Some(TableReference::partial("information_schema", "columns")),
                    Arc::new(Field::new("numeric_precision", DataType::UInt64, true)),
                ),
            ],
            HashMap::new(),
        )?))
    }

    /// Two rows for [joined_schema], one of them with a null.
    fn joined_batch(schema: &DFSchemaRef) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(schema.as_arrow().clone()),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(UInt64Array::from(vec![Some(32), None])),
            ],
        )?)
    }

    /// Three rows without columns, as a scan that reads no columns returns.
    fn rows_without_columns() -> Result<RecordBatch> {
        Ok(RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )?)
    }

    #[tokio::test]
    async fn should_detect_show_table_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SHOW TABLES").await?;

        assert!(scans_only_information_schema(df.logical_plan())?);

        Ok(())
    }

    #[tokio::test]
    async fn should_detect_select_from_information_schema_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SELECT * FROM information_schema.df_settings WHERE NAME LIKE 'ballista%'").await?;

        assert!(scans_only_information_schema(df.logical_plan())?);

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_local_table() -> Result<()> {
        let ctx = context();
        ctx.sql("CREATE TABLE tt (c0 INT, c1 INT)")
            .await?
            .show()
            .await?;
        let df = ctx.sql("SELECT * FROM tt").await?;

        assert!(!scans_only_information_schema(df.logical_plan())?);

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_external_table() -> Result<()> {
        let ctx = context();
        ctx.register_csv("tt", "tests/customer.csv", Default::default())
            .await?;
        let df = ctx.sql("SELECT * FROM tt").await?;

        assert!(!scans_only_information_schema(df.logical_plan())?);

        Ok(())
    }

    #[tokio::test]
    async fn should_reject_information_schema_with_other_tables() -> Result<()> {
        let ctx = context();
        ctx.sql("CREATE TABLE big (name VARCHAR)")
            .await?
            .show()
            .await?;

        for sql in [
            "SELECT table_name FROM information_schema.tables \
             WHERE table_name IN (SELECT name FROM big)",
            "SELECT name FROM big \
             WHERE name IN (SELECT table_name FROM information_schema.tables)",
            "SELECT t.table_name FROM information_schema.tables t \
             JOIN big b ON t.table_name = b.name",
        ] {
            // Unoptimized, so subqueries are still expressions rather than the
            // joins the optimizer would decorrelate them into.
            let plan = ctx.state().create_logical_plan(sql).await?;

            assert!(scans_only_information_schema(&plan).is_err(), "{sql}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_create_distributed_explain_analyze_exec() -> Result<()> {
        let ctx = context();
        ctx.sql("CREATE TABLE tt (c0 INT)").await?.show().await?;
        let analyze_df = ctx.sql("EXPLAIN ANALYZE SELECT * FROM tt").await?;
        let planner = BallistaQueryPlanner::<LogicalPlanNode>::new(
            "http://localhost:50050".to_string(),
            BallistaConfig::default(),
        );
        let plan = planner
            .create_physical_plan(analyze_df.logical_plan(), &ctx.state())
            .await?;

        assert!(matches!(analyze_df.logical_plan(), LogicalPlan::Analyze(_)));
        let explain = plan
            .downcast_ref::<DistributedExplainAnalyzeExec<LogicalPlanNode>>()
            .unwrap();
        assert!(
            explain.children()[0]
                .downcast_ref::<DistributedQueryExec<LogicalPlanNode>>()
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn constant_relation_reproduces_rows_and_qualified_schema() -> Result<()> {
        let ctx = context();
        let schema = joined_schema()?;
        let batch = joined_batch(&schema)?;

        let plan = constant_relation(&schema, std::slice::from_ref(&batch))?;

        assert_eq!(qualified_fields(plan.schema()), qualified_fields(&schema));
        let actual = ctx.execute_logical_plan(plan).await?.collect().await?;
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            pretty_format_batches(&[batch])?.to_string()
        );

        Ok(())
    }

    #[tokio::test]
    async fn constant_relation_keeps_the_schema_of_an_empty_result() -> Result<()> {
        let ctx = context();
        let schema = joined_schema()?;

        let plan = constant_relation(&schema, &[])?;

        assert_eq!(qualified_fields(plan.schema()), qualified_fields(&schema));
        assert_eq!(row_count(&ctx, plan).await?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn constant_relation_keeps_the_row_count_of_a_result_without_columns()
    -> Result<()> {
        let ctx = context();
        let schema = Arc::new(DFSchema::empty());

        let with_rows = constant_relation(&schema, &[rows_without_columns()?])?;
        let without_rows = constant_relation(&schema, &[])?;

        assert!(with_rows.schema().fields().is_empty());
        assert_eq!(row_count(&ctx, with_rows).await?, 3);
        assert!(without_rows.schema().fields().is_empty());
        assert_eq!(row_count(&ctx, without_rows).await?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn constant_relations_survive_serialization() -> Result<()> {
        let ctx = context();
        let joined = joined_schema()?;
        let no_columns = Arc::new(DFSchema::empty());

        for (schema, batches, expected_rows) in [
            (&joined, vec![joined_batch(&joined)?], 2),
            (&joined, vec![], 0),
            (&no_columns, vec![rows_without_columns()?], 3),
            (&no_columns, vec![], 0),
        ] {
            let plan = constant_relation(schema, &batches)?;
            let decoded = round_trip(&ctx, &plan)?;

            assert_eq!(row_count(&ctx, decoded.clone()).await?, expected_rows);
            assert_eq!(rows(&ctx, decoded).await?, rows(&ctx, plan).await?);
        }

        Ok(())
    }
}
