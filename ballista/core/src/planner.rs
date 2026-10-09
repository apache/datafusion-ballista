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
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableScan, lit};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use std::collections::{HashMap, HashSet};
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
        if scans_only_information_schema(logical_plan) {
            log::debug!("create_physical_plan - plan can be executed locally");

            self.local_planner
                .create_physical_plan(logical_plan, session_state)
                .await
        } else {
            // the cluster cannot read this context's information_schema,
            // so the parts of the plan that read it run here first
            let logical_plan = inline_information_schema(
                logical_plan.clone(),
                &self.local_planner,
                session_state,
            )
            .await?;
            match &logical_plan {
                LogicalPlan::EmptyRelation(_) => {
                    log::debug!("create_physical_plan - handling empty exec");
                    Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
                }
                LogicalPlan::DescribeTable(_) => {
                    // The plan already carries the table schema resolved from
                    // this context's catalog, and it cannot be serialized, so
                    // it is planned here rather than on the cluster.
                    self.local_planner
                        .create_physical_plan(logical_plan, session_state)
                        .await
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

/// Returns `true` if `plan` reads at least one table and every table it reads,
/// subqueries included, is in `information_schema`. A plan that reads no
/// tables, such as `SELECT 1`, returns `false`.
///
/// Those tables describe the catalog of the session that planned `plan`,
/// which no other node shares, so a plan that reads only them runs where it
/// was planned. A plan that also reads other tables goes to the cluster, after
/// [inline_information_schema] has replaced its `information_schema` parts
/// with their rows.
pub fn scans_only_information_schema(plan: &LogicalPlan) -> bool {
    let (mut information_schema, mut other) = (false, false);
    // Subqueries scan tables too, and `apply` does not descend into them.
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(TableScan { table_name, .. }) = node {
            if table_name.schema() == Some("information_schema") {
                information_schema = true;
            } else {
                other = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    information_schema && !other
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

/// Runs every part of `plan` that reads only `information_schema` in
/// `session`, and replaces it with the rows it produced.
///
/// `information_schema` describes the catalog of the session that planned
/// `plan`, which no other node shares. Once its parts are replaced, the rows
/// travel with the plan, and the cluster never resolves `information_schema`
/// itself. Each part is as large as possible, so its filters and aggregates
/// run here and only their results travel. Returns `plan` unchanged when it
/// reads no `information_schema`.
///
/// `plan` must be analyzed, as every plan a [QueryPlanner] receives is.
pub async fn inline_information_schema(
    plan: LogicalPlan,
    planner: &dyn PhysicalPlanner,
    session: &dyn Session,
) -> Result<LogicalPlan, DataFusionError> {
    // A part's children run with it, so the walk skips them.
    let mut parts = HashSet::new();
    plan.apply_with_subqueries(|node| {
        Ok(if can_inline(node) {
            parts.insert(node.clone());
            TreeNodeRecursion::Jump
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    if parts.is_empty() {
        return Ok(plan);
    }

    // Running a part is async and DataFusion's rewrites are not, so every
    // part runs before the rewrite.
    let mut replacements = HashMap::with_capacity(parts.len());
    for part in parts {
        let physical_plan = planner.create_physical_plan(&part, session).await?;
        let batches = collect(physical_plan, session.task_ctx()).await?;
        let replacement = constant_relation(part.schema(), &batches)?;
        replacements.insert(part, replacement);
    }

    plan.transform_down_with_subqueries(|node| {
        Ok(match replacements.get(&node) {
            Some(replacement) => {
                Transformed::new(replacement.clone(), true, TreeNodeRecursion::Jump)
            }
            None => Transformed::no(node),
        })
    })
    .data()
}

/// Whether `plan` can run on its own where it was planned: it reads only
/// `information_schema`, refers to no outer query, and contains nothing that
/// changes state or needs more than the default planner.
fn can_inline(plan: &LogicalPlan) -> bool {
    if !scans_only_information_schema(plan) {
        return false;
    }
    let mut inlinable = true;
    let _ = plan.apply_with_subqueries(|node| {
        // The walk shows each subquery expression as a `Subquery` node, which
        // a rewrite has to give back unchanged, and which the physical planner
        // cannot plan until the optimizer turns it into a join.
        inlinable = !node.contains_outer_reference()
            && !matches!(
                node,
                LogicalPlan::Extension(_)
                    | LogicalPlan::Dml(_)
                    | LogicalPlan::Ddl(_)
                    | LogicalPlan::Copy(_)
                    | LogicalPlan::Statement(_)
                    | LogicalPlan::Explain(_)
                    | LogicalPlan::Analyze(_)
                    | LogicalPlan::Subquery(_)
            );
        Ok(if inlinable {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    inlinable
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
        common::{DFSchema, DFSchemaRef, TableReference, tree_node::TreeNodeRecursion},
        error::Result,
        execution::{
            SessionStateBuilder, context::QueryPlanner, runtime_env::RuntimeEnvBuilder,
        },
        logical_expr::LogicalPlan,
        physical_plan::{ExecutionPlan, displayable},
        physical_planner::DefaultPhysicalPlanner,
        prelude::{CsvReadOptions, SessionConfig, SessionContext},
    };
    use datafusion_proto::logical_plan::AsLogicalPlan;
    use datafusion_proto::protobuf::LogicalPlanNode;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::{
        BallistaQueryPlanner, constant_relation, inline_information_schema,
        scans_only_information_schema,
    };
    use crate::config::BallistaConfig;
    use crate::execution_plans::{DistributedExplainAnalyzeExec, DistributedQueryExec};
    use crate::extension::SessionConfigExt;
    use crate::serde::BallistaLogicalExtensionCodec;

    /// A session configured like a Ballista client's, so its plans take the
    /// shape `BallistaQueryPlanner` receives.
    fn context() -> SessionContext {
        let runtime_environment = RuntimeEnvBuilder::new().build().unwrap();

        let session_config = SessionConfig::new_with_ballista();

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

    /// The qualifier, name and type of each field of `schema`. Nullability is
    /// left out, since the scheduler can only narrow it.
    fn names_and_types(
        schema: &DFSchema,
    ) -> Vec<(Option<TableReference>, String, DataType)> {
        schema
            .iter()
            .map(|(qualifier, field)| {
                (
                    qualifier.cloned(),
                    field.name().clone(),
                    field.data_type().clone(),
                )
            })
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

    /// Registers `tests/customer.csv` as `name`: four customers, with a name in
    /// `column_1` and an amount in `column_2`.
    async fn register_customers(ctx: &SessionContext, name: &str) -> Result<()> {
        ctx.register_csv(
            name,
            "tests/customer.csv",
            CsvReadOptions::new().has_header(false),
        )
        .await
    }

    /// Plans `sql` as `BallistaQueryPlanner` receives it, analyzed and
    /// optimized.
    async fn optimized(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan> {
        ctx.sql(sql).await?.into_optimized_plan()
    }

    /// Plans `sql` analyzed but not optimized, so its subqueries are still
    /// expressions rather than the joins the optimizer turns them into.
    async fn analyzed(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan> {
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await?;
        state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
    }

    /// Runs [inline_information_schema] the way `BallistaQueryPlanner` does.
    async fn inline(ctx: &SessionContext, plan: LogicalPlan) -> Result<LogicalPlan> {
        let state = ctx.state();
        inline_information_schema(plan, &DefaultPhysicalPlanner::default(), &state).await
    }

    /// The number of `information_schema` scans and of other scans in `plan`,
    /// subqueries included.
    fn scans(plan: &LogicalPlan) -> (usize, usize) {
        let (mut information_schema, mut other) = (0, 0);
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                if scan.table_name.schema() == Some("information_schema") {
                    information_schema += 1;
                } else {
                    other += 1;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        (information_schema, other)
    }

    /// The number of rows in each `Values` node in `plan`, subqueries included.
    fn values_rows(plan: &LogicalPlan) -> Vec<usize> {
        let mut rows = vec![];
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::Values(values) = node {
                rows.push(values.values.len());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        rows
    }

    /// Whether any node in `plan`, subqueries included, satisfies `predicate`.
    fn any_node(plan: &LogicalPlan, predicate: impl Fn(&LogicalPlan) -> bool) -> bool {
        let mut found = false;
        plan.apply_with_subqueries(|node| {
            found = predicate(node);
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
        .unwrap();
        found
    }

    #[tokio::test]
    async fn should_detect_show_table_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SHOW TABLES").await?;

        assert!(scans_only_information_schema(df.logical_plan()));

        Ok(())
    }

    #[tokio::test]
    async fn should_detect_select_from_information_schema_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SELECT * FROM information_schema.df_settings WHERE NAME LIKE 'ballista%'").await?;

        assert!(scans_only_information_schema(df.logical_plan()));

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_plan_without_tables_as_local_plan() -> Result<()> {
        let ctx = context();
        let df = ctx.sql("SELECT 1").await?;

        assert!(!scans_only_information_schema(df.logical_plan()));

        Ok(())
    }

    #[tokio::test]
    async fn should_detect_catalog_qualified_information_schema_as_local_plan()
    -> Result<()> {
        let ctx = context();
        let df = ctx
            .sql("SELECT table_name FROM datafusion.information_schema.tables")
            .await?;

        assert!(scans_only_information_schema(df.logical_plan()));

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

        assert!(!scans_only_information_schema(df.logical_plan()));

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_external_table() -> Result<()> {
        let ctx = context();
        ctx.register_csv("tt", "tests/customer.csv", Default::default())
            .await?;
        let df = ctx.sql("SELECT * FROM tt").await?;

        assert!(!scans_only_information_schema(df.logical_plan()));

        Ok(())
    }

    #[tokio::test]
    async fn should_not_detect_information_schema_with_other_tables_as_local_plan()
    -> Result<()> {
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

            assert!(!scans_only_information_schema(&plan), "{sql}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_plan_describe_table_locally() -> Result<()> {
        let ctx = context();
        ctx.sql("CREATE TABLE tt (c0 INT, c1 INT)")
            .await?
            .show()
            .await?;
        let describe_df = ctx.sql("DESCRIBE tt").await?;
        let planner = BallistaQueryPlanner::<LogicalPlanNode>::new(
            "http://localhost:50050".to_string(),
            BallistaConfig::default(),
        );
        let plan = planner
            .create_physical_plan(describe_df.logical_plan(), &ctx.state())
            .await?;

        assert!(matches!(
            describe_df.logical_plan(),
            LogicalPlan::DescribeTable(_)
        ));
        assert!(
            plan.downcast_ref::<DistributedQueryExec<LogicalPlanNode>>()
                .is_none()
        );
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

            // Printed rows cannot tell a type change apart, so compare types.
            assert_eq!(names_and_types(decoded.schema()), names_and_types(schema));
            assert_eq!(row_count(&ctx, decoded.clone()).await?, expected_rows);
            assert_eq!(rows(&ctx, decoded).await?, rows(&ctx, plan).await?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_leave_a_plan_without_information_schema_unchanged() -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let plan =
            optimized(&ctx, "SELECT column_1 FROM tt WHERE column_2 > 100").await?;

        assert_eq!(inline(&ctx, plan.clone()).await?, plan);

        Ok(())
    }

    #[tokio::test]
    async fn should_inline_the_largest_part_that_reads_only_information_schema()
    -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let plan = optimized(
            &ctx,
            "SELECT table_name, (SELECT count(*) FROM tt) AS row_count \
             FROM information_schema.tables WHERE table_name = 'tt'",
        )
        .await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        assert_eq!(scans(&plan), (1, 1));
        assert_eq!(scans(&inlined), (0, 1));
        // The filter ran with the scan, so only the matching row was inlined.
        assert_eq!(values_rows(&inlined), vec![1]);
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn should_inline_information_schema_inside_a_subquery() -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let plan = analyzed(
            &ctx,
            "SELECT column_1 FROM tt WHERE EXISTS \
             (SELECT 1 FROM information_schema.tables WHERE table_name = 'tt')",
        )
        .await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        assert_eq!(scans(&plan), (1, 1));
        assert_eq!(scans(&inlined), (0, 1));
        assert_eq!(values_rows(&inlined), vec![1]);
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn should_inline_only_the_scan_under_a_correlated_filter() -> Result<()> {
        let ctx = context();
        // Registered under the name of one of its customers, so the correlated
        // subquery below matches one row.
        register_customers(&ctx, "andy").await?;
        let plan = analyzed(
            &ctx,
            "SELECT column_1 FROM andy WHERE EXISTS \
             (SELECT 1 FROM information_schema.tables t \
              WHERE t.table_name = andy.column_1)",
        )
        .await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        assert_eq!(scans(&inlined), (0, 1));
        // The filter refers to the outer query, so it stays, and every table
        // was inlined rather than only the matching one.
        assert!(any_node(&inlined, LogicalPlan::contains_outer_reference));
        let inlined_rows = values_rows(&inlined);
        assert_eq!(inlined_rows.len(), 1);
        assert!(inlined_rows[0] > 1, "{inlined_rows:?}");
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn should_keep_the_qualified_names_of_joined_information_schema_tables()
    -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let plan = optimized(
            &ctx,
            "SELECT t.table_name, c.column_name, max(tt.column_2) AS top \
             FROM information_schema.tables t \
             JOIN information_schema.columns c ON t.table_name = c.table_name \
             CROSS JOIN tt \
             WHERE t.table_name = 'tt' \
             GROUP BY t.table_name, c.column_name",
        )
        .await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        assert_eq!(scans(&plan), (2, 1));
        assert_eq!(scans(&inlined), (0, 1));
        // Both information_schema tables ran together as one part, which
        // returned one row for each column of `tt`.
        assert_eq!(values_rows(&inlined), vec![2]);
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn should_inline_an_empty_information_schema_result() -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let plan = optimized(
            &ctx,
            "SELECT table_name, (SELECT count(*) FROM tt) AS row_count \
             FROM information_schema.tables WHERE table_name = 'missing'",
        )
        .await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        assert_eq!(scans(&inlined), (0, 1));
        // One placeholder row, which the limit removes.
        assert_eq!(values_rows(&inlined), vec![1]);
        assert!(
            inlined
                .display_indent()
                .to_string()
                .contains("Limit: skip=0, fetch=0"),
            "{}",
            inlined.display_indent()
        );
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn should_inline_an_information_schema_part_without_columns() -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let plan = optimized(
            &ctx,
            "SELECT count(*) AS n FROM information_schema.tables CROSS JOIN tt",
        )
        .await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        assert_eq!(scans(&inlined), (0, 1));
        // count(*) needs no columns, so the scan reads none, and its
        // replacement keeps only the row count.
        assert!(
            any_node(&inlined, |node| matches!(
                node,
                LogicalPlan::Projection(projection) if projection.expr.is_empty()
            )),
            "{}",
            inlined.display_indent()
        );
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn should_inline_repeated_information_schema_subqueries() -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let plan = optimized(
            &ctx,
            "SELECT column_1, \
             (SELECT count(*) FROM information_schema.tables) AS a, \
             (SELECT count(*) FROM information_schema.tables) AS b \
             FROM tt",
        )
        .await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        // A Ballista client plans each scalar subquery as a join with its own
        // alias, so the two are separate parts.
        assert!(
            !any_node(&plan, |node| matches!(node, LogicalPlan::Subquery(_))),
            "{}",
            plan.display_indent()
        );
        assert_eq!(values_rows(&inlined), vec![1, 1]);
        assert_eq!(scans(&inlined), (0, 1));
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn should_inline_an_information_schema_part_used_twice() -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let branch = "SELECT a.table_name, count(*) AS c \
                      FROM information_schema.tables a CROSS JOIN tt \
                      GROUP BY a.table_name";
        let plan = optimized(&ctx, &format!("{branch} UNION ALL {branch}")).await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        // Both branches read the same part, which is replaced in both.
        assert_eq!(scans(&plan), (2, 2));
        assert_eq!(scans(&inlined), (0, 2));
        assert_eq!(values_rows(&inlined).len(), 2);
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn should_inline_a_view_over_information_schema() -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        ctx.sql(
            "CREATE VIEW table_names AS \
             SELECT table_name FROM information_schema.tables",
        )
        .await?
        .collect()
        .await?;
        let plan = optimized(
            &ctx,
            "SELECT n.table_name, count(*) AS row_count \
             FROM table_names n CROSS JOIN tt \
             WHERE n.table_name = 'tt' GROUP BY n.table_name",
        )
        .await?;

        let inlined = inline(&ctx, plan.clone()).await?;

        assert_eq!(scans(&inlined), (0, 1));
        assert_eq!(values_rows(&inlined), vec![1]);
        assert_eq!(rows(&ctx, inlined).await?, rows(&ctx, plan).await?);

        Ok(())
    }

    #[tokio::test]
    async fn inlined_plans_survive_serialization() -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;

        for sql in [
            "SELECT table_name, (SELECT count(*) FROM tt) AS row_count \
             FROM information_schema.tables WHERE table_name = 'tt'",
            "SELECT table_name, (SELECT count(*) FROM tt) AS row_count \
             FROM information_schema.tables WHERE table_name = 'missing'",
            "SELECT count(*) AS n FROM information_schema.tables CROSS JOIN tt",
            "SELECT t.table_name, c.column_name, max(tt.column_2) AS top \
             FROM information_schema.tables t \
             JOIN information_schema.columns c ON t.table_name = c.table_name \
             CROSS JOIN tt \
             WHERE t.table_name = 'tt' \
             GROUP BY t.table_name, c.column_name",
        ] {
            let plan = optimized(&ctx, sql).await?;
            let decoded = round_trip(&ctx, &inline(&ctx, plan.clone()).await?)?;

            assert_eq!(
                names_and_types(decoded.schema()),
                names_and_types(plan.schema()),
                "{sql}"
            );
            assert_eq!(rows(&ctx, decoded).await?, rows(&ctx, plan).await?, "{sql}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_distribute_plans_mixing_information_schema_with_other_tables()
    -> Result<()> {
        let ctx = context();
        register_customers(&ctx, "tt").await?;
        let planner = BallistaQueryPlanner::<LogicalPlanNode>::new(
            "http://localhost:50050".to_string(),
            BallistaConfig::default(),
        );
        let query = "SELECT table_name, (SELECT count(*) FROM tt) AS row_count \
                     FROM information_schema.tables WHERE table_name = 'tt'";

        // The plan sent to the cluster, which `DistributedQueryExec` displays,
        // must not scan information_schema.
        let sends_no_information_schema = |physical_plan: &Arc<dyn ExecutionPlan>| {
            let displayed = displayable(physical_plan.as_ref()).indent(true).to_string();
            assert!(
                !displayed.contains("TableScan: information_schema"),
                "{displayed}"
            );
        };

        for sql in [query.to_string(), format!("EXPLAIN {query}")] {
            let plan = optimized(&ctx, &sql).await?;
            let physical_plan = planner.create_physical_plan(&plan, &ctx.state()).await?;

            assert!(
                physical_plan
                    .downcast_ref::<DistributedQueryExec<LogicalPlanNode>>()
                    .is_some(),
                "{sql}"
            );
            sends_no_information_schema(&physical_plan);
        }

        let plan = optimized(&ctx, &format!("EXPLAIN ANALYZE {query}")).await?;
        let physical_plan = planner.create_physical_plan(&plan, &ctx.state()).await?;
        assert!(
            physical_plan
                .downcast_ref::<DistributedExplainAnalyzeExec<LogicalPlanNode>>()
                .is_some()
        );
        sends_no_information_schema(&physical_plan);

        Ok(())
    }
}
