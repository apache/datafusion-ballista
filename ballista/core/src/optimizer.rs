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

//! Ballista-specific logical optimizer rules.
//!
//! [`NotInSubqueryRewrite`](crate::optimizer::NotInSubqueryRewrite) rewrites
//! uncorrelated `NOT IN (subquery)` filter
//! predicates into a plain anti join combined with a one-row count aggregate,
//! so the query never needs DataFusion's null-aware hash join. That join
//! coordinates probe-side state through in-process atomics, which forces
//! Ballista to execute it in a single task (see the discussion on
//! <https://github.com/apache/datafusion-ballista/issues/2198>). The rewritten
//! plan uses only ordinary joins and aggregates and distributes normally.

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchemaRef, Result};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::expr::InSubquery;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{
    Expr, ExprSchemable, Filter, JoinType, LogicalPlan, LogicalPlanBuilder,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::lit;

use crate::config::BallistaConfig;

/// Returns `existing` with Ballista's logical optimizer rules prepended.
///
/// [`NotInSubqueryRewrite`] must run before DataFusion's
/// `DecorrelatePredicateSubquery`, which would otherwise turn eligible `NOT IN`
/// predicates into null-aware anti joins first. A rule with the same name in
/// `existing` is dropped so repeated session upgrades stay idempotent.
pub fn with_ballista_optimizer_rules(
    existing: &[Arc<dyn OptimizerRule + Send + Sync>],
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let ballista_rule = NotInSubqueryRewrite::new();
    let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        Vec::with_capacity(existing.len() + 1);
    let name = ballista_rule.name().to_owned();
    rules.push(Arc::new(ballista_rule));
    rules.extend(existing.iter().filter(|rule| rule.name() != name).cloned());
    rules
}

/// Returns DataFusion's default logical optimizer rules with Ballista's rules
/// prepended. Used by every Ballista session builder.
pub fn ballista_default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    with_ballista_optimizer_rules(&datafusion::optimizer::Optimizer::default().rules)
}

/// Rewrites `expr NOT IN (subquery)` filter conjuncts into a distributable
/// plan shape before DataFusion's `DecorrelatePredicateSubquery` can turn them
/// into null-aware anti joins.
///
/// For a filter `... WHERE e NOT IN (SELECT b FROM s)` the rewrite produces:
///
/// ```text
/// Projection: <original input columns>
///   Filter: __cnt = 0 OR (e IS NOT NULL AND __cnt = __cnt_non_null)
///     CrossJoin
///       Aggregate: count(1) AS __cnt, count(b) AS __cnt_non_null  (over s)
///       LeftAnti Join: e = b
///         <input>
///         SubqueryAlias: s
/// ```
///
/// which is equivalent under SQL three-valued `NOT IN` semantics evaluated in
/// a `WHERE` context:
///
/// - `s` empty: every input row passes (`__cnt = 0`).
/// - `e` matches some `b`: the anti join drops the row.
/// - `e` is NULL and `s` is non-empty: the row is dropped.
/// - `s` contains a NULL `b`: every remaining row is dropped
///   (`__cnt <> __cnt_non_null`).
///
/// The rewrite only fires for predicates where DataFusion would otherwise
/// need null-aware semantics: uncorrelated subqueries with a single output
/// column where either side of the comparison is nullable. Everything else is
/// left for DataFusion's own subquery decorrelation, which already produces
/// distributable plans for those cases.
///
/// The rule is enabled by default and controlled by the
/// `ballista.optimizer.not_in_subquery_rewrite` configuration key.
#[derive(Debug, Default)]
pub struct NotInSubqueryRewrite {}

impl NotInSubqueryRewrite {
    /// Creates a new instance of the rewrite rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for NotInSubqueryRewrite {
    fn name(&self) -> &str {
        "not_in_subquery_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let enabled = config
            .options()
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.not_in_subquery_rewrite_enabled())
            .unwrap_or_else(|| {
                BallistaConfig::default().not_in_subquery_rewrite_enabled()
            });
        if !enabled {
            return Ok(Transformed::no(plan));
        }

        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };
        rewrite_filter(filter, config)
    }
}

/// Returns the [`InSubquery`] when the expression is a negated `IN` predicate,
/// in either its `expr NOT IN (subquery)` or `NOT (expr IN (subquery))` form.
fn as_negated_in_subquery(expr: &Expr) -> Option<&InSubquery> {
    match expr {
        Expr::InSubquery(in_subquery) if in_subquery.negated => Some(in_subquery),
        Expr::Not(inner) => match inner.as_ref() {
            Expr::InSubquery(in_subquery) if !in_subquery.negated => Some(in_subquery),
            _ => None,
        },
        _ => None,
    }
}

/// Returns the negated `IN` subquery to rewrite, or `None` when the conjunct
/// should be left for DataFusion's own subquery decorrelation: correlated
/// subqueries, multi-column subqueries, volatile probe expressions, and
/// predicates whose keys are provably non-nullable (those never become
/// null-aware joins in the first place).
fn rewritable_in_subquery<'a>(
    conjunct: &'a Expr,
    input_schema: &DFSchemaRef,
) -> Option<&'a InSubquery> {
    let in_subquery = as_negated_in_subquery(conjunct)?;
    if !in_subquery.subquery.outer_ref_columns.is_empty() {
        return None;
    }
    let subquery_schema = in_subquery.subquery.subquery.schema();
    if subquery_schema.fields().len() != 1 {
        return None;
    }
    if in_subquery.expr.is_volatile() {
        return None;
    }
    let expr_nullable = in_subquery
        .expr
        .nullable(input_schema.as_ref())
        .unwrap_or(true);
    if !expr_nullable && !subquery_schema.field(0).is_nullable() {
        // Both sides are non-nullable: DataFusion plans a plain anti join for
        // this case already, so the rewrite would only add an aggregate scan.
        return None;
    }
    Some(in_subquery)
}

/// Rewrites every eligible `NOT IN` conjunct of `filter`, returning the
/// original filter untouched when there is nothing to do.
fn rewrite_filter(
    filter: Filter,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let input_schema = filter.input.schema().clone();
    let has_candidate = split_conjunction_owned(filter.predicate.clone())
        .iter()
        .any(|conjunct| rewritable_in_subquery(conjunct, &input_schema).is_some());
    if !has_candidate {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    }

    let mut current = Arc::unwrap_or_clone(filter.input);
    let mut conjuncts = Vec::new();
    for conjunct in split_conjunction_owned(filter.predicate) {
        let Some(in_subquery) = rewritable_in_subquery(&conjunct, &input_schema) else {
            conjuncts.push(conjunct);
            continue;
        };

        let probe_expr = in_subquery.expr.as_ref().clone();
        let subquery_plan =
            Arc::unwrap_or_clone(Arc::clone(&in_subquery.subquery.subquery));

        let alias = config.alias_generator().next("__ballista_not_in");
        let count_all_name = format!("{alias}_cnt");
        let count_non_null_name = format!("{alias}_cnt_non_null");

        // Anti join drops the rows whose probe expression matches a subquery
        // value. The subquery side is aliased so a self-`NOT IN` does not
        // produce ambiguous column names in the join predicate.
        let aliased_subquery = LogicalPlanBuilder::from(subquery_plan.clone())
            .alias(alias.clone())?
            .build()?;
        let subquery_column = Expr::Column(Column::new(
            Some(alias),
            aliased_subquery.schema().field(0).name(),
        ));
        let anti_join = LogicalPlanBuilder::from(current)
            .join_on(
                aliased_subquery,
                JoinType::LeftAnti,
                [probe_expr.clone().eq(subquery_column)],
            )?
            .build()?;

        // One-row aggregate capturing the two global facts three-valued
        // `NOT IN` semantics need: whether the subquery is empty and whether
        // it contains a NULL. Placed on the build (left) side of the cross
        // join so only a single row is collected.
        let count_column =
            Expr::Column(Column::from(subquery_plan.schema().qualified_field(0)));
        let aggregate = LogicalPlanBuilder::from(subquery_plan)
            .aggregate(
                Vec::<Expr>::new(),
                vec![
                    count(lit(1)).alias(&count_all_name),
                    count(count_column).alias(&count_non_null_name),
                ],
            )?
            .build()?;
        current = LogicalPlanBuilder::from(aggregate)
            .cross_join(anti_join)?
            .build()?;

        // Rows surviving the anti join pass when the subquery is empty, or
        // when the probe value is non-NULL and the subquery contains no NULL.
        let count_all = Expr::Column(Column::new_unqualified(&count_all_name));
        let count_non_null = Expr::Column(Column::new_unqualified(&count_non_null_name));
        conjuncts.push(
            count_all
                .clone()
                .eq(lit(0i64))
                .or(probe_expr.is_not_null().and(count_all.eq(count_non_null))),
        );
    }

    let predicate =
        conjunction(conjuncts).expect("at least one rewritten conjunct must exist");
    let restore_columns: Vec<Expr> = input_schema
        .iter()
        .map(|qualified_field| Expr::Column(Column::from(qualified_field)))
        .collect();
    let new_plan = LogicalPlanBuilder::from(current)
        .filter(predicate)?
        .project(restore_columns)?
        .build()?;
    Ok(Transformed::yes(new_plan))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
    use datafusion::optimizer::Optimizer;
    use datafusion::prelude::{SessionConfig, col, lit, not_in_subquery};

    use super::NotInSubqueryRewrite;
    use crate::config::{BALLISTA_NOT_IN_SUBQUERY_REWRITE, BallistaConfig};

    fn scan(table: &str, field: &str, nullable: bool) -> LogicalPlan {
        let schema = Schema::new(vec![Field::new(field, DataType::Int32, nullable)]);
        LogicalPlanBuilder::scan(
            table,
            Arc::new(LogicalTableSource::new(Arc::new(schema))),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn optimize_with_config(plan: LogicalPlan, config: BallistaConfig) -> LogicalPlan {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_option_extension(config))
            .build();
        Optimizer::with_rules(vec![Arc::new(NotInSubqueryRewrite::new())])
            .optimize(plan, &state, |_, _| {})
            .unwrap()
    }

    fn optimize(plan: LogicalPlan) -> LogicalPlan {
        optimize_with_config(plan, BallistaConfig::default())
    }

    fn not_in_plan(nullable: bool) -> LogicalPlan {
        let subquery = scan("t2", "b", nullable);
        LogicalPlanBuilder::from(scan("t1", "a", nullable))
            .filter(not_in_subquery(col("a"), Arc::new(subquery)))
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn rewrites_nullable_not_in_to_anti_join_with_count_aggregate() {
        let plan = not_in_plan(true);
        let schema = plan.schema().clone();

        let optimized = optimize(plan);
        let display = format!("{}", optimized.display_indent());

        assert!(
            !display.contains("IN (<subquery>)"),
            "InSubquery must be rewritten away:\n{display}"
        );
        assert!(
            display.contains("LeftAnti"),
            "rewrite must produce an anti join:\n{display}"
        );
        assert!(
            display.contains("count("),
            "rewrite must produce a count aggregate:\n{display}"
        );
        assert_eq!(
            optimized.schema().as_ref(),
            schema.as_ref(),
            "rewrite must preserve the plan schema"
        );
    }

    #[test]
    fn rewrites_negated_in_subquery_wrapped_in_not() {
        let subquery = scan("t2", "b", true);
        let in_expr = datafusion::prelude::in_subquery(col("a"), Arc::new(subquery));
        let plan = LogicalPlanBuilder::from(scan("t1", "a", true))
            .filter(Expr::Not(Box::new(in_expr)))
            .unwrap()
            .build()
            .unwrap();

        let display = format!("{}", optimize(plan).display_indent());
        assert!(
            !display.contains("IN (<subquery>)"),
            "NOT (IN subquery) must be rewritten away:\n{display}"
        );
        assert!(display.contains("LeftAnti"), "{display}");
    }

    #[test]
    fn preserves_other_filter_conjuncts() {
        let subquery = scan("t2", "b", true);
        let plan = LogicalPlanBuilder::from(scan("t1", "a", true))
            .filter(
                col("a")
                    .gt(lit(5))
                    .and(not_in_subquery(col("a"), Arc::new(subquery))),
            )
            .unwrap()
            .build()
            .unwrap();

        let display = format!("{}", optimize(plan).display_indent());
        assert!(!display.contains("IN (<subquery>)"), "{display}");
        assert!(
            display.contains("t1.a > Int32(5)"),
            "other conjuncts must survive the rewrite:\n{display}"
        );
    }

    #[test]
    fn keeps_correlated_not_in_subquery() {
        let sub = scan("t2", "b", true);
        let mut expr = not_in_subquery(col("a"), Arc::new(sub));
        if let Expr::InSubquery(ref mut in_subquery) = expr {
            in_subquery.subquery.outer_ref_columns.push(
                datafusion::logical_expr::expr_fn::out_ref_col(DataType::Int32, "t1.a"),
            );
        } else {
            unreachable!("not_in_subquery must build an InSubquery expression");
        }
        let plan = LogicalPlanBuilder::from(scan("t1", "a", true))
            .filter(expr)
            .unwrap()
            .build()
            .unwrap();

        let display = format!("{}", optimize(plan).display_indent());
        assert!(
            display.contains("IN (<subquery>)"),
            "correlated subqueries must be left to DataFusion:\n{display}"
        );
    }

    #[test]
    fn keeps_not_in_when_keys_are_not_nullable() {
        let display = format!("{}", optimize(not_in_plan(false)).display_indent());
        assert!(
            display.contains("IN (<subquery>)"),
            "non-nullable keys do not need the rewrite:\n{display}"
        );
    }

    #[test]
    fn keeps_positive_in_subquery() {
        let subquery = scan("t2", "b", true);
        let plan = LogicalPlanBuilder::from(scan("t1", "a", true))
            .filter(datafusion::prelude::in_subquery(
                col("a"),
                Arc::new(subquery),
            ))
            .unwrap()
            .build()
            .unwrap();

        let display = format!("{}", optimize(plan).display_indent());
        assert!(
            display.contains("IN (<subquery>)"),
            "positive IN subqueries must not be rewritten:\n{display}"
        );
    }

    #[test]
    fn disabled_by_configuration() {
        use datafusion::config::ExtensionOptions;

        let mut config = BallistaConfig::default();
        config
            .set(
                BALLISTA_NOT_IN_SUBQUERY_REWRITE
                    .strip_prefix("ballista.")
                    .unwrap(),
                "false",
            )
            .unwrap();

        let display = format!(
            "{}",
            optimize_with_config(not_in_plan(true), config).display_indent()
        );
        assert!(
            display.contains("IN (<subquery>)"),
            "disabling the config key must skip the rewrite:\n{display}"
        );
    }
}
