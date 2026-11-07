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

//! Pushes semi and anti joins below the joins and projections that sit between
//! them and the input their keys come from.
//!
//! DataFusion decorrelates `IN`, `EXISTS` and `NOT EXISTS` subqueries into
//! semi and anti joins placed directly above the `FROM` clause's join tree, so
//! the subquery filters the joined result. In TPC-H q18 the subquery keeps
//! 6,400 orders, but only after `orders` has been joined to `lineitem`:
//!
//! ```text
//! LeftSemi Join: orders.o_orderkey = __correlated_sq_1.l_orderkey
//!   Inner Join: orders.o_orderkey = lineitem.l_orderkey
//!     TableScan: orders
//!     TableScan: lineitem
//!   SubqueryAlias: __correlated_sq_1
//! ```
//!
//! The semi join only reads `orders.o_orderkey`, so this rule moves it onto
//! `orders`, before the join:
//!
//! ```text
//! Inner Join: orders.o_orderkey = lineitem.l_orderkey
//!   LeftSemi Join: orders.o_orderkey = __correlated_sq_1.l_orderkey
//!     TableScan: orders
//!     SubqueryAlias: __correlated_sq_1
//!   TableScan: lineitem
//! ```
//!
//! This is Spark's `PushDownLeftSemiAntiJoin`. A semi or anti join keeps or
//! drops each row of its left input based only on that row's values, so it
//! commutes with any operator that passes the referenced columns through
//! unchanged and never makes a row depend on another row:
//!
//! - a `Projection` that outputs every referenced column as a plain column
//!   and has no volatile expression;
//! - an inner join, onto the side that holds every referenced column, unless
//!   the other side is filtered;
//! - a left (right) outer join, onto its left (right), preserved, side only.
//!   Pushing into the null-supplying side would turn a dropped row into a
//!   null-extended one.
//!
//! It deliberately stops at two places where the move is sound but slower. A
//! `Filter` is the cheaper of the two operators, so the semi join should see
//! its output (TPC-H q4). An inner join whose other input is filtered can
//! remove most rows itself, so a semi join moved below it probes rows the join
//! would have dropped (TPC-H q21's `EXISTS` and `NOT EXISTS`; Spark makes this
//! move unconditionally). `PushDownFilter` would also move a filter back above
//! the semi join on the next pass.
//!
//! The rule runs after DataFusion's own rules in each optimizer pass, so it
//! sees decorrelated joins with their filters already pushed down.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::logical_expr::{Expr, Join, JoinType, LogicalPlan, Projection};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use crate::config::BallistaConfig;

/// Pushes semi and anti joins below inner joins and projections,
/// onto the join input that holds every column they reference. See the
/// module documentation for the rewrite and why it is sound.
///
/// Enabled by default and controlled by the
/// `ballista.optimizer.semi_join_pushdown` configuration key.
#[derive(Debug, Default)]
pub struct SemiJoinPushdown {}

impl SemiJoinPushdown {
    /// Creates a new instance of the rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SemiJoinPushdown {
    fn name(&self) -> &str {
        "semi_join_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Top-down: a semi join pushed one level lands in a child, which the
        // optimizer visits next and can push further.
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Cheap node checks first: this runs on every node of every pass.
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };
        if !is_pushable_semi_join(&join) {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        let enabled = config
            .options()
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.semi_join_pushdown_enabled())
            .unwrap_or_else(|| BallistaConfig::default().semi_join_pushdown_enabled());
        if !enabled {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        let referenced = left_referenced_columns(&join);
        match push_one_level(&join, &referenced)? {
            Some(plan) => Ok(Transformed::yes(plan)),
            None => Ok(Transformed::no(LogicalPlan::Join(join))),
        }
    }
}

/// A left semi or anti join whose keep/drop decision depends only on the
/// left row, over an operator this rule can move it below. Null-aware anti
/// joins (`NOT IN`) also depend only on the left row and the whole subquery,
/// but they are left alone: Ballista executes them in a single task and the
/// `NotInSubqueryRewrite` rule handles them.
fn is_pushable_semi_join(join: &Join) -> bool {
    matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        && !join.null_aware
        && matches!(
            join.left.as_ref(),
            LogicalPlan::Projection(_) | LogicalPlan::Join(_)
        )
}

/// Columns of the semi join's left input that its keys and filter read.
pub(super) fn left_referenced_columns(join: &Join) -> HashSet<Column> {
    let left_schema = join.left.schema();
    let mut cols: HashSet<Column> = HashSet::new();
    for (l, _) in &join.on {
        cols.extend(l.column_refs().into_iter().cloned());
    }
    if let Some(filter) = &join.filter {
        cols.extend(
            filter
                .column_refs()
                .into_iter()
                .filter(|c| left_schema.has_column(c))
                .cloned(),
        );
    }
    cols
}

fn has_all(schema: &DFSchema, cols: &HashSet<Column>) -> bool {
    cols.iter().all(|c| schema.has_column(c))
}

/// Whether `plan` filters any of its inputs, so a join with it may remove
/// rows: a `Filter` anywhere in it, a scan with pushed-down filters, or a
/// semi or anti join. Logical plans carry no statistics, so this syntactic test
/// stands in for "the other input is selective".
fn is_filtered(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| {
        Ok(match node {
            LogicalPlan::Filter(_) => true,
            LogicalPlan::TableScan(scan) => !scan.filters.is_empty(),
            LogicalPlan::Join(j) => matches!(
                j.join_type,
                JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightSemi
                    | JoinType::RightAnti
            ),
            _ => false,
        })
    })
}

/// Rebuilds `join` with new inputs.
pub(super) fn with_inputs(
    join: &Join,
    left: Arc<LogicalPlan>,
    right: Arc<LogicalPlan>,
) -> Result<Join> {
    Join::try_new(
        left,
        right,
        join.on.clone(),
        join.filter.clone(),
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )
}

/// Rebuilds `semi` with a new left input.
fn semi_over(semi: &Join, left: Arc<LogicalPlan>) -> Result<Join> {
    with_inputs(semi, left, Arc::clone(&semi.right))
}

/// Moves `semi` one operator down its left input, or returns `None` when the
/// operator below it does not commute with it.
fn push_one_level(
    semi: &Join,
    referenced: &HashSet<Column>,
) -> Result<Option<LogicalPlan>> {
    step(semi, referenced, true, &mut |moved, _| {
        Ok(LogicalPlan::Join(moved))
    })
}

/// Sinks `semi` as far down its left input as it commutes, returning the
/// rewritten plan and how many joins it moved below.
///
/// Unlike this rule's own pushdown it also moves below joins with a filtered
/// input: the caller adds semi joins against small, selective dimensions,
/// which are worth applying before any other join.
pub(super) fn sink(semi: Join) -> Result<(LogicalPlan, usize)> {
    let referenced = left_referenced_columns(&semi);
    let mut joins_passed = 0;
    let plan = sink_inner(semi, &referenced, &mut joins_passed)?;
    Ok((plan, joins_passed))
}

fn sink_inner(
    semi: Join,
    referenced: &HashSet<Column>,
    joins_passed: &mut usize,
) -> Result<LogicalPlan> {
    let moved = step(&semi, referenced, false, &mut |moved, through_join| {
        if through_join {
            *joins_passed += 1;
        }
        sink_inner(moved, referenced, joins_passed)
    })?;
    Ok(moved.unwrap_or(LogicalPlan::Join(semi)))
}

/// If `semi` commutes with the operator directly below it, rebuilds that
/// operator with `semi` moved beneath it. `place` receives the moved semi join
/// (and whether it moved below a join) and returns the plan to put in its
/// position. Returns `None` when the operator below does not commute, or, with
/// `avoid_filtered_joins`, when it is an inner join whose other input is
/// filtered.
fn step(
    semi: &Join,
    referenced: &HashSet<Column>,
    avoid_filtered_joins: bool,
    place: &mut dyn FnMut(Join, bool) -> Result<LogicalPlan>,
) -> Result<Option<LogicalPlan>> {
    if referenced.is_empty() {
        // An uncorrelated EXISTS keeps all or none of the rows; leave it.
        return Ok(None);
    }
    match semi.left.as_ref() {
        LogicalPlan::Projection(projection) => {
            // A volatile expression must be evaluated on the rows the query
            // asked for, not on the fewer that survive the semi join.
            if projection.expr.iter().any(|e| e.is_volatile()) {
                return Ok(None);
            }
            // Every referenced column must be passed through unchanged, so
            // the name the semi join uses means the same value below.
            let passthrough: HashSet<&Column> = projection
                .expr
                .iter()
                .filter_map(|e| match e {
                    Expr::Column(c) => Some(c),
                    _ => None,
                })
                .collect();
            if !referenced.iter().all(|c| passthrough.contains(c))
                || !has_all(projection.input.schema(), referenced)
            {
                return Ok(None);
            }
            let pushed = place(semi_over(semi, Arc::clone(&projection.input))?, false)?;
            Ok(Some(LogicalPlan::Projection(Projection::try_new(
                projection.expr.clone(),
                Arc::new(pushed),
            )?)))
        }
        LogicalPlan::Join(inner) => {
            if inner.null_aware {
                return Ok(None);
            }
            let (to_left, to_right) = match inner.join_type {
                JoinType::Inner => (true, true),
                JoinType::Left => (true, false),
                JoinType::Right => (false, true),
                _ => (false, false),
            };
            // Moving below an inner join whose other input is filtered would
            // make the semi join see rows that join could have removed first.
            // An outer join keeps every row of its preserved side, so there
            // is nothing to wait for.
            let inner_join = avoid_filtered_joins && inner.join_type == JoinType::Inner;
            let (new_left, new_right) = if to_left
                && has_all(inner.left.schema(), referenced)
                && !(inner_join && is_filtered(&inner.right)?)
            {
                (
                    Arc::new(place(semi_over(semi, Arc::clone(&inner.left))?, true)?),
                    Arc::clone(&inner.right),
                )
            } else if to_right
                && has_all(inner.right.schema(), referenced)
                && !(inner_join && is_filtered(&inner.left)?)
            {
                (
                    Arc::clone(&inner.left),
                    Arc::new(place(semi_over(semi, Arc::clone(&inner.right))?, true)?),
                )
            } else {
                return Ok(None);
            };
            Ok(Some(LogicalPlan::Join(with_inputs(
                inner, new_left, new_right,
            )?)))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinType;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
    use datafusion::optimizer::Optimizer;
    use datafusion::prelude::{SessionConfig, col, lit, random};

    use super::SemiJoinPushdown;
    use crate::config::{BALLISTA_SEMI_JOIN_PUSHDOWN, BallistaConfig};

    fn scan(table: &str, fields: &[&str]) -> LogicalPlan {
        let schema = Schema::new(
            fields
                .iter()
                .map(|f| Field::new(*f, DataType::Int32, true))
                .collect::<Vec<_>>(),
        );
        LogicalPlanBuilder::scan(
            table,
            Arc::new(LogicalTableSource::new(Arc::new(schema))),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn optimize_with(plan: LogicalPlan, config: BallistaConfig) -> String {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_option_extension(config))
            .build();
        let optimized = Optimizer::with_rules(vec![Arc::new(SemiJoinPushdown::new())])
            .optimize(plan.clone(), &state, |_, _| {})
            .unwrap();
        assert_eq!(
            optimized.schema().as_ref(),
            plan.schema().as_ref(),
            "the rewrite must preserve the plan schema"
        );
        format!("{}", optimized.display_indent())
    }

    fn optimize(plan: LogicalPlan) -> String {
        optimize_with(plan, BallistaConfig::default())
    }

    /// `(a JOIN b ON a.k = b.k) <join_type> s ON <key> = s.x`, with an
    /// optional projection between the semi join and the inner join.
    fn semi_over_join(
        inner_type: JoinType,
        semi_type: JoinType,
        key: &str,
        project: bool,
    ) -> LogicalPlan {
        let mut builder = LogicalPlanBuilder::from(scan("a", &["k", "v"]))
            .join_on(
                scan("b", &["k", "w"]),
                inner_type,
                [col("a.k").eq(col("b.k"))],
            )
            .unwrap();
        if project {
            builder = builder
                .project(vec![col("a.k"), col("a.v"), col("b.w")])
                .unwrap();
        }
        builder
            .join_on(scan("s", &["x"]), semi_type, [col(key).eq(col("s.x"))])
            .unwrap()
            .build()
            .unwrap()
    }

    /// The line index of the first line containing `needle`.
    fn line_of(display: &str, needle: &str) -> usize {
        display
            .lines()
            .position(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("{needle:?} not found in:\n{display}"))
    }

    #[test]
    fn pushes_semi_join_onto_the_inner_join_side_holding_its_key() {
        let display = optimize(semi_over_join(
            JoinType::Inner,
            JoinType::LeftSemi,
            "a.v",
            false,
        ));
        let inner = line_of(&display, "Inner Join");
        let semi = line_of(&display, "LeftSemi Join");
        assert!(
            semi > inner,
            "semi join must move below the inner join:\n{display}"
        );
        assert!(
            line_of(&display, "TableScan: a") == semi + 1,
            "semi join must sit directly above `a`:\n{display}"
        );
    }

    #[test]
    fn pushes_onto_the_right_side_of_an_inner_join() {
        let display = optimize(semi_over_join(
            JoinType::Inner,
            JoinType::LeftAnti,
            "b.w",
            false,
        ));
        let semi = line_of(&display, "LeftAnti Join");
        assert!(semi > line_of(&display, "Inner Join"), "{display}");
        assert_eq!(line_of(&display, "TableScan: b"), semi + 1, "{display}");
    }

    #[test]
    fn pushes_through_a_passthrough_projection() {
        let display = optimize(semi_over_join(
            JoinType::Inner,
            JoinType::LeftSemi,
            "a.v",
            true,
        ));
        assert!(
            line_of(&display, "LeftSemi Join") > line_of(&display, "Projection"),
            "{display}"
        );
        assert!(
            line_of(&display, "LeftSemi Join") > line_of(&display, "Inner Join"),
            "{display}"
        );
    }

    #[test]
    fn keeps_semi_join_reading_both_inner_join_sides() {
        // A key computed from both sides cannot move to either side.
        let plan = LogicalPlanBuilder::from(scan("a", &["k", "v"]))
            .join_on(
                scan("b", &["k", "w"]),
                JoinType::Inner,
                [col("a.k").eq(col("b.k"))],
            )
            .unwrap()
            .join_on(
                scan("s", &["x"]),
                JoinType::LeftSemi,
                [(col("a.v") + col("b.w")).eq(col("s.x"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let display = optimize(plan);
        assert!(
            line_of(&display, "LeftSemi Join") < line_of(&display, "Inner Join"),
            "{display}"
        );
    }

    #[test]
    fn keeps_semi_join_off_the_null_supplying_side_of_an_outer_join() {
        let display = optimize(semi_over_join(
            JoinType::Left,
            JoinType::LeftSemi,
            "b.w",
            false,
        ));
        assert!(
            line_of(&display, "LeftSemi Join") < line_of(&display, "Left Join"),
            "pushing into the null-supplying side changes results:\n{display}"
        );
    }

    #[test]
    fn pushes_onto_the_preserved_side_of_an_outer_join() {
        let display = optimize(semi_over_join(
            JoinType::Left,
            JoinType::LeftAnti,
            "a.v",
            false,
        ));
        assert!(
            line_of(&display, "LeftAnti Join") > line_of(&display, "Left Join"),
            "{display}"
        );
    }

    /// `a <join_type> filtered(b)`, semi-joined with `s` on `a.v`.
    fn semi_over_join_with_filtered_input(join_type: JoinType) -> String {
        let plan = LogicalPlanBuilder::from(scan("a", &["k", "v"]))
            .join_on(
                LogicalPlanBuilder::from(scan("b", &["k", "w"]))
                    .filter(col("b.w").eq(lit(1)))
                    .unwrap()
                    .build()
                    .unwrap(),
                join_type,
                [col("a.k").eq(col("b.k"))],
            )
            .unwrap()
            .join_on(
                scan("s", &["x"]),
                JoinType::LeftSemi,
                [col("a.v").eq(col("s.x"))],
            )
            .unwrap()
            .build()
            .unwrap();
        optimize(plan)
    }

    #[test]
    fn stays_above_an_inner_join_with_a_filtered_input() {
        let display = semi_over_join_with_filtered_input(JoinType::Inner);
        assert!(
            line_of(&display, "LeftSemi Join") < line_of(&display, "Inner Join"),
            "the filtered join must run first:\n{display}"
        );
    }

    #[test]
    fn pushes_below_an_outer_join_with_a_filtered_input() {
        let display = semi_over_join_with_filtered_input(JoinType::Left);
        assert!(
            line_of(&display, "LeftSemi Join") > line_of(&display, "Left Join"),
            "an outer join keeps its preserved rows, so nothing is lost by going first:\n{display}"
        );
    }

    #[test]
    fn stays_above_a_filter() {
        let plan = LogicalPlanBuilder::from(scan("a", &["k", "v"]))
            .filter(col("a.v").gt(lit(3)))
            .unwrap()
            .join_on(
                scan("s", &["x"]),
                JoinType::LeftSemi,
                [col("a.k").eq(col("s.x"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let display = optimize(plan);
        assert!(
            line_of(&display, "LeftSemi Join") < line_of(&display, "Filter: a.v"),
            "the filter must run before the semi join:\n{display}"
        );
    }

    #[test]
    fn keeps_projection_that_computes_the_key() {
        let plan = LogicalPlanBuilder::from(scan("a", &["k", "v"]))
            .project(vec![(col("a.v") + lit(1)).alias("v1"), col("a.k")])
            .unwrap()
            .join_on(
                scan("s", &["x"]),
                JoinType::LeftSemi,
                [col("v1").eq(col("s.x"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let display = optimize(plan);
        assert!(
            line_of(&display, "LeftSemi Join") < line_of(&display, "Projection"),
            "{display}"
        );
    }

    #[test]
    fn keeps_projection_with_a_volatile_expression() {
        let plan = LogicalPlanBuilder::from(scan("a", &["k", "v"]))
            .project(vec![col("a.k"), random().alias("r")])
            .unwrap()
            .join_on(
                scan("s", &["x"]),
                JoinType::LeftSemi,
                [col("a.k").eq(col("s.x"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let display = optimize(plan);
        assert!(
            line_of(&display, "LeftSemi Join") < line_of(&display, "Projection"),
            "{display}"
        );
    }

    #[test]
    fn non_equi_filter_columns_decide_the_side() {
        // EXISTS (... WHERE s.x = a.v AND s.x <> b.w): the filter reads `b`,
        // so the semi join cannot move onto `a` alone.
        let plan = LogicalPlanBuilder::from(scan("a", &["k", "v"]))
            .join_on(
                scan("b", &["k", "w"]),
                JoinType::Inner,
                [col("a.k").eq(col("b.k"))],
            )
            .unwrap()
            .join_on(
                scan("s", &["x"]),
                JoinType::LeftSemi,
                [col("a.v").eq(col("s.x")), col("s.x").not_eq(col("b.w"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let display = optimize(plan);
        assert!(
            line_of(&display, "LeftSemi Join") < line_of(&display, "Inner Join"),
            "{display}"
        );
    }

    #[test]
    fn disabled_by_configuration() {
        use datafusion::config::ExtensionOptions;

        let mut config = BallistaConfig::default();
        config
            .set(
                BALLISTA_SEMI_JOIN_PUSHDOWN
                    .strip_prefix("ballista.")
                    .unwrap(),
                "false",
            )
            .unwrap();
        let display = optimize_with(
            semi_over_join(JoinType::Inner, JoinType::LeftSemi, "a.v", false),
            config,
        );
        assert!(
            line_of(&display, "LeftSemi Join") < line_of(&display, "Inner Join"),
            "{display}"
        );
    }
}
