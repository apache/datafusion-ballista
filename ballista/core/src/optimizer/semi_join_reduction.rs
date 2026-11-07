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

//! Applies a selective dimension filter early, below the joins that sit
//! between it and the table it filters.
//!
//! TPC-H q7 joins `supplier ⋈ lineitem ⋈ orders ⋈ customer` before it joins
//! `nation n1` (filtered to two of 25 nations) on `s_nationkey`. Every
//! supplier's lineitems, 182M rows at SF100, are joined with orders and
//! customer and shuffled twice before the nation filter drops 92% of them.
//!
//! For an inner join on `a.k = d.k` where `d` is a small filtered dimension
//! table, every row the join keeps has an `a.k` that appears in `d`. So a
//! semi join `a SEMI JOIN d ON a.k = d.k` can be added to the other side
//! without changing the result, and [`super::semi_join_pushdown`] sinks it
//! down to the input `a.k` comes from:
//!
//! ```text
//! Inner Join: supplier.s_nationkey = n1.n_nationkey
//!   ... Inner Join: supplier.s_suppkey = lineitem.l_suppkey
//!         LeftSemi Join: supplier.s_nationkey = __ballista_reduce_1.n_nationkey
//!           TableScan: supplier
//!           SubqueryAlias: __ballista_reduce_1
//!             Filter: n_name = 'FRANCE' OR n_name = 'GERMANY'
//!               TableScan: nation
//!         TableScan: lineitem
//!   SubqueryAlias: n1 ...
//! ```
//!
//! Spark does the same with a runtime bloom filter (`InjectRuntimeFilter`),
//! gated by table statistics. Logical plans carry no statistics, so
//! [`apply_semi_join_reduction`] runs before physical planning: it finds the
//! candidate dimensions by shape (a single table scan whose filter compares
//! columns only with `=`, `IN` and `LIKE` against literals, such as nation
//! names or part types), plans each one to read its estimated row count, and
//! adds semi joins only for those under
//! `ballista.optimizer.broadcast_join_threshold_rows`, so the copy is cheap to
//! scan and to broadcast. A `l_returnflag = 'R'` filter on `lineitem` has the
//! same shape as `n_name = 'FRANCE'` but is rejected on size. The semi join
//! is only added when it moves below at least one join; directly above `a`
//! it would just repeat the join it came from.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{
    BinaryExpr, Expr, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder,
    Operator,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{Optimizer, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use log::debug;

use super::SemiJoinPushdown;
use super::semi_join_pushdown::sink;
use crate::config::BallistaConfig;

/// Alias prefix of the dimension copies this rule adds. Also how the rule
/// recognises its own semi joins on later optimizer passes.
const REDUCE_ALIAS: &str = "__ballista_reduce";

/// Adds a semi join against a selective dimension below the joins between it
/// and the table its key comes from. See the [module documentation](self).
///
/// Only dimensions listed in `small` (by [`dimension_key`]) are used, so the
/// rule is not part of the default optimizer rules: run it through
/// [`apply_semi_join_reduction`], which sizes the candidates first.
#[derive(Debug, Default)]
pub struct SemiJoinReduction {
    small: Arc<HashSet<String>>,
}

impl SemiJoinReduction {
    /// Creates the rule for the dimensions whose [`dimension_key`] is in `small`.
    pub fn new(small: Arc<HashSet<String>>) -> Self {
        Self { small }
    }
}

/// Identifies a dimension subplan between the candidate scan and the rewrite.
pub fn dimension_key(plan: &LogicalPlan) -> String {
    plan.display_indent().to_string()
}

/// Adds semi joins for the selective dimensions of `plan` (see the
/// [module documentation](self)) and returns the optimized result, or `plan`
/// unchanged when none qualifies or the rule is disabled.
///
/// Optimizes `plan` with the state's rules first, so candidates are found in
/// the same shape the physical planner will see; each candidate dimension is
/// then planned on its own to read its estimated row count.
pub async fn apply_semi_join_reduction(
    state: &SessionState,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    let config = state
        .config_options()
        .extensions
        .get::<BallistaConfig>()
        .cloned()
        .unwrap_or_default();
    if !config.semi_join_reduction_enabled() {
        return Ok(plan.clone());
    }
    let optimized = state.optimize(plan)?;
    let candidates = candidate_dimensions(&optimized);
    if candidates.is_empty() {
        return Ok(plan.clone());
    }
    let threshold = config.broadcast_join_threshold_rows();
    let mut small = HashSet::new();
    for dim in candidates {
        let key = dimension_key(&dim);
        if small.contains(&key) {
            continue;
        }
        let physical = state
            .query_planner()
            .create_physical_plan(&dim, state)
            .await?;
        let rows = StatisticsContext::new()
            .compute(physical.as_ref(), &StatisticsArgs::new())?
            .num_rows;
        debug!("semi join reduction candidate rows={rows:?}:\n{key}");
        if rows.get_value().is_some_and(|r| *r <= threshold) {
            small.insert(key);
        }
    }
    if small.is_empty() {
        return Ok(plan.clone());
    }
    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(SemiJoinReduction::new(Arc::new(small))),
        Arc::new(SemiJoinPushdown::new()),
    ];
    Optimizer::with_rules(rules).optimize(optimized, state, |_, _| {})
}

/// The dimension side of every inner join that [`SemiJoinReduction`] could
/// use, before sizing.
fn candidate_dimensions(plan: &LogicalPlan) -> Vec<LogicalPlan> {
    let mut out = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Join(j) = node
            && j.join_type == JoinType::Inner
            && !j.on.is_empty()
        {
            for (dim, other) in [(&j.right, &j.left), (&j.left, &j.right)] {
                if is_selective_dimension(dim) && contains_join(other) {
                    out.push(dim.as_ref().clone());
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("visiting a plan cannot fail");
    out
}

impl OptimizerRule for SemiJoinReduction {
    fn name(&self) -> &str {
        "semi_join_reduction"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
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
        let enabled = config
            .options()
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.semi_join_reduction_enabled())
            .unwrap_or_else(|| BallistaConfig::default().semi_join_reduction_enabled());
        if !enabled {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };
        if join.join_type != JoinType::Inner || join.on.is_empty() || join.null_aware {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        // Try the right side as the dimension first (the usual shape after
        // DataFusion's FROM-order planning), then the left.
        for dim_on_right in [true, false] {
            if let Some(rewritten) = try_reduce(&join, dim_on_right, &self.small, config)?
            {
                return Ok(Transformed::yes(rewritten));
            }
        }
        Ok(Transformed::no(LogicalPlan::Join(join)))
    }
}

/// Adds the semi join for one orientation of `join`, or returns `None`.
fn try_reduce(
    join: &Join,
    dim_on_right: bool,
    small: &HashSet<String>,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    let (dim, other) = if dim_on_right {
        (&join.right, &join.left)
    } else {
        (&join.left, &join.right)
    };
    if !is_selective_dimension(dim)
        || !contains_join(other)
        || !small.contains(&dimension_key(dim))
    {
        return Ok(None);
    }
    // Plain column keys only: the other side's key must survive unchanged
    // down to the input it comes from, and the dimension key is re-qualified
    // with the copy's alias.
    let mut keys = Vec::with_capacity(join.on.len());
    for (l, r) in &join.on {
        let (other_key, dim_key) = if dim_on_right { (l, r) } else { (r, l) };
        match (other_key, dim_key) {
            (Expr::Column(o), Expr::Column(d)) => keys.push((o.clone(), d.clone())),
            _ => return Ok(None),
        }
    }
    if already_reduced(other, &keys) {
        return Ok(None);
    }

    let alias = config.alias_generator().next(REDUCE_ALIAS);
    let dim_copy = LogicalPlanBuilder::from(Arc::unwrap_or_clone(Arc::clone(dim)))
        .alias(alias.clone())?
        .build()?;
    let on = keys
        .iter()
        .map(|(o, d)| {
            (
                Expr::Column(o.clone()),
                Expr::Column(Column::new(Some(alias.clone()), d.name.clone())),
            )
        })
        .collect();
    let semi = Join::try_new(
        Arc::clone(other),
        Arc::new(dim_copy),
        on,
        None,
        JoinType::LeftSemi,
        JoinConstraint::On,
        join.null_equality,
        false,
    )?;
    let (sunk, joins_passed) = sink(semi)?;
    if joins_passed == 0 {
        return Ok(None);
    }
    let (left, right) = if dim_on_right {
        (Arc::new(sunk), Arc::clone(&join.right))
    } else {
        (Arc::clone(&join.left), Arc::new(sunk))
    };
    Ok(Some(LogicalPlan::Join(Join::try_new(
        left,
        right,
        join.on.clone(),
        join.filter.clone(),
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )?)))
}

/// Whether `plan` already holds a semi join this rule added on the same keys,
/// so later optimizer passes do not add it again.
fn already_reduced(plan: &LogicalPlan, keys: &[(Column, Column)]) -> bool {
    let mut found = false;
    plan.apply(|node| {
        if let LogicalPlan::Join(j) = node
            && j.join_type == JoinType::LeftSemi
            && let LogicalPlan::SubqueryAlias(a) = j.right.as_ref()
            && a.alias.table().starts_with(REDUCE_ALIAS)
            && j.on.len() == keys.len()
            && j.on
                .iter()
                .zip(keys)
                .all(|((l, _), (o, _))| matches!(l, Expr::Column(c) if c == o))
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("visiting a plan cannot fail");
    found
}

fn contains_join(plan: &LogicalPlan) -> bool {
    let mut found = false;
    plan.apply(|node| {
        if matches!(node, LogicalPlan::Join(_)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("visiting a plan cannot fail");
    found
}

/// A single table scan, under optional aliases and projections, whose filters
/// compare columns only for equality or pattern matches against literals.
fn is_selective_dimension(plan: &LogicalPlan) -> bool {
    let mut node = plan;
    let mut predicates: Vec<&Expr> = Vec::new();
    loop {
        match node {
            LogicalPlan::SubqueryAlias(a) => node = a.input.as_ref(),
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::Filter(f) => {
                predicates.push(&f.predicate);
                node = f.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => {
                predicates.extend(scan.filters.iter());
                break;
            }
            _ => return false,
        }
    }
    let mut has_point = false;
    predicates
        .iter()
        .all(|p| is_point_predicate(p, &mut has_point))
        && has_point
}

/// `=`, `IN` and `LIKE` comparisons of a column with literals, combined with
/// `AND` / `OR`, plus `IS NOT NULL` (which DataFusion adds for join keys).
fn is_point_predicate(expr: &Expr, has_point: &mut bool) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And | Operator::Or => {
                is_point_predicate(left, has_point)
                    && is_point_predicate(right, has_point)
            }
            Operator::Eq => {
                let ok = matches!(
                    (left.as_ref(), right.as_ref()),
                    (Expr::Column(_), Expr::Literal(..))
                        | (Expr::Literal(..), Expr::Column(_))
                );
                *has_point |= ok;
                ok
            }
            _ => false,
        },
        Expr::InList(list) => {
            let ok = !list.negated
                && matches!(list.expr.as_ref(), Expr::Column(_))
                && list.list.iter().all(|e| matches!(e, Expr::Literal(..)));
            *has_point |= ok;
            ok
        }
        Expr::Like(like) => {
            let ok = !like.negated
                && matches!(like.expr.as_ref(), Expr::Column(_))
                && matches!(like.pattern.as_ref(), Expr::Literal(..));
            *has_point |= ok;
            ok
        }
        Expr::IsNotNull(inner) => matches!(inner.as_ref(), Expr::Column(_)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinType;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
    use datafusion::optimizer::Optimizer;
    use datafusion::prelude::{SessionConfig, col, lit};

    use std::collections::HashSet;

    use super::{SemiJoinReduction, candidate_dimensions, dimension_key};
    use crate::config::{BALLISTA_SEMI_JOIN_REDUCTION, BallistaConfig};

    fn scan(table: &str, fields: &[(&str, DataType)]) -> LogicalPlan {
        let schema = Schema::new(
            fields
                .iter()
                .map(|(f, t)| Field::new(*f, t.clone(), true))
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

    fn ints(table: &str, fields: &[&str]) -> LogicalPlan {
        scan(
            table,
            &fields
                .iter()
                .map(|f| (*f, DataType::Int32))
                .collect::<Vec<_>>(),
        )
    }

    fn nation(filter: Expr) -> LogicalPlan {
        LogicalPlanBuilder::from(scan(
            "nation",
            &[("n_nationkey", DataType::Int32), ("n_name", DataType::Utf8)],
        ))
        .filter(filter)
        .unwrap()
        .alias("n1")
        .unwrap()
        .build()
        .unwrap()
    }

    /// `(supplier JOIN lineitem ON s_suppkey = l_suppkey) JOIN n1 ON s_nationkey = n_nationkey`
    fn q7_shape(filter: Expr) -> LogicalPlan {
        LogicalPlanBuilder::from(ints("supplier", &["s_suppkey", "s_nationkey"]))
            .join_on(
                ints("lineitem", &["l_suppkey", "l_price"]),
                JoinType::Inner,
                [col("supplier.s_suppkey").eq(col("lineitem.l_suppkey"))],
            )
            .unwrap()
            .join(
                nation(filter),
                JoinType::Inner,
                (vec!["supplier.s_nationkey"], vec!["n1.n_nationkey"]),
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
        // Treat every candidate as small, so these tests cover the shape
        // checks; sizing is covered by `apply_semi_join_reduction`'s callers.
        let small: Arc<HashSet<String>> = Arc::new(
            candidate_dimensions(&plan)
                .iter()
                .map(dimension_key)
                .collect(),
        );
        // Run twice to check a later optimizer pass does not add the same
        // semi join again.
        let rule = || -> Arc<dyn datafusion::optimizer::OptimizerRule + Send + Sync> {
            Arc::new(SemiJoinReduction::new(Arc::clone(&small)))
        };
        let optimized = Optimizer::with_rules(vec![rule()])
            .optimize(plan.clone(), &state, |_, _| {})
            .unwrap();
        let optimized = Optimizer::with_rules(vec![rule()])
            .optimize(optimized, &state, |_, _| {})
            .unwrap();
        assert_eq!(optimized.schema().as_ref(), plan.schema().as_ref());
        format!("{}", optimized.display_indent())
    }

    fn optimize(plan: LogicalPlan) -> String {
        optimize_with(plan, BallistaConfig::default())
    }

    fn name_filter() -> Expr {
        col("n_name")
            .eq(lit("FRANCE"))
            .or(col("n_name").eq(lit("GERMANY")))
    }

    #[test]
    fn adds_semi_join_on_the_table_below_the_inner_join() {
        let display = optimize(q7_shape(name_filter()));
        let semi = display
            .lines()
            .position(|l| {
                l.contains("LeftSemi Join: supplier.s_nationkey = __ballista_reduce")
            })
            .unwrap_or_else(|| panic!("no semi join added:\n{display}"));
        let inner = display
            .lines()
            .position(|l| l.contains("supplier.s_suppkey = lineitem.l_suppkey"))
            .unwrap_or_else(|| panic!("inner join missing:\n{display}"));
        assert!(
            semi > inner,
            "semi join must sit below the inner join:\n{display}"
        );
        assert_eq!(
            display.matches("LeftSemi Join").count(),
            1,
            "added exactly once across passes:\n{display}"
        );
    }

    #[test]
    fn skips_range_filtered_dimensions() {
        let display = optimize(q7_shape(col("n_nationkey").lt(lit(5))));
        assert!(!display.contains("LeftSemi"), "{display}");
    }

    #[test]
    fn skips_when_no_join_lies_below() {
        let plan =
            LogicalPlanBuilder::from(ints("supplier", &["s_suppkey", "s_nationkey"]))
                .join(
                    nation(name_filter()),
                    JoinType::Inner,
                    (vec!["supplier.s_nationkey"], vec!["n1.n_nationkey"]),
                    None,
                )
                .unwrap()
                .build()
                .unwrap();
        let display = optimize(plan);
        assert!(!display.contains("LeftSemi"), "{display}");
    }

    #[test]
    fn skips_unfiltered_dimensions() {
        let plan =
            LogicalPlanBuilder::from(ints("supplier", &["s_suppkey", "s_nationkey"]))
                .join_on(
                    ints("lineitem", &["l_suppkey", "l_price"]),
                    JoinType::Inner,
                    [col("supplier.s_suppkey").eq(col("lineitem.l_suppkey"))],
                )
                .unwrap()
                .join(
                    ints("nation", &["n_nationkey"]),
                    JoinType::Inner,
                    (vec!["supplier.s_nationkey"], vec!["nation.n_nationkey"]),
                    None,
                )
                .unwrap()
                .build()
                .unwrap();
        let display = optimize(plan);
        assert!(!display.contains("LeftSemi"), "{display}");
    }

    #[test]
    fn disabled_by_configuration() {
        use datafusion::config::ExtensionOptions;

        let mut config = BallistaConfig::default();
        config
            .set(
                BALLISTA_SEMI_JOIN_REDUCTION
                    .strip_prefix("ballista.")
                    .unwrap(),
                "false",
            )
            .unwrap();
        let display = optimize_with(q7_shape(name_filter()), config);
        assert!(!display.contains("LeftSemi"), "{display}");
    }
}
