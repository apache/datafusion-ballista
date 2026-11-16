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
//! it would just repeat the join it came from. It is removed again when the
//! input it lands on is tiny (under 100K estimated rows, such as `nation`
//! reduced by `region`), where it only adds a join.

use std::collections::{HashMap, HashSet};
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
use futures::future::try_join_all;
use log::{debug, warn};

use super::SemiJoinPushdown;
use super::semi_join_pushdown::{sink, with_inputs};
use crate::config::BallistaConfig;
use crate::extension::SessionConfigExt;

/// Estimated rows below which reducing the input a semi join lands on is not
/// worth the extra join.
const MIN_LANDING_ROWS: usize = 100_000;

/// Alias prefix of the dimension copies this rule adds. Also how the rule
/// recognises its own semi joins on later optimizer passes.
const REDUCE_ALIAS: &str = "__ballista_reduce";

/// Adds a semi join against a selective dimension below the joins between it
/// and the table its key comes from. See the module documentation.
///
/// Only dimensions listed in `small` are used, so the
/// rule is not part of the default optimizer rules: run it through
/// [`apply_semi_join_reduction`], which sizes the candidates first.
#[derive(Debug, Default)]
pub struct SemiJoinReduction {
    small: Arc<HashSet<LogicalPlan>>,
}

impl SemiJoinReduction {
    /// Creates the rule for the dimensions in `small`.
    pub fn new(small: Arc<HashSet<LogicalPlan>>) -> Self {
        Self { small }
    }
}

/// Adds semi joins for the selective dimensions of `plan` (see the
/// module documentation) and returns the optimized result, or `plan`
/// unchanged when none qualifies or the rule is disabled.
///
/// Optimizes `plan` with the state's rules first, so candidates are found in
/// the same shape the physical planner will see; each candidate dimension is
/// then planned on its own to read its estimated row count.
///
/// The reduction is only an optimization, so a failure while sizing candidates
/// (a table that cannot be planned, say) is logged and `plan` is returned
/// unchanged rather than failing the query.
pub async fn apply_semi_join_reduction(
    state: &SessionState,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    match try_apply_semi_join_reduction(state, plan).await {
        Ok(reduced) => Ok(reduced),
        Err(e) => {
            warn!("Skipping semi join reduction: {e}");
            Ok(plan.clone())
        }
    }
}

/// Estimated row count of `plan`, planned on its own, or `None` when unknown.
async fn estimated_rows(
    state: &SessionState,
    plan: &LogicalPlan,
) -> Result<Option<usize>> {
    let physical = state
        .query_planner()
        .create_physical_plan(plan, state)
        .await?;
    let rows = StatisticsContext::new()
        .compute(physical.as_ref(), &StatisticsArgs::new())?
        .num_rows;
    Ok(rows.get_value().copied())
}

/// Fills `sizes` with the estimated rows of every plan in `plans` not already
/// in it, planning them concurrently.
async fn estimate_all(
    state: &SessionState,
    plans: &[LogicalPlan],
    sizes: &mut HashMap<LogicalPlan, Option<usize>>,
) -> Result<()> {
    let mut missing: Vec<&LogicalPlan> = Vec::new();
    for plan in plans {
        if !sizes.contains_key(plan) && !missing.contains(&plan) {
            missing.push(plan);
        }
    }
    let rows =
        try_join_all(missing.iter().map(|plan| estimated_rows(state, plan))).await?;
    for (plan, rows) in missing.into_iter().zip(rows) {
        debug!(
            "semi join reduction plan rows={rows:?}:\n{}",
            plan.display_indent()
        );
        sizes.insert(plan.clone(), rows);
    }
    Ok(())
}

async fn try_apply_semi_join_reduction(
    state: &SessionState,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    let config = state.config().ballista_config();
    let threshold = config.broadcast_join_threshold_rows();
    // Only plain queries: EXPLAIN, DDL and DML carry their own plans, which
    // must reach the client and the catalog as written. A threshold of 0
    // means no dimension is small enough.
    if !config.semi_join_reduction_enabled()
        || threshold == 0
        || matches!(
            plan,
            LogicalPlan::Explain(_)
                | LogicalPlan::Analyze(_)
                | LogicalPlan::Ddl(_)
                | LogicalPlan::Dml(_)
                | LogicalPlan::Copy(_)
                | LogicalPlan::Statement(_)
        )
    {
        return Ok(plan.clone());
    }
    // A candidate needs two joins, and optimizing only adds joins, so skip the
    // optimizer run for plans without any.
    let mut has_join = false;
    plan.apply_with_subqueries(|node| {
        has_join |= matches!(node, LogicalPlan::Join(_));
        Ok(if has_join {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    if !has_join {
        return Ok(plan.clone());
    }
    let optimized = state.optimize(plan)?;
    let candidates = candidate_dimensions(&optimized)?;
    if candidates.is_empty() {
        return Ok(plan.clone());
    }
    // Planning a plan to read its size can list files, so each plan is sized
    // once, whether it is accepted or not, and the plans are sized together.
    let mut sizes: HashMap<LogicalPlan, Option<usize>> = HashMap::new();
    estimate_all(state, &candidates, &mut sizes).await?;
    let small: HashSet<LogicalPlan> = candidates
        .into_iter()
        .filter(|dim| sizes[dim].is_some_and(|r| r <= threshold))
        .collect();
    if small.is_empty() {
        return Ok(plan.clone());
    }
    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(SemiJoinReduction::new(Arc::new(small))),
        Arc::new(SemiJoinPushdown::new()),
    ];
    let reduced = Optimizer::with_rules(rules).optimize(optimized, state, |_, _| {})?;

    // A semi join that landed on a tiny input (such as `nation` reduced by
    // `region`) saves nothing measurable and adds a join: remove it again.
    let mut landings = Vec::new();
    reduced.apply(|node| {
        if let Some(input) = added_semi_join_input(node) {
            landings.push(input.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    // Sizing a join subtree costs as much as planning it, and one is rarely
    // tiny, so only the scan-like landings are sized.
    let mut scans = Vec::new();
    for input in landings {
        if !contains_join(&input)? {
            scans.push(input);
        }
    }
    estimate_all(state, &scans, &mut sizes).await?;
    let pointless: HashSet<LogicalPlan> = scans
        .into_iter()
        .filter(|input| sizes[input].is_some_and(|r| r <= MIN_LANDING_ROWS))
        .collect();
    if pointless.is_empty() {
        return Ok(reduced);
    }
    let reduced = reduced
        .transform_up(|node| match added_semi_join_input(&node) {
            Some(input) if pointless.contains(input) => {
                Ok(Transformed::yes(input.clone()))
            }
            _ => Ok(Transformed::no(node)),
        })?
        .data;
    if reduced.exists(|node| Ok(added_semi_join_input(node).is_some()))? {
        Ok(reduced)
    } else {
        Ok(plan.clone())
    }
}

/// The dimension copy of a semi join this rule added, or `None` for any other
/// join.
fn added_dimension(join: &Join) -> Option<&LogicalPlan> {
    match join.right.as_ref() {
        LogicalPlan::SubqueryAlias(a)
            if join.join_type == JoinType::LeftSemi
                && a.alias.table().starts_with(REDUCE_ALIAS) =>
        {
            Some(a.input.as_ref())
        }
        _ => None,
    }
}

/// The left input of a semi join this rule added, or `None` for any other node.
fn added_semi_join_input(node: &LogicalPlan) -> Option<&LogicalPlan> {
    match node {
        LogicalPlan::Join(j) if added_dimension(j).is_some() => Some(j.left.as_ref()),
        _ => None,
    }
}

/// The dimension side of every inner join that [`SemiJoinReduction`] could
/// use, before sizing.
fn candidate_dimensions(plan: &LogicalPlan) -> Result<Vec<LogicalPlan>> {
    let mut out = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Join(j) = node
            && j.join_type == JoinType::Inner
            && !j.on.is_empty()
        {
            for (dim, other) in [(&j.right, &j.left), (&j.left, &j.right)] {
                if is_selective_dimension(dim) && contains_join(other)? {
                    out.push(dim.as_ref().clone());
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(out)
}

impl OptimizerRule for SemiJoinReduction {
    fn name(&self) -> &str {
        "semi_join_reduction"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };
        if join.join_type != JoinType::Inner || join.on.is_empty() || join.null_aware {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        let enabled = config
            .options()
            .extensions
            .get::<BallistaConfig>()
            .map(|c| c.semi_join_reduction_enabled())
            .unwrap_or_else(|| BallistaConfig::default().semi_join_reduction_enabled());
        if !enabled {
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
    small: &HashSet<LogicalPlan>,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    let (dim, other) = if dim_on_right {
        (&join.right, &join.left)
    } else {
        (&join.left, &join.right)
    };
    if !is_selective_dimension(dim)
        || !contains_join(other)?
        || !small.contains(dim.as_ref())
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
    if already_reduced(other, &keys, dim)? {
        return Ok(None);
    }

    let alias = config.alias_generator().next(REDUCE_ALIAS);
    let dim_copy = LogicalPlanBuilder::from(dim.as_ref().clone())
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
    Ok(Some(LogicalPlan::Join(with_inputs(join, left, right)?)))
}

/// Whether `plan` already holds a semi join this rule added on the same keys
/// against the same dimension, so later optimizer passes do not add it again.
fn already_reduced(
    plan: &LogicalPlan,
    keys: &[(Column, Column)],
    dim: &LogicalPlan,
) -> Result<bool> {
    plan.exists(|node| {
        let LogicalPlan::Join(j) = node else {
            return Ok(false);
        };
        Ok(added_dimension(j).is_some_and(|added| added == dim)
            && j.on.len() == keys.len()
            && j.on.iter().zip(keys).all(|((l, r), (o, d))| {
                matches!(l, Expr::Column(c) if c == o)
                    && matches!(r, Expr::Column(c) if c.name == d.name)
            }))
    })
}

fn contains_join(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| Ok(matches!(node, LogicalPlan::Join(_))))
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
            Operator::And => {
                is_point_predicate(left, has_point)
                    && is_point_predicate(right, has_point)
            }
            // Each branch must select on its own: `a = 1 OR b IS NOT NULL`
            // keeps almost everything.
            Operator::Or => {
                let (mut left_point, mut right_point) = (false, false);
                let ok = is_point_predicate(left, &mut left_point)
                    && left_point
                    && is_point_predicate(right, &mut right_point)
                    && right_point;
                *has_point |= ok;
                ok
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

    use super::{SemiJoinReduction, candidate_dimensions};
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
        let small: Arc<HashSet<LogicalPlan>> =
            Arc::new(candidate_dimensions(&plan).unwrap().into_iter().collect());
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
    fn skips_an_or_with_an_unselective_branch() {
        let display = optimize(q7_shape(
            col("n_name")
                .eq(lit("FRANCE"))
                .or(col("n_nationkey").is_not_null()),
        ));
        assert!(!display.contains("LeftSemi"), "{display}");
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

    /// Plans `sql` over in-memory TPC-H-like tables through the rules every
    /// Ballista session installs, then through `apply_semi_join_reduction`.
    async fn reduced(customers: i32, config: BallistaConfig) -> String {
        use datafusion::arrow::array::{Int32Array, StringArray};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_option_extension(config))
            .with_optimizer_rules(crate::optimizer::ballista_default_optimizer_rules())
            .build();
        let ctx = SessionContext::new_with_state(state);
        let table = |names: &[&str], rows: i32| {
            let ids = Int32Array::from_iter_values(0..rows);
            let keys = Int32Array::from_iter_values((0..rows).map(|i| i % 25));
            let batch =
                RecordBatch::try_from_iter(names.iter().zip([ids, keys]).map(
                    |(n, a)| (*n, Arc::new(a) as datafusion::arrow::array::ArrayRef),
                ))
                .unwrap();
            MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()
        };
        let nation = RecordBatch::try_from_iter([
            (
                "n_nationkey",
                Arc::new(Int32Array::from_iter_values(0..25))
                    as datafusion::arrow::array::ArrayRef,
            ),
            (
                "n_name",
                Arc::new(StringArray::from_iter_values(
                    (0..25).map(|i| format!("NATION{i}")),
                )),
            ),
        ])
        .unwrap();
        ctx.register_table(
            "nation",
            Arc::new(MemTable::try_new(nation.schema(), vec![vec![nation]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "customer",
            Arc::new(table(&["c_custkey", "c_nationkey"], customers)),
        )
        .unwrap();
        ctx.register_table(
            "orders",
            Arc::new(table(&["o_orderkey", "o_custkey"], 20_000)),
        )
        .unwrap();
        let plan = ctx
            .sql(
                "SELECT o_orderkey FROM orders \
                 JOIN customer ON o_custkey = c_custkey \
                 JOIN nation ON c_nationkey = n_nationkey \
                 WHERE n_name = 'NATION1'",
            )
            .await
            .unwrap()
            .into_unoptimized_plan();
        let state = ctx.state();
        let reduced = super::apply_semi_join_reduction(&state, &plan)
            .await
            .unwrap();
        format!("{}", reduced.display_indent())
    }

    #[tokio::test]
    async fn adds_a_semi_join_that_lands_on_a_large_table() {
        let display = reduced(150_000, BallistaConfig::default()).await;
        assert!(display.contains("__ballista_reduce"), "{display}");
    }

    #[tokio::test]
    async fn removes_a_semi_join_that_lands_on_a_tiny_table() {
        let display = reduced(1_000, BallistaConfig::default()).await;
        assert!(!display.contains("__ballista_reduce"), "{display}");
    }

    #[tokio::test]
    async fn leaves_the_plan_alone_when_disabled() {
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
        let display = reduced(150_000, config).await;
        assert!(!display.contains("__ballista_reduce"), "{display}");
    }
}
