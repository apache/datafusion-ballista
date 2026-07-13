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

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Rows per `facts` partition file.
const FACT_ROWS: i64 = 2_000;
/// Number of `facts` partition files. Multiple files give the scan enough
/// parallelism to spread across executors.
const FACT_FILES: i64 = 8;
/// Number of distinct keys in `dims`. The join on `key` forces a shuffle, which
/// is what creates the multi-stage plan the HA paths need.
const DIM_KEYS: i64 = 50;

/// A small deterministic Parquet dataset: `facts(key, value)` joined to
/// `dims(key, name)`.
pub struct Fixture {
    facts_dir: PathBuf,
    dims_dir: PathBuf,
}

impl Fixture {
    pub async fn write(dir: &Path) -> Result<Self> {
        let ctx = SessionContext::new();
        let facts_dir = dir.join("facts");
        let dims_dir = dir.join("dims");
        std::fs::create_dir_all(&facts_dir)?;
        std::fs::create_dir_all(&dims_dir)?;

        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        for file in 0..FACT_FILES {
            let keys: Vec<i64> = (0..FACT_ROWS)
                .map(|r| (r + file * FACT_ROWS) % DIM_KEYS)
                .collect();
            let values: Vec<i64> = (0..FACT_ROWS).map(|r| r + file * FACT_ROWS).collect();
            let batch = RecordBatch::try_new(
                fact_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(values)),
                ],
            )?;
            let df = ctx.read_batch(batch)?;
            df.write_parquet(
                facts_dir
                    .join(format!("part-{file}.parquet"))
                    .to_str()
                    .unwrap(),
                datafusion::dataframe::DataFrameWriteOptions::new(),
                None,
            )
            .await?;
        }

        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            dim_schema,
            vec![
                Arc::new(Int64Array::from((0..DIM_KEYS).collect::<Vec<i64>>())),
                Arc::new(StringArray::from(
                    (0..DIM_KEYS)
                        .map(|k| format!("dim-{k}"))
                        .collect::<Vec<String>>(),
                )),
            ],
        )?;
        ctx.read_batch(batch)?
            .write_parquet(
                dims_dir.join("part-0.parquet").to_str().unwrap(),
                datafusion::dataframe::DataFrameWriteOptions::new(),
                None,
            )
            .await?;

        Ok(Self {
            facts_dir,
            dims_dir,
        })
    }

    /// `CREATE EXTERNAL TABLE` statements, to run against any SessionContext
    /// (local for the baseline, Ballista for the cluster run).
    pub fn register_sql(&self) -> Vec<String> {
        vec![
            format!(
                "CREATE EXTERNAL TABLE facts STORED AS PARQUET LOCATION '{}'",
                self.facts_dir.display()
            ),
            format!(
                "CREATE EXTERNAL TABLE dims STORED AS PARQUET LOCATION '{}'",
                self.dims_dir.display()
            ),
        ]
    }

    /// The chaos-free query. A join plus a grouped aggregate: at least two
    /// stages, with a shuffle between them.
    pub fn baseline_query() -> &'static str {
        "SELECT d.name, COUNT(*) AS n, SUM(f.value) AS total \
         FROM facts f JOIN dims d ON f.key = d.key \
         GROUP BY d.name ORDER BY d.name"
    }

    /// The same query with a chaos UDF spliced into the WHERE clause.
    ///
    /// `injection` is a complete `chaos_*(...)` call returning BOOLEAN. Its own
    /// guard argument selects which rows fault (and hence which partitions and
    /// tasks). The predicate is written so every row passes through regardless:
    /// the guard decides only *where the fault fires*, never which rows survive,
    /// so a chaos run must return exactly the baseline result.
    ///
    /// The predicate uses `IS NOT NULL` rather than `OR TRUE`: DataFusion's
    /// optimizer constant-folds `expr OR TRUE` to the literal `TRUE` and drops
    /// the volatile UDF call entirely (verified empirically in the
    /// `or_true_predicate_is_optimized_away_and_never_fires` and
    /// `chaos_query_predicate_survives_optimization_and_fires` tests below),
    /// which would silently disarm every fault injection. `chaos_fail`/
    /// `chaos_delay` always return `Some(guard)` (see udf.rs), so
    /// `... IS NOT NULL` is always true without being foldable to a constant,
    /// and the optimizer must still evaluate the call to determine nullness.
    ///
    /// Example: `chaos_query("chaos_fail(f.key = 7, 'io', '/tmp/b')")`
    pub fn chaos_query(injection: &str) -> String {
        format!(
            "SELECT d.name, COUNT(*) AS n, SUM(f.value) AS total \
             FROM facts f JOIN dims d ON f.key = d.key \
             WHERE {injection} IS NOT NULL \
             GROUP BY d.name ORDER BY d.name"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn baseline_query_is_deterministic_and_non_empty() {
        let dir = tempfile::tempdir().unwrap();
        let fixture = Fixture::write(dir.path()).await.unwrap();

        let ctx = SessionContext::new();
        for stmt in fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }

        let rows = ctx
            .sql(Fixture::baseline_query())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert!(total > 0, "baseline query must return rows");

        // Determinism: the same query over the same data must give the same answer.
        let rows2 = ctx
            .sql(Fixture::baseline_query())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            datafusion::arrow::util::pretty::pretty_format_batches(&rows)
                .unwrap()
                .to_string(),
            datafusion::arrow::util::pretty::pretty_format_batches(&rows2)
                .unwrap()
                .to_string(),
        );
    }

    /// Empirically confirms the risk called out on `chaos_query`: DataFusion's
    /// constant-folding simplifies `expr OR TRUE` to the literal `TRUE` during
    /// logical optimization, and once the predicate is a literal the join/filter
    /// no longer references the UDF call at all, so it is never invoked. If this
    /// test ever starts failing (budget still consumed), the optimizer's folding
    /// behavior changed and `chaos_query`'s `IS NOT NULL` predicate should be
    /// re-verified instead of reverting to `OR TRUE`.
    #[tokio::test]
    async fn or_true_predicate_is_optimized_away_and_never_fires() {
        let dir = tempfile::tempdir().unwrap();
        let fixture = Fixture::write(dir.path()).await.unwrap();
        let budget_dir = dir.path().join("budget");
        let budget = crate::budget::FaultBudget::create(&budget_dir, 1).unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(crate::udf::chaos_fail_udf().as_ref().clone());
        for stmt in fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }

        // key = 7 genuinely exists in the generated data (keys cycle 0..DIM_KEYS),
        // so an empty-partition short-circuit is not what's suppressing the call.
        let sql = format!(
            "SELECT d.name, COUNT(*) AS n, SUM(f.value) AS total \
             FROM facts f JOIN dims d ON f.key = d.key \
             WHERE chaos_fail(f.key = 7, 'io', '{}') OR TRUE \
             GROUP BY d.name ORDER BY d.name",
            budget_dir.display()
        );

        let explain = ctx
            .sql(&format!("EXPLAIN {sql}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let explain_str =
            datafusion::arrow::util::pretty::pretty_format_batches(&explain)
                .unwrap()
                .to_string();
        println!("EXPLAIN (OR TRUE form):\n{explain_str}");
        assert!(
            !explain_str.contains("chaos_fail"),
            "expected the optimizer to eliminate the chaos_fail call from the plan, but it is still present:\n{explain_str}"
        );

        let result = ctx.sql(&sql).await.unwrap().collect().await;
        assert!(
            result.is_ok(),
            "expected the query to succeed because the fault never fires, got {result:?}"
        );
        assert_eq!(
            budget.remaining(),
            1,
            "the token must be untouched: `OR TRUE` is constant-folded away, so chaos_fail is never invoked"
        );
    }

    /// The predicate form `Fixture::chaos_query` actually uses. Unlike `OR TRUE`,
    /// `chaos_fail(...) IS NOT NULL` is not foldable to a constant (the UDF's
    /// return type/value is not known to the optimizer without invoking it), so
    /// the call must survive into the physical plan and the fault must fire.
    #[tokio::test]
    async fn chaos_query_predicate_survives_optimization_and_fires() {
        let dir = tempfile::tempdir().unwrap();
        let fixture = Fixture::write(dir.path()).await.unwrap();
        let budget_dir = dir.path().join("budget");
        let budget = crate::budget::FaultBudget::create(&budget_dir, 1).unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(crate::udf::chaos_fail_udf().as_ref().clone());
        for stmt in fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }

        let injection =
            format!("chaos_fail(f.key = 7, 'io', '{}')", budget_dir.display());
        let sql = Fixture::chaos_query(&injection);

        let explain = ctx
            .sql(&format!("EXPLAIN {sql}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let explain_str =
            datafusion::arrow::util::pretty::pretty_format_batches(&explain)
                .unwrap()
                .to_string();
        println!("EXPLAIN (IS NOT NULL form):\n{explain_str}");
        assert!(
            explain_str.contains("chaos_fail"),
            "expected the chaos_fail call to survive into the plan, but it is missing:\n{explain_str}"
        );

        let err = ctx.sql(&sql).await.unwrap().collect().await.unwrap_err();
        // The join fans the same filtered stream out to multiple consumers
        // (CollectLeft build side plus the probe side), so DataFusion wraps the
        // propagated error in `DataFusionError::Shared` rather than surfacing the
        // bare `IoError` directly; `find_root` unwraps that layer.
        assert!(
            matches!(
                err.find_root(),
                datafusion::error::DataFusionError::IoError(_)
            ),
            "expected an IoError (possibly Shared-wrapped) from the injected fault, got {err:?}"
        );
        assert_eq!(
            budget.remaining(),
            0,
            "the token must be consumed: the predicate must force chaos_fail to be evaluated"
        );
    }

    /// CRITICAL INVARIANT: When a chaos fault cannot fire (budget exhausted),
    /// `chaos_query()` must return EXACTLY the same result as `baseline_query()`.
    ///
    /// This invariant underpins every HA scenario: we detect when a re-run stage
    /// duplicates or drops partitions by asserting that the result after a fault
    /// is identical to the baseline. The chaos_query predicate `WHERE ... IS NOT
    /// NULL` preserves the row set only because chaos_fail/chaos_delay always
    /// return `Some(guard)` (never NULL). If a future change ever returned NULL
    /// from either UDF, `IS NOT NULL` would silently drop rows and no test would
    /// catch it — until a real distributed run failed mysteriously. This test
    /// pins that invariant before any such refactor happens.
    #[tokio::test]
    async fn chaos_query_without_a_firing_fault_equals_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let fixture = Fixture::write(dir.path()).await.unwrap();
        let budget_dir = dir.path().join("budget");
        // 0 tokens: the fault cannot fire, no matter how many times it is called.
        let _budget = crate::budget::FaultBudget::create(&budget_dir, 0).unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(crate::udf::chaos_fail_udf().as_ref().clone());
        for stmt in fixture.register_sql() {
            ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
        }

        let injection =
            format!("chaos_fail(f.key = 7, 'io', '{}')", budget_dir.display());
        let chaos_sql = Fixture::chaos_query(&injection);
        let baseline_sql = Fixture::baseline_query();

        let chaos_rows = ctx.sql(&chaos_sql).await.unwrap().collect().await.unwrap();
        let baseline_rows = ctx
            .sql(baseline_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let chaos_str =
            datafusion::arrow::util::pretty::pretty_format_batches(&chaos_rows)
                .unwrap()
                .to_string();
        let baseline_str =
            datafusion::arrow::util::pretty::pretty_format_batches(&baseline_rows)
                .unwrap()
                .to_string();

        assert_eq!(
            chaos_str, baseline_str,
            "chaos_query with a non-firing fault must return the same rows as baseline_query"
        );
    }
}
