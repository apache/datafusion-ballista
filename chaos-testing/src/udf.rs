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

use crate::budget::FaultBudget;
use arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_boolean_array;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use std::path::Path;
use std::sync::Arc;

/// `chaos_fail(guard BOOLEAN, mode UTF8, budget_dir UTF8) -> BOOLEAN`
///
/// Pass-through: returns `guard` unchanged. If any row in the batch has
/// `guard = true` and a fault token is available in `budget_dir`, consumes one
/// token and injects a fault:
///
/// - `io`    -> `DataFusionError::IoError`   (Ballista: retryable, counts to failures)
/// - `exec`  -> `DataFusionError::Execution` (Ballista: non-retryable)
/// - `panic` -> `panic!`                     (Ballista: caught, becomes non-retryable Internal)
///
/// Volatile so DataFusion neither constant-folds nor CSEs it away.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ChaosFail {
    signature: Signature,
}

impl Default for ChaosFail {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Boolean,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for ChaosFail {
    fn name(&self) -> &str {
        "chaos_fail"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let guard = args.args[0].clone().into_array(args.number_rows)?;
        let mode = scalar_utf8(&args.args[1], "mode")?;
        let budget_dir = scalar_utf8(&args.args[2], "budget_dir")?;

        let guard = as_boolean_array(&guard)?;
        let selected = (0..guard.len()).any(|i| !guard.is_null(i) && guard.value(i));

        if selected && FaultBudget::open(Path::new(&budget_dir)).try_consume() {
            match mode.as_str() {
                "io" => {
                    let msg = "chaos_fail: injected retryable IO fault";
                    log::error!("{msg}");
                    return Err(DataFusionError::IoError(std::io::Error::other(msg)));
                }
                "exec" => {
                    let msg =
                        "chaos_fail: injected non-retryable execution fault".to_string();
                    log::error!("{msg}");
                    return Err(DataFusionError::Execution(msg));
                }
                "panic" => {
                    log::error!("chaos_fail: injected panic");
                    panic!("chaos_fail: injected panic");
                }
                other => {
                    return Err(DataFusionError::Configuration(format!(
                        "chaos_fail: unknown mode {other:?} (expected io, exec, or panic)"
                    )));
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(
            (0..guard.len())
                .map(|i| (!guard.is_null(i)).then(|| guard.value(i)))
                .collect::<Vec<Option<bool>>>(),
        ))))
    }
}

/// `chaos_delay(guard BOOLEAN, ms INT64) -> BOOLEAN`
///
/// Pass-through: returns `guard` unchanged, sleeping `ms` milliseconds per batch
/// in which any row has `guard = true`. Used to hold a stage open long enough for
/// the harness to kill an executor while the stage is genuinely running.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ChaosDelay {
    signature: Signature,
}

impl Default for ChaosDelay {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean, DataType::Int64]),
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for ChaosDelay {
    fn name(&self) -> &str {
        "chaos_delay"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let guard = args.args[0].clone().into_array(args.number_rows)?;
        let ms = match &args.args[1] {
            ColumnarValue::Scalar(s) => match s {
                datafusion::scalar::ScalarValue::Int64(Some(v)) => *v as u64,
                other => {
                    return Err(DataFusionError::Configuration(format!(
                        "chaos_delay: ms must be a non-null INT64 literal, got {other:?}"
                    )));
                }
            },
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::Configuration(
                    "chaos_delay: ms must be a literal, not a column".to_string(),
                ));
            }
        };

        let guard = as_boolean_array(&guard)?;
        let selected = (0..guard.len()).any(|i| !guard.is_null(i) && guard.value(i));
        if selected {
            std::thread::sleep(std::time::Duration::from_millis(ms));
        }

        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(
            (0..guard.len())
                .map(|i| (!guard.is_null(i)).then(|| guard.value(i)))
                .collect::<Vec<Option<bool>>>(),
        ))))
    }
}

fn scalar_utf8(value: &ColumnarValue, arg: &str) -> Result<String> {
    match value {
        ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s))) => {
            Ok(s.clone())
        }
        other => Err(DataFusionError::Configuration(format!(
            "chaos_fail: {arg} must be a non-null UTF8 literal, got {other:?}"
        ))),
    }
}

/// The `chaos_fail` UDF.
pub fn chaos_fail_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(ChaosFail::default()))
}

/// The `chaos_delay` UDF.
pub fn chaos_delay_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(ChaosDelay::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::FaultBudget;
    use arrow::array::BooleanArray;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ballista-chaos-udf-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Build a one-column table `t(guard BOOLEAN)` with the given values.
    async fn ctx_with_guards(guards: Vec<bool>) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(chaos_fail_udf().as_ref().clone());
        ctx.register_udf(chaos_delay_udf().as_ref().clone());

        let schema = Arc::new(Schema::new(vec![Field::new(
            "guard",
            DataType::Boolean,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BooleanArray::from(guards))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    #[tokio::test]
    async fn io_mode_errors_when_a_token_is_available() {
        let dir = temp_dir("io-fires");
        FaultBudget::create(&dir, 1).unwrap();
        let ctx = ctx_with_guards(vec![true]).await;

        let sql = format!("SELECT chaos_fail(guard, 'io', '{}') FROM t", dir.display());
        let err = ctx.sql(&sql).await.unwrap().collect().await.unwrap_err();

        // Must be an IoError: that is the only variant Ballista treats as retryable.
        assert!(
            matches!(err, datafusion::error::DataFusionError::IoError(_)),
            "expected IoError, got {err:?}"
        );
    }

    #[tokio::test]
    async fn passes_through_when_budget_is_exhausted() {
        let dir = temp_dir("io-exhausted");
        FaultBudget::create(&dir, 0).unwrap();
        let ctx = ctx_with_guards(vec![true, false]).await;

        let sql = format!(
            "SELECT chaos_fail(guard, 'io', '{}') AS g FROM t",
            dir.display()
        );
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();

        // Pass-through: output must equal the input guard column.
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.value(0));
        assert!(!col.value(1));
    }

    #[tokio::test]
    async fn does_not_fire_when_no_guard_row_is_true() {
        let dir = temp_dir("io-no-guard");
        FaultBudget::create(&dir, 1).unwrap();
        let ctx = ctx_with_guards(vec![false, false]).await;

        let sql = format!("SELECT chaos_fail(guard, 'io', '{}') FROM t", dir.display());
        ctx.sql(&sql).await.unwrap().collect().await.unwrap();

        // The token must be untouched: the guard never selected this data.
        assert_eq!(FaultBudget::open(&dir).remaining(), 1);
    }

    // The panic crosses the async stream boundary and unwinds this test's own
    // thread before `result.is_err()` below is ever reached, so the test always
    // reports FAILED regardless of the UDF's behavior. In-process `panic!` is not
    // observable as an `Err` here; the authoritative check is the end-to-end
    // Scenario C in Task 8, where the executor's `catch_unwind` converts the
    // panic into a `FailedTask`. See `.superpowers/sdd/task-2-brief.md`.
    #[ignore]
    #[tokio::test]
    async fn panic_mode_panics() {
        let dir = temp_dir("panic-fires");
        FaultBudget::create(&dir, 1).unwrap();
        let ctx = ctx_with_guards(vec![true]).await;

        let sql = format!(
            "SELECT chaos_fail(guard, 'panic', '{}') FROM t",
            dir.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await;

        // The panic branch consumes the token before it panics, so an empty budget
        // proves the branch was actually reached.
        assert_eq!(
            FaultBudget::open(&dir).remaining(),
            0,
            "panic branch must have been reached"
        );
        // However the panic surfaces in-process, it must never look like success.
        assert!(result.is_err(), "a panicking UDF must not yield a result");
    }

    #[tokio::test]
    async fn delay_sleeps_and_passes_through() {
        let ctx = ctx_with_guards(vec![true]).await;
        let start = std::time::Instant::now();

        let batches = ctx
            .sql("SELECT chaos_delay(guard, 150) AS g FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert!(start.elapsed() >= std::time::Duration::from_millis(150));
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.value(0));
    }
}
