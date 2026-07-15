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

mod common;

use ballista::prelude::DataFrameExt;
use datafusion::assert_batches_eq;
use datafusion::logical_expr::LogicalPlan;
use tempfile::TempDir;

#[tokio::test]
async fn should_checkpoint_dataframe() -> datafusion::error::Result<()> {
    let checkpoint_dir = TempDir::new().unwrap();
    let checkpoint_path = checkpoint_dir.path().to_str().unwrap().to_string();

    let ctx = common::standalone_context_with_checkpoint_dir(&checkpoint_path).await;

    let test_data = common::example_test_data();
    ctx.register_parquet(
        "test",
        &format!("{test_data}/alltypes_plain.parquet"),
        Default::default(),
    )
    .await?;

    let df = ctx
        .sql("select string_col, timestamp_col from test where id > 4")
        .await?;

    let expected = [
        "+------------+---------------------+",
        "| string_col | timestamp_col       |",
        "+------------+---------------------+",
        "| 31         | 2009-03-01T00:01:00 |",
        "| 30         | 2009-04-01T00:00:00 |",
        "| 31         | 2009-04-01T00:01:00 |",
        "+------------+---------------------+",
    ];

    // sanity check the plan pre-checkpoint isn't already a bare TableScan
    assert!(!matches!(df.logical_plan(), LogicalPlan::TableScan(_)));

    let checkpointed = df.checkpoint().await?;

    // lineage is broken: the new DataFrame is a scan over the checkpoint files,
    // not the filter/projection chain that produced them
    assert!(matches!(
        checkpointed.logical_plan(),
        LogicalPlan::TableScan(_)
    ));

    let result = checkpointed.collect().await?;
    assert_batches_eq!(expected, &result);

    // the checkpoint actually landed on the configured storage
    let written_any_files = std::fs::read_dir(checkpoint_dir.path())
        .unwrap()
        .next()
        .is_some();
    assert!(written_any_files, "expected checkpoint files to be written");

    Ok(())
}

#[tokio::test]
async fn checkpoint_without_configured_dir_errors() -> datafusion::error::Result<()> {
    // no with_ballista_checkpoint_dir() call
    let ctx = common::standalone_context_with_state().await;

    let test_data = common::example_test_data();
    ctx.register_parquet(
        "test",
        &format!("{test_data}/alltypes_plain.parquet"),
        Default::default(),
    )
    .await?;

    let df = ctx.sql("select * from test").await?;

    let err = df.checkpoint().await.unwrap_err();
    assert!(err.to_string().contains("ballista.checkpoint.dir"));

    Ok(())
}
