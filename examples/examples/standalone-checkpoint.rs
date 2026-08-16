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

//! Demonstrates `DataFrame::checkpoint()` and `DataFrame::checkpoint_lazy()`.
//!
//! Checkpointing materialises a DataFrame to `ballista.checkpoint.dir` and
//! continues from a scan of that data, which truncates the logical plan. It is
//! useful when an expensive subplan is reused, or when a plan has grown deep
//! enough that planning it repeatedly becomes the bottleneck.
//!
//! The two variants mirror Spark:
//!
//! * `checkpoint()` is eager, like Spark's `checkpoint(eager = true)`, which is
//!   Spark's default. The job runs before the call returns.
//! * `checkpoint_lazy()` is lazy, like Spark's `checkpoint(eager = false)`.
//!   Nothing runs until an action, and the scheduler then splits the plan into
//!   a job that writes the checkpoint and a job that reads it back.
//!
//! Two things to keep in mind outside of this example:
//!
//! * `ballista.checkpoint.dir` must be reachable from every node in the
//!   cluster. A local directory works here because the standalone cluster runs
//!   in a single process, but a real deployment needs an object store URL or a
//!   shared filesystem, in the same way Spark expects an HDFS compatible path.
//! * Checkpoints are not cleaned up. This example writes into a temporary
//!   directory that is removed when it exits, but in a real deployment the
//!   data persists until something else deletes it, as with Spark's reliable
//!   checkpoints.

use ballista::datafusion::{
    common::Result,
    execution::{SessionStateBuilder, options::ParquetReadOptions},
    prelude::{SessionConfig, SessionContext, col, lit},
};
use ballista::prelude::{DataFrameExt, SessionConfigExt, SessionContextExt};
use ballista_examples::test_util;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<()> {
    // Checkpoints are written here. Calling either checkpoint method without
    // this set returns an error. (see the config spec)
    let checkpoint_dir = TempDir::new()?;

    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(1)
        .with_ballista_standalone_parallelism(2)
        .with_ballista_checkpoint_dir(
            checkpoint_dir.path().to_string_lossy().to_string(),
        );

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::standalone_with_state(state).await?;

    let test_data = test_util::examples_test_data();

    ctx.register_parquet(
        "test",
        &format!("{test_data}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    // -----------------------------------------------------------------------
    // Eager checkpoint: materialises now
    // -----------------------------------------------------------------------

    let df = ctx
        .sql("select id, string_col, timestamp_col from test where id > 4")
        .await?;

    println!("Plan before checkpointing:");
    println!("{}", df.logical_plan().display_indent());

    // Runs the plan as a distributed job and returns a DataFrame that reads the
    // result back.
    let checkpointed = df.checkpoint().await?;

    println!("\nPlan after checkpointing, the lineage above is gone:");
    println!("{}", checkpointed.logical_plan().display_indent());

    // Further operations build on the scan rather than the original plan.
    println!("\nFiltering the checkpointed data:");
    checkpointed.filter(col("id").lt_eq(lit(6)))?.show().await?;

    // -----------------------------------------------------------------------
    // Lazy checkpoint: materialises on the first action
    // -----------------------------------------------------------------------

    let df = ctx
        .sql("select id, string_col from test where id > 4")
        .await?;

    // Returns immediately: no job is submitted here.
    let lazy = df.checkpoint_lazy()?;

    println!("\nLazy checkpoint marks the plan without executing it:");
    println!("{}", lazy.logical_plan().display_indent());

    // The action below is what triggers the work. The scheduler splits the plan
    // into a job that writes the checkpoint and a job that reads it back.
    println!("\nFirst action materialises the checkpoint:");
    lazy.clone().show().await?;

    // The checkpoint location is fixed when checkpoint_lazy() is called, so a
    // second action reuses the materialised data instead of recomputing it.
    println!("\nSecond action reuses it, the source is not read again:");
    lazy.show().await?;

    Ok(())
}
