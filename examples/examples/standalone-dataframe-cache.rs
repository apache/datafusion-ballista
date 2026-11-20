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

//! `DataFrame::cache()` keeps a DataFrame's result on the executors, so queries
//! over it read the stored result instead of computing it again.
//!
//! This example:
//! 1. turns caching on (it is off by default),
//! 2. caches a prepared DataFrame (nothing runs yet),
//! 3. builds the cache with the first query and reads it with the next ones,
//!    in staged mode,
//! 4. switches the session to pipelined mode, which reads the same cache,
//! 5. runs three pipelined queries in parallel, printing rows as they arrive.
//!
//! Good to know: a cache belongs to its session and is not evicted yet, so
//! cache only what you reuse. Cached data lost with an executor is rebuilt, and
//! `EXPLAIN` never builds a cache. See the tuning guide for details.

use ballista::datafusion::{
    arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches},
    common::Result,
    execution::{SessionStateBuilder, options::CsvReadOptions},
    functions_aggregate::expr_fn::{count, sum},
    prelude::{DataFrame, SessionConfig, SessionContext, col, lit},
};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_examples::test_util;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. A standalone cluster, with caching turned on. Queries start staged.
    let ctx = standalone().await?;

    // 2. The DataFrame the queries start from. Nothing runs here: `cache()`
    //    only marks it to be kept. In an application this is the expensive
    //    part, such as scans, joins and aggregations.
    let path = format!("{}/aggregate_test_100.csv", test_util::examples_test_data());
    let sales = ctx
        .read_csv(path, CsvReadOptions::new())
        .await?
        .select(vec![
            col("c1").alias("category"),
            col("c2").alias("store"),
            col("c4").alias("amount"),
        ])?
        .filter(col("amount").gt(lit(0)))?
        .cache()
        .await?;

    // 3. Staged: the first query builds the cache, as a staged job, and then
    //    reads it. Later queries, and DataFrames derived from `sales`, read it.
    println!("== staged: the first query builds the cache ==");
    println!("{} sales", sales.clone().count().await?);

    println!("== staged: later queries read the cache ==");
    let staged = by_category(sales.clone())?.collect().await?;
    println!("{}", pretty_format_batches(&staged)?);

    // 4. Pipelined: one SET. A DataFrame keeps the settings of the session it
    //    came from, so rebuild it from the session's state to pick up the new
    //    mode. The plan, and the cache it reads, are the same.
    ctx.sql("SET ballista.execution.mode = 'pipelined'").await?;
    let sales = DataFrame::new(ctx.state(), sales.into_unoptimized_plan());

    // 5. Three queries in parallel, each streaming its rows from the cache as
    //    they are produced. Read every stream you start, and keep the queries
    //    running at once within the cluster: each holds the vcores of its
    //    widest stage until it ends, here 8 vcores / 2 partitions = 4 at once.
    println!("== pipelined: three queries in parallel ==");
    let (pipelined, _, _) = tokio::try_join!(
        stream("by category", by_category(sales.clone())?),
        stream(
            "sales per store",
            sales
                .clone()
                .aggregate(vec![col("store")], vec![count(lit(1)).alias("sales")])?
        ),
        stream(
            "top 5 amounts",
            sales
                .clone()
                .sort(vec![col("amount").sort(false, false)])?
                .limit(0, Some(5))?
        ),
    )?;

    // The cache gives the same result in either mode.
    assert_eq!(
        pretty_format_batches(&staged)?.to_string(),
        pretty_format_batches(&pipelined)?.to_string(),
    );
    Ok(())
}

/// Total amount per category, a query over the cached `sales`.
fn by_category(sales: DataFrame) -> Result<DataFrame> {
    sales
        .aggregate(
            vec![col("category")],
            vec![sum(col("amount")).alias("total")],
        )?
        .sort_by(vec![col("category")])
}

/// Starts a standalone cluster that can run staged and pipelined queries, and
/// returns a session on it with caching turned on.
async fn standalone() -> Result<SessionContext> {
    let mut config = SessionConfig::new_with_ballista()
        .with_target_partitions(2)
        .with_ballista_standalone_parallelism(8);
    // Pipelined queries need the scheduler to push tasks to executors.
    config
        .options_mut()
        .set("ballista.standalone.task_scheduling_policy", "push-staged")?;
    // `DataFrame::cache()` does nothing unless caching is turned on.
    config.options_mut().set("ballista.cache.noop", "false")?;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    SessionContext::standalone_with_state(state).await
}

/// Runs `df`, printing each batch of rows as it arrives, and returns them.
async fn stream(name: &str, df: DataFrame) -> Result<Vec<RecordBatch>> {
    let mut stream = df.execute_stream().await?;
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        println!(
            "{name}:\n{}",
            pretty_format_batches(std::slice::from_ref(&batch))?
        );
        batches.push(batch);
    }
    Ok(batches)
}
