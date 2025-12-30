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

use ballista::datafusion::{
    common::Result,
    execution::{SessionStateBuilder, options::ParquetReadOptions},
    prelude::{SessionConfig, SessionContext},
};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_examples::test_util;

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(1)
        .with_ballista_standalone_parallelism(2);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::standalone_with_state(state).await?;

    let test_data = test_util::examples_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "test",
        &format!("{test_data}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx.sql("select count(1) from test").await?;

    df.show().await?;
    Ok(())
}
