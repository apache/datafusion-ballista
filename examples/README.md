<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Ballista Examples

This directory contains examples for executing distributed queries with Ballista.

## Standalone Examples

The standalone example is the easiest to get started with. Ballista supports a standalone mode where a scheduler
and executor are started in-process.

```bash
cargo run --example standalone_sql --features="ballista/standalone"
```

### Source code for standalone SQL example

```rust
use ballista::{
    extension::SessionConfigExt,
    prelude::*
};
use datafusion::{
    execution::{options::ParquetReadOptions, SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
};

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

```

```bash
cargo run --example standalone-substrait --features="ballista/standalone","substrait"
```

### Source code for standalone Substrait example

```rust
use std::sync::Arc;

use ballista::datafusion::common::Result;
use ballista::extension::{Extension, SubstraitExec};
use ballista_core::extension::SessionConfigExt;
use ballista_examples::test_util;
use datafusion::catalog::MemoryCatalogProviderList;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::serializer::serialize_bytes;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let catalog_list = Arc::new(MemoryCatalogProviderList::new());

    let config = SessionConfig::new_with_ballista();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_catalog_list(catalog_list.clone())
        .build();
    let ctx = SessionContext::new_with_state(state.clone());

    // Use any frontend to serialize a Substrait plan
    let test_data = test_util::examples_test_data();
    let ddl_plan_bytes = serialize_bytes(&format!(
        "CREATE EXTERNAL TABLE IF NOT EXISTS another_data \
         STORED AS PARQUET \
         LOCATION '{}/alltypes_plain.parquet'",
        test_data
    ), &ctx).await?;
    let select_plan_bytes = serialize_bytes(
        "SELECT id, string_col FROM another_data",
        &ctx)
        .await?;

    let session_id = ctx.session_id();
    let scheduler_url = Extension::setup_standalone(Some(&state)).await?;
    let client = SubstraitSchedulerClient::new(scheduler_url, session_id.to_string()).await?;

    client.execute_query(ddl_plan_bytes).await?;
    let mut stream = client.execute_query(select_plan_bytes).await?;

    let mut batch_count = 0;
    let mut total_rows = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        batch_count += 1;
        total_rows += batch.num_rows();

        println!("Batch {}: {} rows", batch_count, batch.num_rows());
        println!("{:?}", batch);
    }
    println!("---------");
    println!("Query executed successfully!");
    println!("Total batches: {}, Total rows: {}", batch_count, total_rows);

    Ok(())
}
```

## Distributed Examples

For background information on the Ballista architecture, refer to
the [Ballista README](../ballista/client/README.md).

### Start a standalone cluster

From the root of the project, build release binaries.

```bash
cargo build --release
```

Start a Ballista scheduler process in a new terminal session.

```bash
RUST_LOG=info ./target/release/ballista-scheduler
```

Start one or more Ballista executor processes in new terminal sessions. When starting more than one
executor, a unique port number must be specified for each executor.

```bash
RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50051
RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50052
```

### Running the examples

The examples can be run using the `cargo run --bin` syntax.

### Distributed SQL Example

```bash
cargo run --release --example remote-sql
```

#### Source code for distributed SQL example

```rust
use ballista::{extension::SessionConfigExt, prelude::*};
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{CsvReadOptions, SessionConfig, SessionContext},
};

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results, using SQL
#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("Remote SQL Example");

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    let test_data = test_util::examples_test_data();

    ctx.register_csv(
        "test",
        &format!("{test_data}/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;

    let df = ctx
        .sql(
            "SELECT c1, MIN(c12), MAX(c12) \
        FROM test \
        WHERE c11 > 0.1 AND c11 < 0.9 \
        GROUP BY c1",
        )
        .await?;

    df.show().await?;

    Ok(())
}
```

### Distributed DataFrame Example

```bash
cargo run --release --example remote-dataframe
```

#### Source code for distributed DataFrame example

```rust
use ballista::{extension::SessionConfigExt, prelude::*};
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{col, lit, ParquetReadOptions, SessionConfig, SessionContext},
};

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new_with_ballista().with_target_partitions(4);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    let test_data = test_util::examples_test_data();
    let filename = format!("{test_data}/alltypes_plain.parquet");

    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    df.show().await?;

    Ok(())
}
```
