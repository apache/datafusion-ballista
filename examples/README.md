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

# Standalone Examples

The standalone example is the easiest to get started with. Ballista supports a standalone mode where a scheduler
and executor are started in-process.

```bash
cargo run --example standalone_sql --features="ballista/standalone"
```

### Source code for standalone SQL example

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "1")
        .build()?;

    let ctx = BallistaContext::standalone(&config, 2).await?;

    ctx.register_csv(
        "test",
        "testdata/aggregate_test_100.csv",
        CsvReadOptions::new(),
    )
    .await?;

    let df = ctx.sql("select count(1) from test").await?;

    df.show().await?;
    Ok(())
}

```

# Distributed Examples

For background information on the Ballista architecture, refer to
the [Ballista README](../ballista/client/README.md).

## Start a standalone cluster

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

## Running the examples

The examples can be run using the `cargo run --bin` syntax.

## Distributed SQL Example

```bash
cargo run --release --bin sql
```

### Source code for distributed SQL example

```rust
use ballista::prelude::*;
use datafusion::prelude::CsvReadOptions;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results, using SQL
#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "4")
        .build()?;
    let ctx = BallistaContext::remote("localhost", 50050, &config).await?;

    // register csv file with the execution context
    ctx.register_csv(
        "test",
        "testdata/aggregate_test_100.csv",
        CsvReadOptions::new(),
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT c1, MIN(c12), MAX(c12) \
        FROM test \
        WHERE c11 > 0.1 AND c11 < 0.9 \
        GROUP BY c1",
        )
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}
```

## Distributed DataFrame Example

```bash
cargo run --release --bin dataframe
```

### Source code for distributed DataFrame example

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "4")
        .build()?;
    let ctx = BallistaContext::remote("localhost", 50050, &config).await?;

    let filename = "testdata/alltypes_plain.parquet";

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    // print the results
    df.show().await?;

    Ok(())
}
```
