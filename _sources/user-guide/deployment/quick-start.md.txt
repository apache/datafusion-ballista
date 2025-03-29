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

# Ballista Quickstart

A simple way to start a local cluster for testing purposes is to use cargo to build the project and then run the scheduler and executor binaries directly.

Project Requirements:

- [Rust](https://www.rust-lang.org/tools/install)
- [Protobuf Compiler](https://protobuf.dev/downloads/)

## Build the project

From the root of the project, build release binaries.

```shell
cargo build --release
```

Start a Ballista scheduler process in a new terminal session.

```shell
RUST_LOG=info ./target/release/ballista-scheduler
```

Start one or more Ballista executor processes in new terminal sessions. When starting more than one
executor, a unique port number must be specified for each executor.

```shell
RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50051 --bind-grpc-port 50052

RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50053 --bind-grpc-port 50054
```

## Running the examples

The examples can be run using the `cargo run --bin` syntax. Open a new terminal session and run the following commands.

### Distributed SQL Example

```bash
cd examples
cargo run --release --example remote-sql
```

#### Source code for distributed SQL example

```rust
use ballista::prelude::*;
use ballista_examples::test_util;
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
cd examples
cargo run --release --example remote-dataframe
```

#### Source code for distributed DataFrame example

```rust
use ballista::prelude::*;
use ballista_examples::test_util;
use datafusion::{
    prelude::{col, lit, ParquetReadOptions, SessionContext},
};

#[tokio::main]
async fn main() -> Result<()> {
    // creating SessionContext with default settings
    let ctx = SessionContext::remote("df://localhost:50050").await?;

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
