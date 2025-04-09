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

# Ballista: Distributed Scheduler for Apache DataFusion

Ballista is a distributed compute platform primarily implemented in Rust, and powered by Apache Arrow and DataFusion. It is built on an architecture that allows other programming languages (such as Python, C++, and Java) to be supported as first-class citizens without paying a penalty for serialization costs.

![logo](https://github.com/apache/datafusion-ballista/blob/main/docs/source/_static/images/ballista-logo.png?raw=true)

Ballista is a distributed query execution engine that enhances [Apache DataFusion](https://github.com/apache/datafusion) by enabling the parallelized execution of workloads across multiple nodes in a distributed environment.

Existing DataFusion application:

```rust,no_run
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // datafusion context
  let ctx = SessionContext::new();

  // register the table
  ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

can be distributed with few lines changed:

```rust,no_run
use ballista::prelude::*;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // create SessionContext with ballista support
    // standalone context will start all required
    // ballista infrastructure in the background as well
    let ctx = SessionContext::standalone().await?;

    // everything else remains the same

    // register the table
    ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new())
        .await?;

    // create a plan to run a SQL query
    let df = ctx
        .sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100")
        .await?;

    // execute and print results
    df.show().await?;
    Ok(())
}
```

## Starting a cluster

![architecture](https://github.com/apache/datafusion-ballista/blob/main/docs/source/contributors-guide/ballista_architecture.excalidraw.svg?raw=true)

A simple way to start a local cluster for testing purposes is to use cargo to install the scheduler and executor crates.

```bash
cargo install --locked ballista-scheduler
cargo install --locked ballista-executor
```

With these crates installed, it is now possible to start a scheduler process.

```bash
RUST_LOG=info ballista-scheduler
```

The scheduler will bind to port `50050` by default.

Next, start an executor processes in a new terminal session with the specified concurrency level.

```bash
RUST_LOG=info ballista-executor -c 4
```

The executor will bind to port `50051` by default. Additional executors can be started by manually specifying a bind port.

For full documentation, refer to the deployment section of the
[Ballista User Guide](https://datafusion.apache.org/ballista/user-guide/deployment/)

## Executing a Query

Ballista provides a custom `SessionContext` as a starting point for creating queries. DataFrames can be created by invoking the `read_csv`, `read_parquet`, and `sql` methods.

To build a simple ballista example, run the following command to add the dependencies to your `Cargo.toml` file:

```bash
cargo add ballista datafusion tokio
```

```rust,no_run
use ballista::prelude::*;
use datafusion::common::Result;
use datafusion::prelude::{col, SessionContext, ParquetReadOptions};
use datafusion::functions_aggregate::{min_max::min, min_max::max, sum::sum, average::avg};

#[tokio::main]
async fn main() -> Result<()> {

    // connect to Ballista scheduler
    let ctx = SessionContext::remote("df://localhost:50050").await?;

    let filename = "testdata/yellow_tripdata_2022-01.parquet";

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["passenger_count", "fare_amount"])?
        .aggregate(
            vec![col("passenger_count")],
            vec![
                min(col("fare_amount")),
                max(col("fare_amount")),
                avg(col("fare_amount")),
                sum(col("fare_amount")),
            ],
        )?
        .sort(vec![col("passenger_count").sort(true, true)])?;

    df.show().await?;

    Ok(())
}
```

The output should look similar to the following table.

```text
+-----------------+--------------------------+--------------------------+--------------------------+--------------------------+
| passenger_count | MIN(?table?.fare_amount) | MAX(?table?.fare_amount) | AVG(?table?.fare_amount) | SUM(?table?.fare_amount) |
+-----------------+--------------------------+--------------------------+--------------------------+--------------------------+
|                 | -159.5                   | 285.2                    | 17.60577640099004        | 1258865.829999991        |
| 0               | -115                     | 500                      | 11.794859107585335       | 614052.1600000001        |
| 1               | -480                     | 401092.32                | 12.61028389876563        | 22623542.879999973       |
| 2               | -250                     | 640.5                    | 13.79501011585127        | 4732047.139999998        |
| 3               | -130                     | 480                      | 13.473184817311106       | 1139427.2400000002       |
| 4               | -250                     | 464                      | 14.232650547832726       | 502711.4499999997        |
+-----------------+--------------------------+--------------------------+--------------------------+--------------------------+
```

More [examples](../../examples/examples/) can be found in the datafusion-ballista repository.

## Performance

We run some simple benchmarks comparing Ballista with Apache Spark to track progress with performance optimizations.

These are benchmarks derived from TPC-H and not official TPC-H benchmarks. These results are from running individual queries at scale factor 100 (100 GB) on a single node with a single executor and 8 concurrent tasks.

### Overall Speedup

The overall speedup is 2.9x

![benchmarks](https://github.com/apache/datafusion-ballista/blob/main/docs/source/_static/images/tpch_allqueries.png?raw=true)

### Per Query Comparison

![benchmarks](https://github.com/apache/datafusion-ballista/blob/main/docs/source/_static/images/tpch_queries_compare.png?raw=true)

### Relative Speedup

![benchmarks](https://github.com/apache/datafusion-ballista/blob/main/docs/source/_static/images/tpch_queries_speedup_rel.png?raw=true)

### Absolute Speedup

![benchmarks](https://github.com/apache/datafusion-ballista/blob/main/docs/source/_static/images/tpch_queries_speedup_abs.png?raw=true)
