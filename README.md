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

# Ballista: Making DataFusion Applications Distributed

Ballista is a distributed execution engine which makes [Apache DataFusion](https://github.com/apache/datafusion) applications distributed.

Existing DataFusion application:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
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

can be distributed with few lines of code changed:

> [!IMPORTANT]  
> There is a gap between DataFusion and Ballista, which may bring incompatibilities. The community is working hard to close this gap

```rust
use ballista::prelude::*;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  // create DataFusion SessionContext with ballista standalone cluster started
  let ctx = SessionContext::standalone();

  // register the table
  ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;

  // create a plan to run a SQL query
  let df = ctx.sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100").await?;

  // execute and print results
  df.show().await?;
  Ok(())
}
```

If you are looking for documentation or more examples, please refer to the [Ballista User Guide][user-guide].

## Architecture

A Ballista cluster consists of one or more scheduler processes and one or more executor processes. These processes
can be run as native binaries and are also available as Docker Images, which can be easily deployed with
[Docker Compose](https://datafusion.apache.org/ballista/user-guide/deployment/docker-compose.html) or
[Kubernetes](https://datafusion.apache.org/ballista/user-guide/deployment/kubernetes.html).

The following diagram shows the interaction between clients and the scheduler for submitting jobs, and the interaction
between the executor(s) and the scheduler for fetching tasks and reporting task status.

![Ballista Cluster Diagram](docs/source/contributors-guide/ballista_architecture.excalidraw.svg)

See the [architecture guide](docs/source/contributors-guide/architecture.md) for more details.

## Performance

We run some simple benchmarks comparing Ballista with Apache Spark to track progress with performance optimizations.
These are benchmarks derived from TPC-H and not official TPC-H benchmarks. These results are from running individual
queries at scale factor 100 (100 GB) on a single node with a single executor and 8 concurrent tasks.

### Overall Speedup

The overall speedup is 2.9x

![benchmarks](docs/source/_static/images/tpch_allqueries.png)

### Per Query Comparison

![benchmarks](docs/source/_static/images/tpch_queries_compare.png)

### Relative Speedup

![benchmarks](docs/source/_static/images/tpch_queries_speedup_rel.png)

### Absolute Speedup

![benchmarks](docs/source/_static/images/tpch_queries_speedup_abs.png)

# Getting Started

The easiest way to get started is to run one of the standalone or distributed [examples](./examples/README.md). After
that, refer to the [Getting Started Guide](ballista/client/README.md).

## Project Status

Ballista supports a wide range of SQL, including CTEs, Joins, and subqueries and can execute complex queries at scale,
but still there is a gap between DataFusion and Ballista which we want to bridge in near future.

Refer to the [DataFusion SQL Reference](https://datafusion.apache.org/user-guide/sql/index.html) for more
information on supported SQL.

## Contribution Guide

Please see the [Contribution Guide](CONTRIBUTING.md) for information about contributing to Ballista.

[user-guide]: https://datafusion.apache.org/ballista/
