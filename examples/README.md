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

Source: [`examples/examples/standalone-sql.rs`](https://github.com/apache/datafusion-ballista/blob/main/examples/examples/standalone-sql.rs)

```bash
cargo run --example standalone-substrait --features="ballista/standalone,ballista-scheduler/substrait"
```

Source: [`examples/examples/standalone-substrait.rs`](https://github.com/apache/datafusion-ballista/blob/main/examples/examples/standalone-substrait.rs)

## Distributed Examples

For background information on the Ballista architecture, refer to
the [Ballista README](../ballista/client/README.md).

### Start a cluster

The distributed examples need a running scheduler and at least one executor. Follow
[Ballista Quickstart](https://datafusion.apache.org/ballista/user-guide/deployment/quick-start.html)
to build the binaries and start them, then come back here.

### Running the examples

The examples can be run using the `cargo run --example` syntax, from the `examples` directory.

### Distributed SQL Example

```bash
cargo run --release --example remote-sql
```

Source: [`examples/examples/remote-sql.rs`](https://github.com/apache/datafusion-ballista/blob/main/examples/examples/remote-sql.rs)

### Distributed DataFrame Example

```bash
cargo run --release --example remote-dataframe
```

Source: [`examples/examples/remote-dataframe.rs`](https://github.com/apache/datafusion-ballista/blob/main/examples/examples/remote-dataframe.rs)

### Distributed datafusion-spark example

The scheduler and executor binaries must be built with the `spark-compat` feature enabled for this
example.

```bash
cargo build --release --features spark-compat
```

```bash
cargo run --release --example remote-spark-functions --features="ballista-core/spark-compat"
```

Source: [`examples/examples/remote-spark-functions.rs`](https://github.com/apache/datafusion-ballista/blob/main/examples/examples/remote-spark-functions.rs)

## All examples

Every example in [`examples/examples/`](https://github.com/apache/datafusion-ballista/tree/main/examples/examples),
including those not walked through above. Run each from this directory.

| Example                        | Cluster     | Command                                                                              |
| ------------------------------ | ----------- | ------------------------------------------------------------------------------------ |
| `standalone-sql.rs`            | in-process  | `cargo run --example standalone_sql`                                                 |
| `standalone-broadcast-join.rs` | in-process  | `cargo run --example standalone_broadcast_join`                                      |
| `standalone-substrait.rs`      | in-process  | `cargo run --example standalone-substrait --features="ballista-scheduler/substrait"` |
| `remote-sql.rs`                | distributed | `cargo run --example remote-sql`                                                     |
| `remote-dataframe.rs`          | distributed | `cargo run --example remote-dataframe`                                               |
| `remote-spark-functions.rs`    | distributed | `cargo run --example remote-spark-functions --features="ballista-core/spark-compat"` |
| `custom-client.rs`             | distributed | `cargo run --example custom-client`                                                  |
| `custom-scheduler.rs`          | n/a         | `cargo run --example custom-scheduler`                                               |
| `custom-executor.rs`           | n/a         | `cargo run --example custom-executor`                                                |
| `mtls-cluster.rs`              | in-process  | `cargo run --example mtls-cluster --features=tls`                                    |

The `ballista/standalone` feature the in-process examples need is on by default, so it only has to be
named explicitly when building with `--no-default-features`.
