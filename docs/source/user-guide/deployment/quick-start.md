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
RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50051 --bind-grpc-port 50052 --bind-health-port 50053

RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50054 --bind-grpc-port 50055 --bind-health-port 50056
```

## Running the examples

The examples can be run using the `cargo run --example` syntax. Open a new terminal session and run the following commands.

### Distributed SQL Example

```bash
cd examples
cargo run --release --example remote-sql
```

#### Source code for distributed SQL example

```{literalinclude} ../../../../examples/examples/remote-sql.rs
:language: rust
:lines: 18-
```

### Distributed DataFrame Example

```bash
cd examples
cargo run --release --example remote-dataframe
```

#### Source code for distributed DataFrame example

```{literalinclude} ../../../../examples/examples/remote-dataframe.rs
:language: rust
:lines: 18-
```
