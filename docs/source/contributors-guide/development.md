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

# Ballista Development

We welcome participation from everyone and encourage you to join us, ask
questions, and get involved.

All participation in the Apache DataFusion Ballista project is governed by the
Apache Software Foundation's [code of
conduct](https://www.apache.org/foundation/policies/conduct.html).

## Development Environment

The easiest way to get started if you are using VSCode or IntelliJ IDEA is to open the provided [Dev Container](https://containers.dev/overview)
which will install all the required dependencies including Rust, Docker, Node.js and Yarn. A Dev Container is a
development environment that runs in a Docker container. It is configured with all the required dependencies to
build and test the project. It also includes VS Code and the Rust and Node.js extensions. Other supporting tools
that use Dev Containers can be seen [here](https://containers.dev/supporting)

To use the Dev Container, open the project in VS Code and then click the "Reopen in Container" button in the
bottom right corner of the IDE.

If you are not using the Dev Container or VScode, you will need to install these dependencies yourself.

- [Rust](https://www.rust-lang.org/tools/install)
- [Protobuf Compiler](https://protobuf.dev/downloads/) is required to build the project.
- [Node.js](https://nodejs.org/en/download/) is required to build the project.
- [Yarn](https://classic.yarnpkg.com/en/docs/install) is required to build the UI.
- [Docker](https://docs.docker.com/get-docker/) is required to run the integration tests.

## Build the project

From the root of the project, build release binaries.

```shell
cargo build --release
```

## Testing the project

```shell
cargo test
```

## Running the examples

```shell
cd examples
cargo run --example standalone_sql --features=ballista/standalone
```

## Benchmarking

For performance testing and benchmarking with TPC-H and other datasets, see the [benchmarks README](../../../benchmarks/README.md).

This includes instructions for:

- Generating TPC-H test data
- Running benchmarks against DataFusion and Ballista
- Comparing performance with Apache Spark
- Running load tests
