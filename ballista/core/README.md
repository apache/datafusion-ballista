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

# Ballista Core

Shared library underpinning the [Ballista](https://datafusion.apache.org/ballista/) distributed
query engine. It is a dependency of the `ballista` client, `ballista-scheduler`, and
`ballista-executor` crates rather than something you run on its own.

Embedders depend on it directly when they need Ballista's plan serialization or shuffle operators
without taking the scheduler or executor binaries.

## What is in here

| Module            | Purpose                                                                                   |
| ----------------- | ----------------------------------------------------------------------------------------- |
| `serde`           | Protobuf encoding of logical and physical plans, plus the extension-codec hooks           |
| `execution_plans` | Distributed operators, including `ShuffleWriterExec`, `ShuffleReaderExec`, and the reader |
| `config`          | The `ballista.*` configuration registry that the user guide's tables are generated from   |
| `extension`       | `SessionConfigExt`, for reading and setting Ballista options on a DataFusion session      |
| `object_store`    | S3-capable object-store registry and the `S3Options` config extension                     |
| `planner`         | Shared planning helpers used by the scheduler's distributed planner                       |
| `client`          | Arrow Flight client used to fetch result and shuffle partitions                           |
| `error`           | `BallistaError` and the crate's `Result` alias                                            |

## Cargo features

| Feature                   | Default | Description                                                          |
| ------------------------- | ------- | -------------------------------------------------------------------- |
| `arrow-ipc-optimizations` | Yes     | Arrow IPC fast paths for shuffle read and write                      |
| `build-binary`            | No      | Pulls in `clap` and an AWS-capable `object_store`, for binary builds |
| `spark-compat`            | No      | Registers Spark-compatible functions from `datafusion-spark`         |
| `force_hash_collisions`   | No      | Testing only: forces every value to the same hash bucket             |

## Documentation

- [Ballista user guide](https://datafusion.apache.org/ballista/)
- [Architecture](https://datafusion.apache.org/ballista/contributors-guide/architecture.html)
- [Extending Ballista](https://datafusion.apache.org/ballista/user-guide/extending-components.html)
