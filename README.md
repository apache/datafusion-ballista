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

# Ballista: Distributed SQL Query Engine, built on Apache Arrow

Ballista is a distributed SQL query engine powered by the Rust implementation of [Apache Arrow][arrow] and
[DataFusion][datafusion].

If you are looking for documentation for a released version of Ballista, please refer to the
[Ballista User Guide][user-guide].

## Overview

Ballista implements a similar design to Apache Spark (particularly Spark SQL), but there are some key differences:

- The choice of Rust as the main execution language avoids the overhead of GC pauses and results in deterministic
  processing times.
- Ballista is designed from the ground up to use columnar data, enabling a number of efficiencies such as vectorized
  processing (SIMD) and efficient compression. Although Spark does have some columnar support, it is still
  largely row-based today.
- The combination of Rust and Arrow provides excellent memory efficiency and memory usage can be 5x - 10x lower than
  Apache Spark in some cases, which means that more processing can fit on a single node, reducing the overhead of
  distributed compute.
- The use of Apache Arrow as the memory model and network protocol means that data can be exchanged efficiently between
  executors using the [Flight Protocol][flight], and between clients and schedulers/executors using the
  [Flight SQL Protocol][flight-sql]

## Features

- Supports HDFS as well as cloud object stores. S3 is supported today and GCS and Azure support is planned.
- DataFrame and SQL APIs available from Python and Rust.
- Clients can connect to a Ballista cluster using [Flight SQL][flight-sql].
- JDBC support via Arrow Flight SQL JDBC Driver
- Scheduler web interface and REST UI for monitoring query progress and viewing query plans and metrics.
- Support for Docker, Docker Compose, and Kubernetes deployment, as well as manual deployment on bare metal.

# Getting Started

The easiest way to get started is to run one of the standalone or distributed [examples](./examples/README.md). After
that, refer to the [Getting Started Guide](ballista/client/README.md).

## Project Status

Ballista supports a wide range of SQL, including CTEs, Joins, and Subqueries and can execute complex queries at scale.

Refer to the [DataFusion SQL Reference](https://arrow.apache.org/datafusion/user-guide/sql/index.html) for more
information on supported SQL.

Ballista is maturing quickly and is now working towards being production ready. See the following roadmap for more details.

## Roadmap

There is an excellent discussion in https://github.com/apache/arrow-ballista/issues/30 about the future of the project,
and we encourage you to participate and add your feedback there if you are interested in using or contributing to
Ballista.

The current focus is on the following items:

- Make production ready
  - Shuffle file cleanup
    - Periodically
    - Add gRPC & REST interfaces for clients/UI to actively call the cleanup for a job or the whole system
  - Fill functional gaps between DataFusion and Ballista
  - Improve task scheduling and data exchange efficiency
  - Better error handling
    - Schedule restart
  - Improve monitoring, logging, and metrics
  - Auto scaling support
  - Better configuration management
- All-at-once job task scheduling
- Shuffle improvement
  - Shuffle memory control
  - Improve shuffle IO to avoid producing too many files
  - Support sort-based shuffle
  - Support range partition
  - Support broadcast shuffle
- Support for multi-scheduler deployments. Initially for resiliency and fault tolerance but ultimately to support
  sharding for scalability and more efficient caching.
- Executor deployment grouping based on resource allocation

## Architecture Overview

There are currently no up-to-date architecture documents available. You can get a general overview of the architecture
by watching the [Ballista: Distributed Compute with Rust and Apache Arrow][ballista-talk] talk from the New York Open
Statistical Programming Meetup (Feb 2021).

## Contribution Guide

Please see [Contribution Guide](CONTRIBUTING.md) for information about contributing to DataFusion.

[arrow]: https://arrow.apache.org/
[datafusion]: https://github.com/apache/arrow-datafusion
[flight]: https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/
[flight-sql]: https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/
[ballista-talk]: https://www.youtube.com/watch?v=ZZHQaOap9pQ
