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

## Performance

We run some simple benchmarks comparing Ballista with Apache Spark to track progress with performance optimizations.
These are benchmarks derived from TPC-H and not official TPC-H benchmarks. These results are from running individual
queries at scale factor 10 (10 GB) on a single node with a single executor and 24 concurrent tasks.

The tracking issue for improving these results is [#339](https://github.com/apache/arrow-ballista/issues/339).

![benchmarks](https://sqlbenchmarks.io/sqlbench-h/results/env/workstation/sf10/distributed/sqlbench-h-workstation-10-distributed-perquery.png)

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
    - Periodically ([#185](https://github.com/apache/arrow-ballista/issues/185))
    - Add gRPC & REST interfaces for clients/UI to actively call the cleanup for a job or the whole system
  - Fill functional gaps between DataFusion and Ballista
  - Improve task scheduling and data exchange efficiency
  - Better error handling
    - Scheduler restart
  - Improve monitoring, logging, and metrics
  - Auto scaling support
  - Better configuration management
  - Support for multi-scheduler deployments. Initially for resiliency and fault tolerance but ultimately to support
    sharding for scalability and more efficient caching.
- Shuffle improvement
  - Shuffle memory control ([#320](https://github.com/apache/arrow-ballista/issues/320))
  - Improve shuffle IO to avoid producing too many files
  - Support sort-based shuffle
  - Support range partition
  - Support broadcast shuffle ([#342](https://github.com/apache/arrow-ballista/issues/342))
- Scheduler Improvements
  - All-at-once job task scheduling
  - Executor deployment grouping based on resource allocation
- Cloud Support
  - Support Azure Blob Storage ([#294](https://github.com/apache/arrow-ballista/issues/294))
  - Support Google Cloud Storage ([#293](https://github.com/apache/arrow-ballista/issues/293))
- Performance and scalability
  - Implement Adaptive Query Execution ([#387](https://github.com/apache/arrow-ballista/issues/387))
  - Implement bubble execution ([#408](https://github.com/apache/arrow-ballista/issues/408))
  - Improve benchmark results ([#339](https://github.com/apache/arrow-ballista/issues/339))
- Python Support
  - Support Python UDFs ([#173](https://github.com/apache/arrow-ballista/issues/173))

## Architecture Overview

There are currently no up-to-date architecture documents available. You can get a general overview of the architecture
by watching the [Ballista: Distributed Compute with Rust and Apache Arrow][ballista-talk] talk from the New York Open
Statistical Programming Meetup (Feb 2021).

## Contribution Guide

Please see the [Contribution Guide](CONTRIBUTING.md) for information about contributing to Ballista.

[arrow]: https://arrow.apache.org/
[datafusion]: https://github.com/apache/arrow-datafusion
[flight]: https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/
[flight-sql]: https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/
[ballista-talk]: https://www.youtube.com/watch?v=ZZHQaOap9pQ
[user-guide]: https://arrow.apache.org/ballista/
