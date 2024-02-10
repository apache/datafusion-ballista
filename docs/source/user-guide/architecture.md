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

# Ballista Architecture

## Overview

Ballista’s primary purpose is to provide a distributed SQL query engine implemented in the Rust programming
language and using the Apache Arrow memory model.

Ballista also provides a DataFrame API (both in Rust and Python), suitable for constructing ETL pipelines and
analytical queries. The DataFrame API is inspired by Apache Spark and is currently better suited for ETL/SQL work 
than for data science.

### Cluster

A Ballista cluster consists of one or more scheduler processes and one or more executor processes. These processes
can be run as native binaries and are also available as Docker Images, which can be easily deployed with
[Docker Compose](https://arrow.apache.org/ballista/user-guide/deployment/docker-compose.html) or
[Kubernetes](https://arrow.apache.org/ballista/user-guide/deployment/kubernetes.html).

The following diagram shows the interaction between clients and the scheduler for submitting jobs, and the interaction 
between the executor(s) and the scheduler for fetching tasks and reporting task status.

![Ballista Cluster Diagram](ballista.drawio.png)

### Scheduler

The scheduler provides the following capabilities:

- gRPC service for submitting and managing jobs
- Flight SQL API
- REST API for monitoring jobs
- Web user interface for monitoring jobs

Jobs are submitted to the scheduler's gRPC service from a client context, either in the form of a logical query
plan or a SQL string. The scheduler then creates an execution graph, which contains a physical plan broken down into
stages (pipelines) that can be scheduled independently.

It is possible to have multiple schedulers running with shared state in etcd, so that jobs can continue to run 
even if a scheduler process fails.

### Executor

The executor processes connect to a scheduler and poll for tasks to perform. These tasks are physical plans in
protocol buffer format. These physical plans are typically executed against multiple partitions of input data. Executors
can execute multiple partitions of the same plan in parallel.

### Shuffle

Each stage of the execution graph has the same partitioning scheme for all of the operators in the plan. However,
the output of each stage typically needs to be repartitioned before it can be used as the input to the next stage. An
example of this is when a query contains multiple joins. Data needs to be partitioned by the join keys before the join
can be performed.

Each executor will re-partition the output of the stage it is running so that it can be consumed by the next
stage. This mechanism is known as an Exchange or a Shuffle. The logic for this can be found in the ShuffleWriterExec
and ShuffleReaderExec operators.

### Clients

There are multiple clients available for submitting jobs to a Ballista cluster:

- The [Ballista CLI](https://github.com/apache/arrow-ballista/tree/main/ballista-cli) provides a SQL command-line interface
- The Python bindings ([PyBallista](https://github.com/apache/arrow-ballista/tree/main/python)) provide a session context with support for SQL and DataFrame operations
- The [ballista crate](https://crates.io/crates/ballista) provides a native Rust session context with support for SQL and DataFrame operations
- The [Flight SQL JDBC driver](https://arrow.apache.org/docs/java/flight_sql_jdbc_driver.html) can be used from popular SQL tools to execute SQL queries against a cluster

## Design Principles

### Arrow-native

Ballista uses the Apache Arrow memory format during query execution, and Apache Arrow IPC format on disk for
shuffle files and for exchanging data between executors. Queries can be submitted using the Arrow Flight SQL API 
and the Arrow Flight SQL JDBC Driver.

### Language Agnostic

Although most of the implementation code is written in Rust, the scheduler and executor APIs are based on open
standards, including protocol buffers, gRPC, Apache Arrow IPC, and Apache Arrow Flight SQL.

This language agnostic approach will allow Ballista to eventually support UDFs in languages other than Rust,
including Wasm.

### Extensible

Many Ballista users have their own distributed query engines that use Ballista as a foundation, rather than
using Ballista directly. This allows the scheduler and executor processes to be extended with support for
additional data formats, operators, expressions, or custom SQL dialects or other DSLs.

Ballista uses the DataFusion query engine for query execution, but it should be possible to plug in other execution
engines.

## Deployment Options

Ballista supports a wide range of deployment options. See the [deployment guide](./deployment) for more information.
