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

Ballista allows queries to be executed in a distributed cluster. A cluster consists of one or
more scheduler processes and one or more executor processes.

The scheduler accepts logical query plans and translates them into physical query plans using DataFusion and then
runs a secondary planning process to translate the physical query plan into a _distributed_ physical
query plan by replacing any operator in the DataFusion plan which performs a repartition with a stage boundary
(i.e. a shuffle exchange).

This results in a plan that contains a number of query stages that can be executed independently. There are
dependencies between query stages and these dependencies form a directionally-acyclic graph (DAG) because a query
stage cannot start until its child query stages have completed.

Each query stage has one or more partitions that can be processed in parallel by the available
executors in the cluster. This is the basic unit of scalability in Ballista.

The output of each query stage is persisted to disk and future query stages will request this data from the executors
that produced it. The persisted output will be partitioned according to the partitioning scheme that was defined for
the query stage and this typically differs from the partitioning scheme of the query stage that will consume this
intermediate output since it is the changes in partitioning in the plan that define the query stage boundaries.

This exchange of data between query stages is called a "shuffle exchange" in Apache Spark.

The following diagram shows the flow of requests and responses between the client, scheduler, and executor
processes.

![Query Execution Flow](images/query-execution.png)

## Scheduler Process

The scheduler process implements a gRPC interface (defined in
[ballista.proto](../../ballista/core/proto/ballista.proto)). The interface provides the following methods:

| Method               | Description                                                          |
| -------------------- | -------------------------------------------------------------------- |
| ExecuteQuery         | Submit a logical query plan or SQL query for execution               |
| GetExecutorsMetadata | Retrieves a list of executors that have registered with a scheduler  |
| GetJobStatus         | Get the status of a submitted query                                  |
| RegisterExecutor     | Executors call this method to register themselves with the scheduler |

The scheduler currently uses in-memory state storage.

## Executor Process

The executor process implements the Apache Arrow Flight gRPC interface and is responsible for:

- Connecting to the scheduler and requesting tasks to execute
- Executing tasks within a query stage and persisting the results to disk in Apache Arrow IPC Format
- Making query stage output partitions available as "Flights" so that they can be retrieved by other executors as well
  as by clients

## Rust Client

The Rust client provides a `BallistaContext` that allows queries to be built using DataFrames or SQL (or both).

The client executes the query plan by submitting an `ExecuteQuery` request to the scheduler and then calls
`GetJobStatus` to check for completion. On completion, the client receives a list of locations for the Flights
containing the results for the query and will then connect to the appropriate executor processes to retrieve
those results.
