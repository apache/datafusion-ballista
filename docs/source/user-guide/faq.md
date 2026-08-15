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

# Frequently Asked Questions

## What is the relationship between DataFusion and Ballista?

DataFusion is a library for executing queries in-process using the Apache Arrow memory
model and computational kernels. It is designed to run within a single process, using threads
for parallel query execution.

Ballista is a distributed compute platform for DataFusion workloads.

## Why does Ballista write shuffle data to disk instead of streaming it between stages?

Ballista uses a blocking shuffle, like Apache Spark: each query stage runs to completion and
writes its output to local disk before any downstream stage starts. Other DataFusion-based
distributed engines, such as [DataFusion Distributed](https://datafusion-contrib.github.io/datafusion-distributed/)
and [Sail](https://github.com/lakehq/sail), stream data between stages instead.

The blocking model costs latency — a stage cannot start until the slowest task of its input
stage finishes, and every intermediate byte is written and read back. In exchange it lets a
query run wider than the cluster has slots for, lets a lost executor cost a few re-run tasks
rather than the whole query, keeps shuffles larger than cluster memory safe, and gives the
adaptive planner exact statistics from completed stages.

If your queries are interactive, run for seconds, and produce small intermediate results, a
pipelined engine will likely be faster. If they are large, long-running batch or ETL jobs,
especially on preemptible nodes, the blocking model is the reason Ballista finishes them.

For the full reasoning, see [Shuffle Design](../contributors-guide/shuffle.md). For the
mechanics and tuning knobs, see the
[Shuffle Implementation section of the tuning guide](tuning-guide.md#shuffle-implementation).
