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

# Kapôt: Distributed SQL Query Engine, built on Apache Arrow

Kapôt is a distributed SQL query engine powered by the Rust implementation of [Apache Arrow][arrow] and
[Apache Arrow DataFusion][datafusion]. It's based on [Apache Datafusion Ballista][https://datafusion.apache.org/ballista/], but it is being updated.

Kapôt is designed to work primary on a kuberentes cluster, but is also able to work with a simple cluster joining instances. It's already designed to work primary on distributed file systems like S3 compatibles.

If you are looking for documentation for a released version of Kapôt, please refer to the

[Kapôt User Guide][user-guide].

## Overview

Kapôt implements a similar design to Apache Spark (particularly Spark SQL), but there are some key differences:

- The choice of Rust as the main execution language avoids the overhead of GC pauses and results in deterministic
  processing times.
- Kapôt is designed from the ground up to use columnar data, enabling a number of efficiencies such as vectorized
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
- Clients can connect to a Kapôt cluster using [Flight SQL][flight-sql].
- JDBC support via Arrow Flight SQL JDBC Driver
- Scheduler web interface and REST UI for monitoring query progress and viewing query plans and metrics.
- Support for Docker, Docker Compose, and Kubernetes deployment, as well as manual deployment on bare metal.

