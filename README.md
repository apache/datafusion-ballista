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

# Ballista: Distributed Compute with Rust, Apache Arrow, and DataFusion

Ballista is a distributed SQL query engine primarily implemented in Rust, and powered by Apache Arrow and
DataFusion. It is built on an architecture that allows other programming languages (such as Python, C++, and
Java) to be supported as first-class citizens without paying a penalty for serialization costs.

The foundational technologies in Ballista are:

- [Apache Arrow](https://arrow.apache.org/) memory model and compute kernels for efficient processing of data.
- [DataFusion Query Engine](https://github.com/apache/arrow-datafusion) for query execution
- [Apache Arrow Flight Protocol](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for efficient
  data transfer between processes.
- [Google Protocol Buffers](https://developers.google.com/protocol-buffers) for serializing query plans, with [plans to
  eventually use substrait.io here](https://github.com/apache/arrow-ballista/issues/32).

Ballista implements a similar design to Apache Spark (particularly Spark SQL), but there are some key differences:

- The choice of Rust as the main execution language avoids the overhead of GC pauses.
- Ballista is designed from the ground up to use columnar data, enabling a number of efficiencies such as vectorized
  processing (SIMD and GPU) and efficient compression. Although Spark does have some columnar support, it is still
  largely row-based today.
- The combination of Rust and Arrow provides excellent memory efficiency and memory usage can be 5x - 10x lower than
  Apache Spark in some cases, which means that more processing can fit on a single node, reducing the overhead of
  distributed compute.
- The use of Apache Arrow as the memory model and network protocol means that data can be exchanged between executors
  in any programming language with minimal serialization overhead.

Ballista can be deployed as a standalone cluster and also supports [Kubernetes](https://kubernetes.io/). In either
case, the scheduler can be configured to use [etcd](https://etcd.io/) as a backing store to (eventually) provide
redundancy in the case of a scheduler failing.

# Project Status and Roadmap

Ballista is currently a proof-of-concept and provides batch execution of SQL queries. Although it is already capable of
executing complex queries, it is not yet scalable or robust.

There is an excellent discussion in https://github.com/apache/arrow-ballista/issues/30 about the future of the project
and we encourage you to participate and add your feedback there if you are interested in using or contributing to
Ballista.

The current initiatives being considered are:

- Continue to improve the current batch-based execution
- Add support for low-latency query execution based on a streaming model
- Adopt [substrait.io](https://substrait.io/) to allow other query engines to be integrated

# Getting Started

Refer to the core [Ballista crate README](ballista/rust/client/README.md) for the Getting Started guide.

## Architecture Overview

- [Architecture Overview](ballista/docs/architecture.md)
- [Ballista: Distributed Compute with Rust and Apache Arrow](https://www.youtube.com/watch?v=ZZHQaOap9pQ) talk at
  the New York Open Statistical Programming Meetup (Feb 2021)

## Contribution Guide

Please see [Contribution Guide](CONTRIBUTING.md) for information about contributing to DataFusion.
