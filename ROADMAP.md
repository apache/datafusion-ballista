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

## kapot Roadmap

There is an excellent discussion in https://github.com/apache/arrow-kapot/issues/30 about the future of the project,
and we encourage you to participate and add your feedback there if you are interested in using or contributing to
kapot.

The current focus is on the following items:

- Make production ready
  - Shuffle file cleanup
    - Periodically ([#185](https://github.com/apache/arrow-kapot/issues/185))
    - Add gRPC & REST interfaces for clients/UI to actively call the cleanup for a job or the whole system
  - Fill functional gaps between DataFusion and kapot
  - Improve task scheduling and data exchange efficiency
  - Better error handling
    - Scheduler restart
  - Improve monitoring, logging, and metrics
  - Auto scaling support
  - Better configuration management
  - Support for multi-scheduler deployments. Initially for resiliency and fault tolerance but ultimately to support
    sharding for scalability and more efficient caching.
- Shuffle improvement
  - Shuffle memory control ([#320](https://github.com/apache/arrow-kapot/issues/320))
  - Improve shuffle IO to avoid producing too many files
  - Support sort-based shuffle
  - Support range partition
  - Support broadcast shuffle ([#342](https://github.com/apache/arrow-kapot/issues/342))
- Scheduler Improvements
  - All-at-once job task scheduling
  - Executor deployment grouping based on resource allocation
- Cloud Support
  - Support Azure Blob Storage ([#294](https://github.com/apache/arrow-kapot/issues/294))
  - Support Google Cloud Storage ([#293](https://github.com/apache/arrow-kapot/issues/293))
- Performance and scalability
  - Implement Adaptive Query Execution ([#387](https://github.com/apache/arrow-kapot/issues/387))
  - Implement bubble execution ([#408](https://github.com/apache/arrow-kapot/issues/408))
  - Improve benchmark results ([#339](https://github.com/apache/arrow-kapot/issues/339))
- Python Support
  - Support Python UDFs ([#173](https://github.com/apache/arrow-kapot/issues/173))
