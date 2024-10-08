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

## Ballista Code Organization

This section provides links to the source code for major areas of functionality.

### ballista-core crate

- [Crate Source](https://github.com/apache/datafusion-ballista/blob/main/ballista/core)
- [Protocol Buffer Definition](https://github.com/apache/datafusion-ballista/blob/main/ballista/core/proto/ballista.proto)
- [Execution Plans](https://github.com/apache/datafusion-ballista/tree/main/ballista/core/src/execution_plans)
- [Ballista Client](https://github.com/apache/datafusion-ballista/blob/main/ballista/core/src/client.rs)

### ballista-scheduler crate

- [Crate Source](https://github.com/apache/datafusion-ballista/tree/main/ballista/scheduler)
- [Distributed Query Planner](https://github.com/apache/datafusion-ballista/blob/main/ballista/scheduler/src/planner.rs)
- [gRPC Service](https://github.com/apache/datafusion-ballista/blob/main/ballista/scheduler/src/scheduler_server/grpc.rs)
- [Flight SQL Service](https://github.com/apache/datafusion-ballista/blob/main/ballista/scheduler/src/flight_sql.rs)
- [REST API](https://github.com/apache/datafusion-ballista/tree/main/ballista/scheduler/src/api)
- [Prometheus Integration](https://github.com/apache/datafusion-ballista/blob/main/ballista/scheduler/src/metrics/prometheus.rs)

### ballista-executor crate

- [Crate Source](https://github.com/apache/datafusion-ballista/tree/main/ballista/executor)
- [Flight Service](https://github.com/apache/datafusion-ballista/blob/main/ballista/executor/src/flight_service.rs)
- [Executor Server](https://github.com/apache/datafusion-ballista/blob/main/ballista/executor/src/executor_server.rs)

### ballista crate

- [Crate Source](https://github.com/apache/datafusion-ballista/tree/main/ballista/client)
- [Context](https://github.com/apache/datafusion-ballista/blob/main/ballista/client/src/context.rs)

### PyBallista

- [Source](https://github.com/apache/datafusion-ballista/tree/main/python)
- [Context](https://github.com/apache/datafusion-ballista/blob/main/python/src/context.rs)
