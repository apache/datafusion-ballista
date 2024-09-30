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

## kapot Code Organization

This section provides links to the source code for major areas of functionality.

### kapot-core crate

- [Crate Source](https://github.com/apache/datafusion-kapot/blob/main/kapot/core)
- [Protocol Buffer Definition](https://github.com/apache/datafusion-kapot/blob/main/kapot/core/proto/kapot.proto)
- [Execution Plans](https://github.com/apache/datafusion-kapot/tree/main/kapot/core/src/execution_plans)
- [kapot Client](https://github.com/apache/datafusion-kapot/blob/main/kapot/core/src/client.rs)

### kapot-scheduler crate

- [Crate Source](https://github.com/apache/datafusion-kapot/tree/main/kapot/scheduler)
- [Distributed Query Planner](https://github.com/apache/datafusion-kapot/blob/main/kapot/scheduler/src/planner.rs)
- [gRPC Service](https://github.com/apache/datafusion-kapot/blob/main/kapot/scheduler/src/scheduler_server/grpc.rs)
- [Flight SQL Service](https://github.com/apache/datafusion-kapot/blob/main/kapot/scheduler/src/flight_sql.rs)
- [REST API](https://github.com/apache/datafusion-kapot/tree/main/kapot/scheduler/src/api)
- [Web UI](https://github.com/apache/datafusion-kapot/tree/main/kapot/scheduler/ui)
- [Prometheus Integration](https://github.com/apache/datafusion-kapot/blob/main/kapot/scheduler/src/metrics/prometheus.rs)

### kapot-executor crate

- [Crate Source](https://github.com/apache/datafusion-kapot/tree/main/kapot/executor)
- [Flight Service](https://github.com/apache/datafusion-kapot/blob/main/kapot/executor/src/flight_service.rs)
- [Executor Server](https://github.com/apache/datafusion-kapot/blob/main/kapot/executor/src/executor_server.rs)

### kapot crate

- [Crate Source](https://github.com/apache/datafusion-kapot/tree/main/kapot/client)
- [Context](https://github.com/apache/datafusion-kapot/blob/main/kapot/client/src/context.rs)

### Pykapot

- [Source](https://github.com/apache/datafusion-kapot/tree/main/python)
- [Context](https://github.com/apache/datafusion-kapot/blob/main/python/src/context.rs)
