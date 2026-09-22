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

# Ballista Executor

The executor process for the [Ballista](https://datafusion.apache.org/ballista/) distributed query
engine, and the library behind it. Executors register with a scheduler, run the tasks it assigns,
write shuffle output to local storage, and serve those partitions over Arrow Flight.

## Running

```bash,ignore
cargo install --locked ballista-executor
RUST_LOG=info ballista-executor --vcores 4
```

Each executor binds three ports: Arrow Flight on `--bind-port` (50051), gRPC on `--bind-grpc-port`
(50052), and HTTP health on `--bind-health-port` (50053). Running a second executor on one host
means moving all three.

`--vcores` sets how many partitions the executor runs at once, defaulting to the host's physical
core count. The memory pool is auto-sized from the detected host or cgroup limit; see the
[tuning guide](https://datafusion.apache.org/ballista/user-guide/tuning-guide.html).

## Using it as a library

`ExecutorProcessConfig` carries the same override hooks as the scheduler, so an embedder can supply
its own runtime producer, config producer, or plan codecs. See
`examples/examples/custom-executor.rs`. `new_standalone_executor` starts an in-process executor,
which is what the client's standalone mode uses.

## Cargo features

| Feature                   | Default | Description                                              |
| ------------------------- | ------- | -------------------------------------------------------- |
| `arrow-ipc-optimizations` | Yes     | Arrow IPC fast paths for shuffle read and write          |
| `build-binary`            | Yes     | Builds the binary, with CLI parsing, logging, and probes |
| `mimalloc`                | Yes     | mimalloc allocator, enabled through `build-binary`       |
| `spark-compat`            | No      | Registers Spark-compatible functions                     |

## Documentation

- [Ballista user guide](https://datafusion.apache.org/ballista/)
- [Tuning guide](https://datafusion.apache.org/ballista/user-guide/tuning-guide.html)
- [Architecture](https://datafusion.apache.org/ballista/contributors-guide/architecture.html)
