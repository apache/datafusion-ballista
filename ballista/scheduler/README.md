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

# Ballista Scheduler

The scheduler process for the [Ballista](https://datafusion.apache.org/ballista/) distributed query
engine, and the library behind it. The scheduler accepts a logical or physical plan from a client,
splits it into stages at shuffle boundaries, and hands tasks to executors.

## Running

```bash,ignore
cargo install --locked ballista-scheduler
RUST_LOG=info ballista-scheduler
```

It binds port 50050 by default, serving gRPC, the REST API, and the `/healthz` and `/readyz` probes
on that one port. See the
[scheduler guide](https://datafusion.apache.org/ballista/user-guide/scheduler.html) for the full
flag list.

The crate also builds `ballista-history-server`, which replays the event logs a scheduler writes
under `--event-log-dir` and serves the same `/api/*` responses for jobs that have already finished.

## Using it as a library

`SchedulerConfig` carries override hooks, so an embedder can supply its own session builder, config
producer, or plan codecs without forking. See
[extending Ballista](https://datafusion.apache.org/ballista/user-guide/extending-components.html)
and `examples/examples/custom-scheduler.rs`.

## Cargo features

| Feature                    | Default | Description                                          |
| -------------------------- | ------- | ---------------------------------------------------- |
| `build-binary`             | Yes     | Builds the binaries, with CLI parsing and logging    |
| `rest-api`                 | Yes     | REST API endpoints and the history server            |
| `substrait`                | No      | Accepts Substrait plans                              |
| `prometheus-metrics`       | No      | Prometheus metrics behind `GET /api/metrics`         |
| `graphviz-support`         | No      | SVG plan rendering for `GET /api/job/{id}/dot_svg`   |
| `keda-scaler`              | No      | KEDA external-scaler endpoint for executor autoscale |
| `spark-compat`             | No      | Registers Spark-compatible functions                 |
| `disable-stage-plan-cache` | No      | Disables stage plan caching, for plan-rewrite work   |

## Documentation

- [Ballista user guide](https://datafusion.apache.org/ballista/)
- [Scheduler](https://datafusion.apache.org/ballista/user-guide/scheduler.html)
- [History server](https://datafusion.apache.org/ballista/user-guide/history-server.html)
- [Architecture](https://datafusion.apache.org/ballista/contributors-guide/architecture.html)
