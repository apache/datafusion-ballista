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

## Scheduler Identity

Each scheduler has a unique identifier used in cluster state, `/api/state`, and
history event logs. Set it explicitly with `--id` when you want a stable value;
otherwise the scheduler generates the same UUID-backed instance identity used
for executor ids at startup. Use unique scheduler ids when multiple schedulers
write to shared history or cluster state.

Executor ids follow the same model: each active executor registered with the
same scheduler needs a unique id. Set it explicitly with `--id` when you want a
stable value; otherwise the executor generates one at startup. A second active
executor using an id that is already registered is rejected; the id can be
reused after the previous executor is removed from scheduler state.

Executors report task status to the scheduler callback endpoint, which is built
from `--external-host` and `--bind-port`.

## Fetching Query Results

By default a client fetches the result partitions of a query directly from the executors that
produced them, over Arrow Flight. This keeps the scheduler out of the data path, but it requires
every client to have network access to every executor — which is not the case in isolated
environments such as Kubernetes, where clients can usually reach only a few entry points.

For those deployments, run a Result Service (`ballista-result-service`) that clients can reach, and
have the scheduler advertise its address:

```bash
ballista-result-service --bind-port 50055
ballista-scheduler --advertise-flight-endpoint ballista-results.example.com:50055
```

Clients then fetch each partition from the Result Service, which forwards the fetch to the executor
that holds it, so the scheduler serves no result data. The Result Service holds no state: scale it
by running more replicas behind a gRPC-aware load balancer and advertising the load balancer's
address. For Kubernetes manifests, see
[Serving Results to Clients Outside the Cluster](deployment/kubernetes.md#serving-results-to-clients-outside-the-cluster).

### Behind a TLS-terminating ingress

To serve results through an ingress or load balancer that terminates TLS, advertise its address. The
Result Service replicas behind it keep serving plaintext:

```bash
ballista-result-service --bind-port 50055
ballista-scheduler --advertise-flight-endpoint ballista-results.example.com:443
```

Clients choose TLS for result fetches themselves, with `ballista.client.use_tls`, and need TLS roots
for the ingress's certificate, supplied through a gRPC endpoint override as the
[mTLS cluster example] does. `ballista.client.use_tls` applies to every result fetch a client
makes, including direct fetches from executors. The ingress must forward HTTP/2 (gRPC) to the
replicas, for example with ingress-nginx's `nginx.ingress.kubernetes.io/backend-protocol: "GRPC"`
annotation, and its read and send timeouts must be long enough for the largest result stream.

[mtls cluster example]: https://github.com/apache/datafusion-ballista/blob/main/examples/examples/mtls-cluster.rs

> Note: the advertised endpoint serves plain Arrow Flight `DoGet` for Ballista's own partition-fetch
> tickets. Generic Flight SQL, JDBC, and ADBC clients connect to the scheduler's
> [Flight SQL frontend](flightsql.md) instead.

```{warning}
The Result Service checks no credentials, and it dials whichever executor address a fetch ticket
names without checking that the address belongs to the cluster. Keep it on a trusted network.
```

| Option                           | Description                                                                                                                                                                              |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--advertise-flight-endpoint`    | The `HOST:PORT` address clients are told to fetch results from, instead of the executors: a Result Service, or a load balancer in front of one.                                          |
| `--enable-embedded-flight-proxy` | **Deprecated, to be removed in 57.0.0.** Runs an Arrow Flight proxy inside the scheduler process, on the scheduler's own host and port, and points clients at the scheduler for results. |

### The embedded proxy (deprecated)

The scheduler can still proxy results itself with `--enable-embedded-flight-proxy`. This puts result
traffic on the scheduler's process and thread pool, competing with query planning and task
scheduling, and it scales only with the scheduler. It is deprecated in 56.0.0 and will be removed in
57.0.0; the scheduler logs a warning at startup while it is enabled. To migrate, deploy a Result
Service and set `--advertise-flight-endpoint` to its address.

> `--advertise-flight-sql-endpoint` is accepted as a deprecated alias of
> `--advertise-flight-endpoint`. Passing either flag with no value starts the deprecated embedded
> proxy, is itself deprecated, and logs a warning.

## REST API

The scheduler also provides a REST API that allows jobs to be monitored.

> These endpoints require the scheduler's `rest-api` feature, which is enabled by default. Start the
> scheduler with `--disable-rest-api` to turn them off at runtime.

| API                                    | Method | Description                                                       |
| -------------------------------------- | ------ | ----------------------------------------------------------------- |
| /api/openapi.json                      | GET    | Return OpenAPI v3 specification document for the REST API.        |
| /api/state                             | GET    | Get the current scheduler state.                                  |
| /api/version                           | GET    | Get the scheduler's Ballista version.                             |
| /api/executors                         | GET    | Get a list of executors registered with the scheduler.            |
| /api/executor/{executor_id}            | GET    | Get details for a single executor.                                |
| /api/jobs                              | GET    | Get a list of jobs that have been submitted to the cluster.       |
| /api/job/{job_id}                      | GET    | Get a summary of a submitted job.                                 |
| /api/job/{job_id}                      | PATCH  | Cancel a currently running job                                    |
| /api/job/{job_id}/config               | GET    | Get session configuration for a job.                              |
| /api/job/{job_id}/stages               | GET    | Get per-stage and per-task detail for a job.                      |
| /api/job/{job_id}/dot                  | GET    | Produce a query plan in DOT (graphviz) format.                    |
| /api/job/{job_id}/dot_svg              | GET    | Produce a query plan in SVG format. (`graphviz-support` required) |
| /api/job/{job_id}/stage/{stage_id}/dot | GET    | Produces stage plan in DOT (graphviz) format                      |
| /api/metrics                           | GET    | Return current scheduler metric set                               |

Two probe endpoints are always mounted, whether or not the `rest-api` feature is enabled:

| API      | Method | Description                                                                         |
| -------- | ------ | ----------------------------------------------------------------------------------- |
| /healthz | GET    | Liveness. Always `200 OK` while the process runs.                                   |
| /readyz  | GET    | Readiness. `200 OK` once at least `--min-ready-executors` executors are registered. |

## Web TUI Configuration

When the Scheduler is built with the `rest-api` feature, several command-line options control its integration with the Web TUI:

| Option                   | Description                                                                                                                       |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `--web-tui-route`        | HTTP path that redirects to the hosted Web TUI. The default route is `/`.                                                         |
| `--cors-allowed-origins` | Comma-separated list of allowed CORS origins. By default, `http://localhost:8080` and `https://nightlies.apache.org` are allowed. |
| `--cors-allowed-methods` | Comma-separated list of allowed CORS methods. By default, `GET`, `PATCH`, and `OPTIONS` are allowed.                              |

For example, to expose the Web TUI redirect at `http://localhost:50050/tui`:

```bash
ballista-scheduler --web-tui-route /tui
```

The CORS options can be customized when hosting the Web TUI from another origin.
