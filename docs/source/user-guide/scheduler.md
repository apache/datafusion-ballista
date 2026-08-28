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

## Fetching Query Results

By default a client fetches the result partitions of a query directly from the executors that
produced them, over Arrow Flight. This keeps the scheduler out of the data path, but it requires
every client to have network access to every executor — which is not the case in isolated
environments where only the scheduler is reachable.

For those deployments the scheduler can advertise a different address for clients to fetch results
from, and can optionally host an Arrow Flight proxy itself.

> Note: this is plain Arrow Flight, used to move result partitions. It is not Flight SQL, which was
> removed in Ballista 46.0.0 — the proxy only serves Ballista's `FetchPartition` action, so generic
> Flight SQL or JDBC clients cannot use this endpoint.

Two independent options control this:

| Option                           | Description                                                                                                                                                           |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--enable-embedded-flight-proxy` | Runs an Arrow Flight proxy inside the scheduler process, on the scheduler's own host and port. The proxy forwards each fetch to the executor that owns the partition. |
| `--advertise-flight-endpoint`    | The `HOST:PORT` address clients are told to fetch results from, instead of the executors. Use it to point clients at a load balancer or a standalone proxy.           |

The first controls whether a proxy _runs_; the second controls what clients are _told_. Setting
both is a supported combination: the embedded proxy runs, and clients are pointed at the advertised
address, which is how you put a load balancer in front of one or more schedulers.

To let clients fetch results through the scheduler itself:

```bash
ballista-scheduler --enable-embedded-flight-proxy
```

To point clients at a load balancer that fronts the schedulers:

```bash
ballista-scheduler --enable-embedded-flight-proxy \
                   --advertise-flight-endpoint ballista-flight.example.com:50050
```

Both options are disabled by default, and the embedded proxy should be enabled deliberately: it
puts result traffic on the scheduler's process and thread pool, competing with query planning and
task scheduling. Under load this is a known source of scheduler congestion, so prefer a separate
proxy — advertised with `--advertise-flight-endpoint` — for clusters where result volume is
significant.

> `--advertise-flight-sql-endpoint` is accepted as a deprecated alias of
> `--advertise-flight-endpoint`. Passing either flag with no value used to start the embedded proxy;
> that is deprecated too and logs a warning — use `--enable-embedded-flight-proxy` instead.

## REST API

The scheduler also provides a REST API that allows jobs to be monitored.

> This is optional scheduler feature which should be enabled with the `rest-api` feature.

| API                                    | Method | Description                                                       |
| -------------------------------------- | ------ | ----------------------------------------------------------------- |
| /api/jobs                              | GET    | Get a list of jobs that have been submitted to the cluster.       |
| /api/job/{job_id}                      | GET    | Get a summary of a submitted job.                                 |
| /api/job/{job_id}/dot                  | GET    | Produce a query plan in DOT (graphviz) format.                    |
| /api/job/{job_id}/dot_svg              | GET    | Produce a query plan in SVG format. (`graphviz-support` required) |
| /api/job/{job_id}                      | PATCH  | Cancel a currently running job                                    |
| /api/job/{job_id}/config               | GET    | Get session configuration for a job.                              |
| /api/job/{job_id}/stage/{stage_id}/dot | GET    | Produces stage plan in DOT (graphviz) format                      |
| /api/metrics                           | GET    | Return current scheduler metric set                               |

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
