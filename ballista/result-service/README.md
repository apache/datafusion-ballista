<!--
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

# Ballista Result Service

A **stateless data plane** for serving query results, decoupled from the scheduler
(control plane). See the design doc `DECOUPLED_RESULT_SERVICE.md` at the repo root.

In this first increment the service **forwards** each result fetch to the executor named
in the request — the same behavior as the scheduler's (now consolidated) embedded proxy,
but as an independently scalable fleet that sits **off the scheduler's data path**. The
forwarding logic is the shared serving core in `ballista-core` (`serving` module), so the
executor's own serving path, the embedded proxy, and this service never drift apart.

## Wiring (no client changes)

The client already routes result fetches to an advertised endpoint when the scheduler
returns one. Point the scheduler at this service:

```bash
# 1. Start executors as usual.
ballista-executor

# 2. Start the scheduler, advertising the Result Service address (host:port only).
ballista-scheduler --advertise-flight-endpoint=localhost:50055

# 3. Start one (or more) Result Service replicas at that address.
ballista-result-service --bind-port 50055
```

Result bytes then flow **client → Result Service → executor**; the scheduler serves no
result data. Multiple replicas are interchangeable (the service holds no state), so scale
by running more of them behind a gRPC-aware load balancer.

## Options

| Flag | Default | Meaning |
|---|---|---|
| `--bind-host` | `0.0.0.0` | Host/IP the Flight service binds to |
| `--bind-port` | `50055` | Port the Flight service binds to |
| `--use-tls` | `false` | Use TLS when connecting to executors |
| `--grpc-max-decoding-message-size` | `16777216` | Max gRPC message size decoded |
| `--grpc-max-encoding-message-size` | `16777216` | Max gRPC message size encoded |

Set `RUST_LOG=debug` to log each forwarded `FetchPartition` (useful for confirming the
data path bypasses the scheduler).

## Scope

- **In scope now:** forwarding to executors, off the scheduler's data path, horizontally
  scalable.
- **Not yet:** result-scoped authorization (required before any "Kubernetes default"
  claim), object-storage/pre-signed tier, topology-hiding opaque handles. See the design
  doc's roadmap and open questions.
