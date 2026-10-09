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

Serves query results to clients that cannot reach the executors. It forwards each
result fetch to the executor that holds the partition and streams the result back.
It keeps no state, so replicas are interchangeable.

The scheduler's embedded proxy (`--enable-embedded-flight-proxy`) is deprecated in favor
of this service and will be removed in 57.0.0.

## Usage

Point the scheduler at the service:

```bash
ballista-executor
ballista-scheduler --advertise-flight-endpoint=localhost:50055
ballista-result-service --bind-port 50055
```

To scale out, run more replicas behind a gRPC-aware load balancer and advertise its
address. Install the service with `cargo install --locked ballista-result-service`, or
use the `ghcr.io/apache/datafusion-ballista-result-service` image. The [Kubernetes
deployment guide] has example manifests.

On SIGTERM or Ctrl-C the service stops accepting connections and lets in-flight result
streams finish, for up to `--graceful-shutdown-timeout-seconds`.

## Behind a TLS-terminating gateway

Advertise the gateway's address, and have clients enable TLS for result fetches with
`ballista.client.use_tls`, supplying the gateway's TLS roots through a gRPC endpoint
override:

```bash
ballista-scheduler --advertise-flight-endpoint=ballista-results.example.com:443
```

The gateway must forward gRPC (HTTP/2) to the replicas, for example with a Gateway API
`GRPCRoute`, and allow long-lived streams.

## Options

| Flag                                  | Default    | Meaning                                                             |
| ------------------------------------- | ---------- | ------------------------------------------------------------------- |
| `--bind-host`                         | `0.0.0.0`  | Host/IP the Flight service binds to                                 |
| `--bind-port`                         | `50055`    | Port the Flight service binds to                                    |
| `--use-tls`                           | `false`    | Use TLS when connecting to executors                                |
| `--grpc-max-decoding-message-size`    | `16777216` | Max gRPC message size decoded                                       |
| `--grpc-max-encoding-message-size`    | `16777216` | Max gRPC message size encoded                                       |
| `--graceful-shutdown-timeout-seconds` | `10`       | Time in-flight result streams get to finish after SIGTERM or Ctrl-C |

Set `RUST_LOG=ballista_core::serving=debug` to log each forwarded fetch.

## Security

The service checks no credentials, and it dials whichever executor address a fetch
ticket names, so a forged ticket can make it connect to an arbitrary host. The
scheduler's embedded proxy behaves the same way. Run it only on networks you trust; the
Kubernetes example restricts what it can reach with a NetworkPolicy.

[kubernetes deployment guide]: https://datafusion.apache.org/ballista/user-guide/deployment/kubernetes.html#serving-results-to-clients-outside-the-cluster
