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
(control plane). See the proposal in [#2484].

In this first increment the service **forwards** each result fetch to the executor named
in the request, as an independently scalable fleet that sits **off the scheduler's data
path**. The forwarding logic is the shared serving core in `ballista-core` (`serving`
module), so the executor's own serving path, the scheduler's embedded proxy, and this
service never drift apart.

The scheduler's embedded proxy (`--enable-embedded-flight-proxy`) is deprecated in favor
of this service and will be removed in 57.0.0.

## Wiring (no client changes)

The client already routes result fetches to an advertised endpoint when the scheduler
returns one. Point the scheduler at this service:

```bash
# 1. Start executors as usual.
ballista-executor

# 2. Start the scheduler, advertising the Result Service address.
ballista-scheduler --advertise-flight-endpoint=localhost:50055

# 3. Start one (or more) Result Service replicas at that address.
ballista-result-service --bind-port 50055
```

Result bytes then flow **client → Result Service → executor**; the scheduler serves no
result data. Multiple replicas are interchangeable (the service holds no state), so scale
by running more of them behind a gRPC-aware load balancer.

Install it with `cargo install --locked ballista-result-service`, or use the
`ghcr.io/apache/datafusion-ballista-result-service` image (built locally by
`./dev/build-ballista-docker.sh`). The [Kubernetes deployment guide] has example manifests, including a
TLS-terminating ingress and a network policy that limits what the service can reach. The
service shuts down gracefully on SIGTERM or Ctrl-C: it stops accepting connections and
lets in-flight result streams finish, for up to `--graceful-shutdown-timeout-seconds`.

## Behind a TLS-terminating ingress

Advertise the ingress's address, and have clients enable TLS for result fetches with
`ballista.client.use_tls`, supplying the ingress's TLS roots through a gRPC endpoint
override. The replicas behind the ingress keep serving plaintext:

```bash
ballista-scheduler --advertise-flight-endpoint=ballista-results.example.com:443
```

The ingress must forward HTTP/2 (gRPC) to the replicas, for example with ingress-nginx's
`nginx.ingress.kubernetes.io/backend-protocol: "GRPC"`, and allow long-lived streams.

## Options

| Flag                                  | Default    | Meaning                                                             |
| ------------------------------------- | ---------- | ------------------------------------------------------------------- |
| `--bind-host`                         | `0.0.0.0`  | Host/IP the Flight service binds to                                 |
| `--bind-port`                         | `50055`    | Port the Flight service binds to                                    |
| `--use-tls`                           | `false`    | Use TLS when connecting to executors                                |
| `--grpc-max-decoding-message-size`    | `16777216` | Max gRPC message size decoded                                       |
| `--grpc-max-encoding-message-size`    | `16777216` | Max gRPC message size encoded                                       |
| `--graceful-shutdown-timeout-seconds` | `10`       | Time in-flight result streams get to finish after SIGTERM or Ctrl-C |

Set `RUST_LOG=debug` to log each forwarded `FetchPartition` (useful for confirming the
data path bypasses the scheduler).

## Security

The service checks no credentials, and it dials whichever executor address a fetch
ticket names without checking that the address belongs to the cluster. A client that
forges a ticket can therefore make it open a gRPC connection to an arbitrary host and
relay the response. The scheduler's embedded proxy behaves the same way. Do not expose
the service beyond networks you trust until fetch tickets are authenticated.

## Scope

- **In scope now:** forwarding to executors, off the scheduler's data path, horizontally
  scalable.
- **Not yet:** authenticated fetch tickets, storage tiers (for example object storage with
  pre-signed URLs), and opaque result handles that hide executor topology. See [#2484].

[#2484]: https://github.com/apache/datafusion-ballista/issues/2484
[kubernetes deployment guide]: https://datafusion.apache.org/ballista/user-guide/deployment/kubernetes.html#serving-results-to-clients-outside-the-cluster
