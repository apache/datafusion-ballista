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

On SIGTERM or Ctrl-C the service refuses new connections and lets in-flight result
streams finish, for up to `--graceful-shutdown-timeout-seconds`.

## Health and metrics

Like the scheduler, the service also answers HTTP on its port:

- `/healthz` and `/readyz`: 200 while the service accepts connections, which it stops
  doing on a shutdown signal
- `/api/metrics`: Prometheus metrics when built with the `prometheus-metrics` feature
  (`cargo install --locked ballista-result-service --features prometheus-metrics`),
  otherwise 204

The metrics count finished fetches by outcome
(`ballista_result_service_fetches_total`, labelled `completed`, `failed` or
`cancelled`), and track fetches in flight (`ballista_result_service_fetches_in_flight`),
fetch durations (`ballista_result_service_fetch_duration_seconds`) and the bytes relayed
to clients (`ballista_result_service_relayed_bytes_total`). On Linux they include the
process's CPU and memory use.

## Behind a TLS-terminating gateway

Advertise the gateway's address, and have clients enable TLS for result fetches with
`ballista.client.use_tls`, supplying the gateway's TLS roots through a gRPC endpoint
override. That takes a client built with one of tonic's TLS features, as in the [mTLS
cluster example]. TLS covers the connections from clients to the gateway; the service
connects to executors in plaintext. It doesn't authenticate fetches, so make the gateway
reachable only by trusted clients.

```bash
ballista-scheduler --advertise-flight-endpoint=ballista-results.example.com:443
```

The gateway must forward gRPC (HTTP/2) to the replicas, for example with a Gateway API
`GRPCRoute`, and allow long-lived streams.

## Options

| Flag                                  | Default    | Meaning                                                             |
| ------------------------------------- | ---------- | ------------------------------------------------------------------- |
| `--bind-host`                         | `0.0.0.0`  | Host/IP the Flight service binds to                                 |
| `--bind-port`                         | `50055`    | Port the Flight service, health probes and metrics bind to          |
| `--grpc-max-decoding-message-size`    | `16777216` | Max gRPC message size decoded                                       |
| `--grpc-max-encoding-message-size`    | `16777216` | Max gRPC message size encoded                                       |
| `--graceful-shutdown-timeout-seconds` | `10`       | Time in-flight result streams get to finish after SIGTERM or Ctrl-C |

Set `RUST_LOG=ballista_core::serving=debug` to log each forwarded fetch.

## As a library

`ballista_result_service::serve` runs the same service from your own binary. Use it to
customize the connections to executors, for example for mTLS, with the `use_tls` and
`override_create_grpc_client_endpoint` fields of `ResultServiceConfig`, which work like
the same-named `SchedulerConfig` fields do for the embedded proxy:

```rust
use ballista_core::extension::EndpointOverrideFn;
use ballista_result_service::{ResultServiceConfig, serve, shutdown_signal};

let endpoint_override: EndpointOverrideFn =
    Arc::new(move |endpoint| Ok(endpoint.tls_config(client_tls.clone())?));
let config = ResultServiceConfig {
    use_tls: true,
    override_create_grpc_client_endpoint: Some(endpoint_override),
    ..Default::default()
};
serve(config, shutdown_signal()).await?;
```

The connections are only encrypted in a build with one of tonic's TLS features, as in the
[mTLS cluster example].

## Security

The service checks no credentials, and it dials whichever executor address a fetch
ticket names, so a forged ticket can make it connect to an arbitrary host. The
scheduler's embedded proxy behaves the same way. Run it only on networks you trust; the
Kubernetes example restricts what it can reach with a NetworkPolicy.

[mtls cluster example]: https://github.com/apache/datafusion-ballista/blob/main/examples/examples/mtls-cluster.rs
[kubernetes deployment guide]: https://datafusion.apache.org/ballista/user-guide/deployment/kubernetes.html#serving-results-to-clients-outside-the-cluster
