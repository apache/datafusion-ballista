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

# Ballista Quickstart

There are two ways to get a local Ballista cluster running. Choose based on your goal:

| | [Evaluate Ballista](#path-a-evaluate-with-docker-2-min) | [Build from source](#path-b-build-from-source-20-min) |
|---|---|---|
| Goal | Try Ballista against the last stable release | Develop or test against local code changes |
| Prerequisites | Docker | Rust, protoc |
| Cold start time | ~2 min (image pull) | ~20 min (full compile) |
| Terminals needed | 1 | 3 |

> [!IMPORTANT]
> Ballista and DataFusion are developed independently. A given Ballista release may not be compatible
> with the latest DataFusion version. Check the [compatibility matrix](../configs.md) before integrating.

---

## Path A: Evaluate with Docker (~2 min)

The only prerequisite is [Docker](https://docs.docker.com/get-docker/) with Compose v2.

This uses pre-built images from GHCR that are published on each stable release. The `latest` tag
tracks the most recent release, not the `main` branch.

```shell
docker compose -f docker-compose.quick.yml up
```

You should see output similar to:

```
ballista-scheduler-1  | Ballista Scheduler v53.0.0 listening on 0.0.0.0:50050
ballista-executor-1   | Executor registration succeed
ballista-executor-2   | Executor registration succeed
```

Two executors start by default. The scheduler listens on `localhost:50050`.

**Connect from Rust:**

```rust
let ctx = SessionContext::remote("df://localhost:50050").await?;
```

**Connect from the CLI** (requires a local build — no pre-built CLI image is published):

```shell
cargo run -p ballista-cli -- --host localhost --port 50050
```

**To make local data available inside the executors**, uncomment and set the `volumes` block
in `docker-compose.quick.yml`:

```yaml
ballista-executor:
  volumes:
    - /absolute/path/to/your/data:/data:ro
```

Then reference `/data/yourfile.parquet` in your queries. The path must be the same inside
every executor container.

**Tear down:**

```shell
docker compose -f docker-compose.quick.yml down
```

---

## Path B: Build from source (~20 min)

Use this path if you need to test local code changes or run against the `main` branch.

**Prerequisites:**

- [Rust](https://www.rust-lang.org/tools/install)
- [Protobuf Compiler](https://protobuf.dev/downloads/)

**Step 1:** Build release binaries from the repository root:

```shell
cargo build --release
```

**Step 2:** Start the scheduler in a new terminal:

```shell
RUST_LOG=info ./target/release/ballista-scheduler
```

**Step 3:** Start one or more executors, each in a new terminal. When running multiple
executors, each needs a unique pair of ports:

```shell
RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50051 --bind-grpc-port 50052
```

```shell
RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50053 --bind-grpc-port 50054
```

> **Two-port model:** each executor exposes an Arrow Flight port (data, `-p`) and a gRPC
> control port (`--bind-grpc-port`). Both must be reachable by the scheduler.

---

## Running the examples

Examples live in the `examples/` directory and connect to `localhost:50050` by default.

### Distributed SQL example

```bash
cd examples
cargo run --release --example remote-sql
```

### Distributed DataFrame example

```bash
cd examples
cargo run --release --example remote-dataframe
```

### Standalone (single-process) example

No cluster needed — scheduler and executor run in the same process:

```bash
cd examples
cargo run --release --example standalone-sql
```

---

## Troubleshooting

**`protoc` not found during build**

Install the protobuf compiler for your OS, then retry `cargo build --release`:

```shell
# Ubuntu / Debian
sudo apt install protobuf-compiler

# macOS
brew install protobuf
```

**Executor can't reach the scheduler**

The executor connects to the scheduler at startup. Make sure the scheduler is running before
starting executors. Check that the scheduler address (`--scheduler-host`, default `localhost`)
is reachable from the executor's network.

**Port conflict**

If port 50050 is already in use, start the scheduler with `--bind-port <port>` and update
your client connection string accordingly.

**Docker executor containers exit immediately**

Check `docker compose logs`. The most common cause is the scheduler health check failing
because the scheduler itself hasn't started yet — `depends_on: condition: service_healthy`
handles this in `docker-compose.quick.yml`.
