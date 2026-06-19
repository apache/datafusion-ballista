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

# Starting a Ballista Cluster using Docker Compose

Two Compose files are provided. Choose based on whether you need the last stable release
or want to run against local source changes.

## Option 1: Pre-built images (no local build required)

`docker-compose.quick.yml` pulls images directly from GHCR — no Rust toolchain needed.
Images are published on each stable release; `latest` tracks the most recent release,
not the `main` branch.

```bash
docker compose -f docker-compose.quick.yml up
```

See the [quickstart guide](quick-start.md) for connection instructions and data volume setup.

## Option 2: Build from source

`docker-compose.yml` builds executor and scheduler images from the local Dockerfiles.
The Dockerfiles copy pre-compiled binaries — they do **not** run `cargo build` themselves.
You must compile first:

```bash
# Step 1 — compile (requires Rust + protoc, takes ~20 min cold)
cargo build --release

# Step 2 — build Docker images and start the cluster
docker compose up --build
```

Skipping Step 1 will cause the build to fail because the `COPY target/release/ballista-*`
instruction in the Dockerfiles will find no binaries to copy.

Expected output after a successful start:

```
ballista-scheduler_1  | Ballista Scheduler listening on 0.0.0.0:50050
ballista-executor_1   | Executor registration succeed
```

The scheduler listens on port 50050.

## Connect from the Ballista CLI

No pre-built CLI image is published to GHCR. Build it locally from source:

```shell
cargo run -p ballista-cli -- --host localhost --port 50050
```
