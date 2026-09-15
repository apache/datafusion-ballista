# syntax=docker/dockerfile:1

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# One image containing both chaos binaries, used by the Kubernetes backend in
# `chaos-testing`. The pod manifest selects the role via `command`.
#
# The binaries are compiled *inside* this build, so the image is always built
# for the container's architecture. A host `cargo build` would embed the host's
# binary (e.g. a macOS Mach-O on Apple Silicon), which fails in a Linux pod with
# "exec format error". BuildKit cache mounts keep the cargo registry and target
# dir across builds, so an incremental rebuild after a chaos-only change is fast.
#
# Debug info is disabled (CARGO_PROFILE_DEV_DEBUG=0) and symbols are stripped: a
# full debug binary statically links all of DataFusion + aws-lc/ring and can OOM
# `ld` at link time, and the ~250MB result is slow (and sometimes fails) to
# `kind load`. Without DWARF the link is light and each binary is tens of MB.

FROM rust:1-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
ENV CARGO_PROFILE_DEV_DEBUG=0
ENV RUSTFLAGS="-C strip=symbols"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build -p ballista-chaos --bin chaos-scheduler --bin chaos-executor \
    && mkdir -p /out \
    && cp target/debug/chaos-scheduler target/debug/chaos-executor /out/

FROM debian:bookworm-slim
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full
COPY --from=builder /out/chaos-scheduler /root/chaos-scheduler
COPY --from=builder /out/chaos-executor /root/chaos-executor

# scheduler gRPC/REST (50050); executor Arrow Flight (50051), gRPC (50052), and
# HTTP health probes (50053).
EXPOSE 50050 50051 50052 50053

# No ENTRYPOINT: the pod manifest sets `command` to the desired binary.
