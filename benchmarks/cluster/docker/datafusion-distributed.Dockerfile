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
# DataFusion Distributed benchmark image: the `worker` binary from the
# datafusion-distributed repository's benchmarks/remote-worker crate, which the
# project benchmarks itself with. It serves SQL over HTTP on 9000 and
# distributed tasks over gRPC (Arrow Flight) on 9001. Every pod runs it; run.py
# makes one of them the coordinator.
#
# One change is made to the worker before it is built:
# ../datafusion-distributed/worker-memory-limit.patch reads a memory pool size
# and a spill directory from the environment, so that each pod can be given the
# memory limit a Ballista executor gets. Upstream's worker has no limit.
#
# Build with the directory of the patch as the context. DFD_REF is a branch, a
# tag or a commit:
#
#   docker build -f benchmarks/cluster/docker/datafusion-distributed.Dockerfile \
#     --build-arg DFD_REF=main \
#     -t <registry>/datafusion-distributed-bench:main benchmarks/cluster/datafusion-distributed
#
# The binary is built for the platform of the build. Add --platform
# linux/amd64 to build x86_64 binaries on another machine (slow under
# emulation). If the patch no longer applies to DFD_REF, the build stops there:
# the worker's main.rs changed upstream and the patch needs updating.

FROM rust:1-bookworm AS build

ARG DFD_REPO=https://github.com/datafusion-contrib/datafusion-distributed.git
ARG DFD_REF=main

# The repository keeps its test data in Git LFS, which the build does not need.
ENV GIT_LFS_SKIP_SMUDGE=1

WORKDIR /src
RUN git init -q . && \
    git remote add origin "${DFD_REPO}" && \
    git fetch -q --depth 1 origin "${DFD_REF}" && \
    git checkout -q FETCH_HEAD

COPY worker-memory-limit.patch /tmp/
RUN git apply /tmp/worker-memory-limit.patch

# rust-toolchain.toml pins the toolchain; rustup installs it on first use.
RUN cargo build --release -p datafusion-distributed-remote-worker --bin worker && \
    mkdir -p /out && cp target/release/worker /out/ && \
    echo "COMMIT=$(git rev-parse HEAD)" > /out/BUILD_INFO

FROM debian:bookworm-slim

# ca-certificates for TLS to S3.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /out/ /opt/datafusion-distributed/

WORKDIR /opt/datafusion-distributed
EXPOSE 9000 9001
