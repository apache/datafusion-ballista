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

# Ballista benchmark image: the scheduler, the executor, and the tpch and
# tpcds benchmark runners with the queries they read, all built from the same
# commit, so the runner's client always matches the cluster.
#
# Build from the root of the repository (BuildKit reads the
# ballista.Dockerfile.dockerignore next to this file):
#
#   docker build -f benchmarks/cluster/docker/ballista.Dockerfile \
#     --build-arg GIT_COMMIT=$(git rev-parse HEAD) -t <registry>/ballista-bench:<tag> .
#
# The binaries are built for the platform of the build. Add
# --platform linux/amd64 to build x86_64 binaries on another machine (slow
# under emulation).

FROM rust:1-bookworm AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# release, or release-lto for a slower build and faster binaries.
ARG PROFILE=release

WORKDIR /src
COPY . .
RUN cargo build --profile "${PROFILE}" \
      -p ballista-scheduler --bin ballista-scheduler \
      -p ballista-executor --bin ballista-executor \
      -p ballista-benchmarks --bin tpch --bin tpcds && \
    mkdir -p /out && \
    cp target/"${PROFILE}"/ballista-scheduler target/"${PROFILE}"/ballista-executor \
       target/"${PROFILE}"/tpch target/"${PROFILE}"/tpcds /out/

FROM debian:bookworm-slim

# ca-certificates for TLS to S3.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /out/ /opt/ballista/
# The runners read queries/q{N}.sql and queries-tpcds/q{N}.sql relative to the
# working directory.
COPY benchmarks/queries/ /opt/ballista/queries/
COPY benchmarks/queries-tpcds/ /opt/ballista/queries-tpcds/

# The commit the binaries were built from, reported with the results.
ARG GIT_COMMIT=unknown
RUN echo "COMMIT=${GIT_COMMIT}" > /opt/ballista/BUILD_INFO

WORKDIR /opt/ballista
ENV PATH="/opt/ballista:${PATH}"
EXPOSE 50050 50051
