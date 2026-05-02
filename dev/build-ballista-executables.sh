#!/bin/bash

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

# Build the binaries that the per-image Dockerfiles
# (ballista-scheduler/executor/benchmarks) COPY into the runtime images.
#
# This used to run `cargo build` inside a builder container; the host build
# is simpler and avoids dragging a separate Rust toolchain image along. Run
# this on a host with the project's Rust toolchain available (see
# rust-toolchain.toml).

set -euo pipefail

RELEASE_FLAG="${RELEASE_FLAG:=release}"

cargo build \
  --profile "$RELEASE_FLAG" \
  --features rest-api \
  -p ballista-scheduler \
  -p ballista-executor \
  -p ballista-benchmarks --bin tpch
