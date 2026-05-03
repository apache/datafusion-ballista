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
#
# Three separate invocations - `--bin` filters cargo's target list globally,
# so a single call with multiple `--bin` flags still skips binaries whose
# `required-features` aren't enabled across the whole selection. Cargo reuses
# compiled dependencies across invocations, so this is not meaningfully slower
# than one call.

set -euo pipefail

RELEASE_FLAG="${RELEASE_FLAG:=release}"

cargo build --profile "$RELEASE_FLAG" -p ballista-scheduler  --bin ballista-scheduler
cargo build --profile "$RELEASE_FLAG" -p ballista-executor   --bin ballista-executor
cargo build --profile "$RELEASE_FLAG" -p ballista-cli        --bin ballista-cli
cargo build --profile "$RELEASE_FLAG" -p ballista-benchmarks --bin tpch
