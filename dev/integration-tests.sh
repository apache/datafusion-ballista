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
set -euo pipefail

# Env overrides forwarded to tpch-gen.sh / run.sh:
#   SCALE_FACTOR    TPC-H scale factor (default: 10)
#   PARTITIONS      Number of partitions (default: 16)
#   ITERATIONS      Iterations per query (default: 3)

export SCALE_FACTOR="${SCALE_FACTOR:-10}"
export PARTITIONS="${PARTITIONS:-16}"
export ITERATIONS="${ITERATIONS:-3}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

cleanup() {
  echo "Tearing down docker compose stack..."
  docker compose down --remove-orphans || true
}
trap cleanup EXIT

echo "Generating benchmark data (SF=${SCALE_FACTOR}, parts=${PARTITIONS})..."
./benchmarks/tpch-gen.sh

echo "Building Docker images..."
./dev/build-ballista-docker.sh

echo "Starting docker compose in background..."
docker compose up -d --wait

echo "Running benchmarks..."
docker compose run --rm ballista-client /root/run.sh
