#!/bin/bash
#
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

# Generate partitioned TPC-H Parquet using https://crates.io/crates/tpchgen-cli
#
# Env overrides:
#   SCALE_FACTOR    TPC-H scale factor (default: 10)
#   PARTITIONS      Number of partition files per table (default: 16)
#   OUTPUT_DIR      Where data is written (default: ./data, relative to this script)

set -euo pipefail

SCALE_FACTOR="${SCALE_FACTOR:-10}"
PARTITIONS="${PARTITIONS:-16}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${script_dir}/data}"

if ! command -v tpchgen-cli >/dev/null 2>&1; then
  echo "tpchgen-cli not found; installing via cargo..."
  cargo install tpchgen-cli --locked
fi

echo "Generating TPC-H SF${SCALE_FACTOR} as Parquet (${PARTITIONS} partitions) into ${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"
rm -rf "${OUTPUT_DIR:?}/"*

tpchgen-cli \
  --scale-factor "${SCALE_FACTOR}" \
  --parts "${PARTITIONS}" \
  --format=parquet \
  --output-dir "${OUTPUT_DIR}"

echo "Done. Files written to ${OUTPUT_DIR}"
