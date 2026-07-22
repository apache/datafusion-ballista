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

# Generate TPC-DS Parquet using the unified tpcgen-cli
# (https://github.com/clflushopt/tpchgen-rs). TPC-DS is not yet on crates.io,
# so tpcgen-cli is installed from git, pinned to a rev. The tpcds subcommand
# writes one Parquet file per table and has no --parts option.
#
# Env overrides:
#   SCALE_FACTOR   TPC-DS scale factor (default: 10, matching CI)
#   OUTPUT_DIR     Where data is written (default: ./data-tpcds)
#   TPCGEN_REV     tpcgen-cli git rev to install (default: pinned below)
set -euo pipefail

SCALE_FACTOR="${SCALE_FACTOR:-10}"
TPCGEN_REV="${TPCGEN_REV:-573fd0018dd1b41e52880162f615e9903418c397}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${script_dir}/data-tpcds}"

if ! command -v tpcgen-cli >/dev/null 2>&1; then
  echo "tpcgen-cli not found; installing from git (rev ${TPCGEN_REV})..."
  cargo install --git https://github.com/clflushopt/tpchgen-rs --rev "${TPCGEN_REV}" --locked tpcgen-cli
fi

echo "Generating TPC-DS SF${SCALE_FACTOR} as Parquet into ${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"
rm -rf "${OUTPUT_DIR:?}/"*

tpcgen-cli tpcds parquet \
  --scale-factor "${SCALE_FACTOR}" \
  --output-dir "${OUTPUT_DIR}"

echo "Done. Files written to ${OUTPUT_DIR}"
