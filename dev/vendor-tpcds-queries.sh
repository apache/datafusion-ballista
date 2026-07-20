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

# Fetch the 99 DataFusion-materialized TPC-DS queries (branch-54) into
# benchmarks/queries-tpcds/qN.sql. Re-run to refresh when the DataFusion pin
# changes. Source files are named N.sql upstream; we prefix with `q` to match
# the TPC-H convention.
set -euo pipefail

BRANCH="${DATAFUSION_BRANCH:-branch-54}"
BASE="https://raw.githubusercontent.com/apache/datafusion/${BRANCH}/datafusion/core/tests/tpc-ds"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${script_dir}/../benchmarks/queries-tpcds"

mkdir -p "$out_dir"
for n in $(seq 1 99); do
  echo "Fetching q${n}.sql"
  curl -fsSL "${BASE}/${n}.sql" -o "${out_dir}/q${n}.sql"
done
echo "Vendored 99 TPC-DS queries from ${BRANCH} into ${out_dir}"
