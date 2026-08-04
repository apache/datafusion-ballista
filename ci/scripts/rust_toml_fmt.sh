#!/usr/bin/env bash
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

# Checks Cargo.toml formatting using taplo. The formatting rules live in
# taplo.toml at the repository root.
#
# Usage:
#   ci/scripts/rust_toml_fmt.sh            # check formatting (default, used in CI)
#   ci/scripts/rust_toml_fmt.sh --write    # reformat files in place

set -e

if ! command -v taplo &> /dev/null; then
    echo "Installing taplo using cargo"
    cargo install taplo-cli --version 0.10.0 --locked
fi

if [ "${1:-}" = "--write" ]; then
    taplo format
else
    # `taplo format --check` exits non-zero if any file is not correctly
    # formatted. Run `ci/scripts/rust_toml_fmt.sh --write` to fix violations.
    taplo format --check
fi
