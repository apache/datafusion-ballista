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

set -ex
cargo clippy --all-targets --package ballista-core --all-features -- -D warnings
cargo clippy --all-targets --package ballista-scheduler --all-features -- -D warnings
cargo clippy --all-targets --package ballista-executor --all-features -- -D warnings
cargo clippy --all-targets --package ballista --all-features -- -D warnings
cargo clippy --all-targets --package ballista-cli --no-default-features --features cli,tui -- -D warnings
#cargo clippy --all-targets --package ballista-cli --no-default-features --features web -- -D warnings
cargo clippy --all-targets --package ballista-examples --all-features -- -D warnings
cargo clippy --all-targets --package ballista-benchmarks --all-features -- -D warnings
