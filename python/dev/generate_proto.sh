#!/usr/bin/env bash
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

# Generates Python gRPC stubs for the Ballista scheduler protocol into
# `python/ballista/proto`. Run from the `python` directory with the dev
# dependency group installed (`grpcio-tools`).

set -euo pipefail

PROTO_DIR="../ballista/core/proto"
OUT_DIR="python/ballista/proto"

rm -f "$OUT_DIR"/*_pb2*.py "$OUT_DIR"/*_pb2*.pyi
mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --pyi_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/datafusion_common.proto" \
    "$PROTO_DIR/datafusion.proto" \
    "$PROTO_DIR/ballista.proto"

# protoc emits top level imports, make them relative to the package
for f in "$OUT_DIR"/*_pb2*.py "$OUT_DIR"/*_pb2*.pyi; do
    sed -i.bak -E 's/^import (datafusion_common_pb2|datafusion_pb2|ballista_pb2)( as |$)/from . import \1\2/' "$f"
    rm -f "$f.bak"
done
