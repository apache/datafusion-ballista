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

set -e

RELEASE_FLAG=${RELEASE_FLAG:=release}

./dev/build-ballista-executables.sh

# docker compose build

. ./dev/build-set-env.sh

docker build -t "apache/arrow-ballista-standalone:latest" -f dev/docker/ballista-standalone.Dockerfile .
docker build -t "apache/arrow-ballista-scheduler:latest" -f dev/docker/ballista-scheduler.Dockerfile .
docker build -t "apache/arrow-ballista-executor:latest" -f dev/docker/ballista-executor.Dockerfile .
docker build -t "apache/arrow-ballista-cli:latest" -f dev/docker/ballista-cli.Dockerfile .
docker build -t "apache/arrow-ballista-benchmarks:latest" -f dev/docker/ballista-benchmarks.Dockerfile .

