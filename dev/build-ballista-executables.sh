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

# TODO: it would be very nice if we could make CI work the exact same way so the build logic isn't duplicated

# build a docker container in which to run the build - this is to make life easier for Windows & Mac users
docker build -t ballista-builder --build-arg EXT_UID="$(id -u)" -f dev/docker/ballista-builder.Dockerfile .

# run cargo & yarn builds inside the builder container
docker run -v $(pwd):/home/builder/workspace --env RELEASE_FLAG=$RELEASE_FLAG ballista-builder
