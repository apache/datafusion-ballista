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

FROM ubuntu:22.04

ARG RELEASE_FLAG=release

ENV RELEASE_FLAG=${RELEASE_FLAG}
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y nginx netcat

COPY target/$RELEASE_FLAG/ballista-scheduler /root/ballista-scheduler

COPY dev/docker/nginx.conf /etc/nginx/sites-enabled/default

# Expose Ballista Scheduler web UI port
EXPOSE 80

# Expose Ballista Scheduler gRPC port
EXPOSE 50050

COPY dev/docker/scheduler-entrypoint.sh /root/scheduler-entrypoint.sh
ENTRYPOINT ["/root/scheduler-entrypoint.sh"]
