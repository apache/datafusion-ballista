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

LABEL org.opencontainers.image.source="https://github.com/apache/arrow-ballista"
LABEL org.opencontainers.image.description="Apache Arrow Ballista Distributed SQL Query Engine"
LABEL org.opencontainers.image.licenses="Apache-2.0"

ARG RELEASE_FLAG=release

ENV RELEASE_FLAG=${RELEASE_FLAG}
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -qq update && apt-get install -qq -y netcat wget

COPY target/$RELEASE_FLAG/ballista-scheduler /root/ballista-scheduler
COPY target/$RELEASE_FLAG/ballista-executor /root/ballista-executor

RUN chmod a+x /root/ballista-scheduler && \
    chmod a+x /root/ballista-executor

# populate some sample data for ListingSchemaProvider
RUN mkdir -p /data && \
    wget -q https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet -P /data/
ENV DATAFUSION_CATALOG_LOCATION=/data
ENV DATAFUSION_CATALOG_TYPE=csv

# Expose Ballista Scheduler gRPC port
EXPOSE 50050

# Expose Ballista Executor gRPC port
EXPOSE 50051

COPY dev/docker/standalone-entrypoint.sh /root/standalone-entrypoint.sh
ENTRYPOINT ["/root/standalone-entrypoint.sh"]
