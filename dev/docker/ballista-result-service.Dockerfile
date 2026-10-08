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

FROM ubuntu:26.04

LABEL org.opencontainers.image.source="https://github.com/apache/datafusion-ballista"
LABEL org.opencontainers.image.description="Apache DataFusion Ballista Distributed SQL Query Engine"
LABEL org.opencontainers.image.licenses="Apache-2.0"

ARG RELEASE_FLAG=release

# ca-certificates so the Result Service can verify executors' TLS certificates
# when it runs with --use-tls.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV RELEASE_FLAG=${RELEASE_FLAG}
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

COPY target/${RELEASE_FLAG}/ballista-result-service /root/ballista-result-service

# Expose Ballista Result Service Arrow Flight port
EXPOSE 50055

COPY dev/docker/result-service-entrypoint.sh /root/result-service-entrypoint.sh
ENTRYPOINT ["/root/result-service-entrypoint.sh"]
