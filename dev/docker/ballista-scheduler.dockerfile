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

ARG VERSION

# Use node image to build the scheduler UI
FROM node:14.16.0-alpine as ui-build
WORKDIR /app
ENV PATH /app/node_modules/.bin:$PATH
COPY ballista/ui/scheduler ./
RUN yarn
RUN yarn build

FROM apache/arrow-ballista:$VERSION
RUN apt -y install nginx
RUN rm -rf /var/www/html/*
COPY --from=ui-build /app/build /var/www/html

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

# Expose Ballista Scheduler web UI port
EXPOSE 80

# Expose Ballista Scheduler gRPC port
EXPOSE 50050

ADD dev/docker/scheduler-entrypoint.sh /
ENTRYPOINT ["/scheduler-entrypoint.sh"]
