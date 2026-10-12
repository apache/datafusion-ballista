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

set -e

# On SIGTERM or SIGINT, stop the executor first, so it drains while the
# scheduler it reports to is still up, then stop the scheduler.
stop() {
  if [[ -n "${executor:-}" ]]; then
    kill -TERM "$executor" 2>/dev/null || true
    wait "$executor" || status=$?
  fi
  kill -TERM "$scheduler" 2>/dev/null || true
  wait "$scheduler" || true
  exit "${status:-0}"
}

echo "Starting scheduler"
/root/ballista-scheduler &
scheduler=$!
trap stop TERM INT

until (echo >/dev/tcp/127.0.0.1/50050) 2>/dev/null; do
  if ! kill -0 "$scheduler" 2>/dev/null; then
    echo "Scheduler exited before it started listening" >&2
    exit 1
  fi
  sleep 1
done

echo "Starting executor"
/root/ballista-executor &
executor=$!
wait "$executor"
