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

# Checks that `docker stop` reaches the Ballista processes in the images that
# build-ballista-docker.sh builds. Each container must stop well before the stop
# timeout, rather than being SIGKILLed when it runs out (exit code 137), and the
# executor must shut down gracefully.

set -euo pipefail

STOP_TIMEOUT=30
STOP_LIMIT=15
NETWORK=ballista-sigterm-check
SCHEDULER=ballista-sigterm-scheduler
EXECUTOR=ballista-sigterm-executor
STANDALONE=ballista-sigterm-standalone

cleanup() {
  docker rm -f "$SCHEDULER" "$EXECUTOR" "$STANDALONE" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Waits up to a minute for the logs of container $1 to contain $2.
wait_for_log() {
  for _ in $(seq 60); do
    if docker logs "$1" 2>&1 | grep -q "$2"; then
      return 0
    fi
    sleep 1
  done
  echo "$1 did not log '$2' within 60s" >&2
  docker logs "$1" 2>&1 | tail -20 >&2
  return 1
}

# Stops container $1, and fails if it was SIGKILLed or took too long.
check_stop() {
  local start elapsed code
  start=$(date +%s)
  docker stop -t "$STOP_TIMEOUT" "$1" >/dev/null
  elapsed=$(($(date +%s) - start))
  code=$(docker inspect -f '{{.State.ExitCode}}' "$1")
  echo "$1 stopped in ${elapsed}s with exit code $code"
  if [[ $code -eq 137 || $elapsed -ge $STOP_LIMIT ]]; then
    echo "$1 did not stop on SIGTERM" >&2
    docker logs "$1" 2>&1 | tail -20 >&2
    return 1
  fi
}

cleanup
docker network create "$NETWORK" >/dev/null

docker run -d --name "$SCHEDULER" --network "$NETWORK" \
  apache/datafusion-ballista-scheduler:latest \
  --bind-host 0.0.0.0 --external-host "$SCHEDULER" >/dev/null
wait_for_log "$SCHEDULER" "listening on"

docker run -d --name "$EXECUTOR" --network "$NETWORK" \
  apache/datafusion-ballista-executor:latest \
  --bind-host 0.0.0.0 --external-host "$EXECUTOR" --scheduler-host "$SCHEDULER" >/dev/null
wait_for_log "$EXECUTOR" "Executor registration succeed"
# Let the executor finish starting up, so its SIGTERM handler is in place.
sleep 5

check_stop "$EXECUTOR"
wait_for_log "$EXECUTOR" "Executor stopped"
check_stop "$SCHEDULER"

docker run -d --name "$STANDALONE" apache/datafusion-ballista-standalone:latest >/dev/null
wait_for_log "$STANDALONE" "Executor registration succeed"
sleep 5

check_stop "$STANDALONE"
wait_for_log "$STANDALONE" "Executor stopped"

echo "All images stop on SIGTERM"
