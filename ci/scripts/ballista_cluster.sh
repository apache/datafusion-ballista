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

# Starts and stops the Ballista cluster (one scheduler, one executor) that the
# benchmark CI jobs run their suites against.
#
# Usage:
#   ci/scripts/ballista_cluster.sh start   # start both, block until ready
#   ci/scripts/ballista_cluster.sh stop    # dump log tails, then terminate
#
# `start` writes the PIDs to a pidfile so `stop` can run as a separate CI step.
#
# Settings, all overridable via the environment:
#   BALLISTA_BIN_DIR    directory holding ballista-scheduler / ballista-executor
#   CLUSTER_DIR         scratch dir for logs, pidfile and the executor work dir
#   SCHEDULER_PORT      scheduler bind/connect port
#   EXECUTOR_PORT       executor bind port
#   EXECUTOR_VCORES     executor vcore count
#   EXECUTOR_MEMORY     executor memory pool size
#   READY_TIMEOUT_SECONDS     how long to wait for each port to open
#   SHUTDOWN_TIMEOUT_SECONDS  grace period after SIGTERM before SIGKILL
#   STARTUP_SETTLE_SECONDS    pause before the post-readiness liveness re-check

set -euo pipefail

BALLISTA_BIN_DIR="${BALLISTA_BIN_DIR:-./target/tpch-ci}"
CLUSTER_DIR="${CLUSTER_DIR:-${RUNNER_TEMP:-/tmp}}"
SCHEDULER_PORT="${SCHEDULER_PORT:-50050}"
EXECUTOR_PORT="${EXECUTOR_PORT:-50051}"
EXECUTOR_VCORES="${EXECUTOR_VCORES:-4}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2GB}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-30}"
SHUTDOWN_TIMEOUT_SECONDS="${SHUTDOWN_TIMEOUT_SECONDS:-10}"
STARTUP_SETTLE_SECONDS="${STARTUP_SETTLE_SECONDS:-2}"

SCHEDULER_LOG="${CLUSTER_DIR}/scheduler.log"
EXECUTOR_LOG="${CLUSTER_DIR}/executor.log"
WORK_DIR="${CLUSTER_DIR}/work"
PID_FILE="${CLUSTER_DIR}/ballista-cluster.pids"

dump_logs() {
    echo "::group::scheduler log (tail)"
    tail -n 200 "${SCHEDULER_LOG}" 2>/dev/null || true
    echo "::endgroup::"
    echo "::group::executor log (tail)"
    tail -n 200 "${EXECUTOR_LOG}" 2>/dev/null || true
    echo "::endgroup::"
}

# Waits for a port to accept connections, failing fast if the process backing it
# has already exited.
wait_for_port() {
    local name=$1 port=$2 pid=$3

    echo "Waiting for ${name} on 127.0.0.1:${port}..."
    for _ in $(seq 1 "${READY_TIMEOUT_SECONDS}"); do
        if nc -z 127.0.0.1 "${port}"; then
            echo "${name} is up"
            return 0
        fi
        if ! kill -0 "${pid}" 2>/dev/null; then
            echo "${name} exited before opening port ${port}" >&2
            return 1
        fi
        sleep 1
    done

    echo "${name} did not open port ${port} within ${READY_TIMEOUT_SECONDS}s" >&2
    return 1
}

# An open port is not proof of a healthy process: the executor binds its gRPC
# port before the Arrow Flight server and exits if that later bind fails, and a
# stale listener from an earlier run can answer the probe too.
confirm_still_alive() {
    sleep "${STARTUP_SETTLE_SECONDS}"

    while [ $# -gt 0 ]; do
        if ! kill -0 "$2" 2>/dev/null; then
            echo "$1 exited during startup (see log below)" >&2
            return 1
        fi
        shift 2
    done
}

start() {
    mkdir -p "${WORK_DIR}"

    "${BALLISTA_BIN_DIR}/ballista-scheduler" \
        --bind-host 127.0.0.1 \
        --bind-port "${SCHEDULER_PORT}" \
        > "${SCHEDULER_LOG}" 2>&1 &
    local scheduler_pid=$!

    "${BALLISTA_BIN_DIR}/ballista-executor" \
        --bind-host 127.0.0.1 \
        --bind-port "${EXECUTOR_PORT}" \
        --scheduler-host 127.0.0.1 \
        --scheduler-port "${SCHEDULER_PORT}" \
        --scheduler-connect-timeout-seconds 10 \
        --vcores "${EXECUTOR_VCORES}" \
        --memory-pool-size "${EXECUTOR_MEMORY}" \
        --work-dir "${WORK_DIR}" \
        > "${EXECUTOR_LOG}" 2>&1 &
    local executor_pid=$!

    printf '%s\n%s\n' "${scheduler_pid}" "${executor_pid}" > "${PID_FILE}"

    if ! wait_for_port scheduler "${SCHEDULER_PORT}" "${scheduler_pid}" ||
        ! wait_for_port executor "${EXECUTOR_PORT}" "${executor_pid}" ||
        ! confirm_still_alive scheduler "${scheduler_pid}" executor "${executor_pid}"; then
        dump_logs
        return 1
    fi
}

stop() {
    dump_logs

    if [ ! -f "${PID_FILE}" ]; then
        echo "no pidfile at ${PID_FILE}; nothing to stop"
        return 0
    fi

    # These were started by an earlier step's shell and are not our children,
    # so poll for the PID to go away rather than waiting on it.
    while read -r pid; do
        [ -n "${pid}" ] || continue
        kill -0 "${pid}" 2>/dev/null || continue

        kill "${pid}" 2>/dev/null || true
        for _ in $(seq 1 "${SHUTDOWN_TIMEOUT_SECONDS}"); do
            kill -0 "${pid}" 2>/dev/null || break
            sleep 1
        done

        if kill -0 "${pid}" 2>/dev/null; then
            echo "pid ${pid} ignored SIGTERM after ${SHUTDOWN_TIMEOUT_SECONDS}s; sending SIGKILL"
            kill -9 "${pid}" 2>/dev/null || true
        fi
    done < "${PID_FILE}"

    rm -f "${PID_FILE}"
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    *)
        echo "usage: $0 {start|stop}" >&2
        exit 2
        ;;
esac
