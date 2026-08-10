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

# Run the chaos harness's Kubernetes (kind) scenarios locally.
#
#   dev/chaos-kind.sh [command] [-- <extra args passed to cargo test>]
#
# Commands:
#   up     Create the kind cluster (if missing), build + load the chaos images.
#   test   Run the kind chaos scenarios against the current cluster.
#   all    up + test           (default)
#   down   Delete the kind cluster.
#
# Env:
#   CLUSTER_NAME   kind cluster name           (default: ballista-chaos)
#   KEEP_CLUSTER   with the `all` command: "0" tears the cluster down at the end,
#                  any other value (default "1") leaves it running
#
# Examples:
#   dev/chaos-kind.sh                       # build, (re)create, load, run everything
#   dev/chaos-kind.sh up                    # just stand the cluster up
#   dev/chaos-kind.sh test -- --nocapture   # re-run tests on an existing cluster
#   dev/chaos-kind.sh down                  # tear it down

set -euo pipefail

CLUSTER_NAME=${CLUSTER_NAME:-ballista-chaos}
KEEP_CLUSTER=${KEEP_CLUSTER:-1}
# Fixture dir shared host<->pods. Under $HOME (not /tmp) because Docker Desktop
# reliably shares the home directory into its VM/kind node, whereas the VM's
# /tmp is not the host's. Exported so the test's k8s backend uses the same path.
export CHAOS_FIXTURE_DIR=${CHAOS_FIXTURE_DIR:-$HOME/.ballista-chaos-fixtures}
CHAOS_IMAGE=ballista-chaos:test

# Run from the repository root so the relative paths below resolve.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# kind config is generated so its extraMounts match CHAOS_FIXTURE_DIR exactly.
KIND_CONFIG="$(mktemp -t kind-config.XXXXXX.yaml)"
gen_kind_config() {
  cat > "$KIND_CONFIG" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: ${CHAOS_FIXTURE_DIR}
        containerPath: ${CHAOS_FIXTURE_DIR}
EOF
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: '$1' is required but not found on PATH" >&2
    exit 1
  }
}

cluster_exists() {
  kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"
}

up() {
  need_cmd docker
  need_cmd kind
  need_cmd kubectl

  mkdir -p "$CHAOS_FIXTURE_DIR"

  if cluster_exists; then
    echo "==> kind cluster '$CLUSTER_NAME' already exists"
  else
    echo "==> creating kind cluster '$CLUSTER_NAME' (fixture mount: $CHAOS_FIXTURE_DIR)"
    gen_kind_config
    # KIND_NODE_IMAGE lets you pin a node image compatible with your kind
    # binary (e.g. kindest/node:v1.31.4). A mismatch between the kind version
    # and the node image is the usual cause of the "could not find a log line
    # that matches ... Multi-User System" boot failure.
    local create_args=(--name "$CLUSTER_NAME" --config "$KIND_CONFIG")
    if [ -n "${KIND_NODE_IMAGE:-}" ]; then
      create_args+=(--image "$KIND_NODE_IMAGE")
    fi
    if ! kind create cluster "${create_args[@]}"; then
      echo "" >&2
      echo "error: kind failed to create the cluster. This is an environment issue" >&2
      echo "       (Docker/kind), not the Ballista harness. Diagnostics:" >&2
      echo "--- versions ---" >&2
      kind version >&2 || true
      docker version --format '{{.Server.Version}}' >&2 || true
      echo "--- node container logs (if retained) ---" >&2
      docker ps -a --filter "name=${CLUSTER_NAME}-control-plane" >&2 || true
      docker logs "${CLUSTER_NAME}-control-plane" 2>&1 | tail -30 >&2 || true
      echo "" >&2
      echo "Try: upgrade kind (brew upgrade kind), (re)start/enlarge Docker, or pin a" >&2
      echo "     node image:  KIND_NODE_IMAGE=kindest/node:v1.31.4 $0 up" >&2
      exit 1
    fi
  fi

  echo "==> building chaos image (compiled inside Docker)"
  ./dev/build-chaos-docker.sh

  echo "==> loading image into kind"
  kind load docker-image "$CHAOS_IMAGE" --name "$CLUSTER_NAME"
}

run_tests() {
  need_cmd kubectl
  cluster_exists || {
    echo "error: kind cluster '$CLUSTER_NAME' does not exist; run '$0 up' first" >&2
    exit 1
  }
  # Point kubectl at this cluster for the duration of the run.
  kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null

  echo "==> running kind chaos scenarios"
  CHAOS_BACKEND=kind cargo test -p ballista-chaos --features k8s --test k8s \
    -- --test-threads=1 "$@"
}

down() {
  need_cmd kind
  echo "==> deleting kind cluster '$CLUSTER_NAME'"
  kind delete cluster --name "$CLUSTER_NAME"
}

command="all"
if [ $# -gt 0 ] && [[ "$1" != "--" ]]; then
  command="$1"
  shift
fi
# Drop a leading "--" so callers can write: `test -- --nocapture`.
if [ $# -gt 0 ] && [[ "$1" == "--" ]]; then
  shift
fi

case "$command" in
  up) up ;;
  test) run_tests "$@" ;;
  down) down ;;
  all)
    up
    run_tests "$@"
    if [ "$KEEP_CLUSTER" = "0" ]; then
      down
    else
      echo "==> leaving cluster '$CLUSTER_NAME' up (set KEEP_CLUSTER=0 to delete, or run '$0 down')"
    fi
    ;;
  *)
    echo "usage: $0 [up|test|down|all] [-- <extra cargo test args>]" >&2
    exit 1
    ;;
esac
