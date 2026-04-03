#!/bin/bash
set -e

ROLE=${1:-coordinator}

case "$ROLE" in
  coordinator)
    echo "Starting Uniffle Coordinator..."
    mkdir -p ${UNIFFLE_HOME}/logs
    cd ${UNIFFLE_HOME}
    bash bin/start-coordinator.sh

    # Wait for coordinator to be ready
    for i in $(seq 1 30); do
        if curl -sf http://localhost:19995/api/app/total > /dev/null 2>&1; then
            echo "Coordinator is ready!"
            break
        fi
        sleep 2
    done

    # Keep container alive
    tail -f ${UNIFFLE_HOME}/logs/coordinator.log 2>/dev/null || tail -f /dev/null
    ;;

  server)
    COORDINATOR_HOST=${COORDINATOR_HOST:-localhost}
    echo "Starting Riffle Server (coordinator: ${COORDINATOR_HOST}:21000)..."

    # Patch coordinator address in config
    sed -i "s/COORDINATOR_HOST/${COORDINATOR_HOST}/g" /opt/riffle/riffle.toml

    mkdir -p /tmp/riffle-data /tmp/riffle-logs
    WORKER_IP=$(hostname -i) RUST_LOG=info /opt/riffle/riffle-server --config /opt/riffle/riffle.toml
    ;;

  *)
    echo "Unknown role: $ROLE (use 'coordinator' or 'server')"
    exit 1
    ;;
esac
