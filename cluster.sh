#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
BIN="${BUILD_DIR}/raft"
PID_DIR="${ROOT_DIR}/run"
LOG_DIR="${ROOT_DIR}/logs"

start() {
    [[ -x "${BIN}" ]] || {
        echo "missing executable: ${BIN}"
        exit 1
    }

    mkdir -p "${PID_DIR}" "${LOG_DIR}"

    nohup "${BIN}" \
        --listen=1@127.0.0.1:50051 \
        --peer=2@127.0.0.1:50052 \
        --peer=3@127.0.0.1:50053 \
        >"${LOG_DIR}/node1.log" 2>&1 &
    echo "$!" >"${PID_DIR}/node1.pid"

    nohup "${BIN}" \
        --listen=2@127.0.0.1:50052 \
        --peer=1@127.0.0.1:50051 \
        --peer=3@127.0.0.1:50053 \
        >"${LOG_DIR}/node2.log" 2>&1 &
    echo "$!" >"${PID_DIR}/node2.pid"

    nohup "${BIN}" \
        --listen=3@127.0.0.1:50053 \
        --peer=1@127.0.0.1:50051 \
        --peer=2@127.0.0.1:50052 \
        >"${LOG_DIR}/node3.log" 2>&1 &
    echo "$!" >"${PID_DIR}/node3.pid"

    echo "three nodes started"
}

stop() {
    for id in 1 2 3; do
        local pid_file="${PID_DIR}/node${id}.pid"

        if [[ -f "${pid_file}" ]]; then
            kill "$(cat "${pid_file}")" 2>/dev/null || true
            rm -f "${pid_file}"
        fi
    done

    echo "three nodes stopped"
}

case "${1:-}" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    *)
        echo "usage: $0 {start|stop}"
        exit 1
        ;;
esac
