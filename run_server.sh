#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${ROOT_DIR}/build"

if [[ ! -x "${BUILD_DIR}/greeter_server" ]]; then
    "${ROOT_DIR}/build.sh"
fi

exec "${BUILD_DIR}/greeter_server" "$@"
