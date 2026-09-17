#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
JOBS="${JOBS:-$(nproc)}"
GENERATOR="${GENERATOR:-Ninja}"

cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -G "${GENERATOR}" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    "$@"

cmake --build "${BUILD_DIR}" -j"${JOBS}"

# 将 CMake 生成的编译数据库放到项目根目录，
# 方便 clangd 自动识别生成代码的 include 路径。
if [[ -f "${BUILD_DIR}/compile_commands.json" ]]; then
    cp "${BUILD_DIR}/compile_commands.json" \
       "${ROOT_DIR}/compile_commands.json"
fi

echo
echo "Build completed:"
echo "  ${BUILD_DIR}/greeter_server"
echo "  ${BUILD_DIR}/greeter_client"
