#!/usr/bin/env bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-g"
cmake --build build -j$(nproc)