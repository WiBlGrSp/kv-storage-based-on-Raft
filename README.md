# Simple gRPC C++ Example

This directory contains a minimal gRPC example.

It defines one RPC:

```text
Greeter.SayHello
```

Files:

```text
raft/
  CMakeLists.txt
  client.cpp
  server.cpp
  proto/greeter.proto
```

## Build on Ubuntu

Install dependencies:

```bash
sudo apt update
sudo apt install -y \
  build-essential \
  cmake \
  ninja-build \
  pkg-config \
  libgrpc++-dev \
  protobuf-compiler-grpc \
  libprotobuf-dev \
  protobuf-compiler
```

Recommended build:

```bash
chmod +x build.sh run_server.sh run_client.sh
./build.sh
```

Manual build:

```bash
mkdir -p build
cd build

cmake .. -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build . -j"$(nproc)"
```

If CMake cannot find `ProtobufConfig.cmake`, the project automatically
falls back to CMake's built-in `FindProtobuf` module.

If gRPC is installed in a custom prefix:

```bash
cmake .. -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_PREFIX_PATH="$HOME/.local/grpc"
```

## Run

Terminal 1:

```bash
./run_server.sh
```

Optional address:

```bash
./run_server.sh 0.0.0.0:50051
```

Terminal 2:

```bash
./run_client.sh
```

With a custom message:

```bash
./run_client.sh 127.0.0.1:50051 Codex
```

Expected output:

```text
Hello, Codex
```
