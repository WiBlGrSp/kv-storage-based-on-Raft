cmake_minimum_required(VERSION 3.16)
project(raft LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
endif()

find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

set(PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/proto/raftRPC.proto"
)

add_library(raft_proto STATIC)

protobuf_generate(
    TARGET raft_proto
    LANGUAGE cpp
    PROTOS "${PROTO_FILE}"
)

protobuf_generate(
    TARGET raft_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    PROTOS "${PROTO_FILE}"
)

target_include_directories(raft_proto
    PUBLIC
        "${CMAKE_CURRENT_BINARY_DIR}"
        "${CMAKE_CURRENT_BINARY_DIR}/proto"
)

target_link_libraries(raft_proto
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)

add_executable(raft
    raft.cpp
    main.cpp
    RaftRPCClient.cpp
    RaftRPC.cpp
    Persister.cpp
)

target_link_libraries(raft
    PRIVATE
        raft_proto
        gRPC::grpc++
        protobuf::libprotobuf
        Threads::Threads
)
