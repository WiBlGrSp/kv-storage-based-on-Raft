find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)

protobuf_generate(
    LANGUAGE cpp
    OUT_VAR PB_SRCS
    PROTOC_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTOS raftRPC.proto
)

protobuf_generate(
    LANGUAGE grpc
    OUT_VAR GRPC_SRCS
    PROTOC_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
    PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    GENERATE_EXTENSIONS .grpc.pb.cc .grpc.pb.h
    PROTOS raftRPC.proto
)

add_library(raft_proto STATIC ${PB_SRCS} ${GRPC_SRCS})

target_include_directories(raft_proto PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
)

target_link_libraries(raft_proto PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)