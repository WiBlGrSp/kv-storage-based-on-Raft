find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)
SET(PROTO_FILE server_rpc.proto)
SET(TARGET_NAME server_proto)
protobuf_generate(
    LANGUAGE cpp
    OUT_VAR PB_SRCS
    PROTOC_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTOS ${PROTO_FILE}
)

protobuf_generate(
    LANGUAGE grpc
    OUT_VAR GRPC_SRCS
    PROTOC_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
    PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    GENERATE_EXTENSIONS .grpc.pb.cc .grpc.pb.h
    PROTOS ${PROTO_FILE}
)

add_library(${TARGET_NAME} STATIC ${PB_SRCS} ${GRPC_SRCS})

target_include_directories(${TARGET_NAME} PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
)

target_link_libraries(${TARGET_NAME} PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)