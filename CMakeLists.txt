# 最低支持 CMake 3.16，确保 protobuf_generate(TARGET ...) 可用。
cmake_minimum_required(VERSION 3.16)

# 定义项目名称和语言，本示例只使用 C++。
project(raft LANGUAGES CXX)

# 导出 compile_commands.json，供 clangd、VSCode 和其他编辑器分析代码。
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 统一使用 C++17，禁止编译器扩展，保证不同环境下的行为一致。
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 优先使用 Protobuf 的 Config 模式；
# 如果系统包没有提供 ProtobufConfig.cmake，则退回 CMake 内置 FindProtobuf。
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
endif()

# gRPC 使用 Config 模式查找，并查找 gRPC 所需的多线程库。
find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

# 指定需要生成 C++ 和 gRPC 代码的 .proto 文件。
set(PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/proto/raftRPC.proto"
)

# 将生成的消息代码、服务代码和依赖打包成一个静态库。
add_library(raft_proto STATIC)

# 第一次生成：greeter.pb.h 和 greeter.pb.cc，用于 Protobuf 消息类型。
protobuf_generate(
    TARGET raft_proto
    LANGUAGE cpp
    PROTOS "${PROTO_FILE}"
)

# 第二次生成：greeter.grpc.pb.h 和 greeter.grpc.pb.cc，用于 gRPC 服务与 Stub。
# PLUGIN 必须指向 gRPC 提供的 grpc_cpp_plugin。
protobuf_generate(
    TARGET raft_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    PROTOS "${PROTO_FILE}"
)

# proto 文件位于 proto/raftRPC.proto，生成代码会放到 build/proto/。
# PUBLIC 表示链接 greeter_proto 的目标会自动获得这些 include 路径。
target_include_directories(raft_proto
    PUBLIC
        "${CMAKE_CURRENT_BINARY_DIR}"
        "${CMAKE_CURRENT_BINARY_DIR}/proto"
)

# PUBLIC 表示 greeter_proto 的使用方会自动继承 Protobuf 和 gRPC 依赖。
target_link_libraries(raft_proto
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)

# 编译服务端程序。
add_executable(raft raft.cpp main.cpp RaftRPCClient.cpp RaftRPC.cpp)

# PRIVATE 表示这些依赖只用于构建 greeter_server，不向其他目标传播。
target_link_libraries(raft
    PRIVATE
        raft_proto
        gRPC::grpc++
        protobuf::libprotobuf
        Threads::Threads
)

# # 编译客户端程序。
# add_executable(greeter_client client.cpp)

# # 客户端与服务端使用相同的 Proto 和 gRPC 依赖。
# target_link_libraries(greeter_client
#     PRIVATE
#         greeter_proto
#         gRPC::grpc++
#         protobuf::libprotobuf
#         Threads::Threads
# )
