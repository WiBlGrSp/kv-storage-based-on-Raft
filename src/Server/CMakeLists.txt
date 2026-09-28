
add_library(Server STATIC
    server.cpp
    server_rpc.cpp
)

target_include_directories(Server
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(Server
    PUBLIC
        server_proto
        gRPC::grpc++
        raft_core
        kv_store
        raft_storage
)
