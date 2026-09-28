
add_library(Client STATIC
    client.cpp
)

target_include_directories(Client
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(Client
    PUBLIC
        server_proto
        gRPC::grpc++
)
