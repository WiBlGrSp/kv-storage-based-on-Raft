
add_library(raft_core STATIC
    raft.cpp
    RaftRPCClient.cpp
    RaftRPC.cpp
    Persister.cpp
)

target_include_directories(raft_core
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(raft_core
    PUBLIC
        raft_proto
        gRPC::grpc++
    PRIVATE
        Threads::Threads
)