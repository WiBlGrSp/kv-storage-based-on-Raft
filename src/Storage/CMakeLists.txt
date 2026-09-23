
add_library(raft_storage STATIC
    RaftStorage.cpp
)

target_include_directories(raft_storage
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
target_link_libraries(raft_storage
    PUBLIC
        raft_core
        kv_store
)