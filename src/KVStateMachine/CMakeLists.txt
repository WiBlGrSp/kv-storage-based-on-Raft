add_library(kv_store STATIC
    KVStore.cpp
)

target_include_directories(kv_store
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)