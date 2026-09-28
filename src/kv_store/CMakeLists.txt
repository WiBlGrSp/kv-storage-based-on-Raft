add_library(kv_store STATIC
    kv_store.cpp
)

target_include_directories(kv_store
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
