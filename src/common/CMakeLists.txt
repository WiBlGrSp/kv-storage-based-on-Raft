
add_library(thread_pool STATIC
    thread_pool.cpp
)

target_include_directories(thread_pool
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(thread_pool
    PUBLIC
    Threads::Threads
)
