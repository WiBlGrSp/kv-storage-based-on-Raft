add_executable(raft_example
    main.cpp
)

target_link_libraries(raft_example
    PRIVATE
        raft_core
)