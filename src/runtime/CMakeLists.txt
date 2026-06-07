# Phase 7 performance layer: lock-free rings (header-only) + the threaded
# NodeRuntime that replaces RaftEventLoop in the real (non-sim) runtime.
add_library(runtime STATIC
  node_runtime.cpp
)
target_link_libraries(runtime PUBLIC raft transport statemachine)

add_executable(raft_node
  node_main.cpp
)
target_link_libraries(raft_node PRIVATE runtime transport raft storage client
                                        statemachine)

add_executable(kv_cli
  kv_cli_main.cpp
)
target_link_libraries(kv_cli PRIVATE client statemachine transport)
