add_executable(raft_node
  node_main.cpp
)
target_link_libraries(raft_node PRIVATE transport raft storage)
