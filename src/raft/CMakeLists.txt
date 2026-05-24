add_library(raft STATIC
  raft_core.cpp
  event_loop.cpp
)
target_link_libraries(raft PUBLIC rpc Threads::Threads)
