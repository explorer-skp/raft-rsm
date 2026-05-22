add_library(transport STATIC
  frame.cpp
  transport.cpp
)
target_link_libraries(transport PUBLIC rpc Threads::Threads)
