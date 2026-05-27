add_library(storage STATIC
  crc32c.cpp
  durable_state.cpp
  durable_log.cpp
)
target_link_libraries(storage PUBLIC rpc)
