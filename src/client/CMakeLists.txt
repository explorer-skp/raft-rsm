add_library(client STATIC
  client_service.cpp
  kv_client.cpp
)
target_link_libraries(client PUBLIC raft statemachine transport)
