add_library(statemachine STATIC
  kv_store.cpp
)
target_link_libraries(statemachine PUBLIC rpc)
