add_library(statemachine STATIC
  kv_store.cpp
  order_book.cpp
)
target_link_libraries(statemachine PUBLIC rpc)
