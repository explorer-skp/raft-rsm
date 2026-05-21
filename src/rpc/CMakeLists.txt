add_library(rpc STATIC
  messages.cpp
)
target_include_directories(rpc PUBLIC ${CMAKE_SOURCE_DIR}/src)
