add_library(metrics STATIC
  histogram.cpp
)
target_include_directories(metrics PUBLIC ${CMAKE_SOURCE_DIR}/src)
