add_library(faults_sim
  sim_network.cpp
  sim_harness.cpp
  sim_client.cpp
  checkers.cpp
  linearizability.cpp
  fault_schedule.cpp
  chaos.cpp
)
target_include_directories(faults_sim PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(faults_sim PUBLIC raft storage statemachine client rpc)

add_executable(chaos_sim chaos_main.cpp)
target_link_libraries(chaos_sim PRIVATE faults_sim)
