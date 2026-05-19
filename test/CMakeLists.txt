add_executable(unit_tests
  test_main.cpp
  sanity_test.cpp
  rpc_roundtrip_test.cpp
  framing_test.cpp
  malformed_test.cpp
  raft_log_test.cpp
  raft_unit_test.cpp
  raft_replication_test.cpp
  raft_sim_test.cpp
  raft_replication_sim_test.cpp
  durable_storage_test.cpp
  raft_persistence_sim_test.cpp
)
target_link_libraries(unit_tests PRIVATE doctest rpc transport raft storage)

add_test(NAME unit_tests COMMAND unit_tests)

add_executable(transport_tests
  test_main.cpp
  transport_integration_test.cpp
)
target_link_libraries(transport_tests PRIVATE doctest transport)

add_test(NAME transport_tests COMMAND transport_tests)

add_executable(raft_cluster_tests
  test_main.cpp
  raft_cluster_test.cpp
)
target_link_libraries(raft_cluster_tests PRIVATE doctest transport raft storage)

add_test(NAME raft_cluster_tests COMMAND raft_cluster_tests)
set_tests_properties(raft_cluster_tests PROPERTIES TIMEOUT 120)
