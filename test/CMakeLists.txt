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
  kv_store_test.cpp
  client_session_sim_test.cpp
  batching_equivalence_test.cpp
  histogram_test.cpp
)
target_link_libraries(unit_tests PRIVATE doctest rpc transport raft storage
                                         statemachine client metrics)

add_test(NAME unit_tests COMMAND unit_tests)

add_executable(ring_tests
  test_main.cpp
  ring_buffer_test.cpp
)
# Header-only rings; rpc's PUBLIC include dir provides the src/ root.
target_link_libraries(ring_tests PRIVATE doctest rpc Threads::Threads)

add_test(NAME ring_tests COMMAND ring_tests)
set_tests_properties(ring_tests PROPERTIES TIMEOUT 300)

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
target_link_libraries(raft_cluster_tests PRIVATE doctest transport raft storage
                                                 statemachine client runtime)

add_test(NAME raft_cluster_tests COMMAND raft_cluster_tests)
set_tests_properties(raft_cluster_tests PROPERTIES TIMEOUT 120)

add_executable(kv_cluster_tests
  test_main.cpp
  kv_cluster_test.cpp
)
target_link_libraries(kv_cluster_tests PRIVATE doctest transport raft storage
                                               statemachine client runtime)

add_test(NAME kv_cluster_tests COMMAND kv_cluster_tests)
set_tests_properties(kv_cluster_tests PROPERTIES TIMEOUT 120)

# Allocation-free steady state: overrides global operator new, so it gets
# its own binary. Meaningful in every config; the assertion is the same.
add_executable(alloc_tests
  test_main.cpp
  alloc_test.cpp
)
target_link_libraries(alloc_tests PRIVATE doctest transport raft storage
                                          statemachine client runtime metrics)
# False positive with user-replaced new/delete implemented over malloc/free
# in the same TU: GCC pairs the inlined malloc with the library-side delete
# and warns, although every path is consistent (new->malloc, delete->free).
target_compile_options(alloc_tests PRIVATE -Wno-mismatched-new-delete)

add_test(NAME alloc_tests COMMAND alloc_tests)
set_tests_properties(alloc_tests PROPERTIES TIMEOUT 120)

add_executable(faults_tests
  test_main.cpp
  sim_fault_test.cpp
  checker_selftest_test.cpp
  figure8_sim_test.cpp
  chaos_test.cpp
)
target_link_libraries(faults_tests PRIVATE doctest faults_sim)

add_test(NAME faults_tests COMMAND faults_tests)
set_tests_properties(faults_tests PROPERTIES TIMEOUT 600)

# The one-command replay path itself is part of the contract: a seed that
# fails in the suite must reproduce via this exact executable.
add_test(NAME chaos_sim_replay COMMAND chaos_sim --seed 1)
set_tests_properties(chaos_sim_replay PROPERTIES TIMEOUT 120)
