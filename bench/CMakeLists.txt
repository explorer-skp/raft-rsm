# Phase 7 before/after driver. Benchmarks run from the Release build only;
# the rigorous Phase 8 harness will live alongside it here.
add_executable(phase7_bench
  phase7_bench.cpp
)
target_link_libraries(phase7_bench PRIVATE client raft storage statemachine
                                           transport metrics runtime)
