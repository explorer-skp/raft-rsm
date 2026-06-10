# Phase 7 before/after driver (kept as the relative-comparison instrument).
add_executable(phase7_bench
  phase7_bench.cpp
)
target_link_libraries(phase7_bench PRIVATE client raft storage statemachine
                                           transport metrics runtime)

# Phase 8 rigorous harness: instrumented cluster + load generators +
# reporting as a library (the bench self-tests link it), plus the rsm_bench
# driver. Every README number regenerates via run_benchmarks.sh ->
# rsm_bench -> JSON -> plot_results.py.
add_library(bench_lib STATIC
  bench_cluster.cpp
  load_gen.cpp
  bench_report.cpp
)
target_include_directories(bench_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(bench_lib PUBLIC client raft storage statemachine
                                       transport metrics runtime)

add_executable(rsm_bench
  rsm_bench_main.cpp
)
target_link_libraries(rsm_bench PRIVATE bench_lib)
