// Phase 6 seeded chaos suite (ctest entry point). Each seed drives a full
// deterministic run — concurrent KV clients, randomized partitions / drops
// / delays / reorders / crashes / restarts / leader kills over the REAL
// Raft + durable storage + dedup code — followed by a healing window, then
// asserts: the five invariants held throughout, nothing acknowledged was
// lost, the client history is linearizable, and the healed cluster
// converged. A failing seed prints itself and replays with one command:
//
//     ./build/faults/chaos_sim --seed N

#include <cstdio>

#include "doctest/doctest.h"
#include "faults/chaos.h"
#include "raft/logging.h"

using rsm::sim::ChaosOptions;
using rsm::sim::runChaos;

TEST_CASE("seeded chaos suite: invariants, no lost commit, linearizability, "
          "post-heal convergence across many seeds") {
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Error);
    const ChaosOptions options;  // ctest runs EXACTLY the chaos_sim defaults
    std::size_t totalCompleted = 0;
    std::uint64_t maxTerm = 0;
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        CAPTURE(seed);
        const auto report = runChaos(seed, options);
        if (!report.pass) {
            std::printf("CHAOS FAILURE — reproduce with:\n"
                        "  ./build/faults/chaos_sim --seed %llu\n%s\n",
                        static_cast<unsigned long long>(seed),
                        report.summary().c_str());
        }
        REQUIRE_MESSAGE(report.pass, report.summary());
        // The suite must be exercising something: ops completed under
        // faults and elections actually churned.
        CHECK(report.completedOps == report.totalOps);
        CHECK(report.totalOps > 0);
        totalCompleted += report.completedOps;
        maxTerm = std::max(maxTerm, report.finalTerm);
    }
    CHECK(totalCompleted >= 20 * 40);  // ~52 ops per seed
    CHECK(maxTerm > 5);  // fault schedules really forced re-elections
}
