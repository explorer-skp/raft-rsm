#pragma once

// The seeded chaos suite driver (the Phase 6 main deliverable). One call =
// one fully deterministic run: real Raft/KV/persistence/dedup code, a
// seeded concurrent client workload, a seeded fault schedule, a quiescent
// healing window, then the full battery of end-state assertions. Every
// knob below defaults to the values the ctest chaos suite uses, so a seed
// that fails in CI reproduces with exactly:
//
//     ./build/faults/chaos_sim --seed N

#include <cstdint>
#include <string>
#include <vector>

namespace rsm::sim {

struct ChaosOptions {
    int nodes = 3;
    int clients = 4;
    int opsPerClient = 12;
    std::int64_t faultStartMs = 1500;   // let the cluster boot first
    std::int64_t faultEndMs = 20000;    // heal-everything moment
    std::int64_t maxMs = 40000;         // hard cap incl. healing window
    bool durableStorage = true;         // real Phase 4 files in a temp dir
    bool recordTrace = false;           // needed for the determinism test
};

struct ChaosReport {
    std::uint64_t seed = 0;
    bool pass = false;
    std::vector<std::string> violations;

    // Run statistics (all deterministic for a seed).
    std::int64_t endMs = 0;
    std::size_t totalOps = 0;
    std::size_t completedOps = 0;
    std::size_t faultEvents = 0;
    std::size_t crashes = 0;
    std::uint64_t finalTerm = 0;
    std::uint64_t finalCommitIndex = 0;

    std::vector<std::string> trace;  // filled iff options.recordTrace

    std::string summary() const;
};

ChaosReport runChaos(std::uint64_t seed, const ChaosOptions& options = {});

}  // namespace rsm::sim
