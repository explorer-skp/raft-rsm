#pragma once

// Seeded fault schedule for the chaos suite: a list of timed fault events,
// generated entirely from a seed before the run starts. Targets that depend
// on run state (which node is leader, who is dead) are resolved at
// execution time with a dedicated pick-RNG — still a pure function of the
// seed, since the run itself is deterministic.

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "faults/sim_harness.h"

namespace rsm::sim {

struct FaultEvent {
    std::int64_t atMs = 0;
    enum class Kind {
        Partition,   // groups
        Heal,        // remove partition
        SetDrop,     // probability
        SetLatency,  // latencyMin/latencyMax
        SetReorder,  // probability + reorderExtraMs
        Crash,       // a deterministically-picked live node
        Restart,     // a deterministically-picked dead node (no-op if none)
        LeaderKill,  // crash the current live leader (no-op if none)
    } kind{};
    std::vector<std::vector<NodeId>> groups;
    double probability = 0.0;
    std::int64_t latencyMinMs = 0;
    std::int64_t latencyMaxMs = 0;
    std::int64_t reorderExtraMs = 0;

    std::string describe() const;
};

// Events in [startMs, endMs), sorted by time. Crashes auto-schedule a
// restart 500–3000 ms later so the cluster keeps making progress; explicit
// Restart events add extra recovery on top. The caller heals everything at
// endMs itself.
std::vector<FaultEvent> generateFaultSchedule(std::uint64_t seed,
                                              int nodeCount,
                                              std::int64_t startMs,
                                              std::int64_t endMs);

// Applies one event (resolving live/dead/leader targets via pickRng) and
// records it in the harness trace.
void applyFaultEvent(SimHarness& harness, const FaultEvent& ev,
                     std::mt19937_64& pickRng);

}  // namespace rsm::sim
