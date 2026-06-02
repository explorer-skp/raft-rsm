#include "faults/fault_schedule.h"

#include <algorithm>

namespace rsm::sim {

namespace {

std::int64_t pickFrom(std::mt19937_64& rng, std::int64_t lo, std::int64_t hi) {
    return lo + static_cast<std::int64_t>(
                    rng() % static_cast<std::uint64_t>(hi - lo + 1));
}

}  // namespace

std::string FaultEvent::describe() const {
    std::string s = "t=" + std::to_string(atMs) + " fault ";
    switch (kind) {
        case Kind::Partition: {
            s += "partition";
            for (const auto& g : groups) {
                s += " {";
                for (const NodeId id : g) s += std::to_string(id) + ",";
                s += "}";
            }
            return s;
        }
        case Kind::Heal:
            return s + "heal";
        case Kind::SetDrop:
            return s + "drop p=" + std::to_string(probability);
        case Kind::SetLatency:
            return s + "latency " + std::to_string(latencyMinMs) + ".." +
                   std::to_string(latencyMaxMs) + "ms";
        case Kind::SetReorder:
            return s + "reorder p=" + std::to_string(probability) +
                   " extra<=" + std::to_string(reorderExtraMs) + "ms";
        case Kind::Crash:
            return s + "crash";
        case Kind::Restart:
            return s + "restart";
        case Kind::LeaderKill:
            return s + "leader-kill";
    }
    return s + "?";
}

std::vector<FaultEvent> generateFaultSchedule(std::uint64_t seed,
                                              int nodeCount,
                                              std::int64_t startMs,
                                              std::int64_t endMs) {
    std::mt19937_64 rng(seed);
    std::vector<FaultEvent> events;
    const auto n = static_cast<NodeId>(nodeCount);

    for (std::int64_t t = startMs + pickFrom(rng, 0, 800); t < endMs;
         t += pickFrom(rng, 700, 2500)) {
        FaultEvent ev;
        ev.atMs = t;
        const std::uint64_t kind = rng() % 100;
        if (kind < 20) {
            ev.kind = FaultEvent::Kind::Partition;
            if (rng() % 100 < 70) {
                // Isolate one node against the rest.
                const auto victim = static_cast<NodeId>(1 + rng() % n);
                std::vector<NodeId> rest;
                for (NodeId id = 1; id <= n; ++id) {
                    if (id != victim) rest.push_back(id);
                }
                ev.groups = {{victim}, rest};
            } else {
                // Random bipartition (both sides non-empty).
                std::vector<NodeId> a, b;
                for (NodeId id = 1; id <= n; ++id) {
                    (rng() % 2 == 0 ? a : b).push_back(id);
                }
                if (a.empty()) {
                    a.push_back(b.back());
                    b.pop_back();
                } else if (b.empty()) {
                    b.push_back(a.back());
                    a.pop_back();
                }
                ev.groups = {a, b};
            }
        } else if (kind < 35) {
            ev.kind = FaultEvent::Kind::Heal;
        } else if (kind < 47) {
            ev.kind = FaultEvent::Kind::SetDrop;
            constexpr double kLevels[] = {0.0, 0.05, 0.15, 0.30};
            ev.probability = kLevels[rng() % 4];
        } else if (kind < 59) {
            ev.kind = FaultEvent::Kind::SetLatency;
            constexpr std::int64_t kMax[] = {2, 10, 40, 120};
            ev.latencyMaxMs = kMax[rng() % 4];
            ev.latencyMinMs = pickFrom(rng, 1, std::max<std::int64_t>(
                                                   1, ev.latencyMaxMs / 4));
        } else if (kind < 67) {
            ev.kind = FaultEvent::Kind::SetReorder;
            constexpr double kLevels[] = {0.0, 0.10, 0.30};
            ev.probability = kLevels[rng() % 3];
            ev.reorderExtraMs = pickFrom(rng, 5, 30);
        } else if (kind < 80) {
            ev.kind = FaultEvent::Kind::Crash;
        } else if (kind < 92) {
            ev.kind = FaultEvent::Kind::Restart;
        } else {
            ev.kind = FaultEvent::Kind::LeaderKill;
        }
        events.push_back(ev);
        if (ev.kind == FaultEvent::Kind::Crash ||
            ev.kind == FaultEvent::Kind::LeaderKill) {
            FaultEvent back;
            back.atMs = t + pickFrom(rng, 500, 3000);
            back.kind = FaultEvent::Kind::Restart;
            if (back.atMs < endMs) events.push_back(back);
        }
    }
    std::stable_sort(events.begin(), events.end(),
                     [](const FaultEvent& a, const FaultEvent& b) {
                         return a.atMs < b.atMs;
                     });
    return events;
}

void applyFaultEvent(SimHarness& harness, const FaultEvent& ev,
                     std::mt19937_64& pickRng) {
    harness.traceEvent(ev.describe());
    switch (ev.kind) {
        case FaultEvent::Kind::Partition:
            harness.net().partition(ev.groups);
            return;
        case FaultEvent::Kind::Heal:
            harness.net().heal();
            return;
        case FaultEvent::Kind::SetDrop:
            harness.net().setDrop(ev.probability);
            return;
        case FaultEvent::Kind::SetLatency:
            harness.net().setLatency(Duration(ev.latencyMinMs),
                                     Duration(ev.latencyMaxMs));
            return;
        case FaultEvent::Kind::SetReorder:
            harness.net().setReorder(ev.probability,
                                     Duration(ev.reorderExtraMs));
            return;
        case FaultEvent::Kind::Crash: {
            std::vector<NodeId> live;
            for (NodeId id = 1; id <= harness.nodeCount(); ++id) {
                if (harness.alive(id)) live.push_back(id);
            }
            if (!live.empty()) {
                harness.crash(live[pickRng() % live.size()]);
            }
            return;
        }
        case FaultEvent::Kind::Restart: {
            const auto dead = harness.deadNodes();
            if (!dead.empty()) {
                harness.restart(dead[pickRng() % dead.size()]);
            }
            return;
        }
        case FaultEvent::Kind::LeaderKill:
            harness.killLeader();
            return;
    }
}

}  // namespace rsm::sim
