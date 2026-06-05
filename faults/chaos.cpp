#include "faults/chaos.h"

#include <memory>
#include <set>
#include <utility>

#include "faults/fault_schedule.h"
#include "faults/linearizability.h"
#include "faults/sim_client.h"
#include "faults/sim_harness.h"
#include "faults/sim_temp_dir.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"

namespace rsm::sim {

namespace {

// Independent sub-seeds from the master seed (so e.g. the fault schedule
// and the client workload don't correlate).
std::uint64_t splitmix64(std::uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

}  // namespace

std::string ChaosReport::summary() const {
    std::string s = "seed=" + std::to_string(seed) +
                    (pass ? " PASS" : " FAIL") +
                    " endMs=" + std::to_string(endMs) + " ops=" +
                    std::to_string(completedOps) + "/" +
                    std::to_string(totalOps) +
                    " faults=" + std::to_string(faultEvents) +
                    " crashes=" + std::to_string(crashes) +
                    " term=" + std::to_string(finalTerm) +
                    " commit=" + std::to_string(finalCommitIndex);
    for (const auto& v : violations) s += "\n  VIOLATION: " + v;
    return s;
}

ChaosReport runChaos(std::uint64_t seed, const ChaosOptions& options) {
    ChaosReport report;
    report.seed = seed;
    const auto sub = [seed](std::uint64_t k) {
        return splitmix64(seed ^ splitmix64(k));
    };

    // --- storage: the real Phase 4 durable implementations by default, so
    // chaos crashes exercise real replay/recovery. Paths never enter the
    // trace (they vary per run; the trace must not).
    std::unique_ptr<SimTempDir> tmp;
    StorageFactory storage;
    if (options.durableStorage) {
        tmp = std::make_unique<SimTempDir>();
        storage = [&tmpRef = *tmp](NodeId id, int) -> StoragePair {
            const std::string dir = tmpRef.subdir("n" + std::to_string(id));
            return {std::make_unique<rsm::storage::DurablePersistentState>(dir),
                    std::make_unique<rsm::storage::DurableLog>(dir)};
        };
    } else {
        storage = inMemoryStorage;
    }

    HarnessOptions ho;
    for (int i = 0; i < options.nodes; ++i) {
        ho.nodes.push_back(SimNodeConfig{sub(1000 + i), RaftConfig{}});
    }
    ho.netSeed = sub(1);
    ho.recordTrace = options.recordTrace;
    ho.restartSeed = [sub](NodeId id, int incarnation) {
        return sub(2000 + 64ULL * id + static_cast<std::uint64_t>(incarnation));
    };
    SimHarness harness(std::move(ho), storage);
    harness.net().setLatency(Duration(1), Duration(3));

    // --- seeded fault schedule and client workload
    const auto schedule = generateFaultSchedule(
        sub(2), options.nodes, options.faultStartMs, options.faultEndMs);
    std::mt19937_64 pickRng(sub(3));
    report.faultEvents = schedule.size();
    for (const auto& ev : schedule) {
        report.crashes += (ev.kind == FaultEvent::Kind::Crash ||
                           ev.kind == FaultEvent::Kind::LeaderKill)
                              ? 1
                              : 0;
    }

    History history;
    WorkloadConfig wc;
    wc.opsPerClient = options.opsPerClient;
    wc.stopIssuingAtMs = options.faultEndMs;
    wc.finalOpAtMs = options.faultEndMs + 1500;
    std::vector<std::unique_ptr<SimClient>> clients;
    for (int i = 0; i < options.clients; ++i) {
        clients.push_back(std::make_unique<SimClient>(
            /*clientId=*/static_cast<std::uint64_t>(i + 1),
            /*envelopeBase=*/static_cast<NodeId>(1000 + i * 4000),
            /*envelopeRange=*/4000, sub(3000 + i), wc, harness, history));
    }

    // --- the run: faults → clients → one virtual millisecond, repeat
    const auto allClientsDone = [&clients] {
        for (const auto& c : clients) {
            if (!c->done()) return false;
        }
        return true;
    };
    std::size_t nextEvent = 0;
    bool healed = false;
    while (harness.nowMs() < options.maxMs) {
        while (nextEvent < schedule.size() &&
               schedule[nextEvent].atMs <= harness.nowMs()) {
            applyFaultEvent(harness, schedule[nextEvent++], pickRng);
        }
        if (!healed && harness.nowMs() >= options.faultEndMs) {
            healed = true;
            harness.traceEvent("t=" + std::to_string(harness.nowMs()) +
                               " heal-all");
            harness.net().heal();
            harness.net().setDrop(0.0);
            harness.net().setLatency(Duration(1), Duration(3));
            harness.net().setReorder(0.0, Duration(0));
            for (const NodeId id : harness.deadNodes()) harness.restart(id);
        }
        for (auto& c : clients) c->tick();
        harness.stepMs();
        if (healed && harness.nowMs() >= options.faultEndMs + 2000 &&
            allClientsDone() && harness.quiescent()) {
            break;
        }
    }
    report.endMs = harness.nowMs();

    // --- end-state assertions
    if (!allClientsDone()) {
        report.violations.push_back(
            "LIVENESS: " + std::to_string(history.completedCount()) + "/" +
            std::to_string(history.totalCount()) +
            " client ops acknowledged — outstanding ops did not complete "
            "within the healing window");
    }
    if (!harness.quiescent()) {
        report.violations.push_back(
            "CONVERGENCE: cluster did not converge to a single leader with "
            "identical, fully-applied logs after healing");
    }
    harness.finalCheck();
    for (auto& v : harness.violations()) {
        report.violations.push_back(std::move(v));
    }

    // No lost commit: every acknowledged (clientId, seqNo) appears in the
    // committed log. (Continuous non-disappearance is CommitChecker's job.)
    const auto leaders = harness.liveLeaders();
    if (!leaders.empty()) {
        const auto& log = harness.log(leaders[0]);
        const auto commit = harness.core(leaders[0]).commitIndex();
        std::set<std::pair<std::uint64_t, std::uint64_t>> committedIds;
        for (rsm::rpc::LogIndex i = 1; i <= commit; ++i) {
            if (const auto id = commandIdentity(log.entryAt(i).command)) {
                committedIds.insert(*id);
            }
        }
        for (const auto& acked : history.ackedIdentities()) {
            if (!committedIds.contains(acked)) {
                report.violations.push_back(
                    "NO LOST COMMIT: acknowledged op (client " +
                    std::to_string(acked.first) + ", seq " +
                    std::to_string(acked.second) +
                    ") is missing from the final committed log");
            }
        }
        report.finalTerm = harness.core(leaders[0]).term();
        report.finalCommitIndex = commit;
    }

    const auto lin = checkLinearizable(history.ops());
    if (!lin.ok) {
        report.violations.push_back("LINEARIZABILITY: " + lin.explanation);
    }

    report.totalOps = history.totalCount();
    report.completedOps = history.completedCount();
    report.pass = report.violations.empty();
    report.trace = harness.trace();
    return report;
}

}  // namespace rsm::sim
