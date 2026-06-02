#pragma once

// Phase 6 deterministic single-process simulation harness.
//
// Runs N real nodes — production RaftCore + storage (in-memory or the real
// durable Phase 4 implementations) + state machine + ClientService, all
// byte-for-byte the code the TCP cluster runs — over a SimNetwork and one
// shared ManualClock. There are no OS threads: stepMs() advances virtual
// time one millisecond and is the single scheduler — it fires due fault
// events' effects (driven externally), ticks every live node in node-id
// order, then drains every due message (including same-step cascades) in
// the network's seed-determined order. The sim/real seam is exactly
// {Clock, SendFn-and-handle() in place of TCP, this loop in place of
// RaftEventLoop}; RaftCore is single-threaded by contract either way.
//
// The five-invariant monitors run continuously: Election Safety and Leader
// Completeness at every election win, Leader Append-Only and the committed-
// prefix (no-lost-commit) record every step, State Machine Safety at every
// apply, and pairwise Log Matching every `logMatchingEveryMs` steps and on
// finalCheck(). Violations accumulate as strings — the harness never aborts
// mid-run, so a chaos failure reports everything it saw.

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "client/client_service.h"
#include "faults/checkers.h"
#include "faults/sim_network.h"
#include "raft/clock.h"
#include "raft/persistent_state.h"
#include "raft/raft_core.h"
#include "statemachine/state_machine.h"
#include "storage/log.h"

namespace rsm::sim {

using rsm::raft::ManualClock;
using rsm::raft::RaftConfig;
using rsm::raft::RaftCore;
using rsm::raft::Role;

struct StoragePair {
    std::unique_ptr<rsm::raft::PersistentState> persist;
    std::unique_ptr<rsm::storage::RaftLog> log;
};
// (node, incarnation): incarnation counts restarts, letting factories vary
// behavior per life; a durable factory reuses the node's directory so a
// restart replays whatever survived on disk, exactly like a real process.
using StorageFactory = std::function<StoragePair(NodeId, int incarnation)>;
using SmFactory =
    std::function<std::unique_ptr<rsm::statemachine::StateMachine>(NodeId)>;

StoragePair inMemoryStorage(NodeId, int);
std::unique_ptr<rsm::statemachine::StateMachine> kvStateMachine(NodeId);

struct SimNodeConfig {
    std::uint64_t rngSeed = 0;
    RaftConfig raft;
};

struct HarnessOptions {
    std::vector<SimNodeConfig> nodes;  // node ids are 1..nodes.size()
    std::uint64_t netSeed = 0;
    int logMatchingEveryMs = 25;
    bool recordTrace = false;
    // RNG seed for a node's i-th restart; must be deterministic.
    std::function<std::uint64_t(NodeId, int incarnation)> restartSeed;
};

class SimHarness {
public:
    SimHarness(HarnessOptions opts, StorageFactory storage = inMemoryStorage,
               SmFactory sm = kvStateMachine);

    // ---- node fault controls ----
    // Abrupt stop: the node stops processing instantly; volatile state is
    // lost on restart, persisted state (whatever the storage factory keeps)
    // survives. Messages it already sent stay on the wire — real packets
    // don't vanish when their sender dies.
    void crash(NodeId id);
    // Graceful stop. Observably identical to crash() in this system — every
    // durability point is synchronous (fsync-before-ack, Phase 4), so there
    // is never dirty state for a clean shutdown to flush; torn-write
    // recovery is exercised separately by the Phase 4 storage tests.
    void gracefulStop(NodeId id) { crash(id); }
    void restart(NodeId id);
    // Crashes the highest-term live leader, if any; returns who died.
    std::optional<NodeId> killLeader();
    bool alive(NodeId id) const { return nodes_[id - 1]->alive; }
    std::vector<NodeId> deadNodes() const;

    // ---- network fault controls ----
    SimNetwork& net() { return net_; }

    // ---- time ----
    void stepMs();
    std::int64_t nowMs() const;
    template <typename Pred>
    bool runUntil(std::int64_t maxMs, Pred pred) {
        for (std::int64_t i = 0; i < maxMs; ++i) {
            if (pred()) return true;
            stepMs();
        }
        return pred();
    }
    void runFor(std::int64_t ms) {
        for (std::int64_t i = 0; i < ms; ++i) stepMs();
    }

    // ---- access ----
    std::size_t nodeCount() const { return nodes_.size(); }
    RaftCore& core(NodeId id) { return *nodes_[id - 1]->core; }
    const RaftCore& core(NodeId id) const { return *nodes_[id - 1]->core; }
    rsm::storage::RaftLog& log(NodeId id) { return *nodes_[id - 1]->log; }
    rsm::statemachine::StateMachine& sm(NodeId id) {
        return *nodes_[id - 1]->sm;
    }
    std::vector<NodeId> liveLeaders() const;
    // Exactly one live leader and every live node agrees on it (term and id).
    bool converged() const;
    // converged() plus identical logs and fully-applied commit everywhere.
    bool quiescent() const;

    // ---- sim-client plumbing ----
    // Routes deliveries addressed to [idLow, idHigh] to `onDeliver` (sim
    // clients use one envelope id per attempt, mirroring the real client's
    // connection-per-attempt reply correlation).
    void registerEndpoint(
        NodeId idLow, NodeId idHigh,
        std::function<void(NodeId from, NodeId to, Message&&)> fn);
    void clientSend(NodeId fromClient, NodeId toNode, Message m);

    // ---- checking ----
    // All violations seen so far (monitors + periodic log-matching sweeps).
    std::vector<std::string> violations() const;
    void finalCheck();  // run the full pairwise sweep now
    const CommitChecker& commitChecker() const { return commit_; }

    // ---- trace (determinism witness) ----
    const std::vector<std::string>& trace() const { return trace_; }
    void traceEvent(const std::string& s) {
        if (opts_.recordTrace) trace_.push_back(s);
    }

    ManualClock clock;

private:
    struct NodeRt {
        NodeId id = 0;
        int incarnation = 0;
        bool alive = true;
        std::unique_ptr<rsm::raft::PersistentState> persist;
        std::unique_ptr<rsm::storage::RaftLog> log;
        std::unique_ptr<rsm::statemachine::StateMachine> sm;
        std::unique_ptr<RaftCore> core;
        std::unique_ptr<rsm::client::ClientService> service;
    };
    struct Endpoint {
        NodeId low = 0;
        NodeId high = 0;
        std::function<void(NodeId from, NodeId to, Message&&)> fn;
    };

    void buildNode(NodeId id, std::uint64_t seed);
    void deliver(NodeId from, NodeId to, Message&& m);
    void observeStep();

    HarnessOptions opts_;
    SimNetwork net_;
    StorageFactory storage_;
    SmFactory smFactory_;
    std::vector<std::unique_ptr<NodeRt>> nodes_;
    std::vector<Endpoint> endpoints_;

    ElectionSafetyChecker electionSafety_;
    LeaderAppendOnlyChecker appendOnly_;
    AppliedConsistencyChecker applied_;
    CommitChecker commit_;
    std::vector<std::string> sweepViolations_;
    std::vector<std::string> trace_;
    std::int64_t lastLogMatchingMs_ = 0;
};

}  // namespace rsm::sim
