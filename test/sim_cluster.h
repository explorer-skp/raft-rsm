#pragma once

// Deterministic in-memory cluster simulation shared by the Raft integration
// tests: N RaftCores share one ManualClock and exchange messages through a
// simulated network with controllable per-message delay, per-node isolation
// or death, and unidirectional drops. Election Safety (at most one leader
// per term) is asserted over the entire history of every run via the
// transition observer.

#include <algorithm>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "doctest/doctest.h"
#include "raft/raft_core.h"
#include "statemachine/state_machine.h"
#include "storage/log.h"

namespace simtest {

using rsm::raft::Duration;
using rsm::raft::InMemoryPersistentState;
using rsm::raft::ManualClock;
using rsm::raft::RaftConfig;
using rsm::raft::RaftCore;
using rsm::raft::Role;
using rsm::raft::TimePoint;
using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::LogIndex;
using rsm::rpc::Message;
using rsm::rpc::NodeId;
using rsm::rpc::Term;

struct NodeSetup {
    std::uint64_t seed = 0;
    RaftConfig cfg;
};

// Per-node storage created by a StorageFactory: in-memory by default;
// Phase 4 tests pass a factory producing durable implementations over a
// per-node directory, so restart() re-opens real files exactly like a
// restarted process would.
struct StoragePair {
    std::unique_ptr<rsm::raft::PersistentState> persist;
    std::unique_ptr<rsm::storage::RaftLog> log;
};
using StorageFactory = std::function<StoragePair(NodeId)>;

inline StoragePair inMemoryStorage(NodeId) {
    return {std::make_unique<InMemoryPersistentState>(),
            std::make_unique<rsm::storage::InMemoryLog>()};
}

class SimCluster {
public:
    explicit SimCluster(const std::vector<NodeSetup>& setups,
                        std::uint64_t deliveryDelaySeed = 0,
                        Duration maxDeliveryDelay = Duration(0),
                        StorageFactory storage = inMemoryStorage)
        : delayRng_(deliveryDelaySeed),
          maxDelay_(maxDeliveryDelay),
          setups_(setups),
          storage_(std::move(storage)) {
        const auto n = static_cast<NodeId>(setups.size());
        for (NodeId id = 1; id <= n; ++id) {
            nodes_.push_back(std::make_unique<Node>());
            initNode(id, setups_[id - 1].seed);
        }
        for (auto& node : nodes_) node->core->start();
    }

    RaftCore& core(NodeId id) { return *nodes_[id - 1]->core; }
    rsm::storage::RaftLog& log(NodeId id) { return *nodes_[id - 1]->log; }
    const rsm::statemachine::RecordingStateMachine& sm(NodeId id) const {
        return *nodes_[id - 1]->sm;
    }

    void kill(NodeId id) { nodes_[id - 1]->alive = false; }

    // Crash recovery (Phase 4): rebuilds the node the way a restarted
    // process comes up — storage re-created through the factory (a durable
    // factory re-opens and replays the on-disk files), state machine empty,
    // a brand-new RaftCore (role/commitIndex/lastApplied reset by
    // construction). The node must currently be dead. Messages it had on
    // the wire from before the crash may still arrive afterwards; that is
    // realistic (delayed packets) and Raft must tolerate it.
    void restart(NodeId id, std::uint64_t seed) {
        auto& node = *nodes_[id - 1];
        REQUIRE_MESSAGE(!node.alive, "restart() requires a killed node");
        initNode(id, seed);
        node.alive = true;
        node.core->start();
    }
    void isolate(NodeId id, bool on) { nodes_[id - 1]->isolated = on; }
    // Unidirectional: drop only messages addressed TO this node (the node's
    // own sends still go out). For asymmetric failure scenarios.
    void dropTo(NodeId id, bool on) { nodes_[id - 1]->dropInbound = on; }

    // Applies to messages posted from now on; in-flight ones keep their
    // original delivery time.
    void setMaxDeliveryDelay(Duration d) { maxDelay_ = d; }

    // AppendEntries RPCs (not replies) delivered to `id` since the last
    // resetAppendEntriesCount() — for O(terms) backtracking assertions.
    std::size_t appendEntriesDeliveredTo(NodeId id) const {
        return nodes_[id - 1]->aeDelivered;
    }
    void resetAppendEntriesCount() {
        for (auto& node : nodes_) node->aeDelivered = 0;
    }

    // One simulated millisecond: advance time, fire due timers on live
    // nodes, then deliver every due message (cascading same-step replies).
    void stepMs() {
        clock.advance(Duration(1));
        for (auto& node : nodes_) {
            if (node->alive) node->core->tick();
        }
        deliverDue();
        checkElectionSafety();
    }

    // Runs until pred() holds, at most maxMs simulated ms. True if it held.
    template <typename Pred>
    bool runUntil(Duration maxMs, Pred pred) {
        for (std::int64_t i = 0; i < maxMs.count(); ++i) {
            if (pred()) return true;
            stepMs();
        }
        return pred();
    }

    std::vector<NodeId> liveLeaders() const {
        std::vector<NodeId> out;
        for (const auto& node : nodes_) {
            if (node->alive && node->core->role() == Role::Leader) {
                out.push_back(node->core->selfId());
            }
        }
        return out;
    }

    // Exactly one live leader, every other live node a follower that agrees
    // on the leader's term and identity.
    bool converged() const {
        const auto leaders = liveLeaders();
        if (leaders.size() != 1) return false;
        const RaftCore& leader = *nodes_[leaders[0] - 1]->core;
        for (const auto& node : nodes_) {
            if (!node->alive || node->core->selfId() == leaders[0]) continue;
            if (node->core->role() != Role::Follower) return false;
            if (node->core->term() != leader.term()) return false;
            if (node->core->leaderId() != leader.selfId()) return false;
        }
        return true;
    }

    // converged() plus: identical logs and identical applied sequences with
    // lastApplied == commitIndex == the leader's, on every live node.
    bool replicated(LogIndex expectCommit) const {
        if (!converged()) return false;
        const Node* leader = nullptr;
        for (const auto& node : nodes_) {
            if (node->alive && node->core->role() == Role::Leader) {
                leader = node.get();
            }
        }
        if (leader->core->commitIndex() < expectCommit) return false;
        for (const auto& node : nodes_) {
            if (!node->alive) continue;
            if (node->core->commitIndex() != leader->core->commitIndex()) {
                return false;
            }
            if (node->core->lastApplied() != node->core->commitIndex()) {
                return false;
            }
            if (node->log->lastIndex() != leader->log->lastIndex()) {
                return false;
            }
            if (node->log->entriesFrom(1) != leader->log->entriesFrom(1)) {
                return false;
            }
            if (node->sm->applied() != leader->sm->applied()) return false;
        }
        return true;
    }

    void checkElectionSafety() const {
        for (const auto& [term, leaders] : leadersByTerm) {
            if (leaders.size() > 1) {
                CAPTURE(term);
                REQUIRE_MESSAGE(leaders.size() <= 1,
                                "ELECTION SAFETY VIOLATED: two leaders "
                                "elected in one term");
            }
        }
    }

    // Log Matching (invariant 3): for every pair of live logs and every
    // index where both hold an entry of the same term, the entries (and all
    // entries below, by induction over the prefix) must be identical.
    void checkLogMatching() const {
        for (std::size_t a = 0; a < nodes_.size(); ++a) {
            for (std::size_t b = a + 1; b < nodes_.size(); ++b) {
                const auto& la = *nodes_[a]->log;
                const auto& lb = *nodes_[b]->log;
                const LogIndex top = std::min(la.lastIndex(), lb.lastIndex());
                for (LogIndex i = top; i >= 1; --i) {
                    if (la.termAt(i) != lb.termAt(i)) continue;
                    // Same index+term: full prefix must match.
                    for (LogIndex j = i; j >= 1; --j) {
                        REQUIRE(la.entryAt(j) == lb.entryAt(j));
                    }
                    break;
                }
            }
        }
    }

    // State Machine Safety (invariant 5): no two nodes applied different
    // commands at the same sequence position.
    void checkStateMachineSafety() const {
        for (std::size_t a = 0; a < nodes_.size(); ++a) {
            for (std::size_t b = a + 1; b < nodes_.size(); ++b) {
                const auto& sa = nodes_[a]->sm->applied();
                const auto& sb = nodes_[b]->sm->applied();
                const std::size_t n = std::min(sa.size(), sb.size());
                for (std::size_t i = 0; i < n; ++i) {
                    REQUIRE(sa[i] == sb[i]);
                }
            }
        }
    }

    ManualClock clock;
    // Every (term -> nodes that ever won it / started an election in it),
    // recorded over the whole run.
    std::map<Term, std::set<NodeId>> leadersByTerm;
    std::map<Term, std::set<NodeId>> electionsStarted;
    // Optional tap observing every message a node posts (before drop/delay
    // filtering of the target). Used to assert on RPC replies directly.
    std::function<void(NodeId from, NodeId to, const Message&)> onPost;

private:
    struct Node {
        std::unique_ptr<rsm::raft::PersistentState> persist;
        std::unique_ptr<rsm::storage::RaftLog> log;
        std::unique_ptr<rsm::statemachine::RecordingStateMachine> sm;
        std::unique_ptr<RaftCore> core;
        bool alive = true;
        bool isolated = false;
        bool dropInbound = false;
        std::size_t aeDelivered = 0;
    };

    // (Re)builds node `id`'s storage, state machine, and core. The old core
    // is destroyed before its storage so nothing dangles; durable storage
    // re-opened by the factory replays whatever survived on disk.
    void initNode(NodeId id, std::uint64_t seed) {
        auto& node = *nodes_[id - 1];
        const auto n = static_cast<NodeId>(setups_.size());
        std::vector<NodeId> peers;
        for (NodeId p = 1; p <= n; ++p) {
            if (p != id) peers.push_back(p);
        }
        node.core.reset();
        node.persist.reset();
        node.log.reset();
        auto storage = storage_(id);
        node.persist = std::move(storage.persist);
        node.log = std::move(storage.log);
        node.sm = std::make_unique<rsm::statemachine::RecordingStateMachine>();
        node.core = std::make_unique<RaftCore>(
            id, peers, *node.persist, *node.log, *node.sm, clock, seed,
            setups_[id - 1].cfg,
            [this, id](NodeId to, const Message& m) { post(id, to, m); });
        node.core->setTransitionObserver(
            [this, id](Term term, Role role, const char* event) {
                if (role == Role::Leader) leadersByTerm[term].insert(id);
                if (std::string_view(event) == "election-started") {
                    electionsStarted[term].insert(id);
                }
            });
    }

    struct InFlight {
        NodeId from;
        NodeId to;
        TimePoint deliverAt;
        Message msg;
    };

    bool cut(NodeId id) const {
        const auto& n = *nodes_[id - 1];
        return !n.alive || n.isolated;
    }
    bool inboundDropped(NodeId id) const {
        return nodes_[id - 1]->dropInbound;
    }

    void post(NodeId from, NodeId to, const Message& m) {
        if (onPost) onPost(from, to, m);
        if (cut(from) || cut(to) || inboundDropped(to)) return;
        Duration delay(0);
        if (maxDelay_.count() > 0) {
            std::uniform_int_distribution<std::int64_t> d(0,
                                                          maxDelay_.count());
            delay = Duration(d(delayRng_));
        }
        wire_.push_back(InFlight{from, to, clock.now() + delay, m});
    }

    void deliverDue() {
        // Handlers may post zero-delay replies; keep sweeping until no due
        // message remains so a same-instant RPC conversation completes
        // within the step. Messages stay FIFO per sweep.
        bool delivered = true;
        while (delivered) {
            delivered = false;
            std::deque<InFlight> pending;
            std::swap(pending, wire_);
            while (!pending.empty()) {
                InFlight f = std::move(pending.front());
                pending.pop_front();
                if (f.deliverAt > clock.now()) {
                    wire_.push_back(std::move(f));
                    continue;
                }
                if (cut(f.from) || cut(f.to) || inboundDropped(f.to)) {
                    continue;  // dropped in flight
                }
                delivered = true;
                if (std::holds_alternative<rsm::rpc::AppendEntries>(f.msg)) {
                    ++nodes_[f.to - 1]->aeDelivered;
                }
                nodes_[f.to - 1]->core->handle(
                    Envelope{kProtocolVersion, rsm::rpc::typeOf(f.msg),
                             f.from, f.to, 0},
                    f.msg);
            }
        }
    }

    std::vector<std::unique_ptr<Node>> nodes_;
    std::deque<InFlight> wire_;
    std::mt19937_64 delayRng_;
    Duration maxDelay_;
    std::vector<NodeSetup> setups_;
    StorageFactory storage_;
};

inline std::vector<NodeSetup> defaultSetups(std::uint64_t s1, std::uint64_t s2,
                                            std::uint64_t s3) {
    return {NodeSetup{s1, {}}, NodeSetup{s2, {}}, NodeSetup{s3, {}}};
}

}  // namespace simtest
