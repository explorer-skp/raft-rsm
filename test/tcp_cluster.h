#pragma once

// Shared real-TCP in-process cluster harness (extracted verbatim from the
// Phase 2-4 raft_cluster_test.cpp so the Phase 5 client tests can reuse it):
// N nodes, each with its own TCP transport on an ephemeral loopback port,
// the Phase 7 threaded NodeRuntime (rx -> Raft -> {tx, apply} over
// lock-free rings), and steady clock — the full production thread topology,
// which makes every binary using this header a TSan workload for the rings
// and the thread split.
//
// The test thread never touches a RaftCore directly (the core is
// single-threaded by contract). Observations flow through the transition
// observer into a mutex-guarded history, or through client replies.
//
// Modes:
//  - baseDir empty → in-memory storage; otherwise durable storage under
//    baseDir/node<id>, enabling restartNode() (Phase 4).
//  - kv=false → LockedRecordingSM, observable via applied()/awaitApplied().
//  - kv=true  → KVStateMachine + a ClientService per node wired to the
//    client seams, serving real ClientRequests over TCP (Phase 5).

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "client/client_service.h"
#include "doctest/doctest.h"
#include "raft/clock.h"
#include "raft/persistent_state.h"
#include "raft/raft_core.h"
#include "runtime/node_runtime.h"
#include "statemachine/kv_store.h"
#include "statemachine/state_machine.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"
#include "storage/log.h"
#include "transport/transport.h"

namespace tcptest {

using rsm::raft::InMemoryPersistentState;
using rsm::raft::RaftConfig;
using rsm::raft::RaftCore;
using rsm::raft::Role;
using rsm::runtime::NodeRuntime;
using rsm::raft::SteadyClock;
using rsm::rpc::Message;
using rsm::rpc::NodeId;
using rsm::rpc::Term;
using rsm::transport::PeerAddress;
using rsm::transport::Transport;

struct Observation {
    Term term = 0;
    Role role = Role::Follower;
};

// Thread-safe record of every node's transitions across the whole run.
class TransitionLog {
public:
    void record(NodeId node, Term term, Role role) {
        std::lock_guard lock(mu_);
        latest_[node] = Observation{term, role};
        if (role == Role::Leader) leadersByTerm_[term].insert(node);
    }

    std::map<NodeId, Observation> snapshot() const {
        std::lock_guard lock(mu_);
        return latest_;
    }

    // Election Safety over the entire history: no term was ever won twice.
    void requireElectionSafety() const {
        std::lock_guard lock(mu_);
        for (const auto& [term, nodes] : leadersByTerm_) {
            CAPTURE(term);
            REQUIRE(nodes.size() <= 1);
        }
    }

private:
    mutable std::mutex mu_;
    std::map<NodeId, Observation> latest_;
    std::map<Term, std::set<NodeId>> leadersByTerm_;
};

// Wraps a state machine with a fixed per-apply delay: the instrument for
// the apply-backpressure test (a slow SM must never stall Raft timers).
class DelayedSM final : public rsm::statemachine::StateMachine {
public:
    DelayedSM(std::unique_ptr<rsm::statemachine::StateMachine> inner,
              std::chrono::microseconds delay)
        : inner_(std::move(inner)), delay_(delay) {}
    std::string apply(const rsm::statemachine::Command& cmd) override {
        std::this_thread::sleep_for(delay_);
        return inner_->apply(cmd);
    }

private:
    std::unique_ptr<rsm::statemachine::StateMachine> inner_;
    std::chrono::microseconds delay_;
};

// The apply path runs on each node's loop thread while the test thread
// asserts on the applied sequence, so recording is mutex-guarded.
class LockedRecordingSM final : public rsm::statemachine::StateMachine {
public:
    std::string apply(const rsm::statemachine::Command& cmd) override {
        std::lock_guard lock(mu_);
        applied_.push_back(cmd);
        return {};
    }
    std::vector<rsm::statemachine::Command> applied() const {
        std::lock_guard lock(mu_);
        return applied_;
    }

private:
    mutable std::mutex mu_;
    std::vector<rsm::statemachine::Command> applied_;
};

struct ClusterNode {
    std::unique_ptr<rsm::raft::PersistentState> persist;
    std::unique_ptr<rsm::storage::RaftLog> log;
    std::unique_ptr<rsm::statemachine::StateMachine> sm;
    LockedRecordingSM* rec = nullptr;  // non-null iff sm records (kv=false)
    std::unique_ptr<rsm::client::ClientService> service;  // kv mode only
    SteadyClock clock;
    std::unique_ptr<Transport> transport;
    std::unique_ptr<RaftCore> core;
    std::unique_ptr<NodeRuntime> loop;  // declared after core: stops first
    std::string dataDir;     // empty: in-memory storage (Phases 2/3 tests)
    std::uint16_t port = 0;  // fixed after first bind so restarts reuse it
    bool stopped = false;

    void stop() {
        if (stopped) return;
        stopped = true;
        transport->stop();
        loop->stop();
    }
};

class Cluster {
public:
    // baseDir empty: in-memory storage. Otherwise each node persists to
    // baseDir/node<id> with the Phase 4 durable implementations.
    // `batching` (kv mode only) enables Phase 7 group commit on every node.
    // `runtimeCfg` tunes the NodeRuntime (ring sizes, wait mode);
    // `applyDelay` wraps each state machine in a per-apply sleep — the
    // apply-backpressure instrument.
    explicit Cluster(int n, const std::string& baseDir = {}, bool kv = false,
                     rsm::client::ClientService::Batching batching = {},
                     rsm::runtime::NodeRuntimeConfig runtimeCfg = {},
                     std::chrono::microseconds applyDelay = {})
        : kv_(kv), batching_(batching), runtimeCfg_(runtimeCfg),
          applyDelay_(applyDelay) {
        // Bind all transports first so every peer's ephemeral port is known
        // before any core starts campaigning.
        for (NodeId id = 1; id <= n; ++id) {
            auto node = std::make_unique<ClusterNode>();
            if (!baseDir.empty()) {
                node->dataDir = baseDir + "/node" + std::to_string(id);
                std::filesystem::create_directories(node->dataDir);
            }
            node->transport = std::make_unique<Transport>(id, /*port=*/0);
            node->port = node->transport->listenPort();
            nodes_.push_back(std::move(node));
        }
        for (NodeId id = 1; id <= n; ++id) {
            buildNode(id, /*rngSeed=*/1000 + id);
        }
        for (auto& node : nodes_) {
            node->loop->start();
            node->transport->start();
        }
    }

    ~Cluster() {
        for (auto& node : nodes_) node->stop();
    }

    void stopNode(NodeId id) { nodes_[id - 1]->stop(); }

    // Crash recovery (Phase 4): brings a stopped node back the way a
    // restarted process comes up — same listen port, persistent state
    // re-read from its data dir, volatile state (role, commitIndex,
    // state machine) reset. Requires durable storage.
    void restartNode(NodeId id) {
        auto& node = *nodes_[id - 1];
        REQUIRE(node.stopped);
        REQUIRE_FALSE(node.dataDir.empty());
        node.loop.reset();  // joins the loop thread; core no longer driven
        node.service.reset();  // holds a core reference; drop it first
        node.core.reset();
        node.transport.reset();  // closes sockets; port becomes free
        node.transport = std::make_unique<Transport>(id, node.port);
        buildNode(id, /*rngSeed=*/9000 + restarts_++);
        node.stopped = false;
        node.loop->start();
        node.transport->start();
    }

    // The peer-config view a client needs: node id -> address.
    rsm::transport::PeerMap peerMap() const {
        rsm::transport::PeerMap servers;
        for (NodeId id = 1; id <= nodes_.size(); ++id) {
            servers[id] = PeerAddress{"127.0.0.1", nodes_[id - 1]->port};
        }
        return servers;
    }

    // Exactly one live node's latest transition is Leader and every other
    // live node's is Follower at the same term. Returns the leader if so.
    std::optional<NodeId> stableLeader() const {
        const auto snap = log.snapshot();
        std::optional<NodeId> leader;
        Term leaderTerm = 0;
        for (const auto& [id, obs] : snap) {
            if (!alive(id)) continue;
            if (obs.role == Role::Leader) {
                if (leader) return std::nullopt;  // two self-styled leaders
                leader = id;
                leaderTerm = obs.term;
            }
        }
        if (!leader) return std::nullopt;
        for (const auto& [id, obs] : snap) {
            if (!alive(id) || id == *leader) continue;
            if (obs.role != Role::Follower || obs.term != leaderTerm) {
                return std::nullopt;
            }
        }
        // All live nodes must have reported in.
        if (snap.size() < liveCount()) return std::nullopt;
        return leader;
    }

    std::optional<NodeId> awaitStableLeader(std::chrono::seconds limit) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < deadline) {
            if (const auto leader = stableLeader()) return leader;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return stableLeader();
    }

    Term observedTerm(NodeId id) const {
        const auto snap = log.snapshot();
        return snap.at(id).term;
    }

    // Proposes on `id`'s loop and waits for the loop thread's answer.
    std::optional<rsm::rpc::LogIndex> propose(
        NodeId id, std::vector<std::uint8_t> command) {
        std::mutex mu;
        std::condition_variable cv;
        bool answered = false;
        std::optional<rsm::rpc::LogIndex> result;
        nodes_[id - 1]->loop->propose(
            std::move(command),
            [&](std::optional<rsm::rpc::LogIndex> idx) {
                std::lock_guard lk(mu);
                result = idx;
                answered = true;
                cv.notify_one();
            });
        std::unique_lock lk(mu);
        cv.wait_for(lk, std::chrono::seconds(5), [&] { return answered; });
        return result;
    }

    std::vector<rsm::statemachine::Command> applied(NodeId id) const {
        REQUIRE(nodes_[id - 1]->rec != nullptr);
        return nodes_[id - 1]->rec->applied();
    }

    // Waits until every live node's applied sequence equals `expect`.
    // Recording mode only.
    bool awaitApplied(const std::vector<rsm::statemachine::Command>& expect,
                      std::chrono::seconds limit) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < deadline) {
            bool all = true;
            for (const auto& node : nodes_) {
                REQUIRE(node->rec != nullptr);
                if (!node->stopped && node->rec->applied() != expect) {
                    all = false;
                    break;
                }
            }
            if (all) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    TransitionLog log;

private:
    // (Re)creates node `id`'s storage, state machine, core, loop, client
    // service, and handler wiring. Durable storage re-opens and replays
    // whatever is on disk; in-memory storage starts blank.
    void buildNode(NodeId id, std::uint64_t rngSeed) {
        auto& node = *nodes_[id - 1];
        const auto n = static_cast<NodeId>(nodes_.size());
        std::vector<NodeId> peers;
        for (NodeId p = 1; p <= n; ++p) {
            if (p == id) continue;
            peers.push_back(p);
            node.transport->addPeer(
                p, PeerAddress{"127.0.0.1", nodes_[p - 1]->port});
        }
        node.service.reset();  // before the core it references
        node.core.reset();     // before the storage it references
        if (node.dataDir.empty()) {
            node.persist = std::make_unique<InMemoryPersistentState>();
            node.log = std::make_unique<rsm::storage::InMemoryLog>();
        } else {
            node.persist = std::make_unique<
                rsm::storage::DurablePersistentState>(node.dataDir);
            node.log = std::make_unique<rsm::storage::DurableLog>(node.dataDir);
        }
        if (kv_) {
            node.sm = std::make_unique<rsm::statemachine::KVStateMachine>();
            if (applyDelay_.count() > 0) {
                node.sm = std::make_unique<DelayedSM>(std::move(node.sm),
                                                      applyDelay_);
            }
            node.rec = nullptr;
        } else {
            auto rec = std::make_unique<LockedRecordingSM>();
            node.rec = rec.get();
            node.sm = std::move(rec);
        }
        // Sends from the Raft/apply threads encode locally and hand the
        // frame to the tx thread; node.loop is created below, after the
        // core that needs this hook (no sends can happen before start()).
        const auto pipelineSend = [rt = &node.loop,
                                   t = node.transport.get()](
                                      NodeId to, const Message& m) {
            if (*rt) (*rt)->sendFromPipeline(to, m);
            else t->send(to, m);  // unreachable peers: drop, Raft retries
        };
        node.core = std::make_unique<RaftCore>(
            id, peers, *node.persist, *node.log, *node.sm, node.clock,
            rngSeed, RaftConfig{}, pipelineSend);
        node.core->setTransitionObserver(
            [this, id](Term term, Role role, const char*) {
                log.record(id, term, role);
            });
        NodeRuntime::ApplyFn onApplied;
        if (kv_) {
            node.service = std::make_unique<rsm::client::ClientService>(
                *node.core, pipelineSend);
            if (batching_.maxBatch > 1) node.service->setBatching(batching_);
            node.core->setClientRequestHandler(
                [svc = node.service.get()](NodeId from,
                                           const rsm::rpc::ClientRequest& r) {
                    svc->onClientRequest(from, r);
                });
            onApplied = [svc = node.service.get()](
                            rsm::rpc::LogIndex index,
                            const rsm::rpc::LogEntry& entry,
                            const std::string& result) {
                svc->onApplied(index, entry, result);
            };
        }
        node.loop = std::make_unique<NodeRuntime>(
            *node.core, *node.transport, *node.sm, std::move(onApplied),
            runtimeCfg_);
        if (kv_) {
            // Linger-batch driver (no-op while batching is off).
            node.loop->setServiceHook(
                [svc = node.service.get()](rsm::raft::TimePoint now) {
                    return svc->flushIfDue(now);
                });
        }
        node.transport->setRawHandler(
            [l = node.loop.get()](std::span<const std::uint8_t> body) {
                return l->enqueueFrame(body);
            });
    }

    bool alive(NodeId id) const { return !nodes_[id - 1]->stopped; }
    std::size_t liveCount() const {
        std::size_t n = 0;
        for (const auto& node : nodes_) n += node->stopped ? 0 : 1;
        return n;
    }

    const bool kv_;
    const rsm::client::ClientService::Batching batching_;
    const rsm::runtime::NodeRuntimeConfig runtimeCfg_;
    const std::chrono::microseconds applyDelay_;
    std::vector<std::unique_ptr<ClusterNode>> nodes_;
    std::uint64_t restarts_ = 0;
};

}  // namespace tcptest
