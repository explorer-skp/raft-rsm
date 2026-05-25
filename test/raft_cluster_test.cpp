// Real-cluster integration (Phases 2-4): three in-process nodes, each with
// its own TCP transport (ephemeral loopback ports), Raft event loop, and
// steady clock. Exercises the full thread topology (3 transport I/O threads
// + 3 Raft loop threads + the test thread), so this binary is the TSan gate.
// Phase 4 adds durable storage (pass a base directory to Cluster) and
// restartNode(): a stopped node comes back on the same port with its
// persistent state recovered from disk and its volatile state reset.
//
// The test thread never touches a RaftCore directly (the core is
// single-threaded by contract). All observations flow through the
// transition observer, which runs on each node's loop thread and records
// into a mutex-guarded history.

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

#include "doctest/doctest.h"
#include "raft/clock.h"
#include "raft/event_loop.h"
#include "raft/persistent_state.h"
#include "raft/raft_core.h"
#include "statemachine/state_machine.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"
#include "storage/log.h"
#include "temp_dir.h"
#include "transport/transport.h"

using rsm::raft::InMemoryPersistentState;
using rsm::raft::RaftConfig;
using rsm::raft::RaftCore;
using rsm::raft::RaftEventLoop;
using rsm::raft::Role;
using rsm::raft::SteadyClock;
using rsm::rpc::Message;
using rsm::rpc::NodeId;
using rsm::rpc::Term;
using rsm::transport::PeerAddress;
using rsm::transport::Transport;

namespace {

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
    std::unique_ptr<LockedRecordingSM> sm;
    SteadyClock clock;
    std::unique_ptr<Transport> transport;
    std::unique_ptr<RaftCore> core;
    std::unique_ptr<RaftEventLoop> loop;
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
    explicit Cluster(int n, const std::string& baseDir = {}) {
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
        node.core.reset();
        node.transport.reset();  // closes sockets; port becomes free
        node.transport = std::make_unique<Transport>(id, node.port);
        buildNode(id, /*rngSeed=*/9000 + restarts_++);
        node.stopped = false;
        node.loop->start();
        node.transport->start();
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
        return nodes_[id - 1]->sm->applied();
    }

    // Waits until every live node's applied sequence equals `expect`.
    bool awaitApplied(const std::vector<rsm::statemachine::Command>& expect,
                      std::chrono::seconds limit) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < deadline) {
            bool all = true;
            for (const auto& node : nodes_) {
                if (!node->stopped && node->sm->applied() != expect) {
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
    // (Re)creates node `id`'s storage, core, loop, and handler wiring. The
    // transport must already exist (its port is needed by nobody here, but
    // the send lambda captures it). Durable storage re-opens and replays
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
        node.core.reset();  // before the storage it references
        if (node.dataDir.empty()) {
            node.persist = std::make_unique<InMemoryPersistentState>();
            node.log = std::make_unique<rsm::storage::InMemoryLog>();
        } else {
            node.persist = std::make_unique<
                rsm::storage::DurablePersistentState>(node.dataDir);
            node.log = std::make_unique<rsm::storage::DurableLog>(node.dataDir);
        }
        node.sm = std::make_unique<LockedRecordingSM>();
        node.core = std::make_unique<RaftCore>(
            id, peers, *node.persist, *node.log, *node.sm, node.clock,
            rngSeed, RaftConfig{},
            [t = node.transport.get()](NodeId to, const Message& m) {
                t->send(to, m);  // unreachable peers: drop, Raft retries
            });
        node.core->setTransitionObserver(
            [this, id](Term term, Role role, const char*) {
                log.record(id, term, role);
            });
        node.loop = std::make_unique<RaftEventLoop>(*node.core);
        node.transport->setHandler(
            [l = node.loop.get()](const rsm::rpc::Envelope& env,
                                  rsm::rpc::Message&& m) {
                l->enqueue(env, std::move(m));
            });
    }

    bool alive(NodeId id) const { return !nodes_[id - 1]->stopped; }
    std::size_t liveCount() const {
        std::size_t n = 0;
        for (const auto& node : nodes_) n += node->stopped ? 0 : 1;
        return n;
    }

    std::vector<std::unique_ptr<ClusterNode>> nodes_;
    std::uint64_t restarts_ = 0;
};

}  // namespace

TEST_CASE("real 3-node cluster: cold start elects exactly one leader, then "
          "re-elects after the leader is stopped") {
    Cluster cluster(3);

    // Cold start: one leader, two followers, agreeing terms. Generous bound
    // for sanitizer builds; typically converges in well under a second.
    const auto first = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE_MESSAGE(first.has_value(), "no stable leader after cold start");
    const Term firstTerm = cluster.observedTerm(*first);
    cluster.log.requireElectionSafety();

    // Hold leadership across several heartbeat intervals: still the same
    // single leader in the same term.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const auto held = cluster.stableLeader();
    REQUIRE(held.has_value());
    CHECK(*held == *first);
    CHECK(cluster.observedTerm(*held) == firstTerm);

    // Kill the leader (it stays down: recovery is Phase 4). The two
    // survivors must elect exactly one new leader in a higher term.
    cluster.stopNode(*first);
    const auto second = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE_MESSAGE(second.has_value(), "no re-election after leader stop");
    CHECK(*second != *first);
    CHECK(cluster.observedTerm(*second) > firstTerm);
    cluster.log.requireElectionSafety();
}

TEST_CASE("real 3-node cluster: commands replicate, commit, and apply in "
          "identical order on every node, across a leader failover") {
    Cluster cluster(3);
    const auto leader = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE(leader.has_value());

    // Propose 20 commands on the leader; every node must apply exactly that
    // sequence, in order.
    std::vector<rsm::statemachine::Command> expect;
    for (std::uint8_t i = 1; i <= 20; ++i) {
        const rsm::statemachine::Command cmd{i};
        const auto idx = cluster.propose(*leader, cmd);
        REQUIRE_MESSAGE(idx.has_value(), "stable leader rejected propose");
        CHECK(*idx == rsm::rpc::LogIndex{expect.size() + 1});
        expect.push_back(cmd);
    }
    REQUIRE_MESSAGE(
        cluster.awaitApplied(expect, std::chrono::seconds(10)),
        "not all nodes applied the 20 proposed commands identically");

    // A non-leader must reject proposals with not-leader (nullopt).
    for (NodeId id = 1; id <= 3; ++id) {
        if (id == *leader) continue;
        CHECK_FALSE(cluster.propose(id, {0x7F}).has_value());
        break;
    }

    // Kill the leader; the survivors elect a new one, which must still hold
    // every committed command and extend the same sequence.
    cluster.stopNode(*leader);
    const auto second = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE(second.has_value());
    REQUIRE(*second != *leader);
    for (std::uint8_t i = 21; i <= 30; ++i) {
        const rsm::statemachine::Command cmd{i};
        const auto idx = cluster.propose(*second, cmd);
        REQUIRE(idx.has_value());
        expect.push_back(cmd);
    }
    REQUIRE_MESSAGE(
        cluster.awaitApplied(expect, std::chrono::seconds(10)),
        "survivors lost or reordered committed commands across failover");
    cluster.log.requireElectionSafety();
}

TEST_CASE("real 3-node durable cluster: committed entries survive a leader "
          "crash + restart, recovered from disk and re-applied in order") {
    testutil::TempDir base;
    Cluster cluster(3, base.path());
    const auto first = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE_MESSAGE(first.has_value(), "no stable leader after cold start");

    std::vector<rsm::statemachine::Command> expect;
    for (std::uint8_t i = 1; i <= 20; ++i) {
        const rsm::statemachine::Command cmd{i};
        REQUIRE(cluster.propose(*first, cmd).has_value());
        expect.push_back(cmd);
    }
    REQUIRE(cluster.awaitApplied(expect, std::chrono::seconds(10)));

    // Crash the leader; the survivors elect a new leader and commit more.
    cluster.stopNode(*first);
    const auto second = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE_MESSAGE(second.has_value(), "no re-election after leader crash");
    REQUIRE(*second != *first);
    for (std::uint8_t i = 21; i <= 30; ++i) {
        const rsm::statemachine::Command cmd{i};
        REQUIRE(cluster.propose(*second, cmd).has_value());
        expect.push_back(cmd);
    }
    REQUIRE(cluster.awaitApplied(expect, std::chrono::seconds(10)));

    // Restart the crashed ex-leader: it must recover its log and term from
    // disk, rejoin on its old port, learn the new commits, and re-apply the
    // whole sequence from index 1 — including the 10 entries committed
    // while it was down. awaitApplied checks every live node, so this also
    // re-verifies the survivors.
    cluster.restartNode(*first);
    REQUIRE_MESSAGE(
        cluster.awaitApplied(expect, std::chrono::seconds(15)),
        "restarted ex-leader failed to recover and re-apply the committed "
        "sequence");
    cluster.log.requireElectionSafety();
}
