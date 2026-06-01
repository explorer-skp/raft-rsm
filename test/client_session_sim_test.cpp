// Phase 5 integration (deterministic sim): ClientService + KVStateMachine
// over a 3-node SimCluster. The "client" is the test itself, injecting
// ClientRequests through RaftCore::handle (the real seam) and capturing
// ClientReplies from each node's ClientService send hook.
//
// The key property exercised here: exactly-once across a leader failover.
// Dedup lives in the replicated APPLIED state, so a retry of the same
// (clientId, seqNo) against the new leader returns the cached result and
// never re-applies the side effect — verified with the non-idempotent
// APPEND op, whose double-apply would be visible in the value.

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "client/client_service.h"
#include "doctest/doctest.h"
#include "sim_cluster.h"
#include "statemachine/kv_store.h"

using rsm::client::ClientService;
using rsm::rpc::ClientReply;
using rsm::rpc::ClientRequest;
using rsm::rpc::ClientStatus;
using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::Message;
using rsm::rpc::MessageType;
using rsm::rpc::NodeId;
using rsm::statemachine::encodeKvCommand;
using rsm::statemachine::kKvOk;
using rsm::statemachine::KvOp;
using rsm::statemachine::KVStateMachine;
using simtest::defaultSetups;
using simtest::SimCluster;
using rsm::raft::Duration;

namespace {

constexpr NodeId kClient = 99;  // the test's client envelope id

std::unique_ptr<rsm::statemachine::StateMachine> kvSm(NodeId) {
    return std::make_unique<KVStateMachine>();
}

struct CapturedReply {
    NodeId fromNode = 0;
    NodeId toClient = 0;
    ClientReply reply;
};

// Attaches one ClientService per node and records every ClientReply sent.
struct Services {
    explicit Services(SimCluster& sim, NodeId n) {
        for (NodeId id = 1; id <= n; ++id) {
            services.push_back(std::make_unique<ClientService>(
                sim.core(id), [this, id](NodeId to, const Message& m) {
                    if (const auto* r = std::get_if<ClientReply>(&m)) {
                        replies.push_back({id, to, *r});
                    }
                }));
            ClientService* svc = services.back().get();
            sim.core(id).setClientRequestHandler(
                [svc](NodeId from, const ClientRequest& req) {
                    svc->onClientRequest(from, req);
                });
            sim.core(id).setApplyObserver(
                [svc](rsm::rpc::LogIndex index,
                      const rsm::rpc::LogEntry& entry,
                      const std::string& result) {
                    svc->onApplied(index, entry, result);
                });
        }
    }

    std::vector<std::unique_ptr<ClientService>> services;
    std::vector<CapturedReply> replies;
};

void inject(SimCluster& sim, NodeId node, std::uint64_t clientId,
            std::uint64_t seqNo, KvOp op, const std::string& key,
            const std::string& arg = {}, const std::string& arg2 = {}) {
    sim.core(node).handle(
        Envelope{kProtocolVersion, MessageType::ClientRequest, kClient, node,
                 0},
        Message{ClientRequest{
            clientId, seqNo,
            encodeKvCommand(clientId, seqNo, op, key, arg, arg2)}});
}

std::string kvValue(SimCluster& sim, NodeId id, const std::string& key) {
    auto& sm = dynamic_cast<KVStateMachine&>(sim.smAny(id));
    return sm.get(key).value_or("<absent>");
}

NodeId leaderOf(SimCluster& sim) {
    const auto leaders = sim.liveLeaders();
    REQUIRE(leaders.size() == 1);
    return leaders[0];
}

}  // namespace

TEST_CASE("client sim: clean-run PUT/GET through the log — replies carry "
          "the applied results, reads see prior writes") {
    SimCluster sim(defaultSetups(1, 2, 3), 5, Duration(0),
                   simtest::inMemoryStorage, kvSm);
    Services svc(sim, 3);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId leader = leaderOf(sim);

    inject(sim, leader, 7, 1, KvOp::Put, "alpha", "1");
    REQUIRE(sim.runUntil(Duration(1000), [&] { return svc.replies.size() == 1; }));
    CHECK(svc.replies[0].reply.status == ClientStatus::Ok);
    CHECK(svc.replies[0].toClient == kClient);

    inject(sim, leader, 7, 2, KvOp::Get, "alpha");
    REQUIRE(sim.runUntil(Duration(1000), [&] { return svc.replies.size() == 2; }));
    const auto& get = svc.replies[1].reply;
    CHECK(get.status == ClientStatus::Ok);
    REQUIRE_FALSE(get.result.empty());
    CHECK(get.result.front() == static_cast<std::uint8_t>(kKvOk));
    CHECK(std::string(get.result.begin() + 1, get.result.end()) == "1");

    // The write reached every replica's KV state identically.
    for (NodeId id = 1; id <= 3; ++id) CHECK(kvValue(sim, id, "alpha") == "1");
}

TEST_CASE("client sim: a follower redirects with the correct leader hint") {
    SimCluster sim(defaultSetups(11, 12, 13), 6, Duration(0),
                   simtest::inMemoryStorage, kvSm);
    Services svc(sim, 3);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId leader = leaderOf(sim);
    const NodeId follower = leader == 1 ? 2 : 1;

    inject(sim, follower, 7, 1, KvOp::Put, "k", "v");
    // The redirect is synchronous (no consensus involved).
    REQUIRE(svc.replies.size() == 1);
    CHECK(svc.replies[0].fromNode == follower);
    CHECK(svc.replies[0].reply.status == ClientStatus::NotLeader);
    CHECK(svc.replies[0].reply.leaderHint == leader);
    CHECK(kvValue(sim, leader, "k") == "<absent>");  // nothing proposed
}

TEST_CASE("client sim: exactly-once across leader failover — retry of the "
          "same (clientId, seqNo) returns the cached result, side effect "
          "happens once") {
    SimCluster sim(defaultSetups(21, 22, 23), 7, Duration(0),
                   simtest::inMemoryStorage, kvSm);
    Services svc(sim, 3);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId leader = leaderOf(sim);

    // Non-idempotent APPEND committed and applied cluster-wide; the reply
    // went out but we pretend the client never saw it (lost on the wire).
    inject(sim, leader, 7, 1, KvOp::Append, "k", "x");
    REQUIRE(sim.runUntil(Duration(1000), [&] { return svc.replies.size() == 1; }));
    const auto firstResult = svc.replies[0].reply.result;
    REQUIRE_FALSE(firstResult.empty());
    CHECK(firstResult.front() == static_cast<std::uint8_t>(kKvOk));

    // The leader dies before the client learns the outcome.
    sim.kill(leader);
    REQUIRE(sim.runUntil(Duration(3000), [&] {
        const auto l = sim.liveLeaders();
        return l.size() == 1 && l[0] != leader;
    }));
    const NodeId newLeader = leaderOf(sim);

    // The client retries the SAME request against the new leader. Apply-time
    // dedup in the replicated state returns the cached result; the value is
    // NOT appended a second time.
    inject(sim, newLeader, 7, 1, KvOp::Append, "k", "x");
    REQUIRE(sim.runUntil(Duration(2000), [&] { return svc.replies.size() == 2; }));
    CHECK(svc.replies[1].fromNode == newLeader);
    CHECK(svc.replies[1].reply.status == ClientStatus::Ok);
    CHECK(svc.replies[1].reply.result == firstResult);  // cached, identical
    // Followers apply one heartbeat after the leader: wait for convergence,
    // then assert exactly one append everywhere.
    REQUIRE(sim.runUntil(Duration(1000), [&] {
        for (NodeId id = 1; id <= 3; ++id) {
            if (id != leader && kvValue(sim, id, "k") != "x") return false;
        }
        return true;
    }));
    sim.checkLogMatching();
}

TEST_CASE("client sim: retry storm — many duplicates, every reply answered "
          "with the same result, exactly one side effect") {
    SimCluster sim(defaultSetups(31, 32, 33), 8, Duration(0),
                   simtest::inMemoryStorage, kvSm);
    Services svc(sim, 3);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId leader = leaderOf(sim);

    // 10 copies of the same logical request land on the leader (an
    // aggressive client retrying into the same node). Each appends a log
    // entry; dedup collapses them all to one effect and one result.
    for (int i = 0; i < 10; ++i) {
        inject(sim, leader, 7, 1, KvOp::Append, "k", "x");
    }
    REQUIRE(sim.runUntil(Duration(2000),
                         [&] { return svc.replies.size() == 10; }));
    for (const auto& r : svc.replies) {
        CHECK(r.reply.status == ClientStatus::Ok);
        CHECK(r.reply.result == svc.replies[0].reply.result);
    }
    const auto allHold = [&](const std::string& v) {
        return [&sim, v] {
            for (NodeId id = 1; id <= 3; ++id) {
                if (kvValue(sim, id, "k") != v) return false;
            }
            return true;
        };
    };
    REQUIRE(sim.runUntil(Duration(1000), allHold("x")));  // exactly one append

    // And a later request from the same client still works normally.
    inject(sim, leader, 7, 2, KvOp::Append, "k", "y");
    REQUIRE(sim.runUntil(Duration(1000),
                         [&] { return svc.replies.size() == 11; }));
    REQUIRE(sim.runUntil(Duration(1000), allHold("xy")));
}

TEST_CASE("client sim: a pending proposal overwritten by another leader's "
          "entry is answered NOT_LEADER, never a wrong OK") {
    SimCluster sim(defaultSetups(41, 42, 43), 9, Duration(0),
                   simtest::inMemoryStorage, kvSm);
    Services svc(sim, 3);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId oldLeader = leaderOf(sim);

    // The leader is cut off, then accepts a request it can never commit.
    sim.isolate(oldLeader, true);
    inject(sim, oldLeader, 7, 1, KvOp::Put, "k", "from-old-leader");
    CHECK(svc.replies.empty());  // pending: no majority, no apply, no reply

    // The survivors elect a new leader, which commits a DIFFERENT entry at
    // the same log index. (The isolated old leader still styles itself
    // Leader — it cannot learn otherwise until it rejoins — so look for a
    // leader among the survivors rather than expecting a unique one.)
    const auto survivorLeader = [&]() -> NodeId {
        for (NodeId id = 1; id <= 3; ++id) {
            if (id != oldLeader &&
                sim.core(id).role() == rsm::raft::Role::Leader) {
                return id;
            }
        }
        return 0;
    };
    REQUIRE(sim.runUntil(Duration(3000),
                         [&] { return survivorLeader() != 0; }));
    const NodeId newLeader = survivorLeader();
    inject(sim, newLeader, 8, 1, KvOp::Put, "k", "from-new-leader");
    REQUIRE(sim.runUntil(Duration(2000),
                         [&] { return svc.replies.size() == 1; }));
    CHECK(svc.replies[0].fromNode == newLeader);
    CHECK(svc.replies[0].reply.status == ClientStatus::Ok);

    // The old leader rejoins: its uncommitted entry is truncated away, the
    // new leader's entry applies at that index, and the identity mismatch
    // turns the stale pending request into NOT_LEADER.
    sim.isolate(oldLeader, false);
    REQUIRE(sim.runUntil(Duration(3000),
                         [&] { return svc.replies.size() == 2; }));
    CHECK(svc.replies[1].fromNode == oldLeader);
    CHECK(svc.replies[1].reply.status == ClientStatus::NotLeader);
    for (NodeId id = 1; id <= 3; ++id) {
        CHECK(kvValue(sim, id, "k") == "from-new-leader");
    }
    sim.checkLogMatching();
}
