// Phase 9: exactly-once has real teeth in a matching engine. A client that
// retries a NEW order across a leader failover must create EXACTLY one
// order — a re-applied NEW would match again (double fill / double order),
// a serious correctness failure. Same deterministic-sim pattern as the
// Phase 5 KV exactly-once test; only the state machine differs, which is
// the point: the dedup layer lives in replicated applied state behind the
// same interface, so it protects any SM plugged in behind it.

#include <cstdint>
#include <memory>
#include <string>
#include <variant>

#include "client/client_service.h"
#include "doctest/doctest.h"
#include "sim_cluster.h"
#include "statemachine/order_book.h"

using rsm::client::ClientService;
using rsm::rpc::ClientReply;
using rsm::rpc::ClientRequest;
using rsm::rpc::ClientStatus;
using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::Message;
using rsm::rpc::MessageType;
using rsm::rpc::NodeId;
using rsm::statemachine::decodeObResult;
using rsm::statemachine::encodeObNew;
using rsm::statemachine::kObOk;
using rsm::statemachine::ObSide;
using rsm::statemachine::OrderBookStateMachine;
using simtest::defaultSetups;
using simtest::SimCluster;
using rsm::raft::Duration;

namespace {

constexpr NodeId kClient = 99;

std::unique_ptr<rsm::statemachine::StateMachine> obSm(NodeId) {
    return std::make_unique<OrderBookStateMachine>();
}

struct CapturedReply {
    NodeId fromNode = 0;
    ClientReply reply;
};

struct Services {
    explicit Services(SimCluster& sim, NodeId n) {
        for (NodeId id = 1; id <= n; ++id) {
            services.push_back(std::make_unique<ClientService>(
                sim.core(id), [this, id](NodeId, const Message& m) {
                    if (const auto* r = std::get_if<ClientReply>(&m)) {
                        replies.push_back({id, *r});
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

void injectNew(SimCluster& sim, NodeId node, std::uint64_t clientId,
               std::uint64_t seqNo, ObSide side, std::uint64_t price,
               std::uint64_t qty) {
    sim.core(node).handle(
        Envelope{kProtocolVersion, MessageType::ClientRequest, kClient, node,
                 0},
        Message{ClientRequest{clientId, seqNo,
                              encodeObNew(clientId, seqNo, side, price, qty)}});
}

OrderBookStateMachine& book(SimCluster& sim, NodeId id) {
    return dynamic_cast<OrderBookStateMachine&>(sim.smAny(id));
}

NodeId leaderOf(SimCluster& sim) {
    const auto leaders = sim.liveLeaders();
    REQUIRE(leaders.size() == 1);
    return leaders[0];
}

}  // namespace

TEST_CASE("order sim: exactly-once across leader failover — a retried NEW "
          "creates one order, fills once, returns the cached fills") {
    SimCluster sim(defaultSetups(41, 42, 43), 9, Duration(0),
                   simtest::inMemoryStorage, obSm);
    Services svc(sim, 3);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId leader = leaderOf(sim);

    // Liquidity for the retry to (wrongly) hit twice: a resting ask of 10.
    injectNew(sim, leader, 5, 1, ObSide::Ask, 100, 10);
    REQUIRE(sim.runUntil(Duration(1000), [&] { return svc.replies.size() == 1; }));
    const auto rested = decodeObResult(std::string(
        svc.replies[0].reply.result.begin(), svc.replies[0].reply.result.end()));
    REQUIRE(rested.has_value());
    const std::uint64_t askId = rested->orderId;

    // The order under test: buy 4 @ 100 — fills 4 against the ask. The
    // reply goes out but the client "never sees it" (lost on the wire).
    injectNew(sim, leader, 7, 1, ObSide::Bid, 100, 4);
    REQUIRE(sim.runUntil(Duration(1000), [&] { return svc.replies.size() == 2; }));
    const auto firstRaw = svc.replies[1].reply.result;
    const auto first =
        decodeObResult(std::string(firstRaw.begin(), firstRaw.end()));
    REQUIRE(first.has_value());
    CHECK(first->status == kObOk);
    REQUIRE(first->fills.size() == 1);
    CHECK(first->fills[0].makerOrderId == askId);
    CHECK(first->fills[0].qty == 4);

    // Leader dies before the client learns the outcome.
    sim.kill(leader);
    REQUIRE(sim.runUntil(Duration(3000), [&] {
        const auto l = sim.liveLeaders();
        return l.size() == 1 && l[0] != leader;
    }));
    const NodeId newLeader = leaderOf(sim);

    // Retry: SAME (clientId, seqNo), byte-identical command, new leader.
    injectNew(sim, newLeader, 7, 1, ObSide::Bid, 100, 4);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return svc.replies.size() == 3; }));
    CHECK(svc.replies[2].fromNode == newLeader);
    CHECK(svc.replies[2].reply.status == ClientStatus::Ok);
    CHECK(svc.replies[2].reply.result == firstRaw);  // cached, identical fills

    // The book proves one execution: the ask was filled 4, ONCE — 6 remain
    // on every live replica, and no duplicate buy order exists anywhere.
    REQUIRE(sim.runUntil(Duration(1000), [&] {
        for (NodeId id = 1; id <= 3; ++id) {
            if (id == leader) continue;
            const auto o = book(sim, id).order(askId);
            if (!o || o->qty != 6) return false;
        }
        return true;
    }));
    for (NodeId id = 1; id <= 3; ++id) {
        if (id == leader) continue;
        CHECK(book(sim, id).restingCount() == 1);  // just the remaining ask
    }
    sim.checkLogMatching();
}
