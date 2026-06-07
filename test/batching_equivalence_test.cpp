// Phase 7 batching/group-commit equivalence (deterministic sim): the same
// fixed workload run unbatched (maxBatch = 1) and batched (maxBatch = 4 +
// linger) must produce IDENTICAL replicated logs on every node and identical
// per-request results — batching may change grouping and timing, never the
// logical outcome. The workload is 10 ops (10 % 4 != 0), so the final
// partial batch can only flush through the linger path, which exercises both
// flush triggers. APPENDs on a shared key make any reordering or double
// apply visible in the values.

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "client/client_service.h"
#include "doctest/doctest.h"
#include "sim_cluster.h"
#include "statemachine/kv_store.h"

using rsm::client::ClientService;
using rsm::raft::Duration;
using rsm::rpc::ClientReply;
using rsm::rpc::ClientRequest;
using rsm::rpc::ClientStatus;
using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::Message;
using rsm::rpc::MessageType;
using rsm::rpc::NodeId;
using rsm::statemachine::encodeKvCommand;
using rsm::statemachine::KvOp;
using rsm::statemachine::KVStateMachine;
using simtest::defaultSetups;
using simtest::SimCluster;

namespace {

constexpr NodeId kClient = 99;

std::unique_ptr<rsm::statemachine::StateMachine> kvSm(NodeId) {
    return std::make_unique<KVStateMachine>();
}

struct Op {
    std::uint64_t clientId;
    std::uint64_t seqNo;
    KvOp op;
    std::string key;
    std::string arg;
};

// The fixed workload, injected in this exact order in a single simulated
// millisecond on both runs.
std::vector<Op> workload() {
    return {
        {7, 1, KvOp::Put, "x", "a"},     {8, 1, KvOp::Append, "x", "b"},
        {7, 2, KvOp::Append, "x", "c"},  {8, 2, KvOp::Put, "y", "1"},
        {7, 3, KvOp::Append, "y", "2"},  {8, 3, KvOp::Append, "x", "d"},
        {7, 4, KvOp::Get, "x", ""},      {8, 4, KvOp::Append, "y", "3"},
        {7, 5, KvOp::Put, "z", "zz"},    {8, 5, KvOp::Append, "z", "w"},
    };
}

struct RunOutcome {
    // Per node: the full replicated log as raw command bytes, in order.
    std::vector<std::vector<std::vector<std::uint8_t>>> logs;
    // (clientId, seqNo) -> result bytes of the OK reply.
    std::map<std::pair<std::uint64_t, std::uint64_t>,
             std::vector<std::uint8_t>>
        results;
};

RunOutcome run(ClientService::Batching batching) {
    SimCluster sim(defaultSetups(21, 22, 23), 9, Duration(0),
                   simtest::inMemoryStorage, kvSm);

    struct Captured {
        std::vector<std::unique_ptr<ClientService>> services;
        std::map<std::pair<std::uint64_t, std::uint64_t>,
                 std::vector<std::uint8_t>>
            results;
        std::size_t okReplies = 0;
    } cap;

    // Each op is injected with its own envelope id (kClient + i), so the
    // reply's `to` field identifies exactly which op it answers — the same
    // per-attempt correlation the real client uses.
    const auto ops = workload();
    for (NodeId id = 1; id <= 3; ++id) {
        cap.services.push_back(std::make_unique<ClientService>(
            sim.core(id), [&cap, &ops](NodeId to, const Message& m) {
                if (const auto* r = std::get_if<ClientReply>(&m)) {
                    if (r->status == ClientStatus::Ok) {
                        const auto& op = ops[to - kClient];
                        cap.results[{op.clientId, op.seqNo}] = r->result;
                        ++cap.okReplies;
                    }
                }
            }));
        ClientService* svc = cap.services.back().get();
        if (batching.maxBatch > 1) svc->setBatching(batching);
        sim.core(id).setClientRequestHandler(
            [svc](NodeId from, const ClientRequest& req) {
                svc->onClientRequest(from, req);
            });
        sim.core(id).setApplyObserver(
            [svc](rsm::rpc::LogIndex index, const rsm::rpc::LogEntry& entry,
                  const std::string& result) {
                svc->onApplied(index, entry, result);
            });
    }

    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const auto leaders = sim.liveLeaders();
    REQUIRE(leaders.size() == 1);
    const NodeId leader = leaders[0];

    // Inject the whole workload within one simulated millisecond — the
    // arrival order at the leader is the workload order in both runs.
    for (std::size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        sim.core(leader).handle(
            Envelope{kProtocolVersion, MessageType::ClientRequest,
                     static_cast<NodeId>(kClient + i), leader, 0},
            Message{ClientRequest{
                op.clientId, op.seqNo,
                encodeKvCommand(op.clientId, op.seqNo, op.op, op.key,
                                op.arg)}});
        // The linger driver runs after every request, like the runtime's
        // per-iteration hook (no-op when batching is off).
        cap.services[leader - 1]->flushIfDue(sim.clock.now());
    }

    // Drive to completion, stepping the linger driver once per simulated
    // millisecond — the deterministic scheduled flush event.
    const bool done = sim.runUntil(Duration(3000), [&] {
        cap.services[leader - 1]->flushIfDue(sim.clock.now());
        return cap.okReplies == ops.size();
    });
    REQUIRE(done);

    // Let commit/apply propagate everywhere, then snapshot the logs.
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        for (NodeId id = 1; id <= 3; ++id) {
            if (sim.log(id).lastIndex() != ops.size()) return false;
            if (sim.core(id).lastApplied() != ops.size()) return false;
        }
        return true;
    }));

    RunOutcome out;
    for (NodeId id = 1; id <= 3; ++id) {
        std::vector<std::vector<std::uint8_t>> log;
        for (const auto& e : sim.log(id).entriesFrom(1)) {
            log.push_back(e.command);
        }
        out.logs.push_back(std::move(log));
    }
    out.results = std::move(cap.results);
    return out;
}

}  // namespace

TEST_CASE("group commit: batched and unbatched runs produce identical "
          "committed logs and identical results") {
    const auto unbatched = run(ClientService::Batching{1, {}});
    const auto batched = run(ClientService::Batching{
        4, std::chrono::microseconds(5000)});

    // Within each run, all replicas hold identical logs.
    for (const auto& outcome : {std::cref(unbatched), std::cref(batched)}) {
        CHECK(outcome.get().logs[0] == outcome.get().logs[1]);
        CHECK(outcome.get().logs[0] == outcome.get().logs[2]);
    }

    // Across runs: same entries, same order — batching changed grouping
    // only. And every request returned the same result.
    CHECK(unbatched.logs[0] == batched.logs[0]);
    REQUIRE(unbatched.results.size() == workload().size());
    CHECK(unbatched.results == batched.results);
}
