// Phase 6 fault-layer units: every fault control is exercised in isolation
// and shown to be seed-deterministic, crash/restart keeps persisted state
// and loses volatile state through the REAL durable storage, and the
// flagship property — the same seed produces a byte-identical chaos run —
// is asserted on full run traces.

#include <string>
#include <variant>
#include <vector>

#include "doctest/doctest.h"
#include "faults/chaos.h"
#include "faults/sim_harness.h"
#include "raft/logging.h"
#include "statemachine/kv_store.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"
#include "temp_dir.h"

using namespace rsm::sim;
using rsm::raft::Duration;
using rsm::rpc::ClientRequest;
using rsm::rpc::Message;

namespace {

Message marker(std::uint64_t n) {
    return Message{ClientRequest{n, n, {}}};
}

std::uint64_t markerOf(const Message& m) {
    return std::get<ClientRequest>(m).clientId;
}

struct SendRecord {
    NodeId from = 0;
    NodeId to = 0;
    bool dropped = false;
    std::int64_t delayMs = 0;
};

std::vector<SendRecord> blast(std::uint64_t seed, double dropP, int count) {
    SimNetwork net(seed, {1, 2, 3});
    net.setDrop(dropP);
    std::vector<SendRecord> records;
    net.onSend = [&](NodeId from, NodeId to, const Message&, bool dropped,
                     Duration delay) {
        records.push_back({from, to, dropped, delay.count()});
    };
    for (int i = 0; i < count; ++i) {
        net.send(1, 2, marker(static_cast<std::uint64_t>(i)), TimePoint{});
    }
    return records;
}

}  // namespace

TEST_CASE("fault layer: drop decisions are a pure function of the seed, at "
          "roughly the configured fraction") {
    const auto a = blast(99, 0.3, 1000);
    const auto b = blast(99, 0.3, 1000);
    REQUIRE(a.size() == b.size());
    int drops = 0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        CHECK(a[i].dropped == b[i].dropped);  // identical pattern
        CHECK(a[i].delayMs == b[i].delayMs);
        drops += a[i].dropped ? 1 : 0;
    }
    CHECK(drops > 250);  // ~0.3 of 1000
    CHECK(drops < 350);
    // A different seed gives a different pattern (sanity).
    const auto c = blast(100, 0.3, 1000);
    bool anyDifferent = false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        anyDifferent = anyDifferent || (a[i].dropped != c[i].dropped);
    }
    CHECK(anyDifferent);
}

TEST_CASE("fault layer: partition isolates exactly the configured groups, "
          "spares client traffic, kills in-flight cross-group messages, and "
          "heals") {
    SimNetwork net(7, {1, 2, 3});
    net.setLatency(Duration(1), Duration(1));
    std::vector<std::pair<NodeId, NodeId>> delivered;
    const auto sink = [&](NodeId from, NodeId to, Message&&) {
        delivered.push_back({from, to});
    };
    const TimePoint t0{};

    // An in-flight cross-group message is killed by a partition raised
    // after the send but before delivery.
    net.send(1, 2, marker(0), t0);
    net.partition({{1}, {2, 3}});
    net.deliverDue(t0 + Duration(5), sink);
    CHECK(delivered.empty());

    // Within the partition: only intra-group node traffic flows; the
    // client id (1000) reaches everyone.
    const std::vector<std::pair<NodeId, NodeId>> attempts = {
        {1, 2}, {1, 3}, {2, 1}, {3, 1}, {2, 3}, {3, 2}, {1000, 1}, {1, 1000}};
    for (const auto& [from, to] : attempts) {
        net.send(from, to, marker(0), t0 + Duration(5));
    }
    delivered.clear();
    net.deliverDue(t0 + Duration(10), sink);
    const std::vector<std::pair<NodeId, NodeId>> expectPartitioned = {
        {2, 3}, {3, 2}, {1000, 1}, {1, 1000}};
    CHECK(delivered == expectPartitioned);

    // Healed: everything flows again.
    net.heal();
    for (const auto& [from, to] : attempts) {
        net.send(from, to, marker(0), t0 + Duration(10));
    }
    delivered.clear();
    net.deliverDue(t0 + Duration(20), sink);
    CHECK(delivered.size() == attempts.size());
}

TEST_CASE("fault layer: latency jitter reorders messages, deterministically "
          "for a seed") {
    const auto run = [](std::uint64_t seed) {
        SimNetwork net(seed, {1, 2});
        net.setLatency(Duration(1), Duration(20));
        std::vector<std::uint64_t> order;
        for (std::uint64_t i = 0; i < 50; ++i) {
            net.send(1, 2, marker(i), TimePoint{});
        }
        net.deliverDue(TimePoint{} + Duration(30),
                       [&](NodeId, NodeId, Message&& m) {
                           order.push_back(markerOf(m));
                       });
        return order;
    };
    const auto a = run(5);
    CHECK(a == run(5));  // seed-deterministic
    REQUIRE(a.size() == 50);
    bool leapfrog = false;  // jitter actually reorders
    for (std::size_t i = 1; i < a.size(); ++i) {
        leapfrog = leapfrog || (a[i] < a[i - 1]);
    }
    CHECK(leapfrog);
}

TEST_CASE("fault layer: explicit reorder knob delays a seeded subset of "
          "messages past their successors") {
    SimNetwork net(11, {1, 2});
    net.setLatency(Duration(1), Duration(1));  // no jitter: isolate reorder
    net.setReorder(0.3, Duration(10));
    std::vector<std::uint64_t> order;
    for (std::uint64_t i = 0; i < 50; ++i) {
        net.send(1, 2, marker(i), TimePoint{});
    }
    net.deliverDue(TimePoint{} + Duration(20),
                   [&](NodeId, NodeId, Message&& m) {
                       order.push_back(markerOf(m));
                   });
    REQUIRE(order.size() == 50);
    bool leapfrog = false;
    for (std::size_t i = 1; i < order.size(); ++i) {
        leapfrog = leapfrog || (order[i] < order[i - 1]);
    }
    CHECK(leapfrog);
}

TEST_CASE("fault layer: crash loses volatile state, keeps durable state, "
          "and the restarted node rejoins — over the real Phase 4 storage") {
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Error);
    testutil::TempDir dir;
    HarnessOptions ho;
    ho.nodes = {SimNodeConfig{21, {}}, SimNodeConfig{22, {}},
                SimNodeConfig{23, {}}};
    ho.netSeed = 5;
    SimHarness h(std::move(ho), [&dir](NodeId id, int) -> StoragePair {
        const auto d = dir.subdir("n" + std::to_string(id));
        return {std::make_unique<rsm::storage::DurablePersistentState>(d),
                std::make_unique<rsm::storage::DurableLog>(d)};
    });
    h.net().setLatency(Duration(1), Duration(2));

    REQUIRE(h.runUntil(2000, [&] { return h.converged(); }));
    const NodeId leader = h.liveLeaders()[0];
    for (int i = 1; i <= 3; ++i) {
        REQUIRE(h.core(leader).propose(rsm::statemachine::encodeKvCommand(
            0, 0, rsm::statemachine::KvOp::Put, "k" + std::to_string(i),
            "v")));
    }
    REQUIRE(h.runUntil(2000, [&] {
        for (NodeId id = 1; id <= 3; ++id) {
            if (h.core(id).commitIndex() != 3) return false;
        }
        return true;
    }));

    // Crash a follower mid-run.
    const NodeId follower = leader == 1 ? 2 : 1;
    const auto termBefore = h.core(follower).term();
    h.crash(follower);
    CHECK_FALSE(h.alive(follower));
    REQUIRE(h.core(leader).propose(rsm::statemachine::encodeKvCommand(
        0, 0, rsm::statemachine::KvOp::Put, "k4", "v")));
    h.runFor(300);

    // Restart: persisted survives (term, full log replayed from disk),
    // volatile is gone (follower role, commitIndex 0 until re-learned).
    h.restart(follower);
    CHECK(h.core(follower).role() == rsm::raft::Role::Follower);
    CHECK(h.core(follower).term() >= termBefore);  // durable, never regresses
    CHECK(h.log(follower).lastIndex() >= 3);       // durable log survived
    CHECK(h.core(follower).commitIndex() == 0);    // volatile lost
    CHECK(h.core(follower).lastApplied() == 0);

    // It catches up to everything it missed.
    REQUIRE(h.runUntil(3000, [&] {
        return h.core(follower).commitIndex() == 4 &&
               h.core(follower).lastApplied() == 4;
    }));

    // Leader kill: a new leader emerges among the survivors, the old one
    // rejoins after restart, and the cluster quiesces with empty checkers.
    REQUIRE(h.killLeader() == leader);
    REQUIRE(h.runUntil(3000, [&] { return h.liveLeaders().size() == 1; }));
    CHECK(h.liveLeaders()[0] != leader);
    h.restart(leader);
    REQUIRE(h.core(h.liveLeaders()[0]).propose(
        rsm::statemachine::encodeKvCommand(
            0, 0, rsm::statemachine::KvOp::Put, "k5", "v")));
    REQUIRE(h.runUntil(3000, [&] { return h.quiescent(); }));
    h.finalCheck();
    CHECK(h.violations().empty());
}

TEST_CASE("determinism: the same seed produces a byte-identical chaos run "
          "(trace, outcome, stats)") {
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Error);
    ChaosOptions opts;
    opts.clients = 3;
    opts.opsPerClient = 6;
    opts.faultEndMs = 8000;
    opts.maxMs = 20000;
    opts.recordTrace = true;

    const auto a = runChaos(4242, opts);
    const auto b = runChaos(4242, opts);
    CHECK(a.pass);
    CHECK(a.summary() == b.summary());
    REQUIRE(a.trace.size() == b.trace.size());
    for (std::size_t i = 0; i < a.trace.size(); ++i) {
        REQUIRE_MESSAGE(a.trace[i] == b.trace[i],
                        "trace diverged at line " << i);
    }
    CHECK(a.trace.size() > 500);  // a real run, not a trivially empty one

    // And a different seed produces a different run (sanity).
    const auto c = runChaos(4243, opts);
    CHECK(a.trace != c.trace);
}
