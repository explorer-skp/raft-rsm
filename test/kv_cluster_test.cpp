// Phase 5 integration over real TCP: a KvClient (raw sockets, redirects,
// retries) against a 3-node in-process cluster running KVStateMachine +
// ClientService per node. This binary exercises the full client path —
// framing, inbound reply routing, leader redirection, commit-then-reply —
// across real threads, so it is part of the TSan gate.

#include <chrono>
#include <thread>
#include <vector>
#include <string>

#include "client/kv_client.h"
#include "doctest/doctest.h"
#include "statemachine/kv_store.h"
#include "tcp_cluster.h"

using rsm::client::KvClient;
using rsm::statemachine::kKvCasFailed;
using rsm::statemachine::kKvNotFound;
using rsm::statemachine::kKvOk;
using tcptest::Cluster;
using tcptest::NodeId;

namespace {

// Client envelope ids: outside the cluster id range, unique per client.
constexpr NodeId kClientA = 200;
constexpr NodeId kClientB = 201;

}  // namespace

TEST_CASE("kv over TCP: clean-run end-to-end — puts, linearizable-looking "
          "gets, CAS both ways, delete") {
    Cluster cluster(3, /*baseDir=*/{}, /*kv=*/true);
    REQUIRE(cluster.awaitStableLeader(std::chrono::seconds(10)).has_value());
    KvClient client(cluster.peerMap(), /*clientId=*/0xA11CE, kClientA);

    auto r = client.put("alpha", "1");
    REQUIRE(r.has_value());
    CHECK(r->status == kKvOk);

    r = client.get("alpha");  // a read reflects the preceding write
    REQUIRE(r.has_value());
    CHECK(r->status == kKvOk);
    CHECK(r->value == "1");

    r = client.cas("alpha", "1", "10");
    REQUIRE(r.has_value());
    CHECK(r->status == kKvOk);

    r = client.cas("alpha", "1", "999");  // stale expectation must fail
    REQUIRE(r.has_value());
    CHECK(r->status == kKvCasFailed);

    r = client.get("alpha");
    REQUIRE(r.has_value());
    CHECK(r->value == "10");  // the failed CAS had no effect

    r = client.del("alpha");
    REQUIRE(r.has_value());
    CHECK(r->status == kKvOk);

    r = client.get("alpha");
    REQUIRE(r.has_value());
    CHECK(r->status == kKvNotFound);
    cluster.log.requireElectionSafety();
}

TEST_CASE("kv over TCP: a client pointed at a follower is redirected via "
          "the leader hint and succeeds") {
    Cluster cluster(3, /*baseDir=*/{}, /*kv=*/true);
    const auto leader = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE(leader.has_value());
    const NodeId follower = *leader == 1 ? 2 : 1;

    KvClient client(cluster.peerMap(), 0xB0B, kClientA);
    client.setPreferredNode(follower);  // first contact: NOT_LEADER + hint
    const auto r = client.put("k", "v");
    REQUIRE(r.has_value());
    CHECK(r->status == kKvOk);

    client.setPreferredNode(follower);  // and reads redirect the same way
    const auto g = client.get("k");
    REQUIRE(g.has_value());
    CHECK(g->value == "v");
}

TEST_CASE("kv over TCP: exactly-once across leader failover — the retried "
          "request returns the cached result and the non-idempotent append "
          "happens once") {
    Cluster cluster(3, /*baseDir=*/{}, /*kv=*/true);
    const auto leader = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE(leader.has_value());
    KvClient client(cluster.peerMap(), 0xCAFE, kClientA);

    // Committed, applied, acked — but pretend the ack was lost.
    auto r = client.append("k", "x");
    REQUIRE(r.has_value());
    CHECK(r->status == kKvOk);
    CHECK(r->value == "x");

    // The leader dies; the client retries the SAME (clientId, seqNo). It
    // must rotate off the dead node, find the new leader, and receive the
    // CACHED result — not a second append.
    cluster.stopNode(*leader);
    const auto retry = client.resendLast();
    REQUIRE_MESSAGE(retry.has_value(),
                    "retry did not reach the new leader in time");
    CHECK(retry->status == kKvOk);
    CHECK(retry->value == "x");  // cached result, identical to the original

    const auto g = client.get("k");
    REQUIRE(g.has_value());
    CHECK(g->value == "x");  // exactly one append survived the failover

    // The same session keeps working after the failover.
    r = client.append("k", "y");
    REQUIRE(r.has_value());
    CHECK(r->value == "xy");
    cluster.log.requireElectionSafety();
}

TEST_CASE("kv over TCP: retry storm — many resends of one request, one "
          "side effect") {
    Cluster cluster(3, /*baseDir=*/{}, /*kv=*/true);
    REQUIRE(cluster.awaitStableLeader(std::chrono::seconds(10)).has_value());
    KvClient client(cluster.peerMap(), 0xDADA, kClientB);

    auto r = client.append("k", "x");
    REQUIRE(r.has_value());
    CHECK(r->value == "x");
    for (int i = 0; i < 5; ++i) {
        const auto again = client.resendLast();
        REQUIRE(again.has_value());
        CHECK(again->status == kKvOk);
        CHECK(again->value == "x");  // cached every time
    }
    const auto g = client.get("k");
    REQUIRE(g.has_value());
    CHECK(g->value == "x");
}

TEST_CASE("kv over TCP: group commit under sustained concurrent clients — "
          "every op exactly once (threaded-stress TSan workload)") {
    // Batching on (size 8, 200us linger) over the threaded runtime; four
    // concurrent clients hammer non-idempotent APPENDs on their own keys.
    // Every op must be applied exactly once and in the client's issue
    // order, which the accumulated value makes directly visible.
    Cluster cluster(3, /*baseDir=*/{}, /*kv=*/true,
                    rsm::client::ClientService::Batching{
                        8, std::chrono::microseconds(200)});
    REQUIRE(cluster.awaitStableLeader(std::chrono::seconds(10)).has_value());

    constexpr int kClients = 4;
    constexpr int kOps = 50;
    std::vector<std::thread> threads;
    std::vector<int> okCounts(kClients);
    for (int c = 0; c < kClients; ++c) {
        threads.emplace_back([&, c] {
            KvClient client(cluster.peerMap(),
                            /*clientId=*/0xBA7C0 + static_cast<unsigned>(c),
                            static_cast<NodeId>(210 + c));
            const std::string key = "batch-k" + std::to_string(c);
            for (int i = 0; i < kOps; ++i) {
                const auto r = client.append(key, "x");
                if (!r || r->status != kKvOk) break;
                // APPEND returns the whole new value: i+1 x's, in order.
                if (r->value !=
                    std::string(static_cast<std::size_t>(i) + 1, 'x')) {
                    break;
                }
                ++okCounts[static_cast<std::size_t>(c)];
            }
        });
    }
    for (auto& t : threads) t.join();
    for (int c = 0; c < kClients; ++c) {
        CAPTURE(c);
        CHECK(okCounts[static_cast<std::size_t>(c)] == kOps);
    }

    // Final state visible and exact through a fresh client.
    KvClient verifier(cluster.peerMap(), 0xF17A1, kClientA);
    for (int c = 0; c < kClients; ++c) {
        const auto g = verifier.get("batch-k" + std::to_string(c));
        REQUIRE(g.has_value());
        CHECK(g->value == std::string(kOps, 'x'));
    }
    cluster.log.requireElectionSafety();
}

TEST_CASE("apply backpressure: a slow state machine never stalls Raft — "
          "no spurious elections, every op exactly once") {
    // Tiny apply ring (capacity 2) + 25 ms per apply + 16 concurrent
    // clients with group commit (batch 16): one batch commit hands the
    // core 16 committed entries AT ONCE, so the sink refuses almost every
    // offer and the backlog drains through pumpApply(). The properties
    // pinned: (1) the Raft thread never blocks on the state machine —
    // heartbeats keep flowing, so the term NEVER changes during the whole
    // run; (2) losslessness — every committed entry still applies exactly
    // once, in order (the APPEND values prove it). The old blocking sink
    // stalled the Raft thread for (backlog - ring) x 25 ms ≈ 350 ms inside
    // ONE handler — past the 150-300 ms election timeout — and this test
    // fails against it with spurious elections (verified by reverting).
    rsm::runtime::NodeRuntimeConfig cfg;
    cfg.applyRingSize = 2;
    Cluster cluster(3, /*baseDir=*/{}, /*kv=*/true,
                    rsm::client::ClientService::Batching{
                        16, std::chrono::microseconds(500)},
                    cfg, std::chrono::milliseconds(25));
    const auto leader = cluster.awaitStableLeader(std::chrono::seconds(10));
    REQUIRE(leader.has_value());
    const auto termBefore = cluster.observedTerm(*leader);

    constexpr int kClients = 16;
    constexpr int kOps = 6;
    std::vector<std::thread> threads;
    std::vector<int> okCounts(kClients);
    for (int c = 0; c < kClients; ++c) {
        threads.emplace_back([&, c] {
            KvClient client(cluster.peerMap(),
                            /*clientId=*/0x510C0 + static_cast<unsigned>(c),
                            static_cast<NodeId>(220 + c),
                            /*perAttemptTimeoutMs=*/5000);
            const std::string key = "slow-k" + std::to_string(c);
            for (int i = 0; i < kOps; ++i) {
                const auto r = client.append(key, "x");
                if (!r || r->status != kKvOk) break;
                if (r->value !=
                    std::string(static_cast<std::size_t>(i) + 1, 'x')) {
                    break;
                }
                ++okCounts[static_cast<std::size_t>(c)];
            }
        });
    }
    for (auto& t : threads) t.join();
    for (int c = 0; c < kClients; ++c) {
        CAPTURE(c);
        CHECK(okCounts[static_cast<std::size_t>(c)] == kOps);
    }

    // The leadership never wobbled: same leader, same term, and the
    // whole-history election-safety record saw no new election.
    const auto leaderAfter = cluster.stableLeader();
    REQUIRE(leaderAfter.has_value());
    CHECK(*leaderAfter == *leader);
    CHECK(cluster.observedTerm(*leader) == termBefore);
    cluster.log.requireElectionSafety();
}
