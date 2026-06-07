// Real-cluster integration (Phases 2-4) over the shared TCP harness in
// tcp_cluster.h: cold-start election, replication across failover, and
// durable crash+restart recovery. The Phase 5 client-path tests live in
// kv_cluster_test.cpp on the same harness.

#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include "doctest/doctest.h"
#include "statemachine/state_machine.h"
#include "tcp_cluster.h"
#include "temp_dir.h"

using tcptest::Cluster;
using tcptest::NodeId;
using tcptest::Term;

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
