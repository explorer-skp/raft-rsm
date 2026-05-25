// Phase 2 integration tests on the deterministic simulation harness
// (test/sim_cluster.h): leader election under cold start, leader failure,
// forced split votes, seeded chaotic timing, and partition/rejoin.

#include <cstdint>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "doctest/doctest.h"
#include "raft/logging.h"
#include "sim_cluster.h"

using simtest::defaultSetups;
using simtest::NodeSetup;
using simtest::SimCluster;
using rsm::raft::Duration;
using rsm::raft::RaftConfig;
using rsm::raft::Role;
using rsm::rpc::NodeId;
using rsm::rpc::Term;

TEST_CASE("cold start: exactly one leader, all nodes agree on term and "
          "leader") {
    SimCluster sim(defaultSetups(11, 22, 33));
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));

    const NodeId leader = sim.liveLeaders()[0];
    const Term term = sim.core(leader).term();
    CHECK(term >= Term{1});
    for (NodeId id = 1; id <= 3; ++id) {
        CHECK(sim.core(id).term() == term);
        CHECK(sim.core(id).leaderId() == leader);
        if (id != leader) CHECK(sim.core(id).role() == Role::Follower);
    }
    sim.checkElectionSafety();
}

TEST_CASE("leader failure: survivors elect exactly one new leader in a "
          "higher term") {
    SimCluster sim(defaultSetups(11, 22, 33));
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId oldLeader = sim.liveLeaders()[0];
    const Term oldTerm = sim.core(oldLeader).term();

    sim.kill(oldLeader);  // stays down: restart/recovery is Phase 4
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));

    const auto leaders = sim.liveLeaders();
    REQUIRE(leaders.size() == 1);
    CHECK(leaders[0] != oldLeader);
    CHECK(sim.core(leaders[0]).term() > oldTerm);
    sim.checkElectionSafety();
}

TEST_CASE("split vote: simultaneous candidates converge to one leader in a "
          "later term") {
    // Mirror RaftCore's timeout draw (mt19937_64 + uniform_int_distribution
    // over [150, 300]) to find two seeds whose FIRST draws collide but whose
    // SECOND draws differ: both nodes campaign at the same instant, the
    // randomized redraw then breaks the tie.
    const auto draws = [](std::uint64_t seed) {
        std::mt19937_64 rng(seed);
        std::uniform_int_distribution<std::int64_t> d(150, 300);
        const auto first = d(rng);
        const auto second = d(rng);
        return std::pair{first, second};
    };
    const std::uint64_t seedA = 1;
    std::uint64_t seedB = 0;
    for (std::uint64_t s = 2; s < 200000; ++s) {
        if (draws(s).first == draws(seedA).first &&
            draws(s).second != draws(seedA).second) {
            seedB = s;
            break;
        }
    }
    REQUIRE_MESSAGE(seedB != 0, "no colliding seed found in search range");

    // Node 3 never campaigns (huge timeout) and is isolated from the start,
    // so nodes 1 and 2 split term 1's votes between themselves: each votes
    // for itself and denies the other. Only the randomized redraw resolves
    // the next round.
    RaftConfig never;
    never.electionTimeoutMin = Duration(100000);
    never.electionTimeoutMax = Duration(100000);
    SimCluster sim({NodeSetup{seedA, {}}, NodeSetup{seedB, {}},
                    NodeSetup{99, never}});
    sim.isolate(3, true);

    REQUIRE(sim.runUntil(Duration(10000), [&] {
        return sim.liveLeaders().size() == 1;
    }));

    // The forced split really happened: both 1 and 2 campaigned in term 1
    // and neither won it.
    CHECK(sim.electionsStarted[Term{1}] == std::set<NodeId>{1, 2});
    CHECK(sim.leadersByTerm.count(Term{1}) == 0);
    // Convergence took more than one term, and safety held throughout.
    const NodeId winner = sim.liveLeaders()[0];
    CHECK(sim.core(winner).term() >= Term{2});
    sim.checkElectionSafety();
}

TEST_CASE("seeded chaotic timing: Election Safety holds and the cluster "
          "still converges") {
    // Message delays up to 8x the election timeout force overlapping
    // elections, stale replies, and many randomized timeout draws. Safety
    // is checked after every simulated millisecond of every run.
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Warn);  // keep output sane
    for (std::uint64_t chaosSeed = 1; chaosSeed <= 20; ++chaosSeed) {
        CAPTURE(chaosSeed);
        SimCluster sim(defaultSetups(chaosSeed * 3 + 1, chaosSeed * 3 + 2,
                                     chaosSeed * 3 + 3),
                       /*deliveryDelaySeed=*/chaosSeed,
                       /*maxDeliveryDelay=*/Duration(1200));
        for (int ms = 0; ms < 5000; ++ms) sim.stepMs();  // chaos phase
        sim.checkElectionSafety();

        // Calm the network: the same cluster (stale messages still in
        // flight) must then converge to exactly one leader.
        sim.setMaxDeliveryDelay(Duration(5));
        REQUIRE(sim.runUntil(Duration(3000), [&] { return sim.converged(); }));
        sim.checkElectionSafety();
    }
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Info);
}

TEST_CASE("partition: isolated leader keeps its term; rejoin steps it down "
          "to follower") {
    SimCluster sim(defaultSetups(11, 22, 33));
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId oldLeader = sim.liveLeaders()[0];
    const Term oldTerm = sim.core(oldLeader).term();

    // Isolate the leader. It has no election timer, so it keeps believing
    // it leads term oldTerm while the majority moves on.
    sim.isolate(oldLeader, true);
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        const auto leaders = sim.liveLeaders();
        return leaders.size() == 2 ||  // old + new, in different terms
               (leaders.size() == 1 && leaders[0] != oldLeader);
    }));
    const auto duringPartition = sim.liveLeaders();
    REQUIRE(duringPartition.size() == 2);  // both still call themselves leader
    const NodeId newLeader =
        duringPartition[0] == oldLeader ? duringPartition[1]
                                        : duringPartition[0];
    const Term newTerm = sim.core(newLeader).term();
    CHECK(newTerm > oldTerm);
    CHECK(sim.core(oldLeader).term() == oldTerm);  // stale, unaware
    sim.checkElectionSafety();  // two leaders, but never in the same term

    // Heal the partition: the old leader sees the higher term and steps
    // down to follower; exactly one leader remains.
    sim.isolate(oldLeader, false);
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.converged() &&
               sim.core(oldLeader).role() == Role::Follower;
    }));
    CHECK(sim.core(oldLeader).term() == sim.core(newLeader).term());
    CHECK(sim.core(oldLeader).leaderId() == newLeader);
    CHECK(sim.liveLeaders() == std::vector<NodeId>{newLeader});
    sim.checkElectionSafety();
}
