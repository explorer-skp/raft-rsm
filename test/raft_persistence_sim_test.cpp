// Phase 4 integration tests: 3-node SimCluster over REAL durable storage
// (DurablePersistentState + DurableLog in a per-node temp directory), with
// kill + restart. restart() rebuilds the node exactly like a restarted
// process: storage re-opened and replayed from disk, volatile state (role,
// commitIndex, lastApplied, state machine) reset.
//
// The invariant these tests lean on (stated in DESIGN.md): an entry is
// reported committed only after a majority holds it durably — every node
// fsyncs before acking, and commit requires a majority of acks — which is
// exactly why committed entries survive any single-node crash and a
// full-cluster restart.

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "doctest/doctest.h"
#include "sim_cluster.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"
#include "temp_dir.h"

using simtest::defaultSetups;
using simtest::SimCluster;
using simtest::StorageFactory;
using simtest::StoragePair;
using rsm::raft::Duration;
using rsm::raft::Role;
using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::Message;
using rsm::rpc::MessageType;
using rsm::rpc::NodeId;
using rsm::rpc::RequestVote;
using rsm::rpc::RequestVoteReply;
using rsm::rpc::Term;
using rsm::statemachine::Command;
using rsm::storage::DurableLog;
using rsm::storage::DurablePersistentState;
using testutil::TempDir;

namespace {

StorageFactory durableFactory(const TempDir& dir) {
    return [&dir](NodeId id) {
        const auto sub = dir.subdir("node" + std::to_string(id));
        return StoragePair{std::make_unique<DurablePersistentState>(sub),
                           std::make_unique<DurableLog>(sub)};
    };
}

std::string nodeDir(const TempDir& dir, NodeId id) {
    return dir.subdir("node" + std::to_string(id));
}

Command cmd(std::uint8_t b) { return Command{b}; }

NodeId soleLeader(SimCluster& sim) {
    const auto leaders = sim.liveLeaders();
    REQUIRE(leaders.size() == 1);
    return leaders[0];
}

NodeId aFollower(NodeId leader) {
    return leader == 1 ? 2 : 1;
}

}  // namespace

TEST_CASE("durable sim: a granted vote survives crash+restart — the node "
          "refuses a different candidate in the same term") {
    TempDir dir;
    SimCluster sim(defaultSetups(11, 22, 33), 0, Duration(0),
                   durableFactory(dir));

    // Tap every RequestVoteReply node 1 sends, so the externally visible
    // grant/refuse decision (not just internal state) is asserted.
    std::vector<std::pair<NodeId, RequestVoteReply>> repliesFrom1;
    sim.onPost = [&](NodeId from, NodeId to, const Message& m) {
        if (from != 1) return;
        if (const auto* r = std::get_if<RequestVoteReply>(&m)) {
            repliesFrom1.emplace_back(to, *r);
        }
    };

    // No time is stepped in this test: nothing campaigns on its own, so the
    // only traffic is the two hand-crafted RequestVotes below.
    const Envelope rvFrom2{kProtocolVersion, MessageType::RequestVote, 2, 1, 0};
    sim.core(1).handle(rvFrom2, Message{RequestVote{5, 2, 0, 0}});
    REQUIRE(repliesFrom1.size() == 1);
    CHECK(repliesFrom1[0].first == 2);
    CHECK(repliesFrom1[0].second.voteGranted);
    CHECK(repliesFrom1[0].second.term == 5);
    CHECK(sim.core(1).term() == 5);
    CHECK(sim.core(1).votedFor() == 2);

    // Crash and restart node 1: term and vote must come back from disk.
    sim.kill(1);
    sim.restart(1, /*seed=*/444);
    CHECK(sim.core(1).term() == 5);
    CHECK(sim.core(1).votedFor() == 2);

    // A different candidate asks for the same term 5: must be refused.
    const Envelope rvFrom3{kProtocolVersion, MessageType::RequestVote, 3, 1, 0};
    sim.core(1).handle(rvFrom3, Message{RequestVote{5, 3, 0, 0}});
    REQUIRE(repliesFrom1.size() == 2);
    CHECK(repliesFrom1[1].first == 3);
    CHECK_FALSE(repliesFrom1[1].second.voteGranted);
    CHECK(sim.core(1).votedFor() == 2);  // unchanged

    // The original candidate retrying is still granted (idempotent re-vote).
    sim.core(1).handle(rvFrom2, Message{RequestVote{5, 2, 0, 0}});
    REQUIRE(repliesFrom1.size() == 3);
    CHECK(repliesFrom1[2].second.voteGranted);
}

TEST_CASE("durable sim: currentTerm never regresses across restart") {
    TempDir dir;
    SimCluster sim(defaultSetups(101, 202, 303), 7, Duration(0),
                   durableFactory(dir));
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId leader = soleLeader(sim);
    const NodeId follower = aFollower(leader);

    // Follower: restart with no intervening elections — exact term restore.
    const Term followerTerm = sim.core(follower).term();
    sim.kill(follower);
    sim.restart(follower, 555);
    CHECK(sim.core(follower).term() == followerTerm);

    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));

    // Leader: crash it, let the survivors elect at a higher term, then
    // restart it. Its recovered term must be >= its pre-crash term at every
    // observable point (== right after recovery, then it learns the new
    // higher term — never anything lower).
    const Term leaderTerm = sim.core(leader).term();
    sim.kill(leader);
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.liveLeaders().size() == 1 &&
               sim.liveLeaders()[0] != leader;
    }));
    sim.restart(leader, 556);
    CHECK(sim.core(leader).term() >= leaderTerm);  // recovered, no regression
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    CHECK(sim.core(leader).term() > leaderTerm);  // learned the higher term
}

TEST_CASE("durable sim: committed entries survive crash+restart of a "
          "follower and (separately) the leader") {
    TempDir dir;
    SimCluster sim(defaultSetups(1, 2, 3), 42, Duration(0),
                   durableFactory(dir));
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId leader = soleLeader(sim);

    std::vector<Command> expect;
    for (std::uint8_t b = 0xA1; b <= 0xA5; ++b) {
        REQUIRE(sim.core(leader).propose(cmd(b)).has_value());
        expect.push_back(cmd(b));
    }
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.replicated(5); }));

    // Follower crash + restart: log recovered from disk, state machine
    // rebuilt from index 1 in the cluster's order once commitIndex is
    // relearned from the leader's heartbeats.
    const NodeId follower = aFollower(leader);
    sim.kill(follower);
    sim.restart(follower, 661);
    CHECK(sim.log(follower).lastIndex() == 5);  // recovered before any RPC
    CHECK(sim.sm(follower).applied().empty());  // volatile: reset, not loaded
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.replicated(5) && sim.sm(follower).applied() == expect;
    }));

    // Leader crash + restart: survivors re-elect, the old leader rejoins as
    // a follower and re-applies the same committed prefix.
    sim.kill(leader);
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.liveLeaders().size() == 1 &&
               sim.liveLeaders()[0] != leader;
    }));
    sim.restart(leader, 662);
    CHECK(sim.log(leader).lastIndex() == 5);  // its log survived its crash
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.replicated(5) && sim.sm(leader).applied() == expect;
    }));
    CHECK(sim.core(leader).role() == Role::Follower);
    sim.checkLogMatching();
    sim.checkStateMachineSafety();
}

TEST_CASE("durable sim: crash mid-append (torn trailing record) on a "
          "follower — partial entry discarded, leader re-replicates, nothing "
          "committed is lost") {
    TempDir dir;
    SimCluster sim(defaultSetups(7, 8, 9), 13, Duration(0),
                   durableFactory(dir));
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    const NodeId leader = soleLeader(sim);

    std::vector<Command> expect;
    for (std::uint8_t b = 0x51; b <= 0x54; ++b) {
        REQUIRE(sim.core(leader).propose(cmd(b)).has_value());
        expect.push_back(cmd(b));
    }
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.replicated(4); }));

    // Model the crash honestly: a crash MID-append means the follower never
    // acked entry 5 (the ack only goes out after fsync), so the leader must
    // not have counted it. Isolate the follower, commit entry 5 on the
    // leader + the other follower (still a durable majority), then leave a
    // torn record fragment on the isolated follower's disk — what a crash
    // halfway through writing record 5 leaves behind. Tearing an *acked*
    // entry instead would violate the durable-ack invariant this phase
    // establishes, and the leader is entitled to assume it never happens.
    const NodeId follower = aFollower(leader);
    sim.isolate(follower, true);
    REQUIRE(sim.core(leader).propose(cmd(0x55)).has_value());
    expect.push_back(cmd(0x55));
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.core(leader).commitIndex() == 5;
    }));
    sim.kill(follower);
    sim.isolate(follower, false);
    {
        std::ofstream f(nodeDir(dir, follower) + "/log",
                        std::ios::binary | std::ios::app);
        const char fragment[] = {0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                 0x00, 0x03, 0x00};  // 10 of 20 header bytes
        f.write(fragment, sizeof(fragment));
    }

    sim.restart(follower, 909);
    auto& recovered = dynamic_cast<DurableLog&>(sim.log(follower));
    CHECK(recovered.tornBytesDiscarded() == 10);
    CHECK(recovered.lastIndex() == 4);  // partial entry 5 discarded cleanly

    // The leader re-replicates entry 5; the cluster reconverges with the
    // full committed sequence applied identically everywhere. The entry was
    // never at risk: commit required a durable majority, which the leader
    // and the other follower hold.
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.replicated(5) && sim.sm(follower).applied() == expect;
    }));
    CHECK(sim.log(follower).lastIndex() == 5);
    sim.checkLogMatching();
    sim.checkStateMachineSafety();
}

TEST_CASE("durable sim: full-cluster restart — committed prefix present and "
          "identical on all nodes, then re-applied") {
    TempDir dir;
    SimCluster sim(defaultSetups(21, 22, 23), 99, Duration(0),
                   durableFactory(dir));
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    NodeId leader = soleLeader(sim);

    std::vector<Command> expect;
    for (std::uint8_t b = 0xE1; b <= 0xE5; ++b) {
        REQUIRE(sim.core(leader).propose(cmd(b)).has_value());
        expect.push_back(cmd(b));
    }
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.replicated(5); }));
    const auto committedEntries = sim.log(leader).entriesFrom(1);

    // Stop the world, then restart all three from disk.
    for (NodeId id = 1; id <= 3; ++id) sim.kill(id);
    for (NodeId id = 1; id <= 3; ++id) sim.restart(id, 7000 + id);

    // Every node recovered the identical committed prefix from its own disk
    // before exchanging a single message.
    for (NodeId id = 1; id <= 3; ++id) {
        CHECK(sim.log(id).entriesFrom(1) == committedEntries);
        CHECK(sim.core(id).commitIndex() == 0);  // volatile: relearned later
    }

    // A leader is elected among the restarted nodes.
    REQUIRE(sim.runUntil(Duration(3000), [&] { return sim.converged(); }));
    leader = soleLeader(sim);
    sim.checkLogMatching();

    // Commit one new entry: per the Figure 8 rule the new leader cannot
    // count replicas of prior-term entries, so the recovered prefix becomes
    // committed (and is re-applied from index 1, in the cluster's original
    // order) when the first current-term entry commits above it.
    REQUIRE(sim.core(leader).propose(cmd(0xEE)).has_value());
    expect.push_back(cmd(0xEE));
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.replicated(6); }));
    for (NodeId id = 1; id <= 3; ++id) {
        CHECK(sim.sm(id).applied() == expect);
    }
    sim.checkLogMatching();
    sim.checkStateMachineSafety();
}
