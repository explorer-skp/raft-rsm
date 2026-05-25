// Phase 3 integration tests on the deterministic simulation harness:
// normal replication, follower catch-up via conflict-hint backtracking,
// divergent-log repair, no-truncation-of-committed, and the Figure 8
// commit-safety scenario. Log Matching and State Machine Safety are
// asserted across every test.

#include <algorithm>
#include <cstdint>
#include <vector>

#include "doctest/doctest.h"
#include "sim_cluster.h"

using simtest::defaultSetups;
using simtest::SimCluster;
using rsm::raft::Duration;
using rsm::raft::Role;
using rsm::rpc::AppendEntries;
using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::LogIndex;
using rsm::rpc::Message;
using rsm::rpc::NodeId;
using rsm::rpc::Term;

namespace {

// Cold-starts the cluster and returns the elected leader.
NodeId electLeader(SimCluster& sim) {
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.converged(); }));
    return sim.liveLeaders()[0];
}

std::vector<std::uint8_t> cmd(std::uint8_t tag) { return {tag}; }

void checkInvariants(const SimCluster& sim) {
    sim.checkElectionSafety();
    sim.checkLogMatching();
    sim.checkStateMachineSafety();
}

}  // namespace

TEST_CASE("normal replication: proposals commit and apply identically on "
          "all three nodes") {
    SimCluster sim(defaultSetups(11, 22, 33));
    const NodeId leader = electLeader(sim);

    std::vector<LogIndex> indices;
    for (std::uint8_t i = 1; i <= 5; ++i) {
        const auto idx = sim.core(leader).propose(cmd(i));
        REQUIRE(idx.has_value());
        indices.push_back(*idx);
        sim.stepMs();  // let the round trip complete
    }
    // Sequential indices from 1 (no entries existed before).
    CHECK(indices == std::vector<LogIndex>{1, 2, 3, 4, 5});

    // Everyone converges: identical logs, commitIndex == lastApplied == 5
    // on all nodes (followers learn commit via the next heartbeat).
    REQUIRE(sim.runUntil(Duration(1000), [&] { return sim.replicated(5); }));
    for (NodeId id = 1; id <= 3; ++id) {
        CHECK(sim.core(id).commitIndex() == 5);
        REQUIRE(sim.sm(id).applied().size() == 5);
        for (std::uint8_t i = 1; i <= 5; ++i) {
            CHECK(sim.sm(id).applied()[i - 1] == cmd(i));
        }
    }
    checkInvariants(sim);

    // A non-leader must reject proposals.
    for (NodeId id = 1; id <= 3; ++id) {
        if (id != leader) CHECK_FALSE(sim.core(id).propose(cmd(99)));
    }
}

TEST_CASE("follower catch-up: a lagging follower is backfilled in O(terms) "
          "round trips, not O(entries)") {
    SimCluster sim(defaultSetups(11, 22, 33));
    const NodeId firstLeader = electLeader(sim);
    NodeId laggard = 0, other = 0;
    for (NodeId id = 1; id <= 3; ++id) {
        if (id == firstLeader) continue;
        (laggard == 0 ? laggard : other) = id;
    }

    // The laggard misses 30 committed entries, all in one term.
    sim.isolate(laggard, true);
    for (std::uint8_t i = 1; i <= 30; ++i) {
        REQUIRE(sim.core(firstLeader).propose(cmd(i)).has_value());
        sim.stepMs();
    }
    // `other` must learn commitIndex == 30 before taking over: a new leader
    // cannot count-commit old-term entries (Figure 8 rule), so it has to
    // inherit the commit point as a follower via heartbeats.
    REQUIRE(sim.runUntil(Duration(500), [&] {
        return sim.core(other).commitIndex() == 30;
    }));
    REQUIRE(sim.log(laggard).lastIndex() == 0);

    // Force a leadership change to `other` while reconnecting the laggard:
    // the new leader initializes nextIndex[laggard] to ITS lastIndex+1
    // (31), so repair must run the too-short conflict hint, not start from
    // a conveniently stale nextIndex. The laggard cannot win (its log loses
    // the up-to-date check), so `other` becomes leader.
    sim.isolate(firstLeader, true);
    sim.isolate(laggard, false);
    REQUIRE(sim.runUntil(Duration(3000), [&] {
        const auto leaders = sim.liveLeaders();
        return std::find(leaders.begin(), leaders.end(), other) !=
               leaders.end();
    }));

    // Count AppendEntries delivered to the laggard from here: one rejected
    // probe (too short -> conflictIndex = 1) plus one RPC carrying all 30
    // entries, with a couple of heartbeats around them — never one per
    // entry.
    sim.resetAppendEntriesCount();
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.log(laggard).lastIndex() == 30 &&
               sim.core(laggard).commitIndex() == 30;
    }));
    CHECK(sim.appendEntriesDeliveredTo(laggard) <= 5);  // not O(30)
    CHECK(sim.sm(laggard).applied().size() == 30);

    // Heal the old leader too; everyone converges on the same 30 entries.
    sim.isolate(firstLeader, false);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.replicated(30); }));
    checkInvariants(sim);
}

TEST_CASE("divergent-log repair: an old leader's uncommitted suffix is "
          "overwritten; no committed entry is lost") {
    SimCluster sim(defaultSetups(11, 22, 33));
    const NodeId oldLeader = electLeader(sim);

    // Committed baseline 1..3 on everyone.
    for (std::uint8_t i = 1; i <= 3; ++i) {
        REQUIRE(sim.core(oldLeader).propose(cmd(i)).has_value());
        sim.stepMs();
    }
    REQUIRE(sim.runUntil(Duration(1000), [&] { return sim.replicated(3); }));

    // Partition the leader; it keeps appending entries it can no longer
    // replicate — a divergent, uncommitted suffix built by the protocol
    // itself (it still believes it is leader).
    sim.isolate(oldLeader, true);
    for (std::uint8_t i = 1; i <= 3; ++i) {
        REQUIRE(sim.core(oldLeader).propose(cmd(0xE0 + i)).has_value());
        sim.stepMs();
    }
    CHECK(sim.log(oldLeader).lastIndex() == 6);
    CHECK(sim.core(oldLeader).commitIndex() == 3);  // E* never commit

    // The majority side elects a new leader and commits different entries
    // at the same indices 4..5.
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        const auto leaders = sim.liveLeaders();
        return leaders.size() == 2;  // old (stale) + new
    }));
    NodeId newLeader = 0;
    for (const NodeId id : sim.liveLeaders()) {
        if (id != oldLeader) newLeader = id;
    }
    REQUIRE(newLeader != 0);
    for (std::uint8_t i = 1; i <= 2; ++i) {
        REQUIRE(sim.core(newLeader).propose(cmd(0xA0 + i)).has_value());
        sim.stepMs();
    }

    // Heal. The old leader steps down, its E* suffix is truncated and
    // replaced via conflict-hint repair, and every node ends with the
    // committed sequence [1, 2, 3, A1, A2] applied.
    sim.isolate(oldLeader, false);
    REQUIRE(sim.runUntil(Duration(2000), [&] { return sim.replicated(5); }));
    CHECK(sim.core(oldLeader).role() == Role::Follower);
    for (NodeId id = 1; id <= 3; ++id) {
        REQUIRE(sim.log(id).lastIndex() == 5);
        CHECK(sim.log(id).entryAt(4).command == cmd(0xA1));
        CHECK(sim.log(id).entryAt(5).command == cmd(0xA2));
        const auto& applied = sim.sm(id).applied();
        REQUIRE(applied.size() == 5);
        for (std::uint8_t i = 1; i <= 3; ++i) {
            CHECK(applied[i - 1] == cmd(i));  // committed baseline survived
        }
        CHECK(applied[3] == cmd(0xA1));
        CHECK(applied[4] == cmd(0xA2));
    }
    checkInvariants(sim);
}

TEST_CASE("no truncation of committed entries: a delayed duplicate "
          "AppendEntries leaves log and applied sequence untouched") {
    SimCluster sim(defaultSetups(11, 22, 33));
    const NodeId leader = electLeader(sim);
    for (std::uint8_t i = 1; i <= 3; ++i) {
        REQUIRE(sim.core(leader).propose(cmd(i)).has_value());
        sim.stepMs();
    }
    REQUIRE(sim.runUntil(Duration(1000), [&] { return sim.replicated(3); }));

    // Hand-deliver a stale duplicate of the first replication RPC (prefix
    // of the follower's log, old leaderCommit) to one follower.
    const NodeId follower = leader == 1 ? 2 : 1;
    AppendEntries stale;
    stale.term = sim.core(leader).term();
    stale.leaderId = leader;
    stale.prevLogIndex = 0;
    stale.prevLogTerm = 0;
    stale.leaderCommit = 2;
    stale.entries = {sim.log(leader).entryAt(1), sim.log(leader).entryAt(2)};
    sim.core(follower).handle(
        Envelope{kProtocolVersion, rsm::rpc::MessageType::AppendEntries,
                 leader, follower, 0},
        Message{stale});

    CHECK(sim.log(follower).lastIndex() == 3);  // nothing truncated
    CHECK(sim.core(follower).commitIndex() == 3);  // nothing regressed
    REQUIRE(sim.sm(follower).applied().size() == 3);  // nothing re-applied
    REQUIRE(sim.runUntil(Duration(500), [&] { return sim.replicated(3); }));
    checkInvariants(sim);
}

TEST_CASE("Figure 8: an old-term entry on a majority commits only once a "
          "current-term entry commits above it") {
    SimCluster sim(defaultSetups(11, 22, 33));
    const NodeId oldLeader = electLeader(sim);
    REQUIRE(sim.core(oldLeader).propose(cmd(0x01)).has_value());
    REQUIRE(sim.runUntil(Duration(1000), [&] { return sim.replicated(1); }));
    const Term oldTerm = sim.core(oldLeader).term();

    NodeId f1 = 0, f2 = 0;  // f1 will receive X; f2 is cut off
    for (NodeId id = 1; id <= 3; ++id) {
        if (id == oldLeader) continue;
        (f1 == 0 ? f1 : f2) = id;
    }

    // X reaches only f1, and the ack never reaches the old leader (its
    // inbound is dropped), so X stays uncommitted; then the leader dies.
    sim.dropTo(oldLeader, true);
    sim.isolate(f2, true);
    REQUIRE(sim.core(oldLeader).propose(cmd(0x58)) == LogIndex{2});  // X
    for (int i = 0; i < 20; ++i) sim.stepMs();
    REQUIRE(sim.log(f1).lastIndex() == 2);          // f1 has X...
    REQUIRE(sim.log(f2).lastIndex() == 1);          // ...f2 does not...
    REQUIRE(sim.core(oldLeader).commitIndex() == 1);  // ...nothing committed
    sim.kill(oldLeader);
    sim.isolate(f2, false);

    // f1 is the only electable survivor (f2's log is behind, so f2 can
    // never assemble a majority), and it wins a higher term.
    REQUIRE(sim.runUntil(Duration(3000), [&] {
        return sim.liveLeaders() == std::vector<NodeId>{f1};
    }));
    const Term newTerm = sim.core(f1).term();
    REQUIRE(newTerm > oldTerm);

    // Heartbeat backfill puts X on a live majority (f1 + f2) — at its OLD
    // term. It must still not commit: termAt(2) != currentTerm.
    REQUIRE(sim.runUntil(Duration(2000), [&] {
        return sim.log(f2).lastIndex() == 2;
    }));
    REQUIRE(sim.log(f2).entryAt(2).command == cmd(0x58));
    for (int i = 0; i < 300; ++i) sim.stepMs();  // plenty of heartbeats
    CHECK(sim.core(f1).commitIndex() == 1);  // THE Figure 8 assertion
    CHECK(sim.sm(f1).applied().size() == 1);

    // A current-term entry on top commits, and X commits with it —
    // indirectly, exactly once, in order, on both survivors.
    REQUIRE(sim.core(f1).propose(cmd(0x59)) == LogIndex{3});
    REQUIRE(sim.runUntil(Duration(1000), [&] {
        return sim.core(f1).commitIndex() == 3 &&
               sim.core(f2).commitIndex() == 3;
    }));
    for (const NodeId id : {f1, f2}) {
        const auto& applied = sim.sm(id).applied();
        REQUIRE(applied.size() == 3);
        CHECK(applied[0] == cmd(0x01));
        CHECK(applied[1] == cmd(0x58));
        CHECK(applied[2] == cmd(0x59));
    }
    checkInvariants(sim);
}
