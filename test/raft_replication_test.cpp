// Phase 3 unit tests: AppendEntries consistency check, conflict resolution
// with the no-spurious-truncation rule, conflict-hint generation and
// handling, the Figure 8 commit rule, the apply loop, and propose().
// Deterministic: ManualClock + fixed seeds, no threads.

#include <cstdint>
#include <utility>
#include <vector>

#include "doctest/doctest.h"
#include "raft/raft_core.h"
#include "statemachine/state_machine.h"
#include "storage/log.h"

using rsm::raft::Duration;
using rsm::raft::InMemoryPersistentState;
using rsm::raft::ManualClock;
using rsm::raft::RaftConfig;
using rsm::raft::RaftCore;
using rsm::raft::Role;
using rsm::rpc::AppendEntries;
using rsm::rpc::AppendEntriesReply;
using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::LogEntry;
using rsm::rpc::LogIndex;
using rsm::rpc::Message;
using rsm::rpc::NodeId;
using rsm::rpc::RequestVoteReply;
using rsm::rpc::Term;

namespace {

RaftConfig fixedTimeoutConfig() {
    RaftConfig cfg;
    cfg.electionTimeoutMin = Duration(150);
    cfg.electionTimeoutMax = Duration(150);
    cfg.heartbeatInterval = Duration(50);
    return cfg;
}

LogEntry entry(Term term, std::uint8_t tag) {
    return LogEntry{term, {tag}};
}

struct Harness {
    ManualClock clock;
    InMemoryPersistentState persist;
    rsm::storage::InMemoryLog log;
    rsm::statemachine::RecordingStateMachine sm;
    std::vector<std::pair<NodeId, Message>> sent;
    std::vector<NodeId> peerIds;
    RaftCore core;

    explicit Harness(std::vector<NodeId> peers = {2, 3})
        : peerIds(peers),
          core(1, std::move(peers), persist, log, sm, clock, /*rngSeed=*/7,
               fixedTimeoutConfig(), [this](NodeId to, const Message& m) {
                   sent.emplace_back(to, m);
               }) {
        core.start();
    }

    void deliver(NodeId from, const Message& m) {
        core.handle(Envelope{kProtocolVersion, rsm::rpc::typeOf(m), from,
                             core.selfId(), 0},
                    m);
    }

    // Candidate via timer expiry, then leader via granted votes from as
    // many peers as a majority needs. Call with log/persist pre-seeded.
    void winElection() {
        clock.advance(std::chrono::duration_cast<Duration>(
            core.electionDeadline() - clock.now()));
        core.tick();
        REQUIRE(core.role() == Role::Candidate);
        for (const NodeId p : peerIds) {
            if (core.role() == Role::Leader) break;
            deliver(p, Message{RequestVoteReply{core.term(), true}});
        }
        REQUIRE(core.role() == Role::Leader);
    }

    const AppendEntriesReply& lastAppendReply() const {
        return std::get<AppendEntriesReply>(sent.back().second);
    }

    // Most recent AppendEntries sent to `to` (the leader's retry/backfill).
    const AppendEntries& lastAppendTo(NodeId to) const {
        for (auto it = sent.rbegin(); it != sent.rend(); ++it) {
            if (it->first == to &&
                std::holds_alternative<AppendEntries>(it->second)) {
                return std::get<AppendEntries>(it->second);
            }
        }
        FAIL("no AppendEntries sent to peer");
        static AppendEntries dummy;
        return dummy;
    }

    std::vector<std::uint8_t> appliedTags() const {
        std::vector<std::uint8_t> tags;
        for (const auto& cmd : sm.applied()) {
            REQUIRE(cmd.size() == 1);
            tags.push_back(cmd[0]);
        }
        return tags;
    }
};

AppendEntries ae(Term term, NodeId leader, LogIndex prev, Term prevTerm,
                 std::vector<LogEntry> entries, LogIndex commit = 0) {
    return AppendEntries{term, leader, prev, prevTerm, commit,
                         std::move(entries)};
}

}  // namespace

TEST_CASE("consistency check: prevLogIndex == 0 passes on an empty log") {
    Harness h;
    h.deliver(2, Message{ae(1, 2, 0, 0, {entry(1, 0xA)})});
    CHECK(h.lastAppendReply().success);
    CHECK(h.log.lastIndex() == 1);
    // Success ack echoes request.prevLogIndex + request.entries.size().
    CHECK(h.lastAppendReply().conflictIndex == 1);
}

TEST_CASE("consistency check: log too short replies conflictTerm=none, "
          "conflictIndex=lastIndex+1") {
    Harness h;
    h.deliver(2, Message{ae(1, 2, 0, 0, {entry(1, 0xA), entry(1, 0xB)})});
    REQUIRE(h.log.lastIndex() == 2);

    h.deliver(2, Message{ae(1, 2, 5, 1, {entry(1, 0xF)})});
    CHECK_FALSE(h.lastAppendReply().success);
    CHECK(h.lastAppendReply().conflictTerm == 0);
    CHECK(h.lastAppendReply().conflictIndex == 3);
    CHECK(h.log.lastIndex() == 2);  // nothing changed
}

TEST_CASE("consistency check: term mismatch replies the conflicting term "
          "and its first index") {
    Harness h;
    // Local log terms: [1, 1, 2, 2, 3].
    h.deliver(2, Message{ae(3, 2, 0, 0,
                            {entry(1, 1), entry(1, 2), entry(2, 3),
                             entry(2, 4), entry(3, 5)})});
    REQUIRE(h.log.lastIndex() == 5);

    SUBCASE("mismatch at index 5 (term 3): first index of term 3 is 5") {
        h.deliver(2, Message{ae(9, 2, 5, 9, {})});
        CHECK_FALSE(h.lastAppendReply().success);
        CHECK(h.lastAppendReply().conflictTerm == 3);
        CHECK(h.lastAppendReply().conflictIndex == 5);
    }
    SUBCASE("mismatch at index 4 (term 2): first index of term 2 is 3") {
        h.deliver(2, Message{ae(9, 2, 4, 9, {})});
        CHECK_FALSE(h.lastAppendReply().success);
        CHECK(h.lastAppendReply().conflictTerm == 2);
        CHECK(h.lastAppendReply().conflictIndex == 3);
    }
    SUBCASE("mismatch at index 2 (term 1): first index of term 1 is 1") {
        h.deliver(2, Message{ae(9, 2, 2, 9, {})});
        CHECK_FALSE(h.lastAppendReply().success);
        CHECK(h.lastAppendReply().conflictTerm == 1);
        CHECK(h.lastAppendReply().conflictIndex == 1);
    }
}

TEST_CASE("consistency check: matching prevLog accepts and appends") {
    Harness h;
    h.deliver(2, Message{ae(2, 2, 0, 0, {entry(1, 1), entry(2, 2)})});
    h.deliver(2, Message{ae(2, 2, 2, 2, {entry(2, 3)})});
    CHECK(h.lastAppendReply().success);
    CHECK(h.lastAppendReply().conflictIndex == 3);  // ack = prev 2 + 1 entry
    CHECK(h.log.lastIndex() == 3);
    CHECK(h.log.entryAt(3) == entry(2, 3));
}

TEST_CASE("conflict resolution: a conflicting suffix is truncated and "
          "replaced") {
    Harness h;
    // Local: terms [1, 1, 2] — index 2..3 will conflict with the leader.
    h.deliver(2, Message{ae(2, 2, 0, 0,
                            {entry(1, 1), entry(1, 2), entry(2, 3)})});
    REQUIRE(h.log.lastIndex() == 3);

    // New leader at term 3 disagrees from index 2 on.
    h.deliver(3, Message{ae(3, 3, 1, 1, {entry(3, 7), entry(3, 8)})});
    CHECK(h.lastAppendReply().success);
    REQUIRE(h.log.lastIndex() == 3);
    CHECK(h.log.entryAt(1) == entry(1, 1));  // matching prefix kept
    CHECK(h.log.entryAt(2) == entry(3, 7));  // conflicting suffix replaced
    CHECK(h.log.entryAt(3) == entry(3, 8));
}

TEST_CASE("no spurious truncation: a delayed prefix/duplicate AppendEntries "
          "never deletes entries") {
    Harness h;
    const std::vector<LogEntry> full{entry(1, 1), entry(1, 2), entry(1, 3)};
    h.deliver(2, Message{ae(1, 2, 0, 0, full, /*commit=*/3)});
    REQUIRE(h.log.lastIndex() == 3);
    REQUIRE(h.core.commitIndex() == 3);
    REQUIRE(h.appliedTags() == std::vector<std::uint8_t>{1, 2, 3});

    SUBCASE("strict prefix of the log") {
        h.deliver(2, Message{ae(1, 2, 0, 0, {entry(1, 1), entry(1, 2)})});
    }
    SUBCASE("exact duplicate") {
        h.deliver(2, Message{ae(1, 2, 0, 0, full)});
    }
    SUBCASE("empty heartbeat at an old prev") {
        h.deliver(2, Message{ae(1, 2, 1, 1, {})});
    }
    CHECK(h.lastAppendReply().success);
    CHECK(h.log.lastIndex() == 3);  // nothing truncated...
    CHECK(h.log.entryAt(3) == entry(1, 3));
    CHECK(h.core.commitIndex() == 3);
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{1, 2, 3});  // ...or re-applied
}

TEST_CASE("leader hint handling: conflictTerm found in leader log resends "
          "from after its last index of that term") {
    Harness h;
    // Leader log terms [1, 2, 2, 3]; election lifts us to term 4.
    h.persist.save(3, std::nullopt);
    h.log.append({entry(1, 1), entry(2, 2), entry(2, 3), entry(3, 4)});
    h.winElection();
    REQUIRE(h.core.term() == 4);

    // Peer 2 reports a conflict in term 2 starting at index 1. The leader
    // holds term 2 through index 3, so it resends from index 4.
    h.deliver(2, Message{AppendEntriesReply{4, false, 1, 2}});
    CHECK(h.lastAppendTo(2).prevLogIndex == 3);
    CHECK(h.lastAppendTo(2).prevLogTerm == 2);
}

TEST_CASE("leader hint handling: conflictTerm absent from leader log jumps "
          "to the follower's first index of it") {
    Harness h;
    h.persist.save(3, std::nullopt);
    h.log.append({entry(1, 1), entry(2, 2), entry(2, 3), entry(3, 4)});
    h.winElection();

    // Term 9 does not exist in the leader's log: jump to conflictIndex 2.
    h.deliver(2, Message{AppendEntriesReply{4, false, 2, 9}});
    CHECK(h.lastAppendTo(2).prevLogIndex == 1);

    // Too-short hint (conflictTerm == none): jump straight to the end of
    // the follower's log, and clamp keeps nextIndex >= 1.
    h.deliver(3, Message{AppendEntriesReply{4, false, 1, 0}});
    CHECK(h.lastAppendTo(3).prevLogIndex == 0);
    CHECK(h.lastAppendTo(3).entries.size() == 4);  // full backfill
}

TEST_CASE("backtracking costs one RPC per conflicting term, not per entry") {
    Harness h;
    // Leader log: 8 entries of term 1, then 1 of term 3.
    h.persist.save(3, std::nullopt);
    std::vector<LogEntry> entries;
    for (std::uint8_t i = 1; i <= 8; ++i) entries.push_back(entry(1, i));
    entries.push_back(entry(3, 9));
    h.log.append(std::move(entries));
    h.winElection();
    const Term cur = h.core.term();

    // Follower 2's log diverged: it reports term-2 entries from index 3 on.
    // One failed round trip must realign nextIndex below the whole term-2
    // run (leader lacks term 2 entirely -> jump to conflictIndex 3).
    h.sent.clear();
    h.deliver(2, Message{AppendEntriesReply{cur, false, 3, 2}});
    std::size_t aeCount = 0;
    for (const auto& [to, m] : h.sent) {
        aeCount += (to == 2 && std::holds_alternative<AppendEntries>(m));
    }
    CHECK(aeCount == 1);  // exactly one retry...
    CHECK(h.lastAppendTo(2).prevLogIndex == 2);  // ...starting below term 2
    CHECK(h.lastAppendTo(2).entries.size() == 7);  // everything in one shot
}

TEST_CASE("Figure 8 commit rule: a prior-term entry on a majority is never "
          "committed by count, only indirectly") {
    Harness h;
    // An uncommitted term-1 entry exists from an earlier leadership.
    h.persist.save(1, std::nullopt);
    h.log.append({entry(1, 0xA)});
    h.winElection();  // term 2 now
    REQUIRE(h.core.term() == 2);
    REQUIRE(h.core.commitIndex() == 0);

    // Both peers ack the term-1 entry: it now sits on ALL nodes, yet must
    // not commit — termAt(1) == 1 != currentTerm 2.
    h.deliver(2, Message{AppendEntriesReply{2, true, 1, 0}});
    h.deliver(3, Message{AppendEntriesReply{2, true, 1, 0}});
    CHECK(h.core.commitIndex() == 0);
    CHECK(h.appliedTags().empty());

    // A current-term entry on top reaches a majority: both commit together.
    const auto idx = h.core.propose({0xB});
    REQUIRE(idx == LogIndex{2});
    h.deliver(2, Message{AppendEntriesReply{2, true, 2, 0}});
    CHECK(h.core.commitIndex() == 2);
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{0xA, 0xB});
}

TEST_CASE("commit advancement: majority of matchIndex plus the leader's own "
          "log, highest qualifying N") {
    Harness h({2, 3, 4, 5});  // 5-node cluster: majority 3
    h.winElection();
    h.sent.clear();  // ignore election-time heartbeats below

    for (std::uint8_t i = 1; i <= 4; ++i) h.core.propose({i});
    REQUIRE(h.log.lastIndex() == 4);

    // Acks: peer2 -> 4, peer3 -> 2. Counting the leader: idx 2 has 3
    // replicas (majority), idx 4 only 2. commitIndex must be exactly 2.
    h.deliver(2, Message{AppendEntriesReply{1, true, 4, 0}});
    CHECK(h.core.commitIndex() == 0);  // leader + peer2 = 2 < 3
    h.deliver(3, Message{AppendEntriesReply{1, true, 2, 0}});
    CHECK(h.core.commitIndex() == 2);
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{1, 2});

    // A stale lower ack must not regress anything.
    h.deliver(2, Message{AppendEntriesReply{1, true, 1, 0}});
    CHECK(h.core.commitIndex() == 2);
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{1, 2});
}

TEST_CASE("apply loop: in order, exactly once, never past commitIndex") {
    Harness h;
    const std::vector<LogEntry> entries{entry(1, 1), entry(1, 2),
                                        entry(1, 3)};
    // leaderCommit beyond what this RPC confirms is capped at prev+n.
    h.deliver(2, Message{ae(1, 2, 0, 0, entries, /*commit=*/10)});
    CHECK(h.core.commitIndex() == 3);
    CHECK(h.core.lastApplied() == 3);
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{1, 2, 3});

    // Heartbeat with a lower leaderCommit: nothing re-applies, nothing
    // regresses.
    h.deliver(2, Message{ae(1, 2, 3, 1, {}, /*commit=*/2)});
    CHECK(h.core.commitIndex() == 3);
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{1, 2, 3});
}

TEST_CASE("apply loop: commit advances stepwise, applies each entry once") {
    Harness h;
    h.deliver(2, Message{ae(1, 2, 0, 0,
                            {entry(1, 1), entry(1, 2), entry(1, 3)},
                            /*commit=*/1)});
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{1});
    h.deliver(2, Message{ae(1, 2, 3, 1, {}, /*commit=*/2)});
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{1, 2});
    h.deliver(2, Message{ae(1, 2, 3, 1, {}, /*commit=*/3)});
    CHECK(h.appliedTags() == std::vector<std::uint8_t>{1, 2, 3});
    CHECK(h.core.lastApplied() == 3);
}

TEST_CASE("propose: leader assigns sequential indices at its term; "
          "non-leader rejects") {
    Harness h;
    CHECK_FALSE(h.core.propose({0x01}).has_value());  // follower

    h.winElection();
    CHECK(h.core.propose({0x01}) == LogIndex{1});
    CHECK(h.core.propose({0x02}) == LogIndex{2});
    CHECK(h.log.lastIndex() == 2);
    CHECK(h.log.termAt(1) == h.core.term());
    CHECK(h.log.termAt(2) == h.core.term());
    // Replication was triggered toward both peers.
    bool sawAeTo2 = false;
    for (const auto& [to, m] : h.sent) {
        sawAeTo2 |= (to == 2 && std::holds_alternative<AppendEntries>(m));
    }
    CHECK(sawAeTo2);
}

TEST_CASE("ack handling: a duplicate / non-advancing success ack never "
          "re-sends AppendEntries (AE<->ack storm regression, Phase 7)") {
    // The Phase 7 perf bug: resending on EVERY success ack while any entry
    // was unacked let duplicate acks (from pipelined propose-time AEs)
    // spawn redundant AEs, each spawning another ack — a self-sustaining
    // storm (measured >170k msgs/s for ~400 client ops/s). The rule this
    // test pins: a follow-up AE is sent ONLY when the ack ADVANCED
    // matchIndex and entries remain; retransmission of lost AEs belongs to
    // the heartbeat timer alone.
    Harness h;
    h.winElection();
    h.core.propose({0xA1});
    h.core.propose({0xA2});

    const auto aeCountTo = [&](NodeId to) {
        std::size_t n = 0;
        for (const auto& [peer, m] : h.sent) {
            if (peer == to && std::holds_alternative<AppendEntries>(m)) ++n;
        }
        return n;
    };

    // An ADVANCING partial ack (index 1 of 2) continues the catch-up.
    const auto before = aeCountTo(2);
    h.deliver(2, Message{AppendEntriesReply{h.core.term(), true,
                                            /*ack=*/1, 0}});
    CHECK(aeCountTo(2) == before + 1);  // follow-up carries the remainder

    // The SAME ack again (duplicate; matchIndex unchanged): no AE, even
    // though index 2 is still unacked.
    h.deliver(2, Message{AppendEntriesReply{h.core.term(), true, 1, 0}});
    CHECK(aeCountTo(2) == before + 1);

    // A stale lower ack: also no AE.
    h.deliver(2, Message{AppendEntriesReply{h.core.term(), true, 0, 0}});
    CHECK(aeCountTo(2) == before + 1);

    // Liveness is the heartbeat's job: the timer retransmits the suffix.
    h.clock.advance(Duration(50));
    h.core.tick();
    CHECK(aeCountTo(2) == before + 2);
    CHECK(h.lastAppendTo(2).prevLogIndex == 1);  // resends from matchIndex+1

    // Once the ack advances past the end, silence even after more acks.
    h.deliver(2, Message{AppendEntriesReply{h.core.term(), true, 2, 0}});
    const auto afterFinal = aeCountTo(2);
    h.deliver(2, Message{AppendEntriesReply{h.core.term(), true, 2, 0}});
    CHECK(aeCountTo(2) == afterFinal);
}
