// Phase 2 unit tests: up-to-date comparison, vote rules, timer-reset rules,
// step-down. Deterministic: ManualClock + seeded RNG, no threads, sends
// captured into a vector.

#include <cstdint>
#include <utility>
#include <vector>

#include "doctest/doctest.h"
#include "raft/raft_core.h"
#include "statemachine/state_machine.h"
#include "storage/log.h"

using rsm::raft::candidateLogAtLeastAsUpToDate;
using rsm::raft::Duration;
using rsm::raft::InMemoryPersistentState;
using rsm::raft::ManualClock;
using rsm::raft::RaftConfig;
using rsm::raft::RaftCore;
using rsm::raft::Role;
using rsm::raft::TimePoint;
using rsm::rpc::AppendEntries;
using rsm::rpc::AppendEntriesReply;
using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::Message;
using rsm::rpc::NodeId;
using rsm::rpc::RequestVote;
using rsm::rpc::RequestVoteReply;
using rsm::rpc::Term;

namespace {

// Fixed-width election timeout (min == max) so every reset moves the
// deadline to exactly now + 150ms — timer-reset rules become exact asserts.
RaftConfig fixedTimeoutConfig() {
    RaftConfig cfg;
    cfg.electionTimeoutMin = Duration(150);
    cfg.electionTimeoutMax = Duration(150);
    cfg.heartbeatInterval = Duration(50);
    return cfg;
}

struct Harness {
    ManualClock clock;
    InMemoryPersistentState persist;
    rsm::storage::InMemoryLog log;
    rsm::statemachine::RecordingStateMachine sm;
    std::vector<std::pair<NodeId, Message>> sent;
    RaftCore core;

    explicit Harness(std::vector<NodeId> peers = {2, 3},
                     RaftConfig cfg = fixedTimeoutConfig())
        : core(1, std::move(peers), persist, log, sm, clock, /*rngSeed=*/42,
               cfg, [this](NodeId to, const Message& m) {
                   sent.emplace_back(to, m);
               }) {
        core.start();
    }

    void deliver(NodeId from, const Message& m) {
        core.handle(Envelope{kProtocolVersion, rsm::rpc::typeOf(m), from,
                             core.selfId(), 0},
                    m);
    }

    // Advance the virtual clock to the election deadline and tick: the node
    // starts an election (allowed reset event 3).
    void forceElection() {
        REQUIRE(core.electionDeadline() != TimePoint::max());
        clock.advance(std::chrono::duration_cast<Duration>(
            core.electionDeadline() - clock.now()));
        core.tick();
        REQUIRE(core.role() == Role::Candidate);
    }

    // Candidate -> Leader by delivering the needed granted replies.
    void winElection(const std::vector<NodeId>& granters) {
        forceElection();
        const Term t = core.term();
        for (const NodeId g : granters) {
            deliver(g, Message{RequestVoteReply{t, true}});
        }
        REQUIRE(core.role() == Role::Leader);
    }

    const RequestVoteReply& lastVoteReply() const {
        return std::get<RequestVoteReply>(sent.back().second);
    }
    const AppendEntriesReply& lastAppendReply() const {
        return std::get<AppendEntriesReply>(sent.back().second);
    }
};

AppendEntries heartbeat(Term term, NodeId leader) {
    return AppendEntries{term, leader, 0, 0, 0, {}};
}

}  // namespace

TEST_CASE("up-to-date comparison (Raft 5.4.1) truth table") {
    struct Case {
        Term candTerm;
        std::uint64_t candIdx;
        Term myTerm;
        std::uint64_t myIdx;
        bool expectGrantAllowed;
    };
    const Case cases[] = {
        {0, 0, 0, 0, true},    // both logs empty: equally up-to-date
        {3, 7, 3, 7, true},    // identical last entry: equally up-to-date
        {2, 1, 1, 5, true},    // higher last term beats a longer log
        {1, 5, 2, 1, false},   // lower last term loses despite longer log
        {3, 8, 3, 7, true},    // same term, longer log wins
        {3, 6, 3, 7, false},   // same term, shorter log loses
        {1, 1, 0, 0, true},    // anything beats an empty log
        {0, 0, 1, 1, false},   // empty log never beats a non-empty one
        {UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX, true},  // boundary
        {UINT64_MAX, 0, 1, UINT64_MAX, true},                    // boundary
    };
    for (const auto& c : cases) {
        CAPTURE(c.candTerm);
        CAPTURE(c.candIdx);
        CAPTURE(c.myTerm);
        CAPTURE(c.myIdx);
        CHECK(candidateLogAtLeastAsUpToDate(c.candTerm, c.candIdx, c.myTerm,
                                            c.myIdx) == c.expectGrantAllowed);
    }
}

TEST_CASE("vote granted once per term; second candidate denied") {
    Harness h;
    h.deliver(2, Message{RequestVote{1, 2, 0, 0}});
    CHECK(h.lastVoteReply().voteGranted);
    CHECK(h.core.votedFor() == NodeId{2});
    CHECK(h.core.term() == Term{1});

    // A different candidate in the same term is denied.
    h.deliver(3, Message{RequestVote{1, 3, 0, 0}});
    CHECK_FALSE(h.lastVoteReply().voteGranted);
    CHECK(h.core.votedFor() == NodeId{2});

    // The candidate we already voted for is re-granted (idempotent).
    h.deliver(2, Message{RequestVote{1, 2, 0, 0}});
    CHECK(h.lastVoteReply().voteGranted);
}

TEST_CASE("RequestVote with term below currentTerm is denied") {
    Harness h;
    h.deliver(2, Message{RequestVote{5, 2, 0, 0}});  // lifts term to 5
    REQUIRE(h.core.term() == Term{5});
    h.deliver(3, Message{RequestVote{3, 3, 0, 0}});
    CHECK_FALSE(h.lastVoteReply().voteGranted);
    CHECK(h.lastVoteReply().term == Term{5});
    CHECK(h.core.term() == Term{5});
}

TEST_CASE("higher-term RequestVote steps a candidate down, clears votedFor, "
          "then may grant") {
    Harness h;
    h.forceElection();
    REQUIRE(h.core.term() == Term{1});
    REQUIRE(h.core.votedFor() == NodeId{1});  // voted for self

    h.deliver(2, Message{RequestVote{2, 2, 0, 0}});
    CHECK(h.core.role() == Role::Follower);
    CHECK(h.core.term() == Term{2});
    CHECK(h.lastVoteReply().voteGranted);  // cleared vote allowed the grant
    CHECK(h.core.votedFor() == NodeId{2});
}

TEST_CASE("candidate denies a rival candidate at the same term") {
    Harness h;
    h.forceElection();  // votedFor = self at term 1
    h.deliver(2, Message{RequestVote{1, 2, 0, 0}});
    CHECK_FALSE(h.lastVoteReply().voteGranted);
    CHECK(h.core.role() == Role::Candidate);
    CHECK(h.core.votedFor() == NodeId{1});
}

TEST_CASE("timer resets on the three allowed events only") {
    Harness h;  // fixed 150ms timeout: reset <=> deadline == now + 150ms
    const auto expectedAfterReset = [&] {
        return h.clock.now() + Duration(150);
    };

    SUBCASE("valid AppendEntries from current leader resets") {
        h.clock.advance(Duration(10));
        h.deliver(2, Message{heartbeat(1, 2)});
        CHECK(h.core.electionDeadline() == expectedAfterReset());
        CHECK(h.lastAppendReply().success);
        CHECK(h.core.leaderId() == NodeId{2});
    }

    SUBCASE("granting a vote resets") {
        h.clock.advance(Duration(10));
        h.deliver(2, Message{RequestVote{1, 2, 0, 0}});
        REQUIRE(h.lastVoteReply().voteGranted);
        CHECK(h.core.electionDeadline() == expectedAfterReset());
    }

    SUBCASE("starting an election resets") {
        h.forceElection();
        CHECK(h.core.electionDeadline() == expectedAfterReset());
    }

    SUBCASE("stale AppendEntries does NOT reset") {
        h.deliver(2, Message{heartbeat(2, 2)});  // term now 2, timer reset
        const TimePoint armed = h.core.electionDeadline();
        h.clock.advance(Duration(10));
        h.deliver(3, Message{heartbeat(1, 3)});  // stale term 1
        CHECK_FALSE(h.lastAppendReply().success);
        CHECK(h.core.electionDeadline() == armed);
    }

    SUBCASE("stale RequestVote does NOT reset") {
        h.deliver(2, Message{heartbeat(2, 2)});
        const TimePoint armed = h.core.electionDeadline();
        h.clock.advance(Duration(10));
        h.deliver(3, Message{RequestVote{1, 3, 0, 0}});
        CHECK_FALSE(h.lastVoteReply().voteGranted);
        CHECK(h.core.electionDeadline() == armed);
    }

    SUBCASE("un-granted RequestVote at the current term does NOT reset") {
        h.deliver(2, Message{RequestVote{1, 2, 0, 0}});  // grant to 2; resets
        const TimePoint armed = h.core.electionDeadline();
        h.clock.advance(Duration(10));
        h.deliver(3, Message{RequestVote{1, 3, 0, 0}});  // denied: voted for 2
        CHECK_FALSE(h.lastVoteReply().voteGranted);
        CHECK(h.core.electionDeadline() == armed);
    }

    SUBCASE("higher-term step-down via a denied RequestVote does NOT reset") {
        // Non-empty-log up-to-date denial is impossible this phase (all logs
        // are 0/0), so use a rival candidate scenario: candidate at term 1
        // sees RequestVote at term 3 with a stale log... logs are equal, so
        // to observe an un-granted higher-term RPC use a higher-term
        // AppendEntriesReply instead (no grant, no AE-reset path).
        h.forceElection();
        const TimePoint armed = h.core.electionDeadline();
        h.clock.advance(Duration(10));
        h.deliver(2, Message{AppendEntriesReply{4, false, 0, 0}});
        CHECK(h.core.role() == Role::Follower);
        CHECK(h.core.term() == Term{4});
        CHECK(h.core.electionDeadline() == armed);  // step-down didn't reset
    }
}

TEST_CASE("election timeout as candidate starts a new election in a higher "
          "term") {
    Harness h;
    h.forceElection();
    CHECK(h.core.term() == Term{1});
    h.forceElection();  // timer expired without a majority
    CHECK(h.core.term() == Term{2});
    CHECK(h.core.votedFor() == NodeId{1});
}

TEST_CASE("candidate wins with majority and immediately heartbeats") {
    Harness h;
    h.forceElection();
    const std::size_t sendsBefore = h.sent.size();
    h.deliver(2, Message{RequestVoteReply{1, true}});  // self + 2 = majority
    CHECK(h.core.role() == Role::Leader);
    CHECK(h.core.leaderId() == NodeId{1});
    // Immediate empty AppendEntries to both peers.
    REQUIRE(h.sent.size() == sendsBefore + 2);
    for (std::size_t i = sendsBefore; i < h.sent.size(); ++i) {
        const auto& ae = std::get<AppendEntries>(h.sent[i].second);
        CHECK(ae.term == Term{1});
        CHECK(ae.leaderId == NodeId{1});
        CHECK(ae.entries.empty());
    }
}

TEST_CASE("replies from a stale election term are never counted") {
    Harness h({2, 3, 4, 5});  // 5-node cluster: majority is 3
    h.forceElection();        // term 1
    h.forceElection();        // timed out; now term 2
    REQUIRE(h.core.term() == Term{2});

    // Grants from the abandoned term-1 election arrive late: ignored.
    h.deliver(2, Message{RequestVoteReply{1, true}});
    h.deliver(3, Message{RequestVoteReply{1, true}});
    CHECK(h.core.role() == Role::Candidate);

    // Current-term grants win it: self + 2 + 3 = 3 votes.
    h.deliver(2, Message{RequestVoteReply{2, true}});
    CHECK(h.core.role() == Role::Candidate);
    h.deliver(3, Message{RequestVoteReply{2, true}});
    CHECK(h.core.role() == Role::Leader);
}

TEST_CASE("duplicate grants from one peer count once") {
    Harness h({2, 3, 4, 5});  // majority 3
    h.forceElection();
    h.deliver(2, Message{RequestVoteReply{1, true}});
    h.deliver(2, Message{RequestVoteReply{1, true}});
    h.deliver(2, Message{RequestVoteReply{1, true}});
    CHECK(h.core.role() == Role::Candidate);  // still only self + 2
    h.deliver(4, Message{RequestVoteReply{1, true}});
    CHECK(h.core.role() == Role::Leader);
}

TEST_CASE("denied replies do not count toward majority") {
    Harness h;
    h.forceElection();
    h.deliver(2, Message{RequestVoteReply{1, false}});
    h.deliver(3, Message{RequestVoteReply{1, false}});
    CHECK(h.core.role() == Role::Candidate);
}

TEST_CASE("candidate converts to follower on AppendEntries from a leader at "
          "its own term") {
    Harness h;
    h.forceElection();
    h.deliver(2, Message{heartbeat(1, 2)});
    CHECK(h.core.role() == Role::Follower);
    CHECK(h.core.term() == Term{1});
    CHECK(h.core.leaderId() == NodeId{2});
    CHECK(h.lastAppendReply().success);
}

TEST_CASE("step-down: leader or candidate seeing any higher-term RPC or "
          "reply becomes follower with votedFor cleared") {
    SUBCASE("candidate, via RequestVoteReply") {
        Harness h;
        h.forceElection();
        h.deliver(2, Message{RequestVoteReply{5, false}});
        CHECK(h.core.role() == Role::Follower);
        CHECK(h.core.term() == Term{5});
        CHECK_FALSE(h.core.votedFor().has_value());
    }
    SUBCASE("leader, via AppendEntriesReply") {
        Harness h;
        h.winElection({2});
        h.deliver(3, Message{AppendEntriesReply{7, false, 0, 0}});
        CHECK(h.core.role() == Role::Follower);
        CHECK(h.core.term() == Term{7});
        CHECK_FALSE(h.core.votedFor().has_value());
        // A deposed leader must leave with an armed election timer or it
        // could never campaign again.
        CHECK(h.core.electionDeadline() != TimePoint::max());
    }
    SUBCASE("leader, via higher-term AppendEntries from a new leader") {
        Harness h;
        h.winElection({2});
        h.deliver(3, Message{heartbeat(9, 3)});
        CHECK(h.core.role() == Role::Follower);
        CHECK(h.core.term() == Term{9});
        CHECK(h.core.leaderId() == NodeId{3});
        CHECK(h.lastAppendReply().success);
    }
    SUBCASE("leader, via higher-term RequestVote") {
        Harness h;
        h.winElection({2});
        h.deliver(2, Message{RequestVote{3, 2, 0, 0}});
        CHECK(h.core.role() == Role::Follower);
        CHECK(h.core.term() == Term{3});
        CHECK(h.lastVoteReply().voteGranted);  // votedFor was cleared
    }
}

TEST_CASE("leader sends heartbeats every interval and never election-times "
          "out") {
    Harness h;
    h.winElection({2});
    const std::size_t sendsAfterWin = h.sent.size();
    CHECK(h.core.electionDeadline() == TimePoint::max());

    // Far past any election timeout: a leader must not campaign.
    for (int i = 0; i < 20; ++i) {
        h.clock.advance(Duration(50));
        h.core.tick();
    }
    CHECK(h.core.role() == Role::Leader);
    CHECK(h.core.term() == Term{1});
    // 20 heartbeat intervals elapsed -> 20 rounds x 2 peers.
    CHECK(h.sent.size() == sendsAfterWin + 40);
}

TEST_CASE("client messages are ignored without state change in Phase 2") {
    Harness h;
    const TimePoint armed = h.core.electionDeadline();
    h.deliver(2, Message{rsm::rpc::ClientRequest{1, 1, {0x01}}});
    CHECK(h.core.role() == Role::Follower);
    CHECK(h.core.term() == Term{0});
    CHECK(h.core.electionDeadline() == armed);
    CHECK(h.sent.empty());
}
