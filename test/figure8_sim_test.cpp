// Figure 8 commit safety, end to end, closing the Phase 3 deferral: the
// full five-node scenario from the Raft paper, constructed deterministically
// with the Phase 6 partition/crash controls (fixed per-node election
// timeouts make every election winner scripted, not lucky).
//
// Shape (terms are captured at runtime, not hardcoded):
//   1. S1 leads termA; E1 at index 1 commits everywhere.
//   2. Partition {S1,S2} | {S3,S4,S5}. S1 appends E2 (index 2, termA) —
//      reaches only S2: NOT a majority, uncommitted.
//   3. S5 wins termB on the majority side, appends E2' (index 2, termB)
//      locally, and is immediately isolated: E2' lives only on S5.
//   4. Partition {S1,S2,S3,S4} | {S5}. S1 steps down on seeing termB,
//      re-wins as leader of termC, and its replication spreads E2 (termA)
//      to S3/S4 — E2 now sits on a MAJORITY, but at an old term.
//   5. THE assertion: commitIndex stays at 1 across hundreds of
//      heartbeat-rich milliseconds — replica count alone never commits an
//      old-term entry.
//   6a. Overwrite branch: kill S1, heal S5. S5's last term (termB) beats
//       everyone's termA tail, it wins, and E2' OVERWRITES the
//       majority-replicated E2 — legal precisely because E2 never
//       committed, and exactly what counting replicas would have broken.
//   6b. Commit branch: S1 instead appends E3 in termC; when E3 commits, E2
//       commits indirectly beneath it and survives everything after.

#include <string>

#include "doctest/doctest.h"
#include "faults/sim_harness.h"
#include "raft/logging.h"
#include "statemachine/kv_store.h"

using namespace rsm::sim;
using rsm::raft::Duration;
using rsm::raft::RaftConfig;
using rsm::raft::Role;
using rsm::rpc::Term;

namespace {

RaftConfig fixedTimeout(int ms) {
    RaftConfig cfg;
    cfg.electionTimeoutMin = Duration(ms);
    cfg.electionTimeoutMax = Duration(ms);  // min == max: scripted elections
    cfg.heartbeatInterval = Duration(50);
    return cfg;
}

std::vector<std::uint8_t> cmd(const std::string& key) {
    return rsm::statemachine::encodeKvCommand(
        0, 0, rsm::statemachine::KvOp::Put, key, "v");
}

struct Figure8 {
    SimHarness h;
    Term termA = 0;  // S1's first leadership (E1, E2)
    Term termB = 0;  // S5's leadership (E2')
    Term termC = 0;  // S1's second leadership

    Figure8()
        : h([] {
              HarnessOptions ho;
              // Fixed, distinct timeouts script every election: S1 always
              // campaigns first cluster-wide, S5 first on the {3,4,5} side.
              ho.nodes = {SimNodeConfig{1, fixedTimeout(150)},
                          SimNodeConfig{2, fixedTimeout(500)},
                          SimNodeConfig{3, fixedTimeout(600)},
                          SimNodeConfig{4, fixedTimeout(600)},
                          SimNodeConfig{5, fixedTimeout(300)}};
              ho.netSeed = 8;
              return ho;
          }()) {
        h.net().setLatency(Duration(1), Duration(2));

        // 1. S1 leads; E1 commits everywhere.
        REQUIRE(h.runUntil(2000, [&] {
            return h.core(1).role() == Role::Leader && h.converged();
        }));
        termA = h.core(1).term();
        REQUIRE(h.core(1).propose(cmd("e1")) == 1);
        REQUIRE(h.runUntil(2000, [&] {
            for (NodeId id = 1; id <= 5; ++id) {
                if (h.core(id).commitIndex() != 1) return false;
            }
            return true;
        }));

        // 2. Partition; E2 reaches only S2.
        h.net().partition({{1, 2}, {3, 4, 5}});
        REQUIRE(h.core(1).propose(cmd("e2")) == 2);
        REQUIRE(h.runUntil(1000, [&] { return h.log(2).lastIndex() == 2; }));
        CHECK(h.log(3).lastIndex() == 1);
        CHECK(h.core(1).commitIndex() == 1);  // 2/5 is not a majority

        // 3. S5 wins the majority side, appends E2' locally, is cut off.
        REQUIRE(h.runUntil(2000, [&] {
            return h.core(5).role() == Role::Leader;
        }));
        termB = h.core(5).term();
        CHECK(termB > termA);
        REQUIRE(h.core(5).propose(cmd("e2prime")) == 2);
        h.net().partition({{1, 2, 3, 4}, {5}});  // E2' never leaves S5

        // 4. S1 steps down on termB evidence, re-wins as termC, and its
        // replication spreads the old-term E2 to S3/S4 — majority.
        REQUIRE(h.runUntil(3000, [&] {
            return h.core(1).role() == Role::Leader &&
                   h.core(1).term() > termB && h.log(3).lastIndex() == 2 &&
                   h.log(4).lastIndex() == 2;
        }));
        termC = h.core(1).term();
        CHECK(h.log(3).termAt(2) == termA);
        CHECK(h.log(4).termAt(2) == termA);

        // 5. THE Figure 8 assertion: majority-replicated, but old-term —
        // not committed, across 500 heartbeat-rich milliseconds.
        for (int i = 0; i < 500; ++i) {
            h.stepMs();
            for (NodeId id = 1; id <= 4; ++id) {
                REQUIRE(h.core(id).commitIndex() == 1);
            }
        }
    }
};

}  // namespace

TEST_CASE("figure 8 / overwrite branch: the uncommitted old-term entry on a "
          "majority is legally overwritten by a leader that never saw it") {
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Error);
    Figure8 f;
    auto& h = f.h;

    // 6a. S1 dies; S5 rejoins. S5's last entry carries termB > termA tails,
    // so S5 out-up-to-dates everyone and must win.
    h.crash(1);
    h.net().heal();
    REQUIRE(h.runUntil(3000, [&] {
        return h.core(5).role() == Role::Leader;
    }));

    // S5's replication overwrites E2 (termA) with E2' (termB) on S2/S3/S4:
    // a majority-replicated entry vanished — legal, it never committed.
    REQUIRE(h.runUntil(2000, [&] {
        for (NodeId id = 2; id <= 5; ++id) {
            if (h.log(id).lastIndex() < 2 ||
                h.log(id).termAt(2) != f.termB) {
                return false;
            }
        }
        return true;
    }));

    // E2' (still old-term for S5's new leadership) commits only beneath a
    // current-term entry, exactly like step 5 taught.
    CHECK(h.core(5).commitIndex() == 1);
    REQUIRE(h.core(5).propose(cmd("e3")) == 3);
    REQUIRE(h.runUntil(2000, [&] { return h.core(5).commitIndex() == 3; }));

    // The crashed S1 returns and is repaired to the winning history.
    h.restart(1);
    REQUIRE(h.runUntil(3000, [&] { return h.quiescent(); }));
    for (NodeId id = 1; id <= 5; ++id) {
        CHECK(h.log(id).termAt(2) == f.termB);  // E2 is gone everywhere
    }
    h.finalCheck();
    CHECK(h.violations().empty());  // incl. CommitChecker: E2 never committed
}

TEST_CASE("figure 8 / commit branch: a current-term entry on top commits "
          "the old-term entry indirectly, and it then survives everything") {
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Error);
    Figure8 f;
    auto& h = f.h;

    // 6b. S1 appends E3 in its CURRENT term; committing E3 commits E2
    // beneath it. This is the only sound way an old-term entry commits.
    REQUIRE(h.core(1).propose(cmd("e3")) == 3);
    REQUIRE(h.runUntil(2000, [&] {
        for (NodeId id = 1; id <= 4; ++id) {
            if (h.core(id).commitIndex() != 3) return false;
        }
        return true;
    }));
    for (NodeId id = 1; id <= 4; ++id) {
        CHECK(h.log(id).termAt(2) == f.termA);  // E2, now committed
    }

    // S5 rejoins with its competing E2'. Its log loses the up-to-date
    // check now (its last term is termB < E3's term), so no future leader
    // lacks E2: Leader Completeness holds and S5 is repaired.
    h.net().heal();
    REQUIRE(h.runUntil(4000, [&] { return h.quiescent(); }));
    for (NodeId id = 1; id <= 5; ++id) {
        CHECK(h.log(id).termAt(2) == f.termA);
        CHECK(h.core(id).commitIndex() >= 3);
    }
    h.finalCheck();
    CHECK(h.violations().empty());
}
