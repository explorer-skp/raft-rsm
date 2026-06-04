// Phase 6 checker self-tests. A checker that never fails is worthless: each
// invariant checker is fed a fabricated violation and must FLAG it, plus a
// clean input it must accept. The linearizability checker gets both known-
// linearizable histories (must accept) and hand-crafted non-linearizable
// ones (must reject) — soundness (no false accepts) is the property the
// chaos suite's green light rests on.

#include <string>
#include <vector>

#include "doctest/doctest.h"
#include "faults/checkers.h"
#include "faults/linearizability.h"

using namespace rsm::sim;
using rsm::statemachine::KvOp;

namespace {

EntryImage entry(Term term, const std::string& cmd) {
    rsm::rpc::LogEntry e{term, {cmd.begin(), cmd.end()}};
    return entryImage(e);
}

ClientOp op(std::uint64_t client, std::uint64_t seq, KvOp kind,
            const std::string& key, std::int64_t invoke, std::int64_t ret,
            const std::string& result, const std::string& arg = {},
            const std::string& arg2 = {}) {
    ClientOp o;
    o.clientId = client;
    o.seqNo = seq;
    o.op = kind;
    o.key = key;
    o.arg = arg;
    o.arg2 = arg2;
    o.invokeMs = invoke;
    o.returnMs = ret;
    o.result = result;
    return o;
}

}  // namespace

TEST_CASE("election safety checker: flags a second leader in the same term, "
          "accepts re-observations and distinct terms") {
    ElectionSafetyChecker ok;
    ok.observeLeader(3, 1);
    ok.observeLeader(3, 1);  // same node again: fine
    ok.observeLeader(4, 2);
    CHECK(ok.violations().empty());

    ElectionSafetyChecker bad;
    bad.observeLeader(7, 1);
    bad.observeLeader(7, 3);  // fabricated double leader
    REQUIRE(bad.violations().size() == 1);
    CHECK(bad.violations()[0].find("ELECTION SAFETY") != std::string::npos);
}

TEST_CASE("leader append-only checker: flags shrink and in-place overwrite, "
          "allows growth and follower truncation") {
    const LogImage two = {entry(1, "a"), entry(1, "b")};
    const LogImage three = {entry(1, "a"), entry(1, "b"), entry(2, "c")};

    LeaderAppendOnlyChecker ok;
    ok.observe(1, true, 2, two);
    ok.observe(1, true, 2, three);  // append: fine
    ok.observe(1, false, 3, two);   // truncated as a FOLLOWER: fine
    ok.observe(1, true, 4, two);    // new leadership, new baseline
    CHECK(ok.violations().empty());

    LeaderAppendOnlyChecker shrink;
    shrink.observe(1, true, 2, three);
    shrink.observe(1, true, 2, two);  // leader deleted its own entry
    REQUIRE(shrink.violations().size() == 1);
    CHECK(shrink.violations()[0].find("LEADER APPEND-ONLY") !=
          std::string::npos);

    LeaderAppendOnlyChecker overwrite;
    overwrite.observe(1, true, 2, {entry(1, "a"), entry(1, "b")});
    overwrite.observe(1, true, 2, {entry(1, "a"), entry(1, "X")});
    REQUIRE(overwrite.violations().size() == 1);
}

TEST_CASE("log matching checker: flags same-term divergence beneath the "
          "match point, accepts genuinely consistent prefixes") {
    // Clean: logs agree wherever index+term agree.
    std::map<NodeId, LogImage> clean;
    clean[1] = {entry(1, "a"), entry(1, "b"), entry(2, "c")};
    clean[2] = {entry(1, "a"), entry(1, "b"), entry(3, "x")};
    CHECK(checkLogMatching(clean).empty());

    // Fabricated: same term at index 3, different command at index 2.
    std::map<NodeId, LogImage> bad;
    bad[1] = {entry(1, "a"), entry(1, "b"), entry(2, "c")};
    bad[2] = {entry(1, "a"), entry(1, "X"), entry(2, "c")};
    const auto v = checkLogMatching(bad);
    REQUIRE(v.size() == 1);
    CHECK(v[0].find("LOG MATCHING") != std::string::npos);
}

TEST_CASE("applied-consistency checker: flags divergent commands and "
          "divergent results at one index") {
    AppliedConsistencyChecker ok;
    ok.observeApply(1, 1, entry(1, "a"), 11);
    ok.observeApply(2, 1, entry(1, "a"), 11);  // same everywhere: fine
    ok.observeApply(1, 1, entry(1, "a"), 11);  // re-apply after restart: fine
    CHECK(ok.violations().empty());

    AppliedConsistencyChecker badCmd;
    badCmd.observeApply(1, 5, entry(2, "a"), 11);
    badCmd.observeApply(2, 5, entry(2, "B"), 11);
    REQUIRE(badCmd.violations().size() == 1);
    CHECK(badCmd.violations()[0].find("STATE MACHINE SAFETY") !=
          std::string::npos);

    AppliedConsistencyChecker badResult;
    badResult.observeApply(1, 5, entry(2, "a"), 11);
    badResult.observeApply(2, 5, entry(2, "a"), 99);
    REQUIRE(badResult.violations().size() == 1);
    CHECK(badResult.violations()[0].find("APPLY DETERMINISM") !=
          std::string::npos);
}

TEST_CASE("commit checker: flags a committed entry changing and a new "
          "leader missing committed entries") {
    const LogImage committed = {entry(1, "a"), entry(1, "b")};

    CommitChecker ok;
    ok.observeCommit(1, 2, committed);
    ok.observeCommit(2, 1, committed);  // lagging commitIndex: fine
    ok.observeElectionWin(3, 2, {entry(1, "a"), entry(1, "b"), entry(2, "c")});
    CHECK(ok.violations().empty());
    CHECK(ok.committed().size() == 2);

    CommitChecker mutate;
    mutate.observeCommit(1, 2, committed);
    mutate.observeCommit(2, 2, {entry(1, "a"), entry(2, "X")});  // changed!
    REQUIRE(mutate.violations().size() == 1);
    CHECK(mutate.violations()[0].find("NO LOST COMMIT") != std::string::npos);

    CommitChecker incomplete;
    incomplete.observeCommit(1, 2, committed);
    incomplete.observeElectionWin(2, 3, {entry(1, "a")});  // too short
    REQUIRE(incomplete.violations().size() == 1);
    CHECK(incomplete.violations()[0].find("LEADER COMPLETENESS") !=
          std::string::npos);

    CommitChecker divergent;
    divergent.observeCommit(1, 2, committed);
    divergent.observeElectionWin(2, 3, {entry(1, "a"), entry(3, "z")});
    REQUIRE(divergent.violations().size() == 1);
    CHECK(divergent.violations()[0].find("LEADER COMPLETENESS") !=
          std::string::npos);
}

// ---------------------------------------------------------------------------
// Linearizability checker self-validation.
// ---------------------------------------------------------------------------

TEST_CASE("linearizability: accepts sequential and validly-concurrent "
          "histories") {
    SUBCASE("sequential puts and gets") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Put, "k", 0, 10, "O", "v1"),
            op(1, 2, KvOp::Get, "k", 20, 30, "Ov1"),
            op(2, 1, KvOp::Put, "k", 40, 50, "O", "v2"),
            op(1, 3, KvOp::Get, "k", 60, 70, "Ov2"),
        };
        CHECK(checkLinearizable(h).ok);
    }
    SUBCASE("concurrent put and get: either old or new value is legal") {
        std::vector<ClientOp> seesNew = {
            op(1, 1, KvOp::Put, "k", 0, 10, "O", "v1"),
            op(2, 1, KvOp::Put, "k", 20, 60, "O", "v2"),
            op(3, 1, KvOp::Get, "k", 30, 40, "Ov2"),  // during the PUT
        };
        CHECK(checkLinearizable(seesNew).ok);
        std::vector<ClientOp> seesOld = {
            op(1, 1, KvOp::Put, "k", 0, 10, "O", "v1"),
            op(2, 1, KvOp::Put, "k", 20, 60, "O", "v2"),
            op(3, 1, KvOp::Get, "k", 30, 40, "Ov1"),  // not yet: also legal
        };
        CHECK(checkLinearizable(seesOld).ok);
    }
    SUBCASE("cas chain with absent-means-empty semantics") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Cas, "k", 0, 10, "O", "", "a"),    // create
            op(2, 1, KvOp::Cas, "k", 20, 30, "F", "x", "b"),  // mismatch
            op(1, 2, KvOp::Cas, "k", 40, 50, "O", "a", "c"),
            op(2, 2, KvOp::Get, "k", 60, 70, "Oc"),
        };
        CHECK(checkLinearizable(h).ok);
    }
    SUBCASE("append observes accumulated value; delete returns N on absent") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Delete, "k", 0, 10, "N"),
            op(1, 2, KvOp::Append, "k", 20, 30, "Ox", "x"),
            op(2, 1, KvOp::Append, "k", 40, 50, "Oxy", "y"),
            op(1, 3, KvOp::Delete, "k", 60, 70, "O"),
            op(2, 2, KvOp::Get, "k", 80, 90, "N"),
        };
        CHECK(checkLinearizable(h).ok);
    }
    SUBCASE("incomplete op may have taken effect — or not") {
        // Pending PUT, later GET sees its value: only linearizable if the
        // pending op is allowed to take effect.
        std::vector<ClientOp> tookEffect = {
            op(1, 1, KvOp::Put, "k", 0, -1, "", "v1"),  // never acked
            op(2, 1, KvOp::Get, "k", 10, 20, "Ov1"),
        };
        CHECK(checkLinearizable(tookEffect).ok);
        // Same pending PUT, GET sees nothing: also fine (op never ran).
        std::vector<ClientOp> noEffect = {
            op(1, 1, KvOp::Put, "k", 0, -1, "", "v1"),
            op(2, 1, KvOp::Get, "k", 10, 20, "N"),
        };
        CHECK(checkLinearizable(noEffect).ok);
    }
    SUBCASE("independent keys are checked independently") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Put, "a", 0, 10, "O", "x"),
            op(2, 1, KvOp::Put, "b", 0, 10, "O", "y"),
            op(1, 2, KvOp::Get, "b", 20, 30, "Oy"),
            op(2, 2, KvOp::Get, "a", 20, 30, "Ox"),
        };
        CHECK(checkLinearizable(h).ok);
    }
}

TEST_CASE("linearizability: rejects hand-crafted non-linearizable "
          "histories") {
    SUBCASE("stale read after a completed overwrite") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Put, "k", 0, 10, "O", "v1"),
            op(2, 1, KvOp::Put, "k", 20, 30, "O", "v2"),  // strictly after
            op(1, 2, KvOp::Get, "k", 40, 50, "Ov1"),      // sees v1: stale!
        };
        CHECK_FALSE(checkLinearizable(h).ok);
    }
    SUBCASE("read of a value never written") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Put, "k", 0, 10, "O", "v1"),
            op(2, 1, KvOp::Get, "k", 20, 30, "Oghost"),
        };
        CHECK_FALSE(checkLinearizable(h).ok);
    }
    SUBCASE("two sequential reads observing writes in reverse order") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Put, "k", 0, 10, "O", "v1"),
            op(2, 1, KvOp::Put, "k", 5, 15, "O", "v2"),  // concurrent w/ 1st
            op(3, 1, KvOp::Get, "k", 20, 30, "Ov2"),
            op(3, 2, KvOp::Get, "k", 40, 50, "Ov1"),  // value went BACK
        };
        CHECK_FALSE(checkLinearizable(h).ok);
    }
    SUBCASE("CAS succeeds against an expected value that never existed") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Put, "k", 0, 10, "O", "v1"),
            op(2, 1, KvOp::Cas, "k", 20, 30, "O", "never", "v2"),
        };
        CHECK_FALSE(checkLinearizable(h).ok);
    }
    SUBCASE("lost update: append result missing a committed prior append") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Append, "k", 0, 10, "Ox", "x"),
            op(2, 1, KvOp::Append, "k", 20, 30, "Oy", "y"),  // lost "x"!
        };
        CHECK_FALSE(checkLinearizable(h).ok);
    }
    SUBCASE("read ignores a write that completed strictly before it") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Put, "k", 0, 10, "O", "v1"),
            op(2, 1, KvOp::Get, "k", 20, 30, "N"),  // must see SOMETHING
        };
        CHECK_FALSE(checkLinearizable(h).ok);
    }
    SUBCASE("explanation names the failing key") {
        std::vector<ClientOp> h = {
            op(1, 1, KvOp::Put, "badkey", 0, 10, "O", "v1"),
            op(2, 1, KvOp::Get, "badkey", 20, 30, "N"),
        };
        const auto r = checkLinearizable(h);
        REQUIRE_FALSE(r.ok);
        CHECK(r.explanation.find("badkey") != std::string::npos);
    }
}

TEST_CASE("linearizability model mirrors KVStateMachine semantics") {
    KvRegister reg;
    ClientOp o;
    o.op = KvOp::Get;
    CHECK(kvModelApply(reg, o) == "N");
    o.op = KvOp::Cas;
    o.arg = "";
    o.arg2 = "made";
    CHECK(kvModelApply(reg, o) == "O");  // absent == "" for CAS
    o.op = KvOp::Get;
    CHECK(kvModelApply(reg, o) == "Omade");
    o.op = KvOp::Delete;
    CHECK(kvModelApply(reg, o) == "O");
    o.op = KvOp::Append;
    o.arg = "x";
    CHECK(kvModelApply(reg, o) == "Ox");  // append creates from empty
    o.op = KvOp::Put;
    o.arg = "";
    CHECK(kvModelApply(reg, o) == "O");
    o.op = KvOp::Get;
    CHECK(kvModelApply(reg, o) == "O");  // empty value is NOT absent
}
