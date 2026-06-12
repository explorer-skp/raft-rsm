// Phase 9: the FULL Phase 6 chaos suite with the order-book state machine
// swapped in behind the same interface — same seeded fault schedule
// machinery, same five-invariant checkers, same no-lost-commit and
// post-heal convergence assertions. What replaces the KV linearizability
// check: byte-identical books across replicas at quiescence, a
// fresh-engine fold of the committed sequence landing on the same state
// (both inside runChaos), and HERE the golden-model reference replay of
// the committed command stream from every seed. A failing seed reproduces
// with:
//
//     ./build/faults/chaos_sim --seed N --sm orderbook

#include <cstdio>

#include "doctest/doctest.h"
#include "faults/chaos.h"
#include "ob_reference.h"
#include "raft/logging.h"

using rsm::sim::ChaosOptions;
using rsm::sim::runChaos;

namespace {

ChaosOptions orderBookOptions() {
    ChaosOptions options;  // the chaos_sim defaults...
    options.sm = ChaosOptions::Sm::OrderBook;  // ...with the showcase SM
    return options;
}

}  // namespace

TEST_CASE("order-book chaos suite: five invariants, no lost commit, "
          "identical books, golden-model replay across many seeds") {
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Error);
    const ChaosOptions options = orderBookOptions();
    std::size_t totalCompleted = 0;
    std::uint64_t maxTerm = 0;
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        CAPTURE(seed);
        const auto report = runChaos(seed, options);
        if (!report.pass) {
            std::printf("ORDER-BOOK CHAOS FAILURE — reproduce with:\n"
                        "  ./build/faults/chaos_sim --seed %llu "
                        "--sm orderbook\n%s\n",
                        static_cast<unsigned long long>(seed),
                        report.summary().c_str());
        }
        REQUIRE_MESSAGE(report.pass, report.summary());
        CHECK(report.completedOps == report.totalOps);
        CHECK(report.totalOps > 0);

        // Golden-model equivalence on the real committed stream: the
        // independent reference matcher folds the exact sequence the
        // replicas agreed on and must land on the identical book. (The
        // reference itself is self-validated in ob_golden_test.cpp.)
        obref::ReferenceMatcher reference;
        for (const auto& cmd : report.committedCommands) {
            reference.apply(cmd);
        }
        REQUIRE_MESSAGE(
            reference.bookImage() == report.finalBookImage,
            "golden-model divergence at seed "
                << seed << "\nreference:\n" << reference.bookImage()
                << "replicas:\n" << report.finalBookImage);
        totalCompleted += report.completedOps;
        maxTerm = std::max(maxTerm, report.finalTerm);
    }
    CHECK(totalCompleted >= 20 * 40);  // the workload really ran
    CHECK(maxTerm > 5);  // fault schedules really forced re-elections
}

TEST_CASE("order-book determinism: the same seed produces a byte-identical "
          "chaos run (trace, outcome, final book)") {
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Error);
    ChaosOptions opts = orderBookOptions();
    opts.clients = 3;
    opts.opsPerClient = 6;
    opts.faultEndMs = 8000;
    opts.maxMs = 20000;
    opts.recordTrace = true;

    const auto a = runChaos(4242, opts);
    const auto b = runChaos(4242, opts);
    CHECK(a.pass);
    CHECK(a.summary() == b.summary());
    CHECK(a.finalBookImage == b.finalBookImage);
    CHECK(a.committedCommands == b.committedCommands);
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
