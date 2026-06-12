// Phase 9 golden-model equivalence: the production matching engine vs the
// independent reference matcher (ob_reference.h) over seeded randomized
// command streams — every per-command result byte-identical, final books
// identical. Self-validated: each deliberately mis-implemented reference
// variant must be FLAGGED by the same comparison (a check that cannot fail
// proves nothing — the Phase 6 checker discipline).

#include <cstdint>
#include <string>

#include "doctest/doctest.h"
#include "ob_reference.h"
#include "statemachine/order_book.h"

using obref::ReferenceMatcher;
using obref::StreamGen;
using rsm::statemachine::OrderBookStateMachine;

namespace {

struct StreamVerdict {
    bool match = true;
    std::string detail;
};

// Runs one seeded stream through both implementations; reports the first
// divergence (result bytes or final book image).
StreamVerdict runStream(std::uint64_t seed, int ops, ReferenceMatcher::Bug bug) {
    OrderBookStateMachine engine;
    ReferenceMatcher reference(bug);
    StreamGen gen(seed);
    for (int i = 0; i < ops; ++i) {
        const auto cmd = gen.command();
        const std::string a = engine.apply(cmd);
        const std::string b = reference.apply(cmd);
        if (a != b) {
            return {false, "result mismatch at op " + std::to_string(i) +
                               " (seed " + std::to_string(seed) + ")"};
        }
    }
    if (engine.bookImage() != reference.bookImage()) {
        return {false, "final book mismatch (seed " + std::to_string(seed) +
                           ")\nengine:\n" + engine.bookImage() +
                           "reference:\n" + reference.bookImage()};
    }
    return {};
}

}  // namespace

TEST_CASE("ob golden: engine == independent reference over seeded random "
          "streams (results and final book)") {
    for (std::uint64_t seed = 1; seed <= 10; ++seed) {
        CAPTURE(seed);
        const auto v = runStream(seed, 2000, ReferenceMatcher::Bug::None);
        REQUIRE_MESSAGE(v.match, v.detail);
    }
}

TEST_CASE("ob golden self-validation: each deliberately broken reference is "
          "rejected by the same comparison") {
    // One stream is enough per bug if it diverges; sweep a few seeds so the
    // assertion does not hinge on one lucky schedule.
    const auto diverges = [](ReferenceMatcher::Bug bug) {
        for (std::uint64_t seed = 1; seed <= 5; ++seed) {
            if (!runStream(seed, 2000, bug).match) return true;
        }
        return false;
    };
    CHECK(diverges(ReferenceMatcher::Bug::IgnoreTimePriority));
    CHECK(diverges(ReferenceMatcher::Bug::IgnorePricePriority));
    CHECK(diverges(ReferenceMatcher::Bug::TakerPrice));
}
