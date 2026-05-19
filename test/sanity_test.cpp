#include <doctest/doctest.h>

// Phase 0 wiring check: proves the framework compiles, links, registers with
// ctest, and actually evaluates assertions. Real tests replace this from Phase 1.
TEST_CASE("test framework is wired") {
    CHECK(1 + 1 == 2);
}
