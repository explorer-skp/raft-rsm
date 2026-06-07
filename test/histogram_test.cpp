#include <cstdint>

#include "doctest/doctest.h"
#include "metrics/histogram.h"

using rsm::metrics::LatencyHistogram;

TEST_CASE("histogram bucket mapping is monotone and self-consistent") {
    // indexOf must be nondecreasing in the value, and every value must not
    // exceed the upper edge of its own bucket (quantiles report upper edges,
    // so this is what bounds the error from above).
    int prev = 0;
    for (std::uint64_t v = 0; v < 100'000; v += 7) {
        const int idx = LatencyHistogram::indexOf(v);
        CHECK(idx >= prev);
        CHECK(v <= LatencyHistogram::upperEdge(idx));
        prev = idx;
    }
    // Spot-check the giant range, including the clamp at the top.
    CHECK(LatencyHistogram::indexOf(UINT64_MAX) == LatencyHistogram::kBuckets - 1);
    CHECK(LatencyHistogram::indexOf(1) == 1);
    CHECK(LatencyHistogram::indexOf(63) == 63);
}

TEST_CASE("histogram quantiles are correct within bucket resolution") {
    LatencyHistogram h;
    // 1..1000 recorded once each: p50 ~ 500, p99 ~ 990, max = 1000. Bucket
    // width at v=1000 is 1024/64 = 16, so allow that much slack upward.
    for (std::uint64_t v = 1; v <= 1000; ++v) h.record(v);
    CHECK(h.count() == 1000);
    CHECK(h.minValue() == 1);
    CHECK(h.maxValue() == 1000);
    CHECK(h.mean() == doctest::Approx(500.5));

    const auto p50 = h.valueAtQuantile(0.50);
    CHECK(p50 >= 500);
    CHECK(p50 <= 516);
    const auto p99 = h.valueAtQuantile(0.99);
    CHECK(p99 >= 990);
    CHECK(p99 <= 1000);
    CHECK(h.valueAtQuantile(1.0) == 1000);
    CHECK(h.valueAtQuantile(0.0) <= 16);
}

TEST_CASE("histogram merge equals recording into one") {
    LatencyHistogram a, b, all;
    for (std::uint64_t v = 0; v < 5000; ++v) {
        ((v % 2) ? a : b).record(v * 31);
        all.record(v * 31);
    }
    a.merge(b);
    CHECK(a.count() == all.count());
    CHECK(a.minValue() == all.minValue());
    CHECK(a.maxValue() == all.maxValue());
    for (double q : {0.1, 0.5, 0.9, 0.99, 0.999}) {
        CHECK(a.valueAtQuantile(q) == all.valueAtQuantile(q));
    }
}

TEST_CASE("empty histogram is well-defined") {
    LatencyHistogram h;
    CHECK(h.count() == 0);
    CHECK(h.valueAtQuantile(0.99) == 0);
    CHECK(h.minValue() == 0);
    CHECK(h.maxValue() == 0);
    CHECK(h.mean() == 0.0);
}
