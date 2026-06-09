// Phase 8: the benchmark harness's own validation suite. A measurement
// instrument that cannot be shown to bite proves nothing (the same rule the
// Phase 6 checker self-tests follow), so each property here is demonstrated
// against the real cluster:
//
//  1. Workload reproducibility — same seed => identical key sequence.
//  2. JSON writer sanity — the raw-data files must be well-formed.
//  3. Open-loop rate fidelity — the generator holds the target arrival
//     rate when the system keeps up.
//  4. Coordinated-omission stall self-test (the crux): inject an artificial
//     stall into the SYSTEM mid-run and assert the intended-send-time tail
//     REPORTS it while the actual-send-time tail HIDES it. A harness that
//     hides an injected stall is broken.
//  5. Leadership stability under the stress regime (high concurrency +
//     group commit + sustained): constant term, zero elections in the
//     measurement window. This is the benchmark-side regression net for the
//     Phase 7 apply-backpressure class of bug — a failure here is a bug,
//     not noise.
//
// Sanitizer builds run reduced workloads/thresholds: ASan/TSan multiply CPU
// cost several-fold and this suite asserts wall-clock behavior; the
// sanitizer value here is race/UB coverage of the harness itself. The
// full-strength assertions run in Release ctest and (longer) in
// bench/run_benchmarks.sh.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

#include "bench_cluster.h"
#include "bench_report.h"
#include "bench_util.h"
#include "doctest/doctest.h"
#include "load_gen.h"

using namespace rsm::bench;

#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
constexpr bool kSanitized = true;
#else
constexpr bool kSanitized = false;
#endif

namespace {
constexpr std::uint64_t kSecond = 1'000'000'000ull;
}

TEST_CASE("workload: same seed => same key sequence, different seed => different") {
    XorShift64 a(deriveSeed(42, 7));
    XorShift64 b(deriveSeed(42, 7));
    XorShift64 c(deriveSeed(43, 7));
    bool anyDiff = false;
    for (int i = 0; i < 10000; ++i) {
        const auto ka = nextKeyIndex(a, 64);
        REQUIRE(ka == nextKeyIndex(b, 64));
        anyDiff = anyDiff || (ka != nextKeyIndex(c, 64));
    }
    CHECK(anyDiff);
}

TEST_CASE("json writer: nesting, commas, escaping") {
    JsonWriter w;
    w.beginObject();
    w.kv("a", std::uint64_t{1});
    w.kv("b", "x\"y\\z\n");
    w.key("c");
    w.beginArray();
    w.value(std::uint64_t{1});
    w.beginObject();
    w.kv("d", true);
    w.endObject();
    w.endArray();
    w.kv("e", 2.5);
    w.endObject();
    CHECK(w.str() ==
          "{\"a\":1,\"b\":\"x\\\"y\\\\z\\n\",\"c\":[1,{\"d\":true}],"
          "\"e\":2.5}");
}

TEST_CASE("open-loop generator holds the target arrival rate") {
    BenchClusterConfig ccfg;
    ccfg.seed = 11;
    BenchCluster cluster(ccfg);
    REQUIRE(cluster.awaitReady(std::chrono::seconds(15)).has_value());

    const double rate = kSanitized ? 500.0 : 2000.0;
    GenConfig gcfg;
    gcfg.threads = 8;
    gcfg.seed = 11;
    gcfg.ratePerSec = rate;
    const std::uint64_t t0 = nowNs();
    const std::uint64_t ms = t0 + 1 * kSecond;   // 1s warmup
    const std::uint64_t me = ms + 3 * kSecond;   // 3s window
    const auto stats = runOpenLoop(cluster.peerMap(), gcfg, t0, ms, me);

    const auto expected = static_cast<std::uint64_t>(rate * 3);
    CAPTURE(stats.scheduled);
    CAPTURE(stats.sends);
    CAPTURE(stats.failures);
    // Every scheduled in-window request was issued and answered.
    CHECK(stats.failures == 0);
    CHECK(stats.abandoned == 0);
    // The schedule covers the window to within edge effects (one op per
    // thread at each boundary).
    CHECK(stats.scheduled >= expected - 16);
    CHECK(stats.scheduled <= expected + 16);
    // Sends track the schedule: when the system keeps up, dispatch lateness
    // stays far below the request interval.
    const double meanLateNs =
        static_cast<double>(stats.sumSendLatenessNs) /
        static_cast<double>(stats.sends ? stats.sends : 1);
    CAPTURE(meanLateNs);
    CHECK(meanLateNs < (kSanitized ? 20e6 : 2e6));  // 2ms / 20ms sanitized
}

TEST_CASE("coordinated-omission stall self-test: the reported tail must move") {
    BenchClusterConfig ccfg;
    ccfg.seed = 12;
    BenchCluster cluster(ccfg);
    REQUIRE(cluster.awaitReady(std::chrono::seconds(15)).has_value());

    // Open-loop run with a 500ms system stall (every node's state machine
    // sleeps once) injected ~1.5s into a 4s measurement window. ~12% of the
    // window's requests are scheduled during the stall: their queueing is
    // real and MUST appear in the intended-send-time tail.
    const double rate = kSanitized ? 600.0 : 1500.0;
    GenConfig gcfg;
    gcfg.threads = 6;
    gcfg.seed = 12;
    gcfg.ratePerSec = rate;
    const std::uint64_t t0 = nowNs();
    const std::uint64_t ms = t0 + 1 * kSecond;
    const std::uint64_t me = ms + 4 * kSecond;

    std::thread arm([&] {
        std::this_thread::sleep_until(
            std::chrono::steady_clock::time_point(
                std::chrono::nanoseconds(ms + 1500 * 1000 * 1000ull)));
        cluster.stall().arm(/*ms=*/500);
    });
    const auto stats = runOpenLoop(cluster.peerMap(), gcfg, t0, ms, me);
    arm.join();

    const auto intendedP99 = stats.intended.valueAtQuantile(0.99);
    const auto intendedP999 = stats.intended.valueAtQuantile(0.999);
    const auto actualP99 = stats.actual.valueAtQuantile(0.99);
    CAPTURE(intendedP99);
    CAPTURE(intendedP999);
    CAPTURE(actualP99);
    CAPTURE(stats.oks);
    CHECK(stats.failures == 0);

    // The corrected tail reports the stall (≥ ~250ms of queueing at p99 for
    // a 500ms stall over ~12% of samples; generous margin below the
    // theoretical ~420ms).
    CHECK(intendedP99 >= 250 * 1000 * 1000ull);
    CHECK(intendedP999 >= 350 * 1000 * 1000ull);
    // The uncorrected (actual-send-time) view hides most of it: only the
    // requests already in flight see the stall; everything queued behind
    // them dispatches late and measures small. This gap IS coordinated
    // omission, demonstrated.
    CHECK(intendedP99 >= 2 * actualP99);
    if (!kSanitized) {
        CHECK(actualP99 <= 150 * 1000 * 1000ull);
    }
    std::printf(
        "stall self-test: intended p99=%.1fms p99.9=%.1fms | actual "
        "p99=%.1fms (the correction made the stall visible)\n",
        static_cast<double>(intendedP99) / 1e6,
        static_cast<double>(intendedP999) / 1e6,
        static_cast<double>(actualP99) / 1e6);
}

TEST_CASE("leadership holds at constant term through the stress regime") {
    // The Phase 7 interaction class: high client concurrency + group commit
    // + sustained duration on the real threaded runtime. A clean-load
    // election here is a BUG (the apply-backpressure lesson), so this
    // asserts zero elections and zero term movement across the window.
    BenchClusterConfig ccfg;
    ccfg.seed = 13;
    ccfg.batch = 8;
    ccfg.lingerUs = 200;
    ccfg.wait = kSanitized ? rsm::runtime::WaitMode::Block
                           : rsm::runtime::WaitMode::Spin;
    BenchCluster cluster(ccfg);
    REQUIRE(cluster.awaitReady(std::chrono::seconds(15)).has_value());

    GenConfig gcfg;
    gcfg.threads = kSanitized ? 8 : 16;
    gcfg.seed = 13;
    const std::uint64_t t0 = nowNs();
    const std::uint64_t ms = t0 + 2 * kSecond;
    const std::uint64_t me = ms + (kSanitized ? 4 : 8) * kSecond;
    const auto stats = runClosedLoop(cluster.peerMap(), gcfg, ms, me);

    const auto termAtStart = cluster.monitor().maxTermAt(ms);
    const auto termAtEnd = cluster.monitor().maxTerm();
    const int elections = cluster.monitor().electionsIn(ms, me);
    CAPTURE(stats.oks);
    CAPTURE(termAtStart);
    CAPTURE(termAtEnd);
    // The workload must actually have stressed the cluster.
    REQUIRE(stats.oks > 1000);
    CHECK(stats.failures == 0);
    CHECK(elections == 0);
    CHECK(termAtEnd == termAtStart);
    std::printf("stress regime: %llu ops, term %llu constant, 0 elections\n",
                static_cast<unsigned long long>(stats.oks),
                static_cast<unsigned long long>(termAtEnd));
}
