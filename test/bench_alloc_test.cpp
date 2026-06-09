// Phase 8 measurement-overhead test: the OBSERVER must not perturb the
// observed. The load-generator hot loop (schedule arithmetic, KvClient
// request/response, histogram recording) runs under the same global
// operator-new accounting as the Phase 7 pipeline test, and the assertion
// is the same: ZERO plumbing allocations on every load-generator thread in
// the steady-state window. An observer that allocates (or locks) on the hot
// path inflates the very tail it reports.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <string>
#include <thread>

#include "bench_cluster.h"
#include "bench_util.h"
#include "doctest/doctest.h"
#include "load_gen.h"
#include "metrics/alloc_gate.h"

// ---- global new/delete overrides (whole binary), as in alloc_test.cpp ----

void* operator new(std::size_t n) {
    rsm::metrics::onAlloc();
    if (void* p = std::malloc(n)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    rsm::metrics::onAlloc();
    if (void* p = std::malloc(n)) return p;
    throw std::bad_alloc();
}
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    rsm::metrics::onAlloc();
    return std::malloc(n);
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    rsm::metrics::onAlloc();
    return std::malloc(n);
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept {
    std::free(p);
}

// ----------------------------------------------------------------------------

using namespace rsm::bench;

TEST_CASE("open-loop measurement path: zero plumbing allocations per op") {
    BenchClusterConfig ccfg;
    ccfg.seed = 21;
    BenchCluster cluster(ccfg);
    REQUIRE(cluster.awaitReady(std::chrono::seconds(15)).has_value());

    constexpr std::uint64_t kSecond = 1'000'000'000ull;
    GenConfig gcfg;
    gcfg.threads = 4;
    gcfg.seed = 21;
    gcfg.ratePerSec = 1000.0;
    const std::uint64_t t0 = nowNs();
    const std::uint64_t ms = t0 + 3 * kSecond;  // warmup: connections,
    const std::uint64_t me = ms + 3 * kSecond;  // capacities, routes settle

    GenStats stats;
    std::thread gen([&] {
        stats = runOpenLoop(cluster.peerMap(), gcfg, t0, ms, me);
    });
    // Enable accounting strictly inside the steady-state window.
    std::this_thread::sleep_until(std::chrono::steady_clock::time_point(
        std::chrono::nanoseconds(ms + kSecond / 2)));
    rsm::metrics::allocCountingEnabled().store(true);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    rsm::metrics::allocCountingEnabled().store(false);
    gen.join();

    REQUIRE(stats.oks > 1000);
    CHECK(stats.failures == 0);

    std::printf("%-8s %12s %12s\n", "role", "plumbing", "retained");
    bool sawLoadgen = false;
    for (const auto& s : rsm::metrics::allocStatsSnapshot()) {
        const std::string role = s->role.load();
        const auto plumbing = s->plumbing.load();
        const auto retained = s->retained.load();
        if (role.empty() && plumbing == 0 && retained == 0) continue;
        std::printf("%-8s %12llu %12llu\n",
                    role.empty() ? "(other)" : role.c_str(),
                    static_cast<unsigned long long>(plumbing),
                    static_cast<unsigned long long>(retained));
        if (role == "loadgen") {
            sawLoadgen = true;
            CHECK(plumbing == 0);  // THE assertion: the observer is silent
        }
    }
    REQUIRE(sawLoadgen);
}
