// Phase 7 allocation-free steady state (spec §4.7): this binary overrides
// global operator new to charge every allocation to the calling thread, runs
// a 3-node THREADED cluster under sustained client load, and asserts that
// after warmup the pipeline threads — raft, apply, tx, on every node —
// perform ZERO plumbing allocations.
//
// Two categories (see src/metrics/alloc_gate.h):
//   plumbing — runtime machinery; asserted ZERO on raft/apply/tx.
//   retained — explicitly tagged data-proportional state (the durable log's
//              in-memory copy, KV/session state, the command's single
//              log-bound copy). Reported, expected nonzero on raft.
// The rx thread is held to (near) the same zero: frames decode into the
// inbound ring slots' pooled buffers (decodeMessageInto), so the
// PER-MESSAGE rx path is allocation-free too. Connection lifecycle events
// (an accept after a client reconnect) legitimately allocate a handful of
// times per CONNECTION; rx gets a small constant budget for those — three
// orders of magnitude below what any per-message churn would register.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "client/kv_client.h"
#include "doctest/doctest.h"
#include "metrics/alloc_gate.h"
#include "statemachine/kv_store.h"
#include "tcp_cluster.h"
#include "temp_dir.h"

// ---- global new/delete overrides (whole binary) ----------------------------

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

TEST_CASE("steady state: zero plumbing allocations on raft/apply/tx threads") {
    testutil::TempDir dir;
    tcptest::Cluster cluster(3, dir.path(), /*kv=*/true,
                             rsm::client::ClientService::Batching{
                                 8, std::chrono::microseconds(200)});
    REQUIRE(cluster.awaitStableLeader(std::chrono::seconds(10)).has_value());

    // Sustained closed-loop load: 4 clients, small values, fixed keys.
    std::atomic<bool> stopClients{false};
    std::atomic<std::uint64_t> ops{0};
    std::atomic<std::uint64_t> failures{0};
    std::vector<std::thread> clients;
    for (int c = 0; c < 4; ++c) {
        clients.emplace_back([&, c] {
            rsm::client::KvClient kv(
                cluster.peerMap(), /*clientId=*/0xA110C + static_cast<unsigned>(c),
                static_cast<rsm::rpc::NodeId>(230 + c));
            const std::string value(16, 'v');
            std::uint64_t i = 0;
            while (!stopClients.load(std::memory_order_relaxed)) {
                const std::string key = "k" + std::to_string(i++ % 16);
                const auto r = kv.put(key, value);
                if (r && r->status == rsm::statemachine::kKvOk) {
                    ops.fetch_add(1, std::memory_order_relaxed);
                } else {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    // Warmup: elections settle, every ring slot / scratch buffer reaches its
    // high-water capacity, client connections and routes stabilize.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    const auto opsBefore = ops.load();

    rsm::metrics::allocCountingEnabled().store(true);
    std::this_thread::sleep_for(std::chrono::seconds(3));
    rsm::metrics::allocCountingEnabled().store(false);

    const auto opsMeasured = ops.load() - opsBefore;
    stopClients.store(true);
    for (auto& t : clients) t.join();

    // The workload must have actually exercised steady state.
    CHECK(failures.load() == 0);
    REQUIRE(opsMeasured > 1000);

    std::printf("measured window: %llu ops\n",
                static_cast<unsigned long long>(opsMeasured));
    std::printf("%-8s %12s %12s\n", "role", "plumbing", "retained");
    for (const auto& s : rsm::metrics::allocStatsSnapshot()) {
        const char* role = s->role.load();
        const auto plumbing = s->plumbing.load();
        const auto retained = s->retained.load();
        if (role[0] == '\0' && plumbing == 0 && retained == 0) continue;
        std::printf("%-8s %12llu %12llu\n", role[0] ? role : "(other)",
                    static_cast<unsigned long long>(plumbing),
                    static_cast<unsigned long long>(retained));
        const std::string r = role;
        if (r == "raft" || r == "apply" || r == "tx") {
            CAPTURE(r);
            CHECK(plumbing == 0);  // THE assertion of this test
        } else if (r == "rx") {
            // Zero per-message; <= a constant for connection lifecycle
            // (client reconnects mid-window). Per-message churn would
            // register in the thousands here.
            CHECK(plumbing <= 16);
        }
    }
}
