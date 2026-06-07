// Phase 7 ring-buffer correctness: FIFO order, full/empty boundaries,
// wrap-around, move-only payloads, and the high-iteration concurrent stress
// runs that double as the TSan workload for the lock-free code.

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "doctest/doctest.h"
#include "runtime/mpsc_ring.h"
#include "runtime/spsc_ring.h"

using rsm::runtime::MpscRing;
using rsm::runtime::SpscRing;

TEST_CASE("spsc: fifo order, boundaries, wrap-around") {
    SpscRing<int> ring(8);
    CHECK(ring.capacity() == 8);

    int v = -1;
    CHECK_FALSE(ring.tryPop(v));  // empty

    // Fill to capacity, then one more must fail.
    for (int i = 0; i < 8; ++i) CHECK(ring.tryPush(int{i}));
    int extra = 99;
    CHECK_FALSE(ring.tryPush(std::move(extra)));

    // Drain in order, then empty again.
    for (int i = 0; i < 8; ++i) {
        CHECK(ring.tryPop(v));
        CHECK(v == i);
    }
    CHECK_FALSE(ring.tryPop(v));

    // Wrap the indices several laps with mixed push/pop.
    int next = 0, expect = 0;
    for (int round = 0; round < 100; ++round) {
        for (int i = 0; i < 5; ++i) CHECK(ring.tryPush(int{next++}));
        for (int i = 0; i < 5; ++i) {
            CHECK(ring.tryPop(v));
            CHECK(v == expect++);
        }
    }
}

TEST_CASE("spsc: capacity rounds up to a power of two") {
    SpscRing<int> r3(3);
    CHECK(r3.capacity() == 4);
    SpscRing<int> r1(1);
    CHECK(r1.capacity() == 2);
    SpscRing<int> r64(64);
    CHECK(r64.capacity() == 64);
}

TEST_CASE("spsc: move-only payload moves exactly once") {
    SpscRing<std::unique_ptr<int>> ring(4);
    CHECK(ring.tryPush(std::make_unique<int>(42)));
    std::unique_ptr<int> out;
    CHECK(ring.tryPop(out));
    REQUIRE(out != nullptr);
    CHECK(*out == 42);
    CHECK_FALSE(ring.tryPop(out));
}

TEST_CASE("mpsc: fifo order, boundaries, wrap-around (single thread)") {
    MpscRing<int> ring(8);
    CHECK(ring.capacity() == 8);

    int v = -1;
    CHECK_FALSE(ring.tryPop(v));

    for (int i = 0; i < 8; ++i) CHECK(ring.tryPush(int{i}));
    int extra = 99;
    CHECK_FALSE(ring.tryPush(std::move(extra)));

    for (int i = 0; i < 8; ++i) {
        CHECK(ring.tryPop(v));
        CHECK(v == i);
    }
    CHECK_FALSE(ring.tryPop(v));

    int next = 0, expect = 0;
    for (int round = 0; round < 100; ++round) {
        for (int i = 0; i < 5; ++i) CHECK(ring.tryPush(int{next++}));
        for (int i = 0; i < 5; ++i) {
            CHECK(ring.tryPop(v));
            CHECK(v == expect++);
        }
    }
}

TEST_CASE("spsc: concurrent producer/consumer stress preserves order") {
    // High-iteration two-thread stress; the ring is deliberately small so
    // both full and empty edges are hit constantly. TSan gate for SpscRing.
    constexpr std::uint64_t kOps = 1'000'000;
    SpscRing<std::uint64_t> ring(64);

    std::thread producer([&] {
        for (std::uint64_t i = 0; i < kOps; ++i) {
            std::uint64_t v = i;
            while (!ring.tryPush(std::move(v))) {
            }
        }
    });

    std::uint64_t popped = 0;
    bool ordered = true;
    while (popped < kOps) {
        std::uint64_t v;
        if (ring.tryPop(v)) {
            ordered = ordered && (v == popped);
            ++popped;
        }
    }
    producer.join();
    CHECK(popped == kOps);
    CHECK(ordered);  // exact FIFO: 0,1,2,... with nothing lost or duplicated
}

TEST_CASE("mpsc: concurrent multi-producer stress is per-producer FIFO") {
    // 4 producers × 250k tagged values; the single consumer asserts each
    // producer's stream arrives gap-free and in order, and that the total
    // count is exact (nothing lost, nothing duplicated). TSan gate for
    // MpscRing.
    constexpr int kProducers = 4;
    constexpr std::uint64_t kPerProducer = 250'000;
    MpscRing<std::uint64_t> ring(64);

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            const auto tag = static_cast<std::uint64_t>(p) << 32;
            for (std::uint64_t i = 0; i < kPerProducer; ++i) {
                std::uint64_t v = tag | i;
                while (!ring.tryPush(std::move(v))) {
                }
            }
        });
    }

    std::uint64_t expected[kProducers] = {};
    std::uint64_t total = 0;
    bool ordered = true;
    while (total < kProducers * kPerProducer) {
        std::uint64_t v;
        if (!ring.tryPop(v)) continue;
        const auto p = static_cast<int>(v >> 32);
        const std::uint64_t seq = v & 0xFFFFFFFFu;
        ordered = ordered && p >= 0 && p < kProducers && seq == expected[p];
        ++expected[p];
        ++total;
    }
    for (auto& t : producers) t.join();
    CHECK(total == kProducers * kPerProducer);
    CHECK(ordered);
    for (int p = 0; p < kProducers; ++p) CHECK(expected[p] == kPerProducer);
    std::uint64_t leftover;
    CHECK_FALSE(ring.tryPop(leftover));
}

TEST_CASE("mpsc: move-only payload across threads") {
    constexpr std::uint64_t kOps = 50'000;
    MpscRing<std::unique_ptr<std::uint64_t>> ring(32);
    std::thread producer([&] {
        for (std::uint64_t i = 0; i < kOps; ++i) {
            auto v = std::make_unique<std::uint64_t>(i);
            while (!ring.tryPush(std::move(v))) {
                // tryPush only consumes the value on success; retry intact.
            }
        }
    });
    std::uint64_t popped = 0;
    bool valid = true;
    while (popped < kOps) {
        std::unique_ptr<std::uint64_t> v;
        if (ring.tryPop(v)) {
            valid = valid && v != nullptr && *v == popped;
            ++popped;
        }
    }
    producer.join();
    CHECK(valid);
}
