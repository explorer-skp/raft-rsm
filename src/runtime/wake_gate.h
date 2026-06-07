#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

namespace rsm::runtime {

// Consumer wake-up for the lock-free rings (Phase 7, decision point 4).
//
// The rings themselves never block, so each consumer thread chooses how to
// wait when its rings are empty:
//
//  - Block: park on a condition variable until a producer notifies. The
//    hand-off costs a wakeup (~2-10us) but burns no CPU when idle. This is
//    the default: on this 12-thread topology (3 nodes x 4 threads in test
//    runs) it also avoids oversubscription.
//  - Spin: busy-poll with an escalating backoff (cpu pause -> yield ->
//    short park) for the lowest hand-off latency at the cost of a hot core.
//
// Blocking protocol: an event-count. One atomic packs (epoch << 32 |
// waiters); both sides RMW it with seq_cst, which is what kills the classic
// missed-wakeup race WITHOUT std::atomic_thread_fence (which ThreadSanitizer
// cannot model and our -Werror=tsan build rejects):
//
//   consumer: prev = state.fetch_add(WAITER, seq_cst); re-check work;
//             sleep until epoch != prev.epoch (or work, or deadline)
//   producer: publish work (ring release-store);
//             prev = state.fetch_add(EPOCH, seq_cst); if (prev.waiters) wake
//
// The two RMWs hit the SAME atomic, so they are totally ordered. If the
// consumer's runs first, the producer reads waiters > 0 and notifies. If
// the producer's runs first, the consumer's RMW reads a value later in the
// modification order, which (release sequence) makes the producer's ring
// store happen-before the consumer's re-check — it sees the work and never
// sleeps. Either way no wakeup is lost. The cv wait additionally has a
// bounded timeout as a belt-and-braces liveness floor (and because the Raft
// consumer must wake for its own timer deadlines anyway).
class WakeGate {
public:
    // Producer side, AFTER making work visible (the ring's release store).
    void notify() {
        // seq_cst RMW: totally ordered against the consumer's register-RMW
        // and releases the preceding ring store to it (rationale above).
        const std::uint64_t prev =
            state_.fetch_add(kEpochInc, std::memory_order_seq_cst);
        if ((prev & kWaiterMask) != 0) {
            // The mutex orders this notify against a consumer that is
            // between registering and waiting: it either sees the new epoch
            // in the predicate or receives the notify itself.
            std::lock_guard lock(mu_);
            cv_.notify_all();
        }
    }

    // Consumer side: sleep until `hasWork()` is true, `deadline` passes, or
    // a producer notifies. hasWork must read ring state with acquire loads.
    template <typename Pred>
    void waitUntil(Pred&& hasWork,
                   std::chrono::steady_clock::time_point deadline) {
        // seq_cst RMW registers us as a waiter (rationale above).
        const std::uint64_t prev =
            state_.fetch_add(kWaiterInc, std::memory_order_seq_cst);
        const std::uint64_t epoch = prev >> kEpochShift;
        if (!hasWork()) {
            std::unique_lock lock(mu_);
            // Bounded even if deadline is far: a spurious miss costs at
            // most kMaxPark. Waking on an epoch bump (not only on hasWork)
            // keeps notify() one-shot and cheap.
            const auto cap = std::chrono::steady_clock::now() + kMaxPark;
            cv_.wait_until(lock, deadline < cap ? deadline : cap, [&] {
                return (state_.load(std::memory_order_acquire) >>
                        kEpochShift) != epoch ||
                       hasWork();
            });
        }
        state_.fetch_sub(kWaiterInc, std::memory_order_relaxed);
    }

private:
    static constexpr std::chrono::milliseconds kMaxPark{5};
    static constexpr int kEpochShift = 32;
    static constexpr std::uint64_t kEpochInc = std::uint64_t{1} << kEpochShift;
    static constexpr std::uint64_t kWaiterInc = 1;
    static constexpr std::uint64_t kWaiterMask = kEpochInc - 1;

    std::atomic<std::uint64_t> state_{0};  // epoch << 32 | waiter count
    std::mutex mu_;
    std::condition_variable cv_;
};

// Escalating idle backoff for busy-spin mode: stay on-core through the
// pause/yield stages (sub-microsecond reaction), and only once the ring has
// been idle for the whole budget fall through to a short park so a quiet
// cluster does not pin cores at 100% forever. reset() on any work.
class SpinBackoff {
public:
    void onWork() { count_ = 0; }

    void onIdle() {
        ++count_;
        if (count_ <= kPauses) {
#if defined(__x86_64__)
            __builtin_ia32_pause();
#else
            std::this_thread::yield();
#endif
        } else if (count_ <= kPauses + kYields) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(20));
        }
    }

private:
    // ~4k pauses ≈ a few microseconds of hot spinning, then ~64 yields
    // share the core under oversubscription before parking. Defaults
    // documented in DESIGN.md.
    static constexpr int kPauses = 4096;
    static constexpr int kYields = 64;
    int count_ = 0;
};

}  // namespace rsm::runtime
