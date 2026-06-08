#pragma once

// Small shared utilities for the Phase 8 benchmark harness. Everything here
// is allocation-free and self-contained: seeded PRNG streams (workload
// determinism), monotonic-nanosecond helpers, and thread pinning.

#include <pthread.h>
#include <sched.h>

#include <chrono>
#include <cstdint>

namespace rsm::bench {

// splitmix64 — the project's standard seed-derivation mix (same role as the
// Phase 6 sim's stream derivation): cheap, well-distributed, deterministic.
inline std::uint64_t splitmix64(std::uint64_t& state) {
    state += 0x9E3779B97F4A7C15ULL;
    std::uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

// Derives an independent stream seed from (master, streamId).
inline std::uint64_t deriveSeed(std::uint64_t master, std::uint64_t streamId) {
    std::uint64_t s = master ^ (streamId * 0xD6E8FEB86659FD93ULL);
    return splitmix64(s);
}

// xorshift64* — the per-thread fault/workload RNG (one u64 of state, no
// allocation; quality is ample for drop decisions and key picks).
struct XorShift64 {
    std::uint64_t state;
    explicit XorShift64(std::uint64_t seed) : state(seed ? seed : 0x9E3779B9ULL) {}
    std::uint64_t next() {
        std::uint64_t x = state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        state = x;
        return x * 0x2545F4914F6CDD1DULL;
    }
};

// All bench timestamps are steady_clock nanoseconds-since-epoch as u64 —
// one clock everywhere (the same clock the runtime hands to service hooks),
// so cross-thread time arithmetic is always valid.
inline std::uint64_t nowNs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

inline std::uint64_t toNs(std::chrono::steady_clock::time_point tp) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            tp.time_since_epoch())
            .count());
}

// Pins the calling thread to one CPU. Returns false (and changes nothing)
// on failure; callers treat pinning as best-effort and record the outcome.
inline bool pinSelfToCpu(int cpu) {
    if (cpu < 0) return false;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<unsigned>(cpu), &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
}

}  // namespace rsm::bench
