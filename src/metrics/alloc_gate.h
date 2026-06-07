#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

// Allocation accounting for the Phase 7 allocation-free-steady-state test
// (spec §4.7: "no per-message malloc in steady state", verified by hooking
// global new). Header-only and dependency-free so any layer can carry the
// annotations without linking anything.
//
// How it works:
//  - Production binaries: the only cost is the annotations themselves —
//    a thread_local ++/-- (AllocRetention) at data-RETENTION points and one
//    setThreadAllocRole() call per pipeline thread. Nothing else runs,
//    because nothing calls onAlloc().
//  - The allocation TEST binary overrides global operator new/delete and
//    calls onAlloc() from it. While counting is enabled, every allocation
//    is charged to the calling thread, split into:
//      plumbing  — allocations of the runtime machinery itself. The test
//                  asserts these are ZERO on the raft/apply/tx pipeline
//                  threads after warmup.
//      retained  — allocations inside an AllocRetention scope: bytes that
//                  genuinely accumulate state (the durable log's in-memory
//                  copy, the KV store / session table, the command's single
//                  log-bound copy). These scale with data, not with message
//                  count, and are reported rather than asserted to zero.
namespace rsm::metrics {

struct ThreadAllocStats {
    std::atomic<std::uint64_t> plumbing{0};
    std::atomic<std::uint64_t> retained{0};
    std::atomic<const char*> role{""};
};

namespace detail {

struct AllocRegistry {
    std::mutex mu;
    std::vector<std::shared_ptr<ThreadAllocStats>> all;
};

inline AllocRegistry& allocRegistry() {
    static AllocRegistry r;
    return r;
}

// Guards threadAllocStats() first-use (which itself allocates).
inline bool& inAllocGate() {
    thread_local bool in = false;
    return in;
}

}  // namespace detail

inline std::atomic<bool>& allocCountingEnabled() {
    static std::atomic<bool> enabled{false};
    return enabled;
}

inline int& allocExemptDepth() {
    thread_local int depth = 0;
    return depth;
}

inline ThreadAllocStats& threadAllocStats() {
    thread_local std::shared_ptr<ThreadAllocStats> stats = [] {
        auto s = std::make_shared<ThreadAllocStats>();
        auto& reg = detail::allocRegistry();
        std::lock_guard lock(reg.mu);
        reg.all.push_back(s);
        return s;
    }();
    return *stats;
}

// Pipeline threads self-identify so the test can assert per-role.
inline void setThreadAllocRole(const char* role) {
    detail::inAllocGate() = true;
    threadAllocStats().role.store(role);
    detail::inAllocGate() = false;
}

// Snapshot for the test (shared_ptrs keep stats alive past thread exit).
inline std::vector<std::shared_ptr<ThreadAllocStats>> allocStatsSnapshot() {
    auto& reg = detail::allocRegistry();
    std::lock_guard lock(reg.mu);
    return reg.all;
}

// Marks a scope whose allocations RETAIN data (log growth, SM state, the
// command's log-bound copy). Cost outside the test binary: ++/-- of a
// thread_local int.
struct AllocRetention {
    AllocRetention() { ++allocExemptDepth(); }
    ~AllocRetention() { --allocExemptDepth(); }
    AllocRetention(const AllocRetention&) = delete;
    AllocRetention& operator=(const AllocRetention&) = delete;
};

// Called by the test binary's operator new override.
inline void onAlloc() {
    if (!allocCountingEnabled().load(std::memory_order_relaxed)) return;
    if (detail::inAllocGate()) return;  // re-entrant first-use
    detail::inAllocGate() = true;
    auto& stats = threadAllocStats();
    if (allocExemptDepth() > 0) {
        stats.retained.fetch_add(1, std::memory_order_relaxed);
    } else {
        stats.plumbing.fetch_add(1, std::memory_order_relaxed);
    }
    detail::inAllocGate() = false;
}

}  // namespace rsm::metrics
