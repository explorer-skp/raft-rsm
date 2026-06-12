#pragma once

// Phase 8 load generators.
//
// Closed-loop: N threads, each issue -> wait -> issue. Finds saturation
// throughput and *service* latency, but SELF-THROTTLES: when the system
// slows down, a closed-loop client offers less load, so queueing delay that
// real arrivals would experience never appears in its numbers. Closed-loop
// latency is therefore reported as service latency only — never as the tail
// story.
//
// Open-loop: requests arrive at a fixed target rate, independent of
// completions (decision in DESIGN.md: deterministic fixed-rate arrivals
// striped round-robin across T threads, thread j offset by j/rate). Each
// request's latency is measured FROM ITS INTENDED (scheduled) SEND TIME —
// the coordinated-omission correction: when the system falls behind, the
// requests queueing behind the slowdown record the queueing they actually
// suffered, instead of being silently rescheduled. The generator never
// skips a scheduled request (it keeps issuing late ones until the schedule
// is exhausted or the drain cap trips), so no sample is omitted and no
// back-fill is needed. The actual-send-time histogram is kept alongside to
// SHOW the correction working (the stall self-test asserts the intended
// tail moves while the actual tail hides the stall).
//
// Bounded in-flight caveat (documented honestly): each thread issues
// synchronously, so at most T requests are in flight — a true open system
// queues without bound. Measuring from intended time still charges every
// request its full schedule-to-response delay, so stalls and saturation
// appear in the tail; T is sized well above the in-flight demand at every
// offered rate below saturation.
//
// The generator hot loop is allocation- and lock-free in steady state
// (KvClient's reused buffers + the fixed-storage histogram + precomputed
// schedule arithmetic); asserted by the bench allocation test.

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "bench_util.h"
#include "metrics/histogram.h"
#include "transport/transport.h"

namespace rsm::bench {

struct GenConfig {
    int threads = 4;
    std::uint64_t seed = 1;
    int keys = 64;
    int valueBytes = 16;
    // Order-book workload (Phase 9): when set, every op is a NEW limit
    // order — side uniform, price uniform in [priceBase - priceBand,
    // priceBase + priceBand], qty in [1, qtyMax] — against a cluster built
    // with orderBook=true. The symmetric band keeps the resting book in a
    // bounded random walk while making continuous matching the common
    // case, so the benchmark measures the engine matching, not just
    // appending.
    bool orderBook = false;
    std::uint64_t priceBase = 100, priceBand = 10, qtyMax = 10;
    // Open-loop only: total offered rate across all threads.
    double ratePerSec = 1000.0;
    // Open-loop only: give up replaying a late schedule this long past the
    // window end (the abandoned count marks the run as oversaturated).
    std::uint64_t drainCapNs = 10ull * 1000 * 1000 * 1000;
    std::uint64_t clientIdBase = 1'000'000;
    rsm::rpc::NodeId envelopeIdBase = 300;
    int perAttemptTimeoutMs = 1000;
    int maxAttempts = 20;
};

struct GenStats {
    rsm::metrics::LatencyHistogram intended;  // from scheduled send (open)
    rsm::metrics::LatencyHistogram actual;    // from actual send
    std::uint64_t scheduled = 0;  // ops whose intended time fell in-window
    std::uint64_t oks = 0;
    std::uint64_t failures = 0;
    std::uint64_t abandoned = 0;       // open: schedule given up at drain cap
    std::uint64_t sumSendLatenessNs = 0;  // open: actual - intended send
    std::uint64_t maxSendLatenessNs = 0;
    std::uint64_t sends = 0;           // in-window sends (lateness samples)

    void merge(const GenStats& o) {
        intended.merge(o.intended);
        actual.merge(o.actual);
        scheduled += o.scheduled;
        oks += o.oks;
        failures += o.failures;
        abandoned += o.abandoned;
        sumSendLatenessNs += o.sumSendLatenessNs;
        maxSendLatenessNs = std::max(maxSendLatenessNs, o.maxSendLatenessNs);
        sends += o.sends;
    }
};

// Deterministic per-thread key sequence (the workload's reproducibility
// contract: same seed => same sequence; unit-tested).
inline std::uint32_t nextKeyIndex(XorShift64& rng, int keys) {
    return static_cast<std::uint32_t>(rng.next() %
                                      static_cast<std::uint64_t>(keys));
}

// Runs closed-loop PUT load from `threads` clients until measureEndNs (or
// `stop`, if non-null, for run-until-told modes like the failover driver).
// Records actual-send-time latency for ops fully inside
// [measureStartNs, measureEndNs).
GenStats runClosedLoop(const rsm::transport::PeerMap& servers,
                       const GenConfig& cfg, std::uint64_t measureStartNs,
                       std::uint64_t measureEndNs,
                       const std::atomic<bool>* stop = nullptr);

// Runs open-loop PUT load at cfg.ratePerSec from startNs until the schedule
// reaches measureEndNs. Ops with intended time in [measureStartNs,
// measureEndNs) are recorded (intended- and actual-send-time histograms).
GenStats runOpenLoop(const rsm::transport::PeerMap& servers,
                     const GenConfig& cfg, std::uint64_t startNs,
                     std::uint64_t measureStartNs, std::uint64_t measureEndNs);

}  // namespace rsm::bench
