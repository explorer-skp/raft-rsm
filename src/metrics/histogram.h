#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <string>

namespace rsm::metrics {

// HDR-style log-linear latency histogram (spec §4.8), implemented in-repo:
// fixed storage, no allocation, O(1) record, mergeable across threads.
//
// Layout: values < 64 map 1:1 to the first 64 counters ("linear region");
// every value >= 64 lands in one of 64 linear sub-buckets of its power-of-two
// magnitude bucket, i.e. the top 6 bits after the leading 1 select the
// sub-bucket. Worst-case quantization error is therefore 1/64 ≈ 1.6% of the
// value — more than enough resolution for before/after comparisons (the
// rigorous Phase 8 harness can vendor HdrHistogram_c if finer precision or
// coordinated-omission correction APIs are wanted; decision in DESIGN.md).
//
// Units are the caller's (this project records nanoseconds). Range covers
// [0, 2^63); larger values clamp into the top bucket.
//
// Not thread-safe: record into per-thread instances and merge().
class LatencyHistogram {
public:
    static constexpr int kSubBits = 6;                  // 64 sub-buckets
    static constexpr int kSub = 1 << kSubBits;
    // Magnitudes 6..62 each get kSub sub-buckets, after the 1:1 region.
    static constexpr int kBuckets = kSub + (63 - kSubBits) * kSub;

    void record(std::uint64_t value) {
        counts_[indexOf(value)]++;
        total_++;
        sum_ += value;
        min_ = std::min(min_, value);
        max_ = std::max(max_, value);
    }

    void merge(const LatencyHistogram& other) {
        for (int i = 0; i < kBuckets; ++i) counts_[i] += other.counts_[i];
        total_ += other.total_;
        sum_ += other.sum_;
        min_ = std::min(min_, other.min_);
        max_ = std::max(max_, other.max_);
    }

    std::uint64_t count() const { return total_; }
    std::uint64_t minValue() const { return total_ ? min_ : 0; }
    std::uint64_t maxValue() const { return total_ ? max_ : 0; }
    double mean() const {
        return total_ ? static_cast<double>(sum_) / static_cast<double>(total_)
                      : 0.0;
    }

    // Value at quantile q in [0, 1]: the upper edge of the bucket holding the
    // ceil(q * count)-th recorded value (so the true value is <= the answer,
    // within the 1/64 bucket width). 0 if empty.
    std::uint64_t valueAtQuantile(double q) const {
        if (total_ == 0) return 0;
        q = std::clamp(q, 0.0, 1.0);
        const auto rank = static_cast<std::uint64_t>(q * static_cast<double>(total_));
        std::uint64_t seen = 0;
        for (int i = 0; i < kBuckets; ++i) {
            seen += counts_[i];
            if (seen > rank || (seen == total_ && seen >= rank)) {
                return std::min(upperEdge(i), max_);
            }
        }
        return max_;
    }

    void reset() { *this = LatencyHistogram{}; }

    // Exposed for the unit test: bucket mapping must be monotone and the
    // value must fall inside [lowerEdge, upperEdge] of its own bucket.
    static int indexOf(std::uint64_t v) {
        if (v < kSub) return static_cast<int>(v);
        const int top = 63 - std::countl_zero(v);  // index of the leading 1
        const int sub =
            static_cast<int>((v >> (top - kSubBits)) & (kSub - 1));
        const int idx = (top - kSubBits + 1) * kSub + sub;
        return std::min(idx, kBuckets - 1);
    }

    static std::uint64_t upperEdge(int index) {
        if (index < kSub) return static_cast<std::uint64_t>(index);
        const int top = index / kSub + kSubBits - 1;
        const int sub = index % kSub;
        return (std::uint64_t{1} << top) +
               ((static_cast<std::uint64_t>(sub) + 1)
                << (top - kSubBits)) - 1;
    }

private:
    std::array<std::uint64_t, kBuckets> counts_{};
    std::uint64_t total_ = 0;
    std::uint64_t sum_ = 0;
    std::uint64_t min_ = UINT64_MAX;
    std::uint64_t max_ = 0;
};

// Renders the standard one-line summary used by the Phase 7 bench output.
std::string summarizeNs(const LatencyHistogram& h);

}  // namespace rsm::metrics
