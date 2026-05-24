#pragma once

#include <chrono>

namespace rsm::raft {

using TimePoint = std::chrono::steady_clock::time_point;
using Duration = std::chrono::milliseconds;

// All time in Raft logic flows through this seam: production uses
// SteadyClock, tests use ManualClock and advance it explicitly. Raft code
// must never call std::chrono::steady_clock::now() directly.
struct Clock {
    virtual TimePoint now() = 0;
    virtual ~Clock() = default;
};

struct SteadyClock final : Clock {
    TimePoint now() override { return std::chrono::steady_clock::now(); }
};

// Test clock: starts at the steady_clock epoch and only moves when advanced.
class ManualClock final : public Clock {
public:
    TimePoint now() override { return now_; }
    void advance(Duration d) { now_ += d; }

private:
    TimePoint now_{};
};

}  // namespace rsm::raft
