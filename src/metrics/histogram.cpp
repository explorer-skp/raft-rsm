#include "metrics/histogram.h"

#include <cstdio>

namespace rsm::metrics {

namespace {

std::string fmtNs(std::uint64_t ns) {
    char buf[32];
    if (ns >= 1'000'000) {
        std::snprintf(buf, sizeof(buf), "%.2fms",
                      static_cast<double>(ns) / 1e6);
    } else if (ns >= 1'000) {
        std::snprintf(buf, sizeof(buf), "%.1fus",
                      static_cast<double>(ns) / 1e3);
    } else {
        std::snprintf(buf, sizeof(buf), "%lluns",
                      static_cast<unsigned long long>(ns));
    }
    return buf;
}

}  // namespace

std::string summarizeNs(const LatencyHistogram& h) {
    std::string out = "n=" + std::to_string(h.count());
    out += " p50=" + fmtNs(h.valueAtQuantile(0.50));
    out += " p90=" + fmtNs(h.valueAtQuantile(0.90));
    out += " p99=" + fmtNs(h.valueAtQuantile(0.99));
    out += " p99.9=" + fmtNs(h.valueAtQuantile(0.999));
    out += " max=" + fmtNs(h.maxValue());
    return out;
}

}  // namespace rsm::metrics
