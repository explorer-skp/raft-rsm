#include "bench_report.h"

#include <sys/utsname.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>

namespace rsm::bench {

namespace {

std::string readFirstLine(const std::string& path) {
    std::ifstream f(path);
    std::string line;
    if (f && std::getline(f, line)) return line;
    return {};
}

}  // namespace

MachineState captureMachineState() {
    MachineState m;
    // CPU model + logical CPU count.
    {
        std::ifstream f("/proc/cpuinfo");
        std::string line;
        while (std::getline(f, line)) {
            if (line.rfind("processor", 0) == 0) ++m.cpus;
            if (m.cpuModel.empty() && line.rfind("model name", 0) == 0) {
                const auto colon = line.find(':');
                if (colon != std::string::npos) {
                    m.cpuModel = line.substr(colon + 2);
                }
            }
        }
    }
    // Scaling governors + current frequencies across cpufreq policies.
    {
        std::set<std::string> governors;
        long minKHz = -1;
        long maxKHz = -1;
        std::error_code ec;
        const std::string base = "/sys/devices/system/cpu/cpufreq";
        for (const auto& entry :
             std::filesystem::directory_iterator(base, ec)) {
            const auto gov =
                readFirstLine(entry.path().string() + "/scaling_governor");
            if (!gov.empty()) governors.insert(gov);
            const auto cur =
                readFirstLine(entry.path().string() + "/scaling_cur_freq");
            if (!cur.empty()) {
                const long khz = std::stol(cur);
                if (minKHz < 0 || khz < minKHz) minKHz = khz;
                if (khz > maxKHz) maxKHz = khz;
            }
        }
        std::string joined;
        for (const auto& g : governors) {
            if (!joined.empty()) joined += ",";
            joined += g;
        }
        m.governors = joined.empty() ? "n/a" : joined;
        if (minKHz > 0) {
            m.curFreqMHz = std::to_string(minKHz / 1000) + "-" +
                           std::to_string(maxKHz / 1000);
        } else {
            m.curFreqMHz = "n/a";
        }
    }
    {
        const auto v =
            readFirstLine("/sys/devices/system/cpu/intel_pstate/no_turbo");
        m.noTurbo = v.empty() ? "n/a" : v;
    }
    {
        utsname uts{};
        if (uname(&uts) == 0) {
            m.kernel = std::string(uts.sysname) + " " + uts.release;
        }
    }
    {
        const auto la = readFirstLine("/proc/loadavg");
        if (!la.empty()) m.loadavg1 = std::stod(la);
    }
    {
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(
                 "/sys/class/thermal", ec)) {
            const auto name = entry.path().filename().string();
            if (name.rfind("thermal_zone", 0) != 0) continue;
            const auto t = readFirstLine(entry.path().string() + "/temp");
            if (!t.empty()) {
                const long c = std::stol(t) / 1000;
                if (c > m.maxTempC) m.maxTempC = c;
            }
        }
    }
    return m;
}

void JsonWriter::value(std::uint64_t v) {
    comma();
    char buf[24];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(v));
    out_ += buf;
}

void JsonWriter::value(double v) {
    comma();
    char buf[40];
    std::snprintf(buf, sizeof(buf), "%.6g", v);
    out_ += buf;
}

void JsonWriter::appendString(std::string_view s) {
    out_ += '"';
    for (const char c : s) {
        switch (c) {
            case '"': out_ += "\\\""; break;
            case '\\': out_ += "\\\\"; break;
            case '\n': out_ += "\\n"; break;
            case '\t': out_ += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out_ += buf;
                } else {
                    out_ += c;
                }
        }
    }
    out_ += '"';
}

void writeHistogram(JsonWriter& w, const rsm::metrics::LatencyHistogram& h) {
    w.beginObject();
    w.kv("count", h.count());
    w.kv("min_ns", h.minValue());
    w.kv("mean_ns", h.mean());
    w.kv("p50_ns", h.valueAtQuantile(0.50));
    w.kv("p90_ns", h.valueAtQuantile(0.90));
    w.kv("p99_ns", h.valueAtQuantile(0.99));
    w.kv("p999_ns", h.valueAtQuantile(0.999));
    w.kv("p9999_ns", h.valueAtQuantile(0.9999));
    w.kv("max_ns", h.maxValue());
    // Dense quantile grid: enough to regenerate the CDF plot from the file.
    w.key("quantiles");
    w.beginArray();
    const auto point = [&](double q) {
        w.beginArray();
        w.value(q);
        w.value(h.valueAtQuantile(q));
        w.endArray();
    };
    for (int i = 1; i <= 99; ++i) point(i / 100.0);
    for (const double q :
         {0.995, 0.999, 0.9995, 0.9999, 0.99995, 0.99999, 1.0}) {
        point(q);
    }
    w.endArray();
    w.endObject();
}

void writeMachine(JsonWriter& w, const MachineState& m) {
    w.beginObject();
    w.kv("cpu_model", m.cpuModel);
    w.kv("cpus", m.cpus);
    w.kv("governors", m.governors);
    w.kv("intel_pstate_no_turbo", m.noTurbo);
    w.kv("kernel", m.kernel);
    w.kv("loadavg1", m.loadavg1);
    w.kv("max_temp_c", static_cast<std::uint64_t>(
                           m.maxTempC < 0 ? 0 : m.maxTempC));
    w.kv("cur_freq_mhz", m.curFreqMHz);
    w.endObject();
}

bool writeFile(const std::string& path, const std::string& contents) {
    std::ofstream f(path, std::ios::trunc);
    if (!f) return false;
    f << contents;
    return static_cast<bool>(f);
}

}  // namespace rsm::bench
