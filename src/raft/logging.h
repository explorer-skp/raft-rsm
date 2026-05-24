#pragma once

#include <atomic>
#include <cstdio>

namespace rsm::raft {

// Minimal level-gated logging for Raft observability. Default Info: role
// transitions are visible in a test run without per-message noise. The
// chaotic sim tests lower this to Warn so thousands of seeded transitions
// don't drown the output.
enum class LogLevel : int { Debug = 0, Info = 1, Warn = 2, Error = 3, Off = 4 };

inline std::atomic<LogLevel> g_raftLogLevel{LogLevel::Info};

inline void setRaftLogLevel(LogLevel l) { g_raftLogLevel.store(l); }

inline bool raftLogEnabled(LogLevel l) {
    return static_cast<int>(l) >=
           static_cast<int>(g_raftLogLevel.load(std::memory_order_relaxed));
}

template <typename... Args>
void raftLog(LogLevel level, const char* fmt, Args... args) {
    if (!raftLogEnabled(level)) return;
    std::fprintf(stderr, fmt, args...);
    std::fputc('\n', stderr);
}

}  // namespace rsm::raft
