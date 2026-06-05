// chaos_sim: replay one seeded chaos run and print the verdict.
//
//   ./build/faults/chaos_sim --seed N [--nodes K] [--clients C]
//                            [--ops-per-client P] [--fault-end-ms T]
//                            [--max-ms T] [--in-memory] [--trace]
//
// Defaults are EXACTLY the ctest chaos suite's, so a failing seed printed
// by the suite reproduces with just --seed N. Exit code 0 iff the run
// passed.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "faults/chaos.h"
#include "raft/logging.h"

namespace {

void usage(const char* argv0) {
    std::fprintf(stderr,
                 "usage: %s --seed N [--nodes K] [--clients C] "
                 "[--ops-per-client P]\n"
                 "          [--fault-end-ms T] [--max-ms T] [--in-memory] "
                 "[--trace]\n",
                 argv0);
}

}  // namespace

int main(int argc, char** argv) {
    // A chaos run produces thousands of seeded transitions; the run trace
    // (--trace) is the readable record, not the per-node log.
    rsm::raft::setRaftLogLevel(rsm::raft::LogLevel::Error);
    std::uint64_t seed = 0;
    bool haveSeed = false;
    rsm::sim::ChaosOptions options;
    bool dumpTrace = false;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        const auto next = [&]() -> const char* {
            if (i + 1 >= argc) {
                usage(argv[0]);
                std::exit(2);
            }
            return argv[++i];
        };
        if (arg == "--seed") {
            seed = std::strtoull(next(), nullptr, 0);
            haveSeed = true;
        } else if (arg == "--nodes") {
            options.nodes = std::atoi(next());
        } else if (arg == "--clients") {
            options.clients = std::atoi(next());
        } else if (arg == "--ops-per-client") {
            options.opsPerClient = std::atoi(next());
        } else if (arg == "--fault-end-ms") {
            options.faultEndMs = std::atoll(next());
        } else if (arg == "--max-ms") {
            options.maxMs = std::atoll(next());
        } else if (arg == "--in-memory") {
            options.durableStorage = false;
        } else if (arg == "--trace") {
            dumpTrace = true;
            options.recordTrace = true;
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (!haveSeed) {
        usage(argv[0]);
        return 2;
    }

    std::printf("chaos_sim: nodes=%d clients=%d ops-per-client=%d "
                "fault-end-ms=%lld max-ms=%lld storage=%s\n",
                options.nodes, options.clients, options.opsPerClient,
                static_cast<long long>(options.faultEndMs),
                static_cast<long long>(options.maxMs),
                options.durableStorage ? "durable" : "in-memory");

    const auto report = rsm::sim::runChaos(seed, options);
    if (dumpTrace) {
        for (const auto& line : report.trace) std::printf("%s\n", line.c_str());
    }
    std::printf("%s\n", report.summary().c_str());
    return report.pass ? 0 : 1;
}
