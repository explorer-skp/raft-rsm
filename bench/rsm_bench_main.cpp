// Phase 8 benchmark driver. One invocation = one measured run = one JSON
// raw-data file; every number in the README regenerates from these files
// via bench/run_benchmarks.sh (which also records the host state).
//
// Modes:
//   closed    closed-loop clients (saturation throughput, service latency)
//   open      fixed-rate open-loop arrivals, coordinated-omission-safe
//             (latency measured from the intended send time)
//   failover  kill the leader under load, many trials, report the
//             distribution of time-to-first-committed-write
//
// Fault flags (closed/open): --loss-pct, --partition-period-ms +
// --partition-len-ms. A run with faults configured is reported as such and
// exempt from the clean-load leadership-stability verdict; a CLEAN run that
// records an election inside the measurement window is INVALID — the
// harness says so loudly, writes valid=false into the JSON, and exits 3.
// Measuring a cluster that is quietly re-electing is measuring a degraded
// system (DESIGN.md, Phase 8).

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "bench_cluster.h"
#include "bench_report.h"
#include "bench_util.h"
#include "load_gen.h"
#include "metrics/histogram.h"

namespace {

using namespace rsm::bench;

struct Options {
    std::string mode = "closed";
    bool orderBook = false;  // --sm orderbook: NEW-order workload (Phase 9)
    std::uint64_t seed = 1;
    int seconds = 10;
    int warmup = 3;
    int threads = 4;
    double rate = 1000.0;       // open
    int trials = 50;            // failover
    int keys = 64;
    int valueBytes = 16;
    std::string dataBase = "/dev/shm";
    rsm::storage::FsyncPolicy fsync =
        rsm::storage::FsyncPolicy::EveryDurabilityPoint;
    rsm::runtime::WaitMode wait = rsm::runtime::WaitMode::Block;
    int batch = 1;
    int lingerUs = 200;
    bool pin = false;
    double lossPct = 0.0;
    int partitionPeriodMs = 0;
    int partitionLenMs = 0;
    int drainCapS = 10;
    std::string out;            // JSON path ("" = no file)
    std::string label;
};

int usage(const char* argv0) {
    std::fprintf(
        stderr,
        "usage: %s --mode closed|open|failover [options]\n"
        "  --sm kv|orderbook (workload + replicated state machine)\n"
        "  --seed N --seconds S --warmup W --threads T --keys K\n"
        "  --value-bytes B --data-base DIR --fsync every|group\n"
        "  --wait block|spin --batch N --linger-us N --pin\n"
        "  --rate R (open) --drain-cap-s N (open) --trials N (failover)\n"
        "  --loss-pct P --partition-period-ms P --partition-len-ms L\n"
        "  --out FILE.json --label STR\n",
        argv0);
    return 2;
}

const char* waitName(rsm::runtime::WaitMode w) {
    return w == rsm::runtime::WaitMode::Block ? "block" : "spin";
}
const char* fsyncName(rsm::storage::FsyncPolicy f) {
    return f == rsm::storage::FsyncPolicy::EveryDurabilityPoint ? "every"
                                                                : "group";
}

void writeConfig(JsonWriter& w, const Options& o) {
    w.key("config");
    w.beginObject();
    w.kv("mode", o.mode);
    w.kv("sm", o.orderBook ? "orderbook" : "kv");
    w.kv("seed", o.seed);
    w.kv("seconds", o.seconds);
    w.kv("warmup", o.warmup);
    w.kv("threads", o.threads);
    w.kv("rate", o.rate);
    w.kv("trials", o.trials);
    w.kv("keys", o.keys);
    w.kv("value_bytes", o.valueBytes);
    w.kv("data_base", o.dataBase);
    w.kv("fsync", fsyncName(o.fsync));
    w.kv("wait", waitName(o.wait));
    w.kv("batch", o.batch);
    w.kv("linger_us", o.lingerUs);
    w.kv("pin", o.pin);
    w.kv("loss_pct", o.lossPct);
    w.kv("partition_period_ms", o.partitionPeriodMs);
    w.kv("partition_len_ms", o.partitionLenMs);
    w.endObject();
}

void printHist(const char* name, const rsm::metrics::LatencyHistogram& h) {
    if (h.count() == 0) return;
    std::printf(
        "%-14s count=%llu p50=%.1fus p99=%.1fus p99.9=%.1fus "
        "p99.99=%.1fus max=%.1fus\n",
        name, static_cast<unsigned long long>(h.count()),
        h.valueAtQuantile(0.50) / 1e3, h.valueAtQuantile(0.99) / 1e3,
        h.valueAtQuantile(0.999) / 1e3, h.valueAtQuantile(0.9999) / 1e3,
        static_cast<double>(h.maxValue()) / 1e3);
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const auto next = [&]() -> const char* {
            return i + 1 < argc ? argv[++i] : nullptr;
        };
        const auto is = [&](const char* name) {
            return std::strcmp(argv[i], name) == 0;
        };
        if (is("--mode")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.mode = v;
        } else if (is("--sm")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            if (std::strcmp(v, "kv") == 0) {
                opt.orderBook = false;
            } else if (std::strcmp(v, "orderbook") == 0) {
                opt.orderBook = true;
            } else {
                return usage(argv[0]);
            }
        } else if (is("--seed")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.seed = std::strtoull(v, nullptr, 10);
        } else if (is("--seconds")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.seconds = std::atoi(v);
        } else if (is("--warmup")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.warmup = std::atoi(v);
        } else if (is("--threads")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.threads = std::atoi(v);
        } else if (is("--rate")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.rate = std::atof(v);
        } else if (is("--trials")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.trials = std::atoi(v);
        } else if (is("--keys")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.keys = std::atoi(v);
        } else if (is("--value-bytes")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.valueBytes = std::atoi(v);
        } else if (is("--data-base")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.dataBase = v;
        } else if (is("--fsync")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            if (std::strcmp(v, "every") == 0) {
                opt.fsync = rsm::storage::FsyncPolicy::EveryDurabilityPoint;
            } else if (std::strcmp(v, "group") == 0) {
                opt.fsync = rsm::storage::FsyncPolicy::GroupCommit;
            } else {
                return usage(argv[0]);
            }
        } else if (is("--wait")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            if (std::strcmp(v, "block") == 0) {
                opt.wait = rsm::runtime::WaitMode::Block;
            } else if (std::strcmp(v, "spin") == 0) {
                opt.wait = rsm::runtime::WaitMode::Spin;
            } else {
                return usage(argv[0]);
            }
        } else if (is("--batch")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.batch = std::atoi(v);
        } else if (is("--linger-us")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.lingerUs = std::atoi(v);
        } else if (is("--pin")) {
            opt.pin = true;
        } else if (is("--loss-pct")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.lossPct = std::atof(v);
        } else if (is("--partition-period-ms")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.partitionPeriodMs = std::atoi(v);
        } else if (is("--partition-len-ms")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.partitionLenMs = std::atoi(v);
        } else if (is("--drain-cap-s")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.drainCapS = std::atoi(v);
        } else if (is("--out")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.out = v;
        } else if (is("--label")) {
            const char* v = next();
            if (!v) return usage(argv[0]);
            opt.label = v;
        } else {
            return usage(argv[0]);
        }
    }
    if (opt.mode != "closed" && opt.mode != "open" && opt.mode != "failover") {
        return usage(argv[0]);
    }
    if (opt.seconds < 1 || opt.warmup < 0 || opt.threads < 1 ||
        opt.keys < 1 || opt.valueBytes < 0 || opt.batch < 1 ||
        opt.lingerUs < 0 || opt.rate <= 0 || opt.trials < 1 ||
        opt.lossPct < 0 || opt.lossPct > 100) {
        return usage(argv[0]);
    }
    const bool faulted = opt.lossPct > 0 || opt.partitionPeriodMs > 0;

    // Print the full config up front — the reproducibility contract.
    std::printf(
        "rsm_bench mode=%s sm=%s seed=%llu seconds=%d warmup=%d threads=%d "
        "rate=%.0f trials=%d keys=%d value-bytes=%d data-base=%s fsync=%s "
        "wait=%s batch=%d linger-us=%d pin=%d loss-pct=%.2f "
        "partition=%dms/%dms label=%s\n",
        opt.mode.c_str(), opt.orderBook ? "orderbook" : "kv",
        static_cast<unsigned long long>(opt.seed),
        opt.seconds, opt.warmup, opt.threads, opt.rate, opt.trials, opt.keys,
        opt.valueBytes, opt.dataBase.c_str(), fsyncName(opt.fsync),
        waitName(opt.wait), opt.batch, opt.lingerUs, opt.pin ? 1 : 0,
        opt.lossPct, opt.partitionPeriodMs, opt.partitionLenMs,
        opt.label.c_str());

    const MachineState machineStart = captureMachineState();
    std::printf(
        "host: %s | %d cpus | governor=%s no_turbo=%s | %s | loadavg=%.2f "
        "temp=%ldC\n",
        machineStart.cpuModel.c_str(), machineStart.cpus,
        machineStart.governors.c_str(), machineStart.noTurbo.c_str(),
        machineStart.kernel.c_str(), machineStart.loadavg1,
        machineStart.maxTempC);

    BenchClusterConfig ccfg;
    ccfg.orderBook = opt.orderBook;
    ccfg.dataBase = opt.dataBase;
    ccfg.fsync = opt.fsync;
    ccfg.wait = opt.wait;
    ccfg.batch = opt.batch;
    ccfg.lingerUs = opt.lingerUs;
    ccfg.pinRaftThreads = opt.pin;
    ccfg.seed = opt.seed;
    BenchCluster cluster(ccfg);
    cluster.faults().setSeed(opt.seed);

    const auto leader = cluster.awaitReady(std::chrono::seconds(15));
    if (!leader) {
        std::fprintf(stderr, "FATAL: cluster failed to elect within 15s\n");
        return 4;
    }
    std::printf("cluster ready, leader=node%u\n", *leader);

    // Faults activate only once the cluster is formed, so warmup and the
    // window are both fully under fault.
    std::atomic<bool> stopPartition{false};
    std::thread partitionDriver;
    if (opt.lossPct > 0) {
        cluster.faults().setLossPermille(
            static_cast<std::uint32_t>(opt.lossPct * 10.0));
    }
    if (opt.partitionPeriodMs > 0 && opt.partitionLenMs > 0) {
        partitionDriver = std::thread([&] {
            int next = 0;
            while (!stopPartition.load()) {
                for (int waited = 0;
                     waited < opt.partitionPeriodMs && !stopPartition.load();
                     waited += 20) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(20));
                }
                if (stopPartition.load()) break;
                const auto victim = static_cast<rsm::rpc::NodeId>(
                    (next++ % cluster.nodeCount()) + 1);
                cluster.faults().isolate(victim);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(opt.partitionLenMs));
                cluster.faults().heal(victim);
            }
            cluster.faults().healAll();
        });
    }

    GenConfig gcfg;
    gcfg.orderBook = opt.orderBook;
    gcfg.threads = opt.threads;
    gcfg.seed = opt.seed;
    gcfg.keys = opt.keys;
    gcfg.valueBytes = opt.valueBytes;
    gcfg.ratePerSec = opt.rate;
    gcfg.drainCapNs = static_cast<std::uint64_t>(opt.drainCapS) * 1'000'000'000ull;

    const std::uint64_t t0 = nowNs();
    const std::uint64_t measureStart =
        t0 + static_cast<std::uint64_t>(opt.warmup) * 1'000'000'000ull;
    std::uint64_t measureEnd =
        measureStart + static_cast<std::uint64_t>(opt.seconds) * 1'000'000'000ull;
    if (opt.mode == "failover") measureEnd = UINT64_MAX;  // until trials done
    cluster.setCommitWindow(measureStart, measureEnd);

    GenStats stats;
    std::vector<double> failoverMs;
    rsm::metrics::LatencyHistogram failoverHist;

    if (opt.mode == "closed") {
        stats = runClosedLoop(cluster.peerMap(), gcfg, measureStart,
                              measureEnd);
    } else if (opt.mode == "open") {
        stats = runOpenLoop(cluster.peerMap(), gcfg, t0, measureStart,
                            measureEnd);
    } else {  // failover
        std::atomic<bool> stopLoad{false};
        std::thread load([&] {
            stats = runClosedLoop(cluster.peerMap(), gcfg, measureStart,
                                  UINT64_MAX, &stopLoad);
        });
        // Probe client: short attempts, long patience; one call() spans the
        // whole outage, so the success timestamp IS the recovery instant
        // (client-perceived: includes leader discovery via redirects).
        rsm::client::KvClient probe(cluster.peerMap(), /*clientId=*/0,
                                    /*clientNodeId=*/251,
                                    /*perAttemptTimeoutMs=*/100,
                                    /*maxAttempts=*/400);
        for (int trial = 0; trial < opt.trials; ++trial) {
            // Re-stabilize: a committed write must succeed, then settle.
            if (!cluster.awaitReady(std::chrono::seconds(15))) {
                std::fprintf(stderr,
                             "FATAL: no recovery before trial %d\n", trial);
                stopLoad.store(true);
                load.join();
                return 4;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(750));
            const auto victim = cluster.monitor().latestLeader();
            if (victim == 0) continue;
            const std::uint64_t killNs = nowNs();
            cluster.killNode(victim);
            // One committed write in the configured SM's command set spans
            // the outage (the order-book probe rests a 1-lot bid at the
            // minimum tick, same as awaitReady's — it can never cross).
            const auto r =
                opt.orderBook
                    ? probe.obNew(rsm::statemachine::ObSide::Bid, 1, 1)
                    : probe.put("__failover_probe", "x");
            const std::uint64_t okNs = nowNs();
            const char okStatus = opt.orderBook ? rsm::statemachine::kObOk
                                                : rsm::statemachine::kKvOk;
            if (r && r->status == okStatus) {
                const double ms =
                    static_cast<double>(okNs - killNs) / 1e6;
                failoverMs.push_back(ms);
                failoverHist.record(okNs - killNs);
                std::printf("trial %3d: leader node%u killed, recovered in "
                            "%.1f ms\n",
                            trial + 1, victim, ms);
            } else {
                std::fprintf(stderr, "trial %d: probe FAILED to recover\n",
                             trial + 1);
            }
            cluster.restartNode(victim);
        }
        stopLoad.store(true);
        load.join();
        measureEnd = nowNs();  // failover window ends when the trials do
    }

    if (partitionDriver.joinable()) {
        stopPartition.store(true);
        partitionDriver.join();
    }

    // Collect instrumentation before teardown.
    const auto commitHist = cluster.commitHistogram();
    const auto transitions = cluster.monitor().snapshot();
    const int electionsWindow =
        cluster.monitor().electionsIn(measureStart, measureEnd);
    const int electionsTotal = cluster.monitor().electionsIn(0, UINT64_MAX);
    const auto dataBytes = cluster.dataBytes();
    const MachineState machineEnd = captureMachineState();

    // Leadership-stability verdict: a clean steady-state run must hold one
    // leader at one term across the whole measurement window.
    bool valid = true;
    std::string invalidReason;
    if (opt.mode != "failover" && !faulted && electionsWindow > 0) {
        valid = false;
        invalidReason = "unexpected election(s) under clean load — "
                        "investigate as a bug, do not use these numbers";
    }

    const double windowS =
        opt.mode == "failover"
            ? static_cast<double>(measureEnd - measureStart) / 1e9
            : static_cast<double>(opt.seconds);
    const double throughput = static_cast<double>(stats.oks) / windowS;

    std::printf("---\n");
    if (opt.mode == "open") {
        std::printf("offered: %.0f/s  achieved sends: %.0f/s  "
                    "mean lateness: %.1fus  max lateness: %.1fus\n",
                    opt.rate,
                    static_cast<double>(stats.sends) / windowS,
                    stats.sends ? static_cast<double>(stats.sumSendLatenessNs) /
                                      static_cast<double>(stats.sends) / 1e3
                                : 0.0,
                    static_cast<double>(stats.maxSendLatenessNs) / 1e3);
    }
    std::printf("throughput: %.0f committed ops/s (%llu oks, %llu failures, "
                "%llu abandoned)\n",
                throughput, static_cast<unsigned long long>(stats.oks),
                static_cast<unsigned long long>(stats.failures),
                static_cast<unsigned long long>(stats.abandoned));
    printHist("e2e(intended)", stats.intended);
    printHist("e2e(actual)", stats.actual);
    printHist("commit", commitHist);
    printHist("failover", failoverHist);
    std::printf("elections: window=%d total=%d  max term=%llu  "
                "log bytes=%llu  pins=%s\n",
                electionsWindow, electionsTotal,
                static_cast<unsigned long long>(cluster.monitor().maxTerm()),
                static_cast<unsigned long long>(dataBytes),
                opt.pin ? (cluster.raftPinsApplied() ? "applied" : "FAILED")
                        : "off");
    if (!valid) {
        std::printf("RUN INVALID: %s\n", invalidReason.c_str());
    }

    if (!opt.out.empty()) {
        JsonWriter w;
        w.beginObject();
        w.kv("label", opt.label);
        writeConfig(w, opt);
        w.key("machine_start");
        writeMachine(w, machineStart);
        w.key("machine_end");
        writeMachine(w, machineEnd);
        w.key("results");
        w.beginObject();
        w.kv("valid", valid);
        w.kv("invalid_reason", invalidReason);
        w.kv("window_s", windowS);
        w.kv("throughput_cps", throughput);
        w.kv("oks", stats.oks);
        w.kv("failures", stats.failures);
        w.kv("scheduled", stats.scheduled);
        w.kv("abandoned", stats.abandoned);
        w.kv("offered_rate", opt.mode == "open" ? opt.rate : 0.0);
        w.kv("achieved_send_rate",
             static_cast<double>(stats.sends) / windowS);
        w.kv("mean_send_lateness_ns",
             stats.sends ? static_cast<double>(stats.sumSendLatenessNs) /
                               static_cast<double>(stats.sends)
                         : 0.0);
        w.kv("max_send_lateness_ns", stats.maxSendLatenessNs);
        w.kv("elections_in_window", electionsWindow);
        w.kv("elections_total", electionsTotal);
        w.kv("max_term", cluster.monitor().maxTerm());
        w.kv("raft_pins_applied", opt.pin && cluster.raftPinsApplied());
        w.kv("data_bytes", dataBytes);
        w.key("e2e_intended");
        writeHistogram(w, stats.intended);
        w.key("e2e_actual");
        writeHistogram(w, stats.actual);
        w.key("commit");
        writeHistogram(w, commitHist);
        w.key("failover");
        writeHistogram(w, failoverHist);
        w.key("failover_ms");
        w.beginArray();
        for (const double ms : failoverMs) w.value(ms);
        w.endArray();
        w.endObject();
        w.key("transitions");
        w.beginArray();
        std::size_t shown = 0;
        for (const auto& e : transitions) {
            if (++shown > 256) break;  // plenty; keeps files small
            w.beginArray();
            w.value(e.tNs);
            w.value(static_cast<std::uint64_t>(e.node));
            w.value(e.term);
            w.value(static_cast<std::uint64_t>(e.role));
            w.endArray();
        }
        w.endArray();
        w.endObject();
        if (!writeFile(opt.out, w.str())) {
            std::fprintf(stderr, "FATAL: cannot write %s\n",
                         opt.out.c_str());
            return 4;
        }
        std::printf("raw data: %s\n", opt.out.c_str());
    }

    return valid ? 0 : 3;
}
