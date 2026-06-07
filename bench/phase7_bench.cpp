// Phase 7 before/after bench: an in-process 3-node cluster (same wiring as
// node_main) hammered by closed-loop client threads, reporting throughput and
// client-perceived latency percentiles. This is deliberately NOT the rigorous
// Phase 8 harness (no open-loop arrivals, no coordinated-omission correction)
// — it exists to produce defensible RELATIVE numbers for each Phase 7
// optimization on a fixed local workload.
//
// Usage (all optional):
//   phase7_bench --clients 4 --seconds 5 --warmup 2 --keys 16
//                --value-bytes 16 --data-base /dev/shm --fsync every|group
//                --runtime threaded|legacy
//
// The default data dir lives on tmpfs so fsync cost does not drown the
// plumbing being measured; point --data-base at a real disk to see the
// group-commit effect instead.

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client/client_service.h"
#include "client/kv_client.h"
#include "metrics/histogram.h"
#include "raft/clock.h"
#include "raft/event_loop.h"
#include "raft/raft_core.h"
#include "runtime/node_runtime.h"
#include "statemachine/kv_store.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"
#include "transport/transport.h"

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    int clients = 4;
    int seconds = 5;
    int warmup = 2;
    int keys = 16;
    int valueBytes = 16;
    std::string dataBase = "/dev/shm";
    rsm::storage::FsyncPolicy fsync =
        rsm::storage::FsyncPolicy::EveryDurabilityPoint;
    bool threaded = true;  // --runtime threaded (default) | legacy
    rsm::runtime::WaitMode wait = rsm::runtime::WaitMode::Block;
    int batch = 1;      // group-commit batch size (1 = off)
    int lingerUs = 200; // group-commit linger
};

// One cluster node. --runtime threaded wires the Phase 7 NodeRuntime
// (rings + thread split, as node_main does); --runtime legacy keeps the
// Phase 6 wiring (RaftEventLoop, apply inline on the loop thread) so the
// thread-split before/after stays measurable from one binary.
struct BenchNode {
    std::unique_ptr<rsm::storage::DurablePersistentState> persist;
    std::unique_ptr<rsm::storage::DurableLog> log;
    std::unique_ptr<rsm::statemachine::KVStateMachine> sm;
    std::unique_ptr<rsm::client::ClientService> service;
    rsm::raft::SteadyClock clock;
    std::unique_ptr<rsm::transport::Transport> transport;
    std::unique_ptr<rsm::raft::RaftCore> core;
    std::unique_ptr<rsm::raft::RaftEventLoop> loop;        // legacy mode
    std::unique_ptr<rsm::runtime::NodeRuntime> runtime;    // threaded mode

    void stop() {
        transport->stop();
        if (loop) loop->stop();
        if (runtime) runtime->stop();
    }
};

int usage(const char* argv0) {
    std::fprintf(stderr,
                 "usage: %s [--clients N] [--seconds S] [--warmup W] "
                 "[--keys K] [--value-bytes B] [--data-base DIR] "
                 "[--fsync every|group] [--runtime threaded|legacy] "
                 "[--wait block|spin] [--batch N] [--linger-us N]\n",
                 argv0);
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const auto intArg = [&](const char* name, int& out) {
            if (std::strcmp(argv[i], name) == 0 && i + 1 < argc) {
                out = std::atoi(argv[++i]);
                return true;
            }
            return false;
        };
        if (intArg("--clients", opt.clients) ||
            intArg("--seconds", opt.seconds) ||
            intArg("--warmup", opt.warmup) || intArg("--keys", opt.keys) ||
            intArg("--value-bytes", opt.valueBytes) ||
            intArg("--batch", opt.batch) ||
            intArg("--linger-us", opt.lingerUs)) {
            continue;
        }
        if (std::strcmp(argv[i], "--data-base") == 0 && i + 1 < argc) {
            opt.dataBase = argv[++i];
        } else if (std::strcmp(argv[i], "--runtime") == 0 && i + 1 < argc) {
            const std::string v = argv[++i];
            if (v == "threaded") {
                opt.threaded = true;
            } else if (v == "legacy") {
                opt.threaded = false;
            } else {
                return usage(argv[0]);
            }
        } else if (std::strcmp(argv[i], "--wait") == 0 && i + 1 < argc) {
            const std::string v = argv[++i];
            if (v == "block") {
                opt.wait = rsm::runtime::WaitMode::Block;
            } else if (v == "spin") {
                opt.wait = rsm::runtime::WaitMode::Spin;
            } else {
                return usage(argv[0]);
            }
        } else if (std::strcmp(argv[i], "--fsync") == 0 && i + 1 < argc) {
            const std::string v = argv[++i];
            if (v == "every") {
                opt.fsync = rsm::storage::FsyncPolicy::EveryDurabilityPoint;
            } else if (v == "group") {
                opt.fsync = rsm::storage::FsyncPolicy::GroupCommit;
            } else {
                return usage(argv[0]);
            }
        } else {
            return usage(argv[0]);
        }
    }
    if (opt.clients < 1 || opt.seconds < 1 || opt.keys < 1 ||
        opt.batch < 1 || opt.lingerUs < 0) {
        return usage(argv[0]);
    }
    if (opt.batch > 1 && !opt.threaded) {
        std::fprintf(stderr,
                     "--batch requires --runtime threaded (the legacy loop "
                     "has no linger driver)\n");
        return 2;
    }

    const auto runDir =
        opt.dataBase + "/phase7_bench." + std::to_string(::getpid());
    std::filesystem::remove_all(runDir);

    constexpr int kNodes = 3;
    std::vector<std::unique_ptr<BenchNode>> nodes;
    rsm::transport::PeerMap peerMap;

    // Bind all transports first so every peer address is known before any
    // core starts campaigning (same startup order as the test harness).
    for (rsm::rpc::NodeId id = 1; id <= kNodes; ++id) {
        auto n = std::make_unique<BenchNode>();
        n->transport = std::make_unique<rsm::transport::Transport>(id, 0);
        peerMap[id] = rsm::transport::PeerAddress{
            "127.0.0.1", n->transport->listenPort()};
        nodes.push_back(std::move(n));
    }
    for (rsm::rpc::NodeId id = 1; id <= kNodes; ++id) {
        auto& n = *nodes[id - 1];
        const auto dir = runDir + "/node" + std::to_string(id);
        std::filesystem::create_directories(dir);
        std::vector<rsm::rpc::NodeId> peerIds;
        for (rsm::rpc::NodeId p = 1; p <= kNodes; ++p) {
            if (p == id) continue;
            n.transport->addPeer(p, peerMap[p]);
            peerIds.push_back(p);
        }
        n.persist =
            std::make_unique<rsm::storage::DurablePersistentState>(dir);
        n.log = std::make_unique<rsm::storage::DurableLog>(dir, opt.fsync);
        n.sm = std::make_unique<rsm::statemachine::KVStateMachine>();
        // Threaded mode: sends encode on the Raft/apply thread and go out
        // via the tx thread; legacy mode sends synchronously (Phase 6).
        const auto sendHook = [rt = &n.runtime, t = n.transport.get()](
                                  rsm::rpc::NodeId to,
                                  const rsm::rpc::Message& m) {
            if (*rt) (*rt)->sendFromPipeline(to, m);
            else t->send(to, m);
        };
        n.core = std::make_unique<rsm::raft::RaftCore>(
            id, peerIds, *n.persist, *n.log, *n.sm, n.clock,
            /*rngSeed=*/7000 + id, rsm::raft::RaftConfig{}, sendHook);
        n.service = std::make_unique<rsm::client::ClientService>(*n.core,
                                                                 sendHook);
        if (opt.batch > 1) {
            n.service->setBatching(rsm::client::ClientService::Batching{
                static_cast<std::size_t>(opt.batch),
                std::chrono::microseconds(opt.lingerUs)});
        }
        n.core->setClientRequestHandler(
            [svc = n.service.get()](rsm::rpc::NodeId from,
                                    const rsm::rpc::ClientRequest& r) {
                svc->onClientRequest(from, r);
            });
        const auto onApplied = [svc = n.service.get()](
                                   rsm::rpc::LogIndex index,
                                   const rsm::rpc::LogEntry& entry,
                                   const std::string& result) {
            svc->onApplied(index, entry, result);
        };
        if (opt.threaded) {
            rsm::runtime::NodeRuntimeConfig rcfg;
            rcfg.waitMode = opt.wait;
            n.runtime = std::make_unique<rsm::runtime::NodeRuntime>(
                *n.core, *n.transport, *n.sm, onApplied, rcfg);
            n.runtime->setServiceHook(
                [svc = n.service.get()](rsm::raft::TimePoint now) {
                    return svc->flushIfDue(now);
                });
            n.transport->setRawHandler(
                [rt = n.runtime.get()](std::span<const std::uint8_t> body) {
                    return rt->enqueueFrame(body);
                });
        } else {
            n.core->setApplyObserver(onApplied);
            n.loop = std::make_unique<rsm::raft::RaftEventLoop>(*n.core);
            n.transport->setHandler([l = n.loop.get()](
                                        const rsm::rpc::Envelope& env,
                                        rsm::rpc::Message&& m) {
                l->enqueue(env, std::move(m));
            });
        }
    }
    for (auto& n : nodes) {
        if (n->loop) n->loop->start();
        if (n->runtime) n->runtime->start();
        n->transport->start();
    }


    // Client threads: closed-loop PUTs over a small key space. Warmup covers
    // the election and steady-states the caches; only ops whose start AND
    // end fall inside the measurement window are recorded.
    const auto t0 = Clock::now();
    const auto measureStart = t0 + std::chrono::seconds(opt.warmup);
    const auto measureEnd = measureStart + std::chrono::seconds(opt.seconds);
    std::atomic<std::uint64_t> failures{0};
    std::vector<rsm::metrics::LatencyHistogram> hists(
        static_cast<std::size_t>(opt.clients));
    std::vector<std::uint64_t> opsDone(static_cast<std::size_t>(opt.clients));
    std::vector<std::thread> clients;
    const std::string value(static_cast<std::size_t>(opt.valueBytes), 'v');

    for (int c = 0; c < opt.clients; ++c) {
        clients.emplace_back([&, c] {
            rsm::client::KvClient kv(
                peerMap, /*clientId=*/100'000 + static_cast<std::uint64_t>(c),
                /*clientNodeId=*/
                static_cast<rsm::rpc::NodeId>(300 + c));
            std::uint64_t i = 0;
            while (true) {
                const auto start = Clock::now();
                if (start >= measureEnd) break;
                const std::string key =
                    "k" + std::to_string(i++ % static_cast<std::uint64_t>(
                                                  opt.keys));
                const auto r = kv.put(key, value);
                const auto end = Clock::now();
                if (!r || r->status != rsm::statemachine::kKvOk) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (start >= measureStart && end < measureEnd) {
                    hists[static_cast<std::size_t>(c)].record(
                        static_cast<std::uint64_t>(
                            std::chrono::duration_cast<
                                std::chrono::nanoseconds>(end - start)
                                .count()));
                    opsDone[static_cast<std::size_t>(c)]++;
                }
            }
        });
    }
    for (auto& t : clients) t.join();

    for (auto& n : nodes) n->stop();

    rsm::metrics::LatencyHistogram all;
    std::uint64_t totalOps = 0;
    for (int c = 0; c < opt.clients; ++c) {
        all.merge(hists[static_cast<std::size_t>(c)]);
        totalOps += opsDone[static_cast<std::size_t>(c)];
    }
    std::printf(
        "phase7_bench runtime=%s wait=%s batch=%d linger-us=%d clients=%d "
        "seconds=%d warmup=%d keys=%d value-bytes=%d data-base=%s "
        "fsync=%s\n",
        opt.threaded ? "threaded" : "legacy",
        opt.wait == rsm::runtime::WaitMode::Block ? "block" : "spin",
        opt.batch, opt.lingerUs, opt.clients, opt.seconds,
        opt.warmup, opt.keys, opt.valueBytes, opt.dataBase.c_str(),
        opt.fsync == rsm::storage::FsyncPolicy::EveryDurabilityPoint
            ? "every"
            : "group");
    std::printf("throughput: %.0f ops/s (%llu ops in %ds, %llu failures)\n",
                static_cast<double>(totalOps) / opt.seconds,
                static_cast<unsigned long long>(totalOps), opt.seconds,
                static_cast<unsigned long long>(failures.load()));
    std::printf("latency:    %s\n",
                rsm::metrics::summarizeNs(all).c_str());

    std::filesystem::remove_all(runDir);
    return 0;
}
