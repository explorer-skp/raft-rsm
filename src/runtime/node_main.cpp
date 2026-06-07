// Node entrypoint: brings up the transport from a peer config and runs the
// Phase 7 threaded runtime (network rx -> Raft -> {tx, apply} over lock-free
// rings) over durable storage (Phase 4): currentTerm/votedFor and the log
// live in --data-dir and survive restart; recovery (replay, torn tail
// repair) happens during construction, before the node says a word.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <memory>

#include "client/client_service.h"
#include "raft/clock.h"
#include "raft/raft_core.h"
#include "runtime/node_runtime.h"
#include "statemachine/kv_store.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"
#include "transport/transport.h"

namespace {

std::atomic<bool> g_stop{false};

void handleSignal(int) {
    g_stop.store(true);
}

int usage(const char* argv0) {
    std::fprintf(stderr,
                 "usage: %s --id N --config <file> --data-dir <dir> "
                 "[--fsync every|group] [--wait block|spin] [--batch N] "
                 "[--linger-us N]\n",
                 argv0);
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    long id = -1;
    std::string configPath;
    std::string dataDir;
    auto fsyncPolicy = rsm::storage::FsyncPolicy::EveryDurabilityPoint;
    auto waitMode = rsm::runtime::WaitMode::Block;
    long batch = 1;
    long lingerUs = 200;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            id = std::strtol(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--config") == 0 && i + 1 < argc) {
            configPath = argv[++i];
        } else if (std::strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            dataDir = argv[++i];
        } else if (std::strcmp(argv[i], "--fsync") == 0 && i + 1 < argc) {
            const std::string v = argv[++i];
            if (v == "every") {
                fsyncPolicy = rsm::storage::FsyncPolicy::EveryDurabilityPoint;
            } else if (v == "group") {
                fsyncPolicy = rsm::storage::FsyncPolicy::GroupCommit;
            } else {
                return usage(argv[0]);
            }
        } else if (std::strcmp(argv[i], "--batch") == 0 && i + 1 < argc) {
            batch = std::strtol(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--linger-us") == 0 && i + 1 < argc) {
            lingerUs = std::strtol(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--wait") == 0 && i + 1 < argc) {
            const std::string v = argv[++i];
            if (v == "block") {
                waitMode = rsm::runtime::WaitMode::Block;
            } else if (v == "spin") {
                waitMode = rsm::runtime::WaitMode::Spin;
            } else {
                return usage(argv[0]);
            }
        } else {
            return usage(argv[0]);
        }
    }
    if (id < 0 || id > 0xFFFF || configPath.empty() || dataDir.empty() ||
        batch < 1 || lingerUs < 0) {
        return usage(argv[0]);
    }
    const auto selfId = static_cast<rsm::rpc::NodeId>(id);

    try {
        const auto peers = rsm::transport::loadPeerConfig(configPath);
        const auto self = peers.find(selfId);
        if (self == peers.end()) {
            std::fprintf(stderr, "node id %ld not present in %s\n", id,
                         configPath.c_str());
            return 1;
        }

        rsm::transport::Transport transport(selfId, self->second.port);
        std::vector<rsm::rpc::NodeId> peerIds;
        for (const auto& [peerId, addr] : peers) {
            if (peerId == selfId) continue;
            transport.addPeer(peerId, addr);
            peerIds.push_back(peerId);
        }

        rsm::raft::SteadyClock clock;
        std::filesystem::create_directories(dataDir);
        rsm::storage::DurablePersistentState persist(dataDir);
        rsm::storage::DurableLog log(dataDir, fsyncPolicy);
        std::printf(
            "node %ld recovered: term=%llu votedFor=%s lastLogIndex=%llu "
            "(torn bytes discarded: %llu)\n",
            id, static_cast<unsigned long long>(persist.currentTerm()),
            persist.votedFor()
                ? std::to_string(*persist.votedFor()).c_str()
                : "none",
            static_cast<unsigned long long>(log.lastIndex()),
            static_cast<unsigned long long>(log.tornBytesDiscarded()));
        rsm::statemachine::KVStateMachine sm;
        const std::uint64_t seed = std::random_device{}();
        // The runtime is constructed after the core (it needs the core
        // reference), so the core's send hook indirects through this
        // pointer; no message can be sent before runtime->start() anyway.
        std::unique_ptr<rsm::runtime::NodeRuntime> runtime;
        const auto pipelineSend = [&runtime, &transport](
                                      rsm::rpc::NodeId to,
                                      const rsm::rpc::Message& m) {
            // Encode on the calling pipeline thread, socket write on the tx
            // thread; drop-on-failure as before (Raft retries by timer,
            // clients retry by timeout).
            if (runtime) runtime->sendFromPipeline(to, m);
            else transport.send(to, m);
        };
        rsm::raft::RaftCore core(selfId, peerIds, persist, log, sm, clock,
                                 seed, rsm::raft::RaftConfig{}, pipelineSend);
        rsm::client::ClientService clientService(core, pipelineSend);
        if (batch > 1) {
            clientService.setBatching(rsm::client::ClientService::Batching{
                static_cast<std::size_t>(batch),
                std::chrono::microseconds(lingerUs)});
        }
        core.setClientRequestHandler(
            [&clientService](rsm::rpc::NodeId from,
                             const rsm::rpc::ClientRequest& req) {
                clientService.onClientRequest(from, req);
            });
        rsm::runtime::NodeRuntimeConfig runtimeCfg;
        runtimeCfg.waitMode = waitMode;
        runtime = std::make_unique<rsm::runtime::NodeRuntime>(
            core, transport, sm,
            [&clientService](rsm::rpc::LogIndex index,
                             const rsm::rpc::LogEntry& entry,
                             const std::string& result) {
                clientService.onApplied(index, entry, result);
            },
            runtimeCfg);
        runtime->setServiceHook(
            [&clientService](rsm::raft::TimePoint now) {
                return clientService.flushIfDue(now);
            });

        transport.setRawHandler(
            [&runtime](std::span<const std::uint8_t> body) {
                return runtime->enqueueFrame(body);
            });
        runtime->start();
        transport.start();
        std::printf("node %u listening on 127.0.0.1:%u (raft seed %llu)\n",
                    selfId, transport.listenPort(),
                    static_cast<unsigned long long>(seed));
        std::fflush(stdout);

        std::signal(SIGINT, handleSignal);
        std::signal(SIGTERM, handleSignal);
        while (!g_stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        transport.stop();
        runtime->stop();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
    return 0;
}
