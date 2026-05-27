// Node entrypoint: brings up the transport from a peer config and runs the
// Raft event loop over durable storage (Phase 4): currentTerm/votedFor and
// the log live in --data-dir and survive restart; recovery (replay, torn
// tail repair) happens during construction, before the node says a word.

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

#include "raft/clock.h"
#include "raft/event_loop.h"
#include "raft/raft_core.h"
#include "statemachine/state_machine.h"
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
                 "[--fsync every|group]\n",
                 argv0);
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    long id = -1;
    std::string configPath;
    std::string dataDir;
    auto fsyncPolicy = rsm::storage::FsyncPolicy::EveryDurabilityPoint;
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
        } else {
            return usage(argv[0]);
        }
    }
    if (id < 0 || id > 0xFFFF || configPath.empty() || dataDir.empty()) {
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
        rsm::statemachine::RecordingStateMachine sm;
        const std::uint64_t seed = std::random_device{}();
        rsm::raft::RaftCore core(
            selfId, peerIds, persist, log, sm, clock, seed,
            rsm::raft::RaftConfig{},
            [&transport](rsm::rpc::NodeId to, const rsm::rpc::Message& m) {
                transport.send(to, m);  // drop-on-failure; Raft retries by timer
            });
        rsm::raft::RaftEventLoop loop(core);

        transport.setHandler([&loop](const rsm::rpc::Envelope& env,
                                     rsm::rpc::Message&& m) {
            loop.enqueue(env, std::move(m));
        });
        loop.start();
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
        loop.stop();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
    return 0;
}
