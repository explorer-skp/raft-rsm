#include "faults/sim_harness.h"

#include <chrono>
#include <string>

#include "statemachine/kv_store.h"

namespace rsm::sim {

using rsm::rpc::Envelope;
using rsm::rpc::kProtocolVersion;
using rsm::rpc::LogIndex;
using rsm::rpc::Term;

StoragePair inMemoryStorage(NodeId, int) {
    return {std::make_unique<rsm::raft::InMemoryPersistentState>(),
            std::make_unique<rsm::storage::InMemoryLog>()};
}

std::unique_ptr<rsm::statemachine::StateMachine> kvStateMachine(NodeId) {
    return std::make_unique<rsm::statemachine::KVStateMachine>();
}

namespace {

const char* messageTypeName(const Message& m) {
    switch (rsm::rpc::typeOf(m)) {
        case rsm::rpc::MessageType::RequestVote: return "RV";
        case rsm::rpc::MessageType::RequestVoteReply: return "RVR";
        case rsm::rpc::MessageType::AppendEntries: return "AE";
        case rsm::rpc::MessageType::AppendEntriesReply: return "AER";
        case rsm::rpc::MessageType::ClientRequest: return "CREQ";
        case rsm::rpc::MessageType::ClientReply: return "CREP";
    }
    return "?";
}

std::uint64_t messageFp(const Message& m) {
    // Cheap structural fingerprint for the trace: type + encoded payload.
    std::vector<std::uint8_t> buf(rsm::rpc::encodedSize(m));
    const std::size_t n = rsm::rpc::encodeMessage(0, 0, m, buf);
    return fnv1a(buf.data(), n);
}

}  // namespace

SimHarness::SimHarness(HarnessOptions opts, StorageFactory storage,
                       SmFactory sm)
    : opts_(std::move(opts)),
      net_(opts_.netSeed,
           [&] {
               std::vector<NodeId> ids;
               for (NodeId i = 1; i <= opts_.nodes.size(); ++i)
                   ids.push_back(i);
               return ids;
           }()),
      storage_(std::move(storage)),
      smFactory_(std::move(sm)) {
    if (!opts_.restartSeed) {
        opts_.restartSeed = [base = opts_.netSeed](NodeId id, int inc) {
            return base ^ (0x9E3779B97F4A7C15ULL * id) ^
                   (0xBF58476D1CE4E5B9ULL * static_cast<std::uint64_t>(inc));
        };
    }
    const auto n = static_cast<NodeId>(opts_.nodes.size());
    for (NodeId id = 1; id <= n; ++id) {
        nodes_.push_back(std::make_unique<NodeRt>());
        nodes_.back()->id = id;
        buildNode(id, opts_.nodes[id - 1].rngSeed);
    }
    for (auto& node : nodes_) node->core->start();
}

void SimHarness::buildNode(NodeId id, std::uint64_t seed) {
    NodeRt& node = *nodes_[id - 1];
    const auto n = static_cast<NodeId>(opts_.nodes.size());
    std::vector<NodeId> peers;
    for (NodeId p = 1; p <= n; ++p) {
        if (p != id) peers.push_back(p);
    }
    // Destroy the old core before its storage so nothing dangles; a durable
    // factory then re-opens and replays the node's on-disk state.
    node.service.reset();
    node.core.reset();
    node.persist.reset();
    node.log.reset();
    auto storage = storage_(id, node.incarnation);
    node.persist = std::move(storage.persist);
    node.log = std::move(storage.log);
    node.sm = smFactory_(id);
    node.core = std::make_unique<RaftCore>(
        id, peers, *node.persist, *node.log, *node.sm, clock, seed,
        opts_.nodes[id - 1].raft, [this, id](NodeId to, const Message& m) {
            net_.send(id, to, m, clock.now());
        });
    node.service = std::make_unique<rsm::client::ClientService>(
        *node.core, [this, id](NodeId to, const Message& m) {
            net_.send(id, to, m, clock.now());
        });
    node.core->setTransitionObserver(
        [this, id, &node](Term term, Role role, const char* event) {
            traceEvent("t=" + std::to_string(nowMs()) + " n" +
                       std::to_string(id) + " " + event +
                       " term=" + std::to_string(term));
            if (role == Role::Leader) {
                const LogImage image = logImage(*node.log);
                electionSafety_.observeLeader(term, id);
                commit_.observeElectionWin(id, term, image);
            }
        });
    rsm::client::ClientService* svc = node.service.get();
    node.core->setClientRequestHandler(
        [svc](NodeId from, const rsm::rpc::ClientRequest& req) {
            svc->onClientRequest(from, req);
        });
    node.core->setApplyObserver([this, id, svc](
                                    LogIndex index,
                                    const rsm::rpc::LogEntry& entry,
                                    const std::string& result) {
        applied_.observeApply(id, index, entryImage(entry),
                              fnv1a(result.data(), result.size()));
        traceEvent("t=" + std::to_string(nowMs()) + " n" +
                   std::to_string(id) + " apply i=" + std::to_string(index) +
                   " fp=" + std::to_string(entryImage(entry).fp));
        svc->onApplied(index, entry, result);
    });
}

void SimHarness::crash(NodeId id) {
    nodes_[id - 1]->alive = false;
    traceEvent("t=" + std::to_string(nowMs()) + " crash n" +
               std::to_string(id));
}

void SimHarness::restart(NodeId id) {
    NodeRt& node = *nodes_[id - 1];
    if (node.alive) return;
    ++node.incarnation;
    buildNode(id, opts_.restartSeed(id, node.incarnation));
    node.alive = true;
    node.core->start();
    traceEvent("t=" + std::to_string(nowMs()) + " restart n" +
               std::to_string(id));
}

std::optional<NodeId> SimHarness::killLeader() {
    std::optional<NodeId> victim;
    Term best = 0;
    for (const auto& node : nodes_) {
        if (node->alive && node->core->role() == Role::Leader &&
            node->core->term() >= best) {
            best = node->core->term();
            victim = node->id;
        }
    }
    if (victim) crash(*victim);
    return victim;
}

std::vector<NodeId> SimHarness::deadNodes() const {
    std::vector<NodeId> out;
    for (const auto& node : nodes_) {
        if (!node->alive) out.push_back(node->id);
    }
    return out;
}

std::int64_t SimHarness::nowMs() const {
    // ManualClock starts at the epoch, so time-since-epoch IS sim time.
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               const_cast<ManualClock&>(clock).now().time_since_epoch())
        .count();
}

void SimHarness::stepMs() {
    clock.advance(rsm::raft::Duration(1));
    for (auto& node : nodes_) {
        if (node->alive) node->core->tick();
    }
    net_.deliverDue(clock.now(), [this](NodeId from, NodeId to, Message&& m) {
        deliver(from, to, std::move(m));
    });
    observeStep();
}

void SimHarness::deliver(NodeId from, NodeId to, Message&& m) {
    traceEvent("t=" + std::to_string(nowMs()) + " dlv " +
               messageTypeName(m) + " " + std::to_string(from) + "->" +
               std::to_string(to) + " fp=" + std::to_string(messageFp(m)));
    if (to >= 1 && to <= nodes_.size()) {
        NodeRt& node = *nodes_[to - 1];
        if (!node.alive) return;  // a dead node hears nothing
        node.core->handle(Envelope{kProtocolVersion, rsm::rpc::typeOf(m),
                                   from, to, 0},
                          m);
        return;
    }
    for (const Endpoint& ep : endpoints_) {
        if (to >= ep.low && to <= ep.high) {
            ep.fn(from, to, std::move(m));
            return;
        }
    }
}

void SimHarness::observeStep() {
    for (const auto& node : nodes_) {
        if (!node->alive) continue;
        const LogImage image = logImage(*node->log);
        appendOnly_.observe(node->id, node->core->role() == Role::Leader,
                            node->core->term(), image);
        commit_.observeCommit(node->id, node->core->commitIndex(), image);
    }
    if (nowMs() - lastLogMatchingMs_ >= opts_.logMatchingEveryMs) {
        lastLogMatchingMs_ = nowMs();
        std::map<NodeId, LogImage> logs;
        for (const auto& node : nodes_) {
            if (node->alive) logs[node->id] = logImage(*node->log);
        }
        auto v = checkLogMatching(logs);
        sweepViolations_.insert(sweepViolations_.end(), v.begin(), v.end());
    }
}

void SimHarness::registerEndpoint(
    NodeId idLow, NodeId idHigh,
    std::function<void(NodeId from, NodeId to, Message&&)> fn) {
    endpoints_.push_back(Endpoint{idLow, idHigh, std::move(fn)});
}

void SimHarness::clientSend(NodeId fromClient, NodeId toNode, Message m) {
    net_.send(fromClient, toNode, std::move(m), clock.now());
}

std::vector<NodeId> SimHarness::liveLeaders() const {
    std::vector<NodeId> out;
    for (const auto& node : nodes_) {
        if (node->alive && node->core->role() == Role::Leader) {
            out.push_back(node->id);
        }
    }
    return out;
}

bool SimHarness::converged() const {
    const auto leaders = liveLeaders();
    if (leaders.size() != 1) return false;
    const RaftCore& leader = *nodes_[leaders[0] - 1]->core;
    for (const auto& node : nodes_) {
        if (!node->alive || node->id == leaders[0]) continue;
        if (node->core->role() != Role::Follower) return false;
        if (node->core->term() != leader.term()) return false;
        if (node->core->leaderId() != leader.selfId()) return false;
    }
    return true;
}

bool SimHarness::quiescent() const {
    if (!converged()) return false;
    const NodeId leaderId = liveLeaders()[0];
    const NodeRt& leader = *nodes_[leaderId - 1];
    for (const auto& node : nodes_) {
        if (!node->alive) return false;  // quiescence wants everyone back
        if (node->core->commitIndex() != leader.core->commitIndex()) {
            return false;
        }
        if (node->core->lastApplied() != node->core->commitIndex()) {
            return false;
        }
        if (node->log->lastIndex() != leader.log->lastIndex()) return false;
        if (logImage(*node->log) != logImage(*leader.log)) return false;
    }
    return true;
}

std::vector<std::string> SimHarness::violations() const {
    std::vector<std::string> out;
    const auto add = [&out](const std::vector<std::string>& v) {
        out.insert(out.end(), v.begin(), v.end());
    };
    add(electionSafety_.violations());
    add(appendOnly_.violations());
    add(applied_.violations());
    add(commit_.violations());
    add(sweepViolations_);
    return out;
}

void SimHarness::finalCheck() {
    std::map<NodeId, LogImage> logs;
    for (const auto& node : nodes_) {
        if (node->alive) logs[node->id] = logImage(*node->log);
    }
    auto v = checkLogMatching(logs);
    sweepViolations_.insert(sweepViolations_.end(), v.begin(), v.end());
}

}  // namespace rsm::sim
