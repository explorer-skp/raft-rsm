#include "bench_cluster.h"

#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <thread>

namespace rsm::bench {

using rsm::raft::RaftConfig;
using rsm::raft::RaftCore;
using rsm::raft::Role;
using rsm::rpc::LogIndex;
using rsm::rpc::Message;

// ---------------------------------------------------------------------------
// FaultGate

bool FaultGate::shouldDrop(NodeId from, NodeId to) {
    const auto mask = isolatedMask_.load(std::memory_order_relaxed);
    if ((mask & ((1u << from) | (1u << to))) != 0) return true;
    const auto pm = dropPerMille_.load(std::memory_order_relaxed);
    if (pm == 0) return false;
    // Per-thread RNG stream: allocation-free, no shared state on the hot
    // path. Streams derive from the master seed and a thread ordinal (the
    // interleaving across OS threads is inherently nondeterministic in a
    // real-time benchmark; the seed pins the workload, not the loss dice).
    static std::atomic<std::uint64_t> threadOrdinal{0};
    thread_local XorShift64 rng(
        deriveSeed(seed_.load(std::memory_order_relaxed),
                   0xFA017u + threadOrdinal.fetch_add(1)));
    return rng.next() % 1000 < pm;
}

// ---------------------------------------------------------------------------
// LeadershipMonitor

void LeadershipMonitor::record(NodeId node, Term term, std::uint8_t role) {
    const auto idx = count_.fetch_add(1, std::memory_order_relaxed);
    if (idx >= kCap) return;  // full: verdicts still see count_ overflowing
    auto& e = events_[idx];
    e.tNs = nowNs();
    e.node = node;
    e.term = term;
    e.role = role;
    // Publish: pairs with snapshot()'s per-entry acquire load, so a reader
    // never sees a half-written event (slots are reserved by fetch_add but
    // only trusted once their ready flag is set).
    ready_[idx].store(1, std::memory_order_release);
    if (role == static_cast<std::uint8_t>(Role::Leader)) {
        // Packed (term << 16 | node) max-CAS so live readers (the failover
        // driver) can ask "who leads now" without scanning.
        const std::uint64_t packed =
            (static_cast<std::uint64_t>(term) << 16) | node;
        auto cur = latestLeaderPacked_.load(std::memory_order_relaxed);
        while (packed > cur && !latestLeaderPacked_.compare_exchange_weak(
                                   cur, packed, std::memory_order_relaxed)) {
        }
    }
}

std::vector<LeadershipMonitor::Event> LeadershipMonitor::snapshot() const {
    std::vector<Event> out;
    const auto n = std::min(count_.load(std::memory_order_acquire), kCap);
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        if (ready_[i].load(std::memory_order_acquire) == 0) continue;
        out.push_back(events_[i]);
    }
    return out;
}

int LeadershipMonitor::electionsIn(std::uint64_t fromNs,
                                   std::uint64_t toNs) const {
    int n = 0;
    for (const auto& e : snapshot()) {
        if (e.role == static_cast<std::uint8_t>(Role::Leader) &&
            e.tNs >= fromNs && e.tNs < toNs) {
            ++n;
        }
    }
    return n;
}

Term LeadershipMonitor::maxTermAt(std::uint64_t tNs) const {
    Term t = 0;
    for (const auto& e : snapshot()) {
        if (e.tNs <= tNs && e.term > t) t = e.term;
    }
    return t;
}

NodeId LeadershipMonitor::latestLeader() const {
    return static_cast<NodeId>(
        latestLeaderPacked_.load(std::memory_order_relaxed) & 0xFFFF);
}

// ---------------------------------------------------------------------------
// StallableSM

std::string StallableSM::apply(const rsm::statemachine::Command& cmd) {
    std::uint32_t ms = 0;
    if (gate_.shouldStall(seenGen_, ms)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
    return inner_->apply(cmd);
}

// ---------------------------------------------------------------------------
// BenchCluster

namespace {

// Pin targets for the three Raft threads (pin mode). On the recorded host
// (i5-1235U: cpus 0-3 are the two P-cores' hyperthread pairs, 4-11 are
// E-cores) this puts each Raft thread on its own physical core: two P, one
// E. Everything else (apply/tx/rx/load generators) floats — with 12
// hardware threads and 12+ pipeline threads there is no exclusive core to
// give them, and the Raft thread is the one whose scheduling delay can turn
// into a spurious election or a latency cliff.
constexpr int kRaftCpus[] = {0, 2, 4};

inline std::uint64_t readLE64(const std::uint8_t* p) {
    std::uint64_t v = 0;
    for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}

inline std::uint64_t mixIdentity(std::uint64_t client, std::uint64_t seq) {
    std::uint64_t h =
        client * 0x9E3779B97F4A7C15ULL ^ (seq + 0xC2B2AE3D27D4EB4FULL);
    h ^= h >> 29;
    h *= 0xBF58476D1CE4E5B9ULL;
    h ^= h >> 32;
    return h;
}

}  // namespace

struct BenchCluster::Node {
    std::unique_ptr<rsm::storage::DurablePersistentState> persist;
    std::unique_ptr<rsm::storage::DurableLog> log;
    std::unique_ptr<rsm::statemachine::StateMachine> sm;
    std::unique_ptr<rsm::client::ClientService> service;
    rsm::raft::SteadyClock clock;
    std::unique_ptr<rsm::transport::Transport> transport;
    std::unique_ptr<RaftCore> core;
    std::unique_ptr<rsm::runtime::NodeRuntime> runtime;
    std::string dataDir;
    std::uint16_t port = 0;
    bool stopped = false;

    // ---- commit-latency tap (Raft thread only; no locks, no allocation) --
    struct EnqSlot {
        std::uint64_t client = 0;  // 0 = empty
        std::uint64_t seq = 0;
        std::uint64_t tNs = 0;
    };
    static constexpr std::size_t kEnqSlots = 1u << 15;  // power of two
    std::vector<EnqSlot> enq{kEnqSlots};
    LogIndex lastCommitSeen = 0;
    rsm::metrics::LatencyHistogram commitHist;

    // Snapshot handshake: harness bumps snapReq; the Raft thread copies the
    // histogram into snapOut and acks. Reader and writer never touch the
    // live histogram concurrently.
    std::atomic<std::uint32_t> snapReq{0};
    std::uint32_t snapSeen = 0;  // Raft thread only
    std::atomic<std::uint32_t> snapAck{0};
    rsm::metrics::LatencyHistogram snapOut;

    std::atomic<bool> raftPinned{false};
    bool raftPinTried = false;  // Raft thread only
    int raftPinCpu = -1;

    void tapOnRequest(std::uint64_t client, std::uint64_t seq,
                      std::uint64_t tNs) {
        if (client == 0) return;  // sessionless probe traffic: not measured
        const std::uint64_t h = mixIdentity(client, seq);
        const std::size_t mask = kEnqSlots - 1;
        for (std::size_t k = 0; k < 8; ++k) {
            auto& s = enq[(h + k) & mask];
            if (s.client == 0 || (s.client == client && s.seq == seq)) {
                s.client = client;
                s.seq = seq;
                s.tNs = tNs;
                return;
            }
        }
        // Probe window exhausted: evict the home slot (stale entries are
        // requests that never committed here — leader changes, retries).
        auto& s = enq[h & mask];
        s.client = client;
        s.seq = seq;
        s.tNs = tNs;
    }

    void tapOnIteration(std::uint64_t nowNsV, std::uint64_t winStart,
                        std::uint64_t winEnd) {
        const LogIndex ci = core->commitIndex();
        while (lastCommitSeen < ci) {
            ++lastCommitSeen;
            const auto& cmd = log->entryAt(lastCommitSeen).command;
            if (cmd.size() < 16) continue;
            const std::uint64_t client = readLE64(cmd.data());
            const std::uint64_t seq = readLE64(cmd.data() + 8);
            if (client == 0) continue;
            const std::uint64_t h = mixIdentity(client, seq);
            const std::size_t mask = kEnqSlots - 1;
            for (std::size_t k = 0; k < 8; ++k) {
                auto& s = enq[(h + k) & mask];
                if (s.client == client && s.seq == seq) {
                    if (s.tNs >= winStart && s.tNs < winEnd) {
                        commitHist.record(nowNsV - s.tNs);
                    }
                    s.client = 0;
                    break;
                }
            }
        }
        const auto req = snapReq.load(std::memory_order_acquire);
        if (req != snapSeen) {
            snapOut = commitHist;
            snapSeen = req;
            snapAck.store(req, std::memory_order_release);
        }
    }
};

BenchCluster::BenchCluster(BenchClusterConfig cfg) : cfg_(std::move(cfg)) {
    runDir_ = cfg_.dataBase + "/rsm_bench." + std::to_string(::getpid());
    std::filesystem::remove_all(runDir_);

    // Bind all transports first so every peer address is known before any
    // core starts campaigning (the project's standard startup order).
    for (NodeId id = 1; id <= static_cast<NodeId>(cfg_.nodes); ++id) {
        auto n = std::make_unique<Node>();
        n->transport = std::make_unique<rsm::transport::Transport>(id, 0);
        n->port = n->transport->listenPort();
        n->dataDir = runDir_ + "/node" + std::to_string(id);
        std::filesystem::create_directories(n->dataDir);
        if (cfg_.pinRaftThreads && id <= 3) n->raftPinCpu = kRaftCpus[id - 1];
        nodes_.push_back(std::move(n));
    }
    for (NodeId id = 1; id <= static_cast<NodeId>(cfg_.nodes); ++id) {
        buildNode(id, /*incarnation=*/0);
    }
    for (auto& n : nodes_) {
        n->runtime->start();
        n->transport->start();
    }
}

BenchCluster::~BenchCluster() {
    for (auto& n : nodes_) {
        if (!n->stopped) {
            n->transport->stop();
            n->runtime->stop();
        }
    }
    std::filesystem::remove_all(runDir_);
}

void BenchCluster::buildNode(NodeId id, std::uint64_t incarnation) {
    auto& n = *nodes_[id - 1];
    std::vector<NodeId> peers;
    for (NodeId p = 1; p <= static_cast<NodeId>(cfg_.nodes); ++p) {
        if (p == id) continue;
        peers.push_back(p);
        n.transport->addPeer(p, rsm::transport::PeerAddress{
                                    "127.0.0.1", nodes_[p - 1]->port});
    }
    n.service.reset();  // before the core it references
    n.core.reset();     // before the storage it references
    n.persist =
        std::make_unique<rsm::storage::DurablePersistentState>(n.dataDir);
    n.log = std::make_unique<rsm::storage::DurableLog>(n.dataDir, cfg_.fsync);
    n.sm = std::make_unique<StallableSM>(
        cfg_.orderBook
            ? std::unique_ptr<rsm::statemachine::StateMachine>(
                  std::make_unique<rsm::statemachine::OrderBookStateMachine>())
            : std::make_unique<rsm::statemachine::KVStateMachine>(),
        stall_);

    // Tap state for this incarnation.
    std::fill(n.enq.begin(), n.enq.end(), Node::EnqSlot{});
    n.lastCommitSeen = 0;
    n.snapSeen = n.snapReq.load();
    n.snapAck.store(n.snapSeen);
    n.raftPinTried = false;
    n.raftPinned.store(false);

    Node* nodePtr = &n;
    // Sends from the Raft/apply threads encode locally and go out via the
    // tx thread; the fault gate filters CLUSTER traffic only (client
    // replies always pass — clients bypass partitions, as in the sim).
    const auto sendHook = [this, nodePtr, id](NodeId to, const Message& m) {
        if (to >= 1 && to <= static_cast<NodeId>(cfg_.nodes) &&
            faults_.shouldDrop(id, to)) {
            return;
        }
        if (nodePtr->runtime) {
            nodePtr->runtime->sendFromPipeline(to, m);
        } else {
            nodePtr->transport->send(to, m);
        }
    };
    n.core = std::make_unique<RaftCore>(
        id, peers, *n.persist, *n.log, *n.sm, n.clock,
        deriveSeed(cfg_.seed, 1000u * id + incarnation), RaftConfig{},
        sendHook);
    n.core->setTransitionObserver(
        [this, id](Term term, Role role, const char*) {
            monitor_.record(id, term, static_cast<std::uint8_t>(role));
        });
    n.service =
        std::make_unique<rsm::client::ClientService>(*n.core, sendHook);
    if (cfg_.batch > 1) {
        n.service->setBatching(rsm::client::ClientService::Batching{
            static_cast<std::size_t>(cfg_.batch),
            std::chrono::microseconds(cfg_.lingerUs)});
    }
    // Client-request handler wrapper: stamp the enqueue time for the
    // commit-latency tap (Raft thread), then hand to the service.
    n.core->setClientRequestHandler(
        [nodePtr](NodeId from, const rsm::rpc::ClientRequest& r) {
            nodePtr->tapOnRequest(r.clientId, r.seqNo, nowNs());
            nodePtr->service->onClientRequest(from, r);
        });
    const auto onApplied = [svc = n.service.get()](
                               LogIndex index, const rsm::rpc::LogEntry& entry,
                               const std::string& result) {
        svc->onApplied(index, entry, result);
    };
    rsm::runtime::NodeRuntimeConfig rcfg;
    rcfg.waitMode = cfg_.wait;
    n.runtime = std::make_unique<rsm::runtime::NodeRuntime>(
        *n.core, *n.transport, *n.sm, onApplied, rcfg);
    // Service-hook wrapper (runs on the Raft thread every iteration):
    // first call pins the thread (pin mode); every call drives the
    // commit-latency tap and the snapshot handshake, then the linger-batch
    // driver, whose deadline is returned unchanged.
    n.runtime->setServiceHook([this, nodePtr](rsm::raft::TimePoint now) {
        if (nodePtr->raftPinCpu >= 0 && !nodePtr->raftPinTried) {
            nodePtr->raftPinTried = true;
            nodePtr->raftPinned.store(pinSelfToCpu(nodePtr->raftPinCpu));
        }
        nodePtr->tapOnIteration(
            toNs(now), winStartNs_.load(std::memory_order_relaxed),
            winEndNs_.load(std::memory_order_relaxed));
        return nodePtr->service->flushIfDue(now);
    });
    n.transport->setRawHandler(
        [rt = n.runtime.get()](std::span<const std::uint8_t> body) {
            return rt->enqueueFrame(body);
        });
}

rsm::transport::PeerMap BenchCluster::peerMap() const {
    rsm::transport::PeerMap servers;
    for (NodeId id = 1; id <= static_cast<NodeId>(nodes_.size()); ++id) {
        servers[id] =
            rsm::transport::PeerAddress{"127.0.0.1", nodes_[id - 1]->port};
    }
    return servers;
}

std::optional<NodeId> BenchCluster::awaitReady(std::chrono::seconds limit) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    // Sessionless probe (clientId 0 opts out of dedup; envelope id 250 is
    // disjoint from cluster and generator ids).
    rsm::client::KvClient probe(peerMap(), /*clientId=*/0,
                                /*clientNodeId=*/250,
                                /*perAttemptTimeoutMs=*/250,
                                /*maxAttempts=*/2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (monitor_.latestLeader() != 0) {
            // The probe must speak the configured SM's command set. The
            // order-book probe is a 1-lot bid at the minimum price tick:
            // it can never cross the workload's price band, and 'O' means
            // the same thing as the KV put's — committed and applied.
            const auto r = cfg_.orderBook
                               ? probe.obNew(rsm::statemachine::ObSide::Bid,
                                             /*price=*/1, /*qty=*/1)
                               : probe.put("__ready", "1");
            const char ok = cfg_.orderBook ? rsm::statemachine::kObOk
                                           : rsm::statemachine::kKvOk;
            if (r && r->status == ok) {
                return monitor_.latestLeader();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return std::nullopt;
}

void BenchCluster::setCommitWindow(std::uint64_t startNs,
                                   std::uint64_t endNs) {
    winStartNs_.store(startNs, std::memory_order_relaxed);
    winEndNs_.store(endNs, std::memory_order_relaxed);
}

rsm::metrics::LatencyHistogram BenchCluster::commitHistogram() {
    rsm::metrics::LatencyHistogram merged;
    for (auto& n : nodes_) {
        if (n->stopped) continue;
        const auto req = n->snapReq.fetch_add(1) + 1;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (n->snapAck.load(std::memory_order_acquire) < req &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (n->snapAck.load(std::memory_order_acquire) >= req) {
            merged.merge(n->snapOut);
        }
    }
    return merged;
}

void BenchCluster::killNode(NodeId id) {
    auto& n = *nodes_[id - 1];
    if (n.stopped) return;
    n.stopped = true;
    n.transport->stop();
    n.runtime->stop();
}

void BenchCluster::restartNode(NodeId id) {
    auto& n = *nodes_[id - 1];
    if (!n.stopped) return;
    n.runtime.reset();   // joins threads; core no longer driven
    n.service.reset();   // holds a core reference; drop it first
    n.core.reset();
    n.transport.reset();  // closes sockets; the port becomes free
    n.transport = std::make_unique<rsm::transport::Transport>(id, n.port);
    buildNode(id, /*incarnation=*/++restarts_);
    n.stopped = false;
    n.runtime->start();
    n.transport->start();
}

bool BenchCluster::nodeAlive(NodeId id) const {
    return !nodes_[id - 1]->stopped;
}

std::uint64_t BenchCluster::dataBytes() const {
    std::uint64_t total = 0;
    std::error_code ec;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(
             runDir_, std::filesystem::directory_options::skip_permission_denied,
             ec)) {
        if (entry.is_regular_file(ec)) {
            total += static_cast<std::uint64_t>(entry.file_size(ec));
        }
    }
    return total;
}

bool BenchCluster::raftPinsApplied() const {
    for (const auto& n : nodes_) {
        if (n->raftPinCpu >= 0 && !n->raftPinned.load()) return false;
    }
    return true;
}

}  // namespace rsm::bench
