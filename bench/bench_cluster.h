#pragma once

// Phase 8 instrumented in-process 3-node cluster (the system under test).
//
// Same wiring as `node_main`/`phase7_bench` — per node: TCP transport on an
// ephemeral loopback port, durable storage, KVStateMachine, ClientService,
// RaftCore, and the Phase 7 threaded NodeRuntime — plus the benchmark
// instrumentation, all of it attached through the EXISTING seams (send hook,
// client-request handler, service hook, apply observer, transition
// observer). Zero production-code changes; the sim/chaos suites never see
// any of this.
//
// Why in-process rather than three separate processes (decision, DESIGN.md):
// the spec's production topology is "separate processes on ONE host over
// loopback TCP", so the host-level contention picture — total threads, total
// cores, loopback sockets — is identical either way; processes would share
// nothing else that matters (the pipeline is allocation-free, so there is no
// allocator contention to hide). In exchange, in-process gives the
// instrumentation direct, hook-based access: the commit-latency tap, the
// leadership monitor, deterministic fault injection, stall injection, and
// leader kill/restart, none of which would survive a process boundary
// without adding control RPCs to the production binary.
//
// Instrumentation (and why it cannot perturb the measurement):
//  - Leadership monitor: every (term, role) transition from every node's
//    transition observer, timestamped, into a fixed-size lock-free array.
//    A *clean-load* run that records an election after warmup is INVALID —
//    the harness refuses its numbers (the Phase 7 lesson).
//  - Commit-latency tap: "request enqueued at the leader -> entry
//    committed", measured entirely ON the Raft thread (enqueue times keyed
//    by (clientId, seqNo) in a preallocated open-addressed table at the
//    client-request handler; commit times observed by scanning
//    core.commitIndex() each loop iteration from the service hook, reading
//    identities from the log the Raft thread already owns). Single-threaded
//    by construction: no locks, no atomics, no allocation on the hot path.
//    Histograms are exported through a request/publish handshake that runs
//    on the Raft thread, so readers never race the writer.
//  - Fault gate: drop-probability and node-isolation checks in the bench's
//    send hook, applied to CLUSTER traffic only (client replies always
//    pass, matching the Phase 6 sim's "clients bypass partitions" model).
//    Per-thread xorshift, no allocation.
//  - Stall gate: arms a one-shot sleep inside each node's state machine —
//    the instrument for the coordinated-omission self-test.
//  - Optional pinning of the three Raft threads (the latency- and
//    election-critical ones), self-applied on first service-hook call.

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bench_util.h"
#include "client/client_service.h"
#include "client/kv_client.h"
#include "metrics/histogram.h"
#include "raft/clock.h"
#include "raft/raft_core.h"
#include "runtime/node_runtime.h"
#include "statemachine/kv_store.h"
#include "statemachine/order_book.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"
#include "transport/transport.h"

namespace rsm::bench {

using rsm::rpc::NodeId;
using rsm::rpc::Term;

// ---------------------------------------------------------------------------
// Fault gate: consulted by every node's send hook for cluster-bound
// messages. Sender-side dropping models symmetric loss; the isolation mask
// drops everything to or from an isolated node ("partition now" semantics,
// like the Phase 6 sim).
class FaultGate {
public:
    void setLossPermille(std::uint32_t pm) { dropPerMille_.store(pm); }
    void isolate(NodeId node) {
        isolatedMask_.fetch_or(1u << node);
    }
    void heal(NodeId node) {
        isolatedMask_.fetch_and(~(1u << node));
    }
    void healAll() { isolatedMask_.store(0); }
    void setSeed(std::uint64_t s) { seed_.store(s); }

    // Called from pipeline threads (Raft/apply); allocation-free.
    bool shouldDrop(NodeId from, NodeId to);

private:
    std::atomic<std::uint32_t> dropPerMille_{0};
    std::atomic<std::uint32_t> isolatedMask_{0};
    std::atomic<std::uint64_t> seed_{1};
};

// ---------------------------------------------------------------------------
// Leadership monitor: whole-run transition record + "is the term stable"
// verdicts. Writers are the nodes' Raft threads (rare events, lock-free
// fixed array); readers snapshot after the fact.
class LeadershipMonitor {
public:
    struct Event {
        std::uint64_t tNs = 0;
        NodeId node = 0;
        Term term = 0;
        std::uint8_t role = 0;  // rsm::raft::Role
    };

    void record(NodeId node, Term term, std::uint8_t role);
    std::vector<Event> snapshot() const;

    // Leader-win events with tNs in [fromNs, toNs) — a clean-load steady
    // state must report ZERO of these.
    int electionsIn(std::uint64_t fromNs, std::uint64_t toNs) const;
    // Highest term seen at or before tNs vs highest term overall — equal
    // means no term churn after tNs.
    Term maxTermAt(std::uint64_t tNs) const;
    Term maxTerm() const { return maxTermAt(UINT64_MAX); }
    // Node that most recently won an election (0 if none yet).
    NodeId latestLeader() const;

private:
    static constexpr std::size_t kCap = 8192;
    std::array<Event, kCap> events_{};
    std::array<std::atomic<std::uint8_t>, kCap> ready_{};
    std::atomic<std::size_t> count_{0};
    std::atomic<std::uint64_t> latestLeaderPacked_{0};  // term << 16 | node
};

// ---------------------------------------------------------------------------
// Stall gate + wrapper SM: each arm() makes the NEXT apply on EVERY node
// sleep once for `ms` (per-node generation tracking, so a fast leader can't
// consume the others' stalls). This is the injected system stall the
// coordinated-omission self-test must SEE in the reported tail.
class StallGate {
public:
    void arm(std::uint32_t ms) {
        ms_.store(ms);
        gen_.fetch_add(1, std::memory_order_release);
    }
    // Wrapper-side (one apply thread per wrapper): true once per arm().
    bool shouldStall(std::uint64_t& seenGen, std::uint32_t& msOut) const {
        const auto g = gen_.load(std::memory_order_acquire);
        if (g == seenGen) return false;
        seenGen = g;
        msOut = ms_.load();
        return true;
    }

private:
    std::atomic<std::uint64_t> gen_{0};
    std::atomic<std::uint32_t> ms_{0};
};

class StallableSM final : public rsm::statemachine::StateMachine {
public:
    StallableSM(std::unique_ptr<rsm::statemachine::StateMachine> inner,
                const StallGate& gate)
        : inner_(std::move(inner)), gate_(gate) {}
    std::string apply(const rsm::statemachine::Command& cmd) override;

private:
    std::unique_ptr<rsm::statemachine::StateMachine> inner_;
    const StallGate& gate_;
    std::uint64_t seenGen_ = 0;
};

// ---------------------------------------------------------------------------

struct BenchClusterConfig {
    int nodes = 3;
    bool orderBook = false;                   // SM: KV (default) or matching engine
    std::string dataBase = "/dev/shm";       // node data dirs live under this
    rsm::storage::FsyncPolicy fsync =
        rsm::storage::FsyncPolicy::EveryDurabilityPoint;
    rsm::runtime::WaitMode wait = rsm::runtime::WaitMode::Block;
    int batch = 1;                            // group commit (1 = off)
    int lingerUs = 200;
    bool pinRaftThreads = false;              // pin scheme: see kRaftCpus
    std::uint64_t seed = 1;                   // node RNG seeds derive from this
};

class BenchCluster {
public:
    explicit BenchCluster(BenchClusterConfig cfg);
    ~BenchCluster();

    BenchCluster(const BenchCluster&) = delete;
    BenchCluster& operator=(const BenchCluster&) = delete;

    rsm::transport::PeerMap peerMap() const;

    // Waits for SOME node to have won an election and the cluster to answer
    // a probe write. Returns the leader id, or nullopt on timeout.
    std::optional<NodeId> awaitReady(std::chrono::seconds limit);

    LeadershipMonitor& monitor() { return monitor_; }
    FaultGate& faults() { return faults_; }
    StallGate& stall() { return stall_; }

    // Commit-latency tap window: samples whose request arrived inside
    // [startNs, endNs) are recorded. Set before the measurement window.
    void setCommitWindow(std::uint64_t startNs, std::uint64_t endNs);

    // Merged commit-latency histogram, collected via the per-node Raft
    // thread handshake (nodes whose Raft thread is stopped are skipped).
    rsm::metrics::LatencyHistogram commitHistogram();

    // Failover instruments (durable storage: a restarted node replays disk).
    void killNode(NodeId id);
    void restartNode(NodeId id);
    bool nodeAlive(NodeId id) const;
    int nodeCount() const { return static_cast<int>(nodes_.size()); }

    // Total bytes in the nodes' data dirs (log-growth evidence for the
    // snapshotting decision).
    std::uint64_t dataBytes() const;

    // True if every raft thread was successfully pinned (pin mode only).
    bool raftPinsApplied() const;

private:
    struct Node;
    void buildNode(NodeId id, std::uint64_t incarnation);

    BenchClusterConfig cfg_;
    std::string runDir_;
    LeadershipMonitor monitor_;
    FaultGate faults_;
    StallGate stall_;
    std::atomic<std::uint64_t> winStartNs_{UINT64_MAX};
    std::atomic<std::uint64_t> winEndNs_{UINT64_MAX};
    std::vector<std::unique_ptr<Node>> nodes_;
    std::uint64_t restarts_ = 0;
};

}  // namespace rsm::bench
