#pragma once

// Deterministic simulated KV clients for the chaos suite. The real KvClient
// is synchronous over per-attempt TCP connections, which cannot run inside
// the single-threaded simulator — so the sim client replicates its PROTOCOL
// (route to believed leader, follow NOT_LEADER hints, bounded-backoff
// rotation on timeout, and crucially: a retry always reuses the same
// (clientId, seqNo)) as a virtual-time state machine. Exactly-once
// correctness lives in the replicated session table, which only depends on
// that protocol, not on the client implementation.
//
// Reply correlation: the real client reads each reply from the TCP
// connection of the attempt that produced it. The sim equivalent is one
// envelope id per attempt (the node's ClientService replies to the envelope
// `from` it saw), so a stale OK from a slow node can never be mistaken for
// the answer to a newer attempt or a newer operation.

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "faults/linearizability.h"
#include "faults/sim_harness.h"

namespace rsm::sim {

// Shared invocation/response history, in ClientOp form for the checker.
class History {
public:
    void invoke(const ClientOp& op);
    void complete(std::uint64_t clientId, std::uint64_t seqNo,
                  std::int64_t atMs, std::string result);

    std::vector<ClientOp> ops() const;  // deterministic order
    std::set<std::pair<std::uint64_t, std::uint64_t>> ackedIdentities() const;
    std::size_t completedCount() const;
    std::size_t totalCount() const { return ops_.size(); }

private:
    std::map<std::pair<std::uint64_t, std::uint64_t>, ClientOp> ops_;
};

struct WorkloadConfig {
    // Which command set the clients speak. Kv drives the Phase 6 KV
    // workload (and the linearizability checker); OrderBook drives
    // NEW/CANCEL/AMEND against the matching engine (Phase 9) — same
    // protocol, same retry/identity rules, different command bytes.
    enum class Kind { Kv, OrderBook };
    Kind kind = Kind::Kv;
    int opsPerClient = 12;  // upper bound; faults may let fewer fit
    int keySpace = 4;  // keys "k0".."k{keySpace-1}" — small, for contention
    // Op mix weights (PUT/GET/APPEND/CAS/DELETE), normalized internally.
    int wPut = 30, wGet = 25, wAppend = 20, wCas = 15, wDelete = 10;
    // OrderBook workload: NEW prices uniform in [priceBase - priceBand,
    // priceBase + priceBand] (a band tight enough to cross constantly),
    // qty in [1, qtyMax]. CANCEL/AMEND target an order this client saw
    // rest in a NEW/AMEND ack; with none resting they fall back to NEW.
    int wNew = 70, wCancel = 15, wAmend = 15;
    std::uint64_t priceBase = 100, priceBand = 10, qtyMax = 10;
    std::int64_t requestTimeoutMs = 400;
    std::int64_t retryBackoffMs = 25;
    // Seeded think time between an op's acknowledgment and the next
    // invocation, so the workload SPANS the fault window instead of
    // completing before the first fault lands (ops racing partitions,
    // crashes, and elections are the whole point of the suite).
    std::int64_t minGapMs = 200;
    std::int64_t maxGapMs = 1200;
    // No NEW workload operations are invoked at/after this virtual time
    // (outstanding ones keep retrying).
    std::int64_t stopIssuingAtMs = 0;
    // At this time (inside the healed window) each client issues one final
    // marker PUT: it forces a current-term commit after the last fault —
    // which is also what re-propagates commitIndex after a full-cluster
    // restart — and witnesses bounded post-heal liveness end to end.
    std::int64_t finalOpAtMs = 0;
};

class SimClient {
public:
    // `envelopeBase`: this client sends attempt k from envelope id
    // envelopeBase + (k mod envelopeRange); the range must not collide with
    // node ids or other clients.
    SimClient(std::uint64_t clientId, NodeId envelopeBase,
              NodeId envelopeRange, std::uint64_t seed, WorkloadConfig cfg,
              SimHarness& harness, History& history);

    // Call once per simulated millisecond, before harness.stepMs().
    void tick();

    // All issued ops acknowledged, including the post-heal marker.
    bool done() const { return markerIssued_ && state_ == State::Idle; }

private:
    enum class State { Idle, Waiting, Backoff };

    void onDeliver(NodeId from, NodeId to, Message&& m);
    void issueNextOp(bool marker);
    void issueKvOp(bool marker);
    void issueOrderOp(bool marker);
    // Updates myOrders_ from an OK reply's decoded order-book result.
    void onOrderAck(const std::string& result);
    void sendAttempt();
    NodeId nextTarget();
    std::string key();

    std::uint64_t clientId_;
    NodeId base_;
    NodeId range_;
    std::mt19937_64 rng_;
    WorkloadConfig cfg_;
    SimHarness& harness_;
    History& history_;

    State state_ = State::Idle;
    int opsIssued_ = 0;
    bool markerIssued_ = false;
    std::uint64_t seqNo_ = 0;
    ClientOp current_;
    std::vector<std::uint8_t> currentCommand_;
    std::uint64_t attempt_ = 0;
    NodeId attemptEnvelope_ = 0;
    NodeId target_ = 0;        // node the current attempt went to
    NodeId believedLeader_ = 0;
    // OrderBook mode: ids this client saw resting (from NEW/AMEND acks),
    // the CANCEL/AMEND target pool. currentObOp_/currentObTarget_ remember
    // what the in-flight op was so its ack updates the pool correctly.
    std::vector<std::uint64_t> myOrders_;
    int currentObOp_ = 0;
    std::uint64_t currentObTarget_ = 0;
    std::int64_t deadlineMs_ = 0;  // timeout (Waiting) or resend (Backoff)
    std::int64_t nextIssueAtMs_ = 0;
};

}  // namespace rsm::sim
