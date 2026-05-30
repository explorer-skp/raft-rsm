#pragma once

#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "raft/raft_core.h"
#include "rpc/messages.h"

namespace rsm::client {

using rsm::rpc::ClientReply;
using rsm::rpc::ClientRequest;
using rsm::rpc::LogIndex;
using rsm::rpc::NodeId;

// Node-side client session handling (Phase 5): receives ClientRequests on
// the Raft event-loop thread (via RaftCore::setClientRequestHandler), routes
// non-leaders away with a leader hint, and on the leader proposes
// request.command verbatim through the normal log path, replying only after
// the entry is COMMITTED AND APPLIED with the state machine's actual
// result. The command's leading 16 bytes must embed the same
// (clientId, seqNo) as the wire request (validated here, rejected with
// status Error otherwise): the state machine dedups on the embedded copy,
// this layer correlates replies on the wire copy. Exactly-once itself lives
// in the state machine's apply-time dedup, not here.
//
// Threading (Phase 7 update): onClientRequest still always runs on the Raft
// (event-loop) thread — it reads core state and calls core.propose(). In
// the threaded runtime, onApplied runs on the APPLY thread instead, so the
// pending table is mutex-guarded (uncontended in the sim and the legacy
// loop, where both run on one thread), and onApplied must not touch the
// core: the leader hint it needs is cached in an atomic refreshed on the
// Raft thread, and selfId is immutable after construction.
class ClientService {
public:
    using SendFn = std::function<void(NodeId to, const rsm::rpc::Message&)>;

    // Phase 7 group commit (decision point 3): with maxBatch > 1 the leader
    // buffers validated commands and proposes them as ONE multi-entry
    // append (one fsync, one AppendEntries per peer). A batch flushes when
    // it reaches maxBatch commands or when the linger deadline passes —
    // whichever comes first. Defaults keep batching OFF, so every existing
    // path (sim, chaos, legacy runtime) is bit-for-bit unchanged.
    //
    // Batching changes grouping/timing only, never the logical outcome:
    // entries hit the log in arrival order either way (the equivalence test
    // asserts identical applied sequences batched vs unbatched).
    struct Batching {
        std::size_t maxBatch = 1;              // 1 = batching off
        std::chrono::microseconds linger{0};   // max time a command may wait
    };

    ClientService(rsm::raft::RaftCore& core, SendFn send)
        : core_(core), send_(std::move(send)) {}

    // Call before traffic starts (not thread-safe against onClientRequest).
    void setBatching(Batching b) {
        batching_ = b;
        buffer_.reserve(b.maxBatch);        // §4.7: preallocate the batch
        flushScratch_.reserve(b.maxBatch);  // containers up front
    }

    // Wire to RaftCore::setClientRequestHandler.
    void onClientRequest(NodeId from, const ClientRequest& req);

    // Raft-thread driver for the linger timer: flushes the buffered batch
    // if its deadline has passed and returns the next deadline this service
    // needs to be called at (TimePoint::max() when nothing is buffered).
    // The deadline is armed on the first call after a command is buffered —
    // the runtime calls this every loop iteration, so that is "arrival
    // time" to within one iteration, and no clock dependency is needed
    // here. The sim passes its ManualClock time, keeping it deterministic.
    rsm::raft::TimePoint flushIfDue(rsm::raft::TimePoint now);

    // Wire to RaftCore::setApplyObserver. Replies to the pending request at
    // this index — but only if the applied entry still carries the same
    // (clientId, seqNo): after a leadership change the entry we proposed at
    // this index may have been truncated and replaced by the new leader's,
    // in which case the waiting client gets NOT_LEADER and retries (its
    // command may never have been, or may separately get, committed — the
    // retry is safe either way because dedup is in applied state).
    void onApplied(LogIndex index, const rsm::rpc::LogEntry& entry,
                   const std::string& result);

    std::size_t pendingCount() const {
        std::lock_guard lock(mu_);
        return pending_.size();
    }

private:
    struct Pending {
        NodeId replyTo = 0;
        std::uint64_t clientId = 0;
        std::uint64_t seqNo = 0;
    };

    // Fixed-capacity pending table keyed by log index (Phase 7: replaces
    // std::map so the steady-state request path performs zero allocations).
    // Open addressing is unnecessary: live pending indices span a window
    // far smaller than the capacity (bounded by the runtime's ring sizes),
    // so index & mask never collides between two LIVE entries; a colliding
    // STALE entry (left by a step-down, never applied here) is overwritten,
    // which is exactly the old map's behavior of never answering it.
    class PendingTable {
    public:
        explicit PendingTable(std::size_t cap = 8192)
            : slots_(std::bit_ceil(cap)) {}

        void put(LogIndex index, const Pending& p) {
            Slot& s = slots_[index & (slots_.size() - 1)];
            s.index = index;
            s.p = p;
            s.used = true;
        }

        bool take(LogIndex index, Pending& out) {
            Slot& s = slots_[index & (slots_.size() - 1)];
            if (!s.used || s.index != index) return false;
            out = s.p;
            s.used = false;
            return true;
        }

        std::size_t size() const {  // test introspection only; O(capacity)
            std::size_t n = 0;
            for (const auto& s : slots_) n += s.used ? 1 : 0;
            return n;
        }

    private:
        struct Slot {
            LogIndex index = 0;
            Pending p;
            bool used = false;
        };
        std::vector<Slot> slots_;
    };

public:

private:
    struct Buffered {
        NodeId from = 0;
        std::uint64_t clientId = 0;
        std::uint64_t seqNo = 0;
        std::vector<std::uint8_t> command;
    };

    void reply(NodeId to, const ClientReply& r) {
        send_(to, rsm::rpc::Message{r});
    }

    // Proposes the buffered batch (Raft thread only).
    void flushBatch();

    rsm::raft::RaftCore& core_;
    SendFn send_;
    // Proposed-but-not-yet-applied requests, keyed by assigned log index.
    // Not flushed on step-down: the index either applies with our entry
    // (reply OK), applies with someone else's (reply NOT_LEADER via the
    // identity check), or never applies here before the client times out
    // and retries elsewhere. DESIGN.md, Phase 5.
    // Guarded by mu_: inserted on the Raft thread, consumed by onApplied
    // (apply thread in the threaded runtime).
    mutable std::mutex mu_;
    PendingTable pending_;
    // Best-effort leader hint for replies issued off the Raft thread, where
    // core_.leaderId() must not be read. Refreshed on every onClientRequest.
    std::atomic<NodeId> leaderHint_{0};

    // Group-commit state, Raft thread only (no lock needed).
    Batching batching_;
    std::vector<Buffered> buffer_;
    std::vector<std::vector<std::uint8_t>> flushScratch_;  // reused per flush
    rsm::raft::TimePoint batchDeadline_ = rsm::raft::TimePoint::max();
    bool deadlineArmed_ = false;

    // Reused OK-reply message (apply-thread/onApplied only): the result
    // vector keeps its capacity across replies.
    rsm::rpc::Message okReply_{ClientReply{}};
};

}  // namespace rsm::client
