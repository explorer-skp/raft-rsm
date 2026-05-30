#include "client/client_service.h"

#include "metrics/alloc_gate.h"
#include "rpc/wire.h"

namespace rsm::client {

using rsm::rpc::ClientStatus;

namespace {

constexpr std::size_t kSessionPrefixSize = 16;  // u64 clientId + u64 seqNo

// Reads the session prefix off a replicated command.
bool decodePrefix(const std::vector<std::uint8_t>& command,
                  std::uint64_t& clientId, std::uint64_t& seqNo) {
    if (command.size() < kSessionPrefixSize) return false;
    rsm::rpc::Reader r(command);
    clientId = r.u64();
    seqNo = r.u64();
    return r.ok();
}

}  // namespace

void ClientService::onClientRequest(NodeId from, const ClientRequest& req) {
    // Raft thread: core reads are safe here. Cache the hint for replies
    // that onApplied may have to issue from the apply thread, where the
    // core must not be touched.
    leaderHint_.store(core_.leaderId().value_or(0), std::memory_order_relaxed);
    if (core_.role() != rsm::raft::Role::Leader) {
        const auto hint = core_.leaderId();
        reply(from, ClientReply{ClientStatus::NotLeader, hint.value_or(0),
                                {}});
        return;
    }
    // The command bytes already embed the session identity as their leading
    // 16 bytes (the state machine dedups on the embedded copy). The wire
    // fields exist so this layer can correlate without knowing the command
    // format — but they must agree, or dedup would key on different values
    // than the reply correlation.
    std::uint64_t embeddedClient = 0;
    std::uint64_t embeddedSeq = 0;
    if (!decodePrefix(req.command, embeddedClient, embeddedSeq) ||
        embeddedClient != req.clientId || embeddedSeq != req.seqNo) {
        reply(from, ClientReply{ClientStatus::Error, 0, {}});
        return;
    }

    if (batching_.maxBatch > 1) {
        // Group commit: buffer now, propose as one append on size/linger.
        // The command copy is the request's single log-bound allocation.
        {
            const rsm::metrics::AllocRetention allocTag;
            buffer_.push_back(
                Buffered{from, req.clientId, req.seqNo, req.command});
        }
        if (buffer_.size() >= batching_.maxBatch) flushBatch();
        return;
    }

    std::optional<LogIndex> index;
    {
        // The by-value parameter copy is the request's single log-bound
        // allocation; it moves through propose() into the log.
        const rsm::metrics::AllocRetention allocTag;
        index = core_.propose(req.command);
    }
    if (!index) {  // unreachable given the role check, but stay defensive
        reply(from, ClientReply{ClientStatus::NotLeader, 0, {}});
        return;
    }
    // A duplicate retry while the original is still in flight just appends
    // another entry with the same (clientId, seqNo); apply-time dedup turns
    // the second apply into a cached-result no-op, and both pending entries
    // get answered. No leader-side short-circuit (DESIGN.md decision 4).
    //
    // Ordering note (threaded runtime): the commit of *index requires an
    // AppendEntries ack from a peer, which can only be handled AFTER this
    // handler returns (one event at a time on the Raft thread), so the
    // insert below always happens-before the apply-thread lookup. A 1-node
    // cluster would commit inside propose() and break this; the runtime is
    // 3-node by spec.
    std::lock_guard lock(mu_);
    pending_.put(*index, Pending{from, req.clientId, req.seqNo});
}

rsm::raft::TimePoint ClientService::flushIfDue(rsm::raft::TimePoint now) {
    if (buffer_.empty()) return rsm::raft::TimePoint::max();
    if (!deadlineArmed_) {
        // First look at this batch: arm the linger deadline. The runtime
        // polls every loop iteration, so this is arrival time + O(one
        // iteration); the imprecision only shifts WHEN a batch flushes,
        // never what it contains relative to arrival order.
        batchDeadline_ = now + batching_.linger;
        deadlineArmed_ = true;
    }
    if (now >= batchDeadline_) flushBatch();
    return buffer_.empty() ? rsm::raft::TimePoint::max() : batchDeadline_;
}

void ClientService::flushBatch() {
    if (buffer_.empty()) return;
    // flushScratch_ keeps its capacity across flushes; the command buffers
    // move out of it into the log.
    flushScratch_.clear();
    for (auto& b : buffer_) flushScratch_.push_back(std::move(b.command));
    const auto first = core_.proposeBatch(flushScratch_);
    if (!first) {
        // Stepped down between buffering and flushing: every buffered
        // client gets the same answer an immediate propose failure gives.
        const auto hint = core_.leaderId().value_or(0);
        for (const auto& b : buffer_) {
            reply(b.from, ClientReply{ClientStatus::NotLeader, hint, {}});
        }
    } else {
        std::lock_guard lock(mu_);
        for (std::size_t i = 0; i < buffer_.size(); ++i) {
            pending_.put(*first + i,
                         Pending{buffer_[i].from, buffer_[i].clientId,
                                 buffer_[i].seqNo});
        }
    }
    buffer_.clear();
    deadlineArmed_ = false;
    batchDeadline_ = rsm::raft::TimePoint::max();
}

void ClientService::onApplied(LogIndex index, const rsm::rpc::LogEntry& entry,
                              const std::string& result) {
    // May run on the apply thread (threaded runtime): only the pending
    // table (locked), the cached leader hint, and the immutable selfId are
    // touched — never live core state.
    Pending p;
    {
        std::lock_guard lock(mu_);
        if (!pending_.take(index, p)) return;
    }

    std::uint64_t clientId = 0;
    std::uint64_t seqNo = 0;
    const bool same = decodePrefix(entry.command, clientId, seqNo) &&
                      clientId == p.clientId && seqNo == p.seqNo;
    if (!same) {
        // Our proposal was truncated away and a different leader's entry
        // committed at this index instead. The hint is the best-effort
        // value cached on the Raft thread; staleness only costs the client
        // one extra redirect.
        reply(p.replyTo,
              ClientReply{ClientStatus::NotLeader,
                          leaderHint_.load(std::memory_order_relaxed),
                          {}});
        return;
    }
    // Reused reply message: assign() recycles the result vector's capacity,
    // so the steady-state OK path allocates nothing.
    auto& ok = std::get<ClientReply>(okReply_);
    ok.status = ClientStatus::Ok;
    ok.leaderHint = core_.selfId();
    ok.result.assign(result.begin(), result.end());
    send_(p.replyTo, okReply_);
}

}  // namespace rsm::client
