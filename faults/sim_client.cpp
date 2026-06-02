#include "faults/sim_client.h"

#include <variant>

#include "statemachine/kv_store.h"

namespace rsm::sim {

using rsm::rpc::ClientReply;
using rsm::rpc::ClientRequest;
using rsm::rpc::ClientStatus;
using rsm::statemachine::encodeKvCommand;
using rsm::statemachine::KvOp;

void History::invoke(const ClientOp& op) {
    ops_[{op.clientId, op.seqNo}] = op;
}

void History::complete(std::uint64_t clientId, std::uint64_t seqNo,
                       std::int64_t atMs, std::string result) {
    auto& op = ops_.at({clientId, seqNo});
    op.returnMs = atMs;
    op.result = std::move(result);
}

std::vector<ClientOp> History::ops() const {
    std::vector<ClientOp> out;
    out.reserve(ops_.size());
    for (const auto& [id, op] : ops_) out.push_back(op);
    return out;
}

std::set<std::pair<std::uint64_t, std::uint64_t>> History::ackedIdentities()
    const {
    std::set<std::pair<std::uint64_t, std::uint64_t>> out;
    for (const auto& [id, op] : ops_) {
        if (op.complete()) out.insert(id);
    }
    return out;
}

std::size_t History::completedCount() const {
    std::size_t n = 0;
    for (const auto& [id, op] : ops_) n += op.complete() ? 1 : 0;
    return n;
}

SimClient::SimClient(std::uint64_t clientId, NodeId envelopeBase,
                     NodeId envelopeRange, std::uint64_t seed,
                     WorkloadConfig cfg, SimHarness& harness, History& history)
    : clientId_(clientId),
      base_(envelopeBase),
      range_(envelopeRange),
      rng_(seed),
      cfg_(cfg),
      harness_(harness),
      history_(history) {
    harness_.registerEndpoint(base_, static_cast<NodeId>(base_ + range_ - 1),
                              [this](NodeId from, NodeId to, Message&& m) {
                                  onDeliver(from, to, std::move(m));
                              });
}

std::string SimClient::key() {
    return "k" + std::to_string(rng_() %
                                static_cast<std::uint64_t>(cfg_.keySpace));
}

void SimClient::issueNextOp(bool marker) {
    const std::int64_t now = harness_.nowMs();
    current_ = ClientOp{};
    current_.clientId = clientId_;
    current_.seqNo = ++seqNo_;
    current_.invokeMs = now;
    current_.key = key();
    const int total = cfg_.wPut + cfg_.wGet + cfg_.wAppend + cfg_.wCas +
                      cfg_.wDelete;
    const int pick = marker ? 0  // the post-heal marker is always a PUT
                            : static_cast<int>(
                                  rng_() % static_cast<std::uint64_t>(total));
    // A value unique to (client, seq) makes every write distinguishable,
    // which is what gives the linearizability checker its teeth.
    const std::string unique =
        "c" + std::to_string(clientId_) + "s" + std::to_string(current_.seqNo);
    if (pick < cfg_.wPut) {
        current_.op = KvOp::Put;
        current_.arg = unique;
    } else if (pick < cfg_.wPut + cfg_.wGet) {
        current_.op = KvOp::Get;
    } else if (pick < cfg_.wPut + cfg_.wGet + cfg_.wAppend) {
        current_.op = KvOp::Append;
        current_.arg = unique;
    } else if (pick < cfg_.wPut + cfg_.wGet + cfg_.wAppend + cfg_.wCas) {
        current_.op = KvOp::Cas;
        // Half the CAS ops expect "absent" (succeed on fresh keys), half a
        // value some client may have written; both outcomes are recorded
        // and both constrain the linearization.
        current_.arg = (rng_() % 2 == 0)
                           ? ""
                           : "c" + std::to_string(1 + rng_() % 4) + "s" +
                                 std::to_string(1 + rng_() % 6);
        current_.arg2 = unique;
    } else {
        current_.op = KvOp::Delete;
    }
    currentCommand_ = encodeKvCommand(clientId_, current_.seqNo, current_.op,
                                      current_.key, current_.arg,
                                      current_.arg2);
    history_.invoke(current_);
    ++opsIssued_;
    harness_.traceEvent("t=" + std::to_string(now) + " c" +
                        std::to_string(clientId_) + " invoke seq=" +
                        std::to_string(current_.seqNo));
    sendAttempt();
}

NodeId SimClient::nextTarget() {
    if (believedLeader_ != 0) return believedLeader_;
    const auto n = static_cast<NodeId>(harness_.nodeCount());
    return static_cast<NodeId>(1 + rng_() % n);
}

void SimClient::sendAttempt() {
    attemptEnvelope_ = static_cast<NodeId>(base_ + (attempt_ % range_));
    ++attempt_;
    target_ = nextTarget();
    harness_.clientSend(
        attemptEnvelope_, target_,
        Message{ClientRequest{clientId_, current_.seqNo, currentCommand_}});
    state_ = State::Waiting;
    deadlineMs_ = harness_.nowMs() + cfg_.requestTimeoutMs;
}

void SimClient::tick() {
    const std::int64_t now = harness_.nowMs();
    switch (state_) {
        case State::Idle: {
            if (!markerIssued_ && now >= cfg_.finalOpAtMs) {
                markerIssued_ = true;
                issueNextOp(/*marker=*/true);
            } else if (!markerIssued_ && now < cfg_.stopIssuingAtMs &&
                       opsIssued_ < cfg_.opsPerClient &&
                       now >= nextIssueAtMs_) {
                issueNextOp(/*marker=*/false);
            }
            return;
        }
        case State::Waiting:
            if (now >= deadlineMs_) {
                // Timed out: forget the leader preference and try elsewhere.
                believedLeader_ = 0;
                sendAttempt();
            }
            return;
        case State::Backoff:
            if (now >= deadlineMs_) sendAttempt();
            return;
    }
}

void SimClient::onDeliver(NodeId from, NodeId to, Message&& m) {
    const auto* reply = std::get_if<ClientReply>(&m);
    if (reply == nullptr) return;
    // Only the reply to the CURRENT attempt counts; anything addressed to a
    // previous attempt's envelope id is a stale answer from a slow node
    // (the sim analogue of a reply arriving on an abandoned connection).
    if (state_ != State::Waiting || to != attemptEnvelope_ ||
        from != target_) {
        return;
    }
    const std::int64_t now = harness_.nowMs();
    switch (reply->status) {
        case ClientStatus::Ok: {
            history_.complete(clientId_, current_.seqNo, now,
                              std::string(reply->result.begin(),
                                          reply->result.end()));
            believedLeader_ = from;
            state_ = State::Idle;
            nextIssueAtMs_ =
                now + cfg_.minGapMs +
                static_cast<std::int64_t>(
                    rng_() % static_cast<std::uint64_t>(
                                 cfg_.maxGapMs - cfg_.minGapMs + 1));
            harness_.traceEvent("t=" + std::to_string(now) + " c" +
                                std::to_string(clientId_) + " ok seq=" +
                                std::to_string(current_.seqNo));
            return;
        }
        case ClientStatus::NotLeader:
        case ClientStatus::Error: {
            const auto hint = reply->leaderHint;
            if (reply->status == ClientStatus::NotLeader && hint != 0 &&
                hint != target_) {
                believedLeader_ = hint;
            } else {
                believedLeader_ = 0;
            }
            state_ = State::Backoff;
            deadlineMs_ = now + cfg_.retryBackoffMs;
            return;
        }
    }
}

}  // namespace rsm::sim
