#include "raft/raft_core.h"

#include <algorithm>
#include <variant>

#include "metrics/alloc_gate.h"
#include "raft/logging.h"

namespace rsm::raft {

using rsm::rpc::AppendEntries;
using rsm::rpc::AppendEntriesReply;
using rsm::rpc::Envelope;
using rsm::rpc::Message;
using rsm::rpc::RequestVote;
using rsm::rpc::RequestVoteReply;

const char* roleName(Role r) {
    switch (r) {
        case Role::Follower: return "Follower";
        case Role::Candidate: return "Candidate";
        case Role::Leader: return "Leader";
    }
    return "?";
}

bool candidateLogAtLeastAsUpToDate(Term candLastTerm, LogIndex candLastIndex,
                                   Term myLastTerm, LogIndex myLastIndex) {
    if (candLastTerm != myLastTerm) return candLastTerm > myLastTerm;
    return candLastIndex >= myLastIndex;
}

namespace {

// Term carried by an RPC or reply, for the universal term rule. Client
// messages carry no term.
std::optional<Term> termOf(const Message& m) {
    return std::visit(
        [](const auto& msg) -> std::optional<Term> {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, RequestVote> ||
                          std::is_same_v<T, RequestVoteReply> ||
                          std::is_same_v<T, AppendEntries> ||
                          std::is_same_v<T, AppendEntriesReply>) {
                return msg.term;
            } else {
                return std::nullopt;
            }
        },
        m);
}

}  // namespace

RaftCore::RaftCore(NodeId self, std::vector<NodeId> peers,
                   PersistentState& persist, rsm::storage::RaftLog& log,
                   rsm::statemachine::StateMachine& sm, Clock& clock,
                   std::uint64_t rngSeed, RaftConfig cfg, SendFn send)
    : self_(self),
      peers_(std::move(peers)),
      persist_(persist),
      log_(log),
      sm_(sm),
      clock_(clock),
      cfg_(cfg),
      send_(std::move(send)),
      rng_(rngSeed) {
    // Preallocate the hot-path scratch pools (spec §4.7): enough entry
    // shells and command buffers for any steady-state batch, so the
    // propose/replicate cycle never allocates from the very first message.
    // Bigger one-off traffic (a long catch-up resend) grows them once and
    // the capacity sticks.
    constexpr std::size_t kScratchEntries = 64;
    constexpr std::size_t kScratchCommandBytes = 4096;
    auto& ae = std::get<rsm::rpc::AppendEntries>(aeMsg_);
    ae.entries.reserve(kScratchEntries);
    entryScratch_.reserve(kScratchEntries);
    cmdPool_.reserve(kScratchEntries);
    for (std::size_t i = 0; i < kScratchEntries; ++i) {
        std::vector<std::uint8_t> buf;
        buf.reserve(kScratchCommandBytes);
        cmdPool_.push_back(std::move(buf));
    }
}

void RaftCore::start() {
    resetElectionTimer();
}

void RaftCore::persistTermAndVote(Term term, std::optional<NodeId> votedFor) {
    // Discipline for Phase 4: persistent state is saved before any message
    // that depends on it is sent. Memory-backed today, fsync later.
    persist_.save(term, votedFor);
}

void RaftCore::becomeFollower(Term newTerm, const char* event) {
    if (newTerm > persist_.currentTerm()) {
        persistTermAndVote(newTerm, std::nullopt);
        leaderId_.reset();
    }
    if (role_ == Role::Leader) {
        // A leader runs no election timer, so on deposition it must arm one
        // or it could never campaign again. This is arming a timer that did
        // not exist — not one of the three reset events, which govern an
        // already-armed follower/candidate timer.
        heartbeatDeadline_ = TimePoint::max();
        resetElectionTimer();
    }
    role_ = Role::Follower;
    raftLog(LogLevel::Info, "[raft] node=%u term=%llu role=Follower event=%s",
            self_, static_cast<unsigned long long>(persist_.currentTerm()),
            event);
    if (onTransition_) onTransition_(persist_.currentTerm(), role_, event);
}

void RaftCore::startElection() {
    persistTermAndVote(persist_.currentTerm() + 1, self_);
    role_ = Role::Candidate;
    leaderId_.reset();
    votesFrom_.clear();
    votesFrom_.insert(self_);
    heartbeatDeadline_ = TimePoint::max();
    resetElectionTimer();
    raftLog(LogLevel::Info, "[raft] node=%u term=%llu role=Candidate event=%s",
            self_, static_cast<unsigned long long>(persist_.currentTerm()),
            "election-started");
    if (onTransition_) {
        onTransition_(persist_.currentTerm(), role_, "election-started");
    }
    const RequestVote rv{persist_.currentTerm(), self_, log_.lastIndex(),
                         log_.lastTerm()};
    for (const NodeId peer : peers_) send_(peer, Message{rv});
    if (votesFrom_.size() >= majority()) becomeLeader();  // 1-node cluster
}

void RaftCore::becomeLeader() {
    role_ = Role::Leader;
    leaderId_ = self_;
    nextIndex_.clear();
    matchIndex_.clear();
    for (const NodeId peer : peers_) {
        nextIndex_[peer] = log_.lastIndex() + 1;
        matchIndex_[peer] = 0;
    }
    // A leader has no election timeout: nothing it receives may reset its
    // own timer, and it never campaigns against itself.
    electionDeadline_ = TimePoint::max();
    raftLog(LogLevel::Info, "[raft] node=%u term=%llu role=Leader event=%s",
            self_, static_cast<unsigned long long>(persist_.currentTerm()),
            "won-election");
    if (onTransition_) {
        onTransition_(persist_.currentTerm(), role_, "won-election");
    }
    sendHeartbeats();
}

void RaftCore::sendHeartbeats() {
    // Heartbeats are just AppendEntries that happen to carry no entries
    // (when the peer is caught up); they always carry prevLog* and
    // leaderCommit, so they double as backfill and commit propagation.
    for (const NodeId peer : peers_) sendAppendEntries(peer);
    heartbeatDeadline_ = clock_.now() + cfg_.heartbeatInterval;
}

void RaftCore::sendAppendEntries(NodeId peer) {
    const LogIndex next = nextIndex_[peer];
    // The AE message object is a reused member: its entries' command
    // buffers cycle through cmdPool_ when the entry count shrinks (e.g.
    // heartbeats between batches), so steady state sends allocate nothing.
    // Hot path; same bytes on the wire as building a fresh message.
    auto& ae = std::get<AppendEntries>(aeMsg_);
    ae.term = persist_.currentTerm();
    ae.leaderId = self_;
    ae.prevLogIndex = next - 1;
    ae.prevLogTerm = log_.termAt(next - 1);
    ae.leaderCommit = commitIndex_;
    const auto src = log_.entriesSpan(next);
    while (ae.entries.size() > src.size()) {
        cmdPool_.push_back(std::move(ae.entries.back().command));
        ae.entries.pop_back();
    }
    while (ae.entries.size() < src.size()) {
        rsm::rpc::LogEntry e;
        if (!cmdPool_.empty()) {
            e.command = std::move(cmdPool_.back());
            cmdPool_.pop_back();
        }
        ae.entries.push_back(std::move(e));
    }
    for (std::size_t i = 0; i < src.size(); ++i) {
        ae.entries[i].term = src[i].term;
        ae.entries[i].command.assign(src[i].command.begin(),
                                     src[i].command.end());
    }
    raftLog(LogLevel::Debug,
            "[raft] node=%u term=%llu append-> peer=%u prev=%llu n=%zu "
            "commit=%llu",
            self_, static_cast<unsigned long long>(ae.term), peer,
            static_cast<unsigned long long>(ae.prevLogIndex),
            ae.entries.size(),
            static_cast<unsigned long long>(ae.leaderCommit));
    send_(peer, aeMsg_);
}

std::optional<LogIndex> RaftCore::propose(std::vector<std::uint8_t> command) {
    if (role_ != Role::Leader) return std::nullopt;
    const Term cur = persist_.currentTerm();
    // Stage in the reusable scratch and move through to the log: the
    // command buffer is allocated once (by the caller) and retained by the
    // log; nothing else on this path allocates in steady state.
    entryScratch_.clear();
    entryScratch_.push_back(rsm::rpc::LogEntry{cur, std::move(command)});
    log_.append(std::span<rsm::rpc::LogEntry>(entryScratch_));
    const LogIndex idx = log_.lastIndex();
    raftLog(LogLevel::Debug, "[raft] node=%u term=%llu appended index=%llu",
            self_, static_cast<unsigned long long>(cur),
            static_cast<unsigned long long>(idx));
    advanceCommit();  // a 1-node cluster commits immediately
    for (const NodeId peer : peers_) sendAppendEntries(peer);
    heartbeatDeadline_ = clock_.now() + cfg_.heartbeatInterval;
    return idx;
}

std::optional<LogIndex> RaftCore::proposeBatch(
    std::vector<std::vector<std::uint8_t>>& commands) {
    if (commands.empty() || role_ != Role::Leader) return std::nullopt;
    const Term cur = persist_.currentTerm();
    // The command buffers move out of the caller's container (which keeps
    // its capacity for the next batch) through the scratch into the log.
    entryScratch_.clear();
    for (auto& c : commands) {
        entryScratch_.push_back(rsm::rpc::LogEntry{cur, std::move(c)});
    }
    const std::size_t n = entryScratch_.size();
    // One append call == one durability point == one fsync (group commit);
    // persist-before-send holds exactly as in propose().
    log_.append(std::span<rsm::rpc::LogEntry>(entryScratch_));
    const LogIndex first = log_.lastIndex() - n + 1;
    raftLog(LogLevel::Debug,
            "[raft] node=%u term=%llu appended batch [%llu, %llu]", self_,
            static_cast<unsigned long long>(cur),
            static_cast<unsigned long long>(first),
            static_cast<unsigned long long>(log_.lastIndex()));
    advanceCommit();  // a 1-node cluster commits immediately
    for (const NodeId peer : peers_) sendAppendEntries(peer);
    heartbeatDeadline_ = clock_.now() + cfg_.heartbeatInterval;
    return first;
}

void RaftCore::resetElectionTimer() {
    std::uniform_int_distribution<std::int64_t> dist(
        cfg_.electionTimeoutMin.count(), cfg_.electionTimeoutMax.count());
    electionDeadline_ = clock_.now() + Duration(dist(rng_));
}

TimePoint RaftCore::nextDeadline() const {
    return std::min(electionDeadline_, heartbeatDeadline_);
}

void RaftCore::tick() {
    const TimePoint now = clock_.now();
    if (role_ != Role::Leader && now >= electionDeadline_) {
        // Follower with no leader traffic, or candidate whose election did
        // not win in time: start a (new) election.
        startElection();
        return;
    }
    if (role_ == Role::Leader && now >= heartbeatDeadline_) sendHeartbeats();
}

void RaftCore::handle(const Envelope& env, const Message& m) {
    // Universal term rule: any RPC or reply with a term above ours moves us
    // to that term as a follower (clearing votedFor) before normal handling.
    if (const auto t = termOf(m); t && *t > persist_.currentTerm()) {
        becomeFollower(*t, "higher-term-seen");
    }
    std::visit(
        [&](const auto& msg) {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, RequestVote>) {
                onRequestVote(env.from, msg);
            } else if constexpr (std::is_same_v<T, RequestVoteReply>) {
                onRequestVoteReply(env.from, msg);
            } else if constexpr (std::is_same_v<T, AppendEntries>) {
                onAppendEntries(env.from, msg);
            } else if constexpr (std::is_same_v<T, AppendEntriesReply>) {
                onAppendEntriesReply(env.from, msg);
            } else if constexpr (std::is_same_v<T, rsm::rpc::ClientRequest>) {
                if (onClientRequest_) {
                    onClientRequest_(env.from, msg);
                } else {
                    raftLog(LogLevel::Debug,
                            "[raft] node=%u dropping client request "
                            "(no handler registered)",
                            self_);
                }
            } else {
                raftLog(LogLevel::Debug,
                        "[raft] node=%u dropping unexpected client reply",
                        self_);
            }
        },
        m);
}

void RaftCore::onRequestVote(NodeId from, const RequestVote& rv) {
    const Term cur = persist_.currentTerm();
    if (rv.term < cur) {
        send_(from, Message{RequestVoteReply{cur, false}});
        return;  // stale candidate; no timer reset
    }
    // rv.term == cur here (the universal rule lifted us if it was higher).
    const bool logOk = candidateLogAtLeastAsUpToDate(
        rv.lastLogTerm, rv.lastLogIndex, log_.lastTerm(), log_.lastIndex());
    const auto voted = persist_.votedFor();
    const bool grant =
        logOk && (!voted.has_value() || *voted == rv.candidateId);
    if (grant) {
        // Persist the vote before replying; reset the election timer only
        // when granting (rule 2 of the three allowed reset events).
        persistTermAndVote(cur, rv.candidateId);
        resetElectionTimer();
        raftLog(LogLevel::Debug,
                "[raft] node=%u term=%llu granted vote to %u", self_,
                static_cast<unsigned long long>(cur), rv.candidateId);
    }
    send_(from, Message{RequestVoteReply{cur, grant}});
}

void RaftCore::onRequestVoteReply(NodeId from, const RequestVoteReply& r) {
    if (role_ != Role::Candidate) return;
    // Count only replies from this candidacy's term: a stale grant from an
    // earlier election must not contribute to the current majority.
    if (r.term != persist_.currentTerm() || !r.voteGranted) return;
    votesFrom_.insert(from);
    if (votesFrom_.size() >= majority()) becomeLeader();
}

void RaftCore::onAppendEntries(NodeId from, const AppendEntries& ae) {
    const Term cur = persist_.currentTerm();
    if (ae.term < cur) {
        send_(from, Message{AppendEntriesReply{cur, false, 0, 0}});
        return;  // stale leader; no timer reset
    }
    // ae.term == cur. Two leaders in one term would violate Election Safety;
    // a leader therefore never sees a valid AppendEntries for its own term.
    if (role_ == Role::Leader) {
        raftLog(LogLevel::Error,
                "[raft] node=%u term=%llu ELECTION SAFETY VIOLATION: "
                "AppendEntries from %u in my leadership term",
                self_, static_cast<unsigned long long>(cur), ae.leaderId);
        return;
    }
    leaderId_ = ae.leaderId;
    if (role_ == Role::Candidate) {
        becomeFollower(cur, "leader-discovered");
    }
    // Rule 1 of the three allowed reset events: valid AppendEntries from the
    // current leader — the timer resets even if the consistency check below
    // fails (the leader is live; its retry will repair us).
    resetElectionTimer();

    // Consistency check. prevLogIndex == 0 passes trivially (termAt(0)==0).
    if (ae.prevLogIndex > log_.lastIndex()) {
        // Log too short: hint where our log ends so the leader jumps there.
        send_(from, Message{AppendEntriesReply{cur, false,
                                               log_.lastIndex() + 1, 0}});
        return;
    }
    if (log_.termAt(ae.prevLogIndex) != ae.prevLogTerm) {
        // Term mismatch: hint the conflicting term and its first index, so
        // the leader skips the whole term in one step.
        const Term conflictTerm = log_.termAt(ae.prevLogIndex);
        LogIndex first = ae.prevLogIndex;
        while (first > 1 && log_.termAt(first - 1) == conflictTerm) --first;
        send_(from, Message{AppendEntriesReply{cur, false, first,
                                               conflictTerm}});
        return;
    }

    // Conflict resolution + append. Walk the incoming entries against the
    // local log: skip entries already present (same index AND term); on the
    // first term conflict, truncate from there and append the rest. An RPC
    // that is a prefix/duplicate of what we already have falls off the end
    // of this loop with nothing to truncate and nothing to append — a
    // delayed or duplicate AppendEntries must never delete entries we hold
    // (they may be committed).
    std::size_t i = 0;
    for (; i < ae.entries.size(); ++i) {
        const LogIndex at = ae.prevLogIndex + 1 + i;
        if (at > log_.lastIndex()) break;  // everything from here is new
        if (log_.termAt(at) != ae.entries[i].term) {
            if (at <= commitIndex_) {
                // Should be impossible (Log Matching): a committed entry
                // never conflicts with the leader of the term that holds it.
                raftLog(LogLevel::Error,
                        "[raft] node=%u LOG MATCHING VIOLATION: conflict at "
                        "committed index %llu",
                        self_, static_cast<unsigned long long>(at));
            }
            log_.truncateSuffixFrom(at);
            break;
        }
    }
    if (i < ae.entries.size()) {
        // Copy the new suffix into the scratch (these copies are the
        // follower's retained log bytes) and move it into the log.
        entryScratch_.clear();
        {
            const rsm::metrics::AllocRetention allocTag;
            entryScratch_.assign(
                ae.entries.begin() + static_cast<std::ptrdiff_t>(i),
                ae.entries.end());
        }
        log_.append(std::span<rsm::rpc::LogEntry>(entryScratch_));
    }

    // Advance follower commit, capped at the last index this RPC vouches
    // for (our log may extend beyond what this leader has confirmed).
    const LogIndex confirmedThrough = ae.prevLogIndex + ae.entries.size();
    if (ae.leaderCommit > commitIndex_) {
        commitIndex_ = std::min(ae.leaderCommit, confirmedThrough);
        applyCommitted();
    }

    // Success ack: echo prev + entries.size() OF THIS REQUEST in
    // conflictIndex, so the leader updates matchIndex from the request that
    // produced this reply even across reordered/lost replies.
    send_(from, Message{AppendEntriesReply{cur, true, confirmedThrough, 0}});
}

void RaftCore::onAppendEntriesReply(NodeId from, const AppendEntriesReply& r) {
    if (role_ != Role::Leader) return;
    // Replies are only meaningful in the term the RPC was sent (== current:
    // higher terms stepped us down above, lower are stale candidacies).
    if (r.term != persist_.currentTerm()) return;

    if (r.success) {
        const LogIndex ack = r.conflictIndex;  // echoed request match point
        if (ack > log_.lastIndex()) {
            raftLog(LogLevel::Error,
                    "[raft] node=%u peer=%u acked %llu beyond my log",
                    self_, from, static_cast<unsigned long long>(ack));
            return;
        }
        if (ack > matchIndex_[from]) {
            matchIndex_[from] = ack;
            nextIndex_[from] = ack + 1;
            raftLog(LogLevel::Debug,
                    "[raft] node=%u peer=%u match=%llu next=%llu", self_,
                    from, static_cast<unsigned long long>(ack),
                    static_cast<unsigned long long>(ack + 1));
            advanceCommit();
            // Continue the catch-up only when this ack moved the window:
            // resending on every duplicate ack turns the pipelined
            // propose-time AEs into a self-amplifying AE<->ack storm under
            // concurrent load (measured: the leader sustained >170k msgs/s
            // for ~400 client ops/s, latency growing with queue depth).
            // Liveness is unaffected: a lost AE is retransmitted by the
            // next heartbeat, exactly as for any dropped message.
            if (nextIndex_[from] <= log_.lastIndex()) sendAppendEntries(from);
        }
        return;
    }

    // Failed consistency check: lower nextIndex by the conflict hint.
    LogIndex ni;
    if (r.conflictTerm == 0) {
        ni = r.conflictIndex;  // follower log too short: jump to its end
    } else {
        // Last index in our log with the follower's conflicting term; if we
        // have that term, resend from just after it, else skip the whole
        // term down to the follower's first index of it.
        LogIndex lastOfTerm = 0;
        for (LogIndex i = log_.lastIndex(); i >= 1; --i) {
            if (log_.termAt(i) == r.conflictTerm) {
                lastOfTerm = i;
                break;
            }
        }
        ni = lastOfTerm != 0 ? lastOfTerm + 1 : r.conflictIndex;
    }
    // Never above what a stale reply would resurrect, never below a proven
    // match, never below 1.
    ni = std::min(ni, nextIndex_[from]);
    ni = std::max(ni, matchIndex_[from] + 1);
    ni = std::max<LogIndex>(ni, 1);
    nextIndex_[from] = ni;
    sendAppendEntries(from);  // retry immediately with the lowered window
}

void RaftCore::advanceCommit() {
    // Figure 8 rule: only an entry of the CURRENT term may be committed by
    // counting replicas. Entry terms are nondecreasing in the log, so once
    // termAt(n) != currentTerm we can stop scanning — nothing below n is a
    // current-term entry either. Prior-term entries become committed only
    // indirectly, when commitIndex_ moves past them here.
    const Term cur = persist_.currentTerm();
    for (LogIndex n = log_.lastIndex(); n > commitIndex_; --n) {
        if (log_.termAt(n) != cur) break;
        std::size_t replicas = 1;  // the leader's own log counts
        for (const auto& [peer, match] : matchIndex_) {
            if (match >= n) ++replicas;
        }
        if (replicas >= majority()) {
            commitIndex_ = n;
            raftLog(LogLevel::Debug,
                    "[raft] node=%u term=%llu commit=%llu", self_,
                    static_cast<unsigned long long>(cur),
                    static_cast<unsigned long long>(n));
            applyCommitted();
            break;
        }
    }
}

void RaftCore::applyCommitted() {
    while (lastApplied_ < commitIndex_) {
        const LogIndex next = lastApplied_ + 1;
        const rsm::rpc::LogEntry& entry = log_.entryAt(next);
        if (applySink_) {
            // Phase 7 hand-off: committed entries leave the core in index
            // order; the runtime's apply thread performs sm_.apply. Nothing
            // about WHAT gets applied changes — only where. A refusal
            // (apply backpressure) stops the hand-off WITHOUT advancing
            // lastApplied_: the entry is re-offered by the runtime's
            // pumpApply() — the Raft thread never blocks on the state
            // machine, so heartbeats and elections stay live.
            if (!applySink_(next, entry)) return;
            lastApplied_ = next;
            continue;
        }
        lastApplied_ = next;
        const std::string result = sm_.apply(entry.command);
        raftLog(LogLevel::Debug, "[raft] node=%u applied index=%llu", self_,
                static_cast<unsigned long long>(lastApplied_));
        if (onApply_) onApply_(lastApplied_, entry, result);
    }
}

}  // namespace rsm::raft
