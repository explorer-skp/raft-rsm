#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <vector>

#include "raft/clock.h"
#include "raft/persistent_state.h"
#include "rpc/messages.h"
#include "statemachine/state_machine.h"
#include "storage/log.h"

namespace rsm::raft {

using rsm::rpc::LogIndex;

enum class Role : std::uint8_t { Follower, Candidate, Leader };

const char* roleName(Role r);

// Raft §5.4.1 up-to-date check, as a pure function: is a candidate whose log
// ends at (candLastTerm, candLastIndex) at least as up-to-date as a log
// ending at (myLastTerm, myLastIndex)? Higher last term wins; equal last
// terms compare last index; equal both ⇒ equally up-to-date (grant allowed).
bool candidateLogAtLeastAsUpToDate(Term candLastTerm, LogIndex candLastIndex,
                                   Term myLastTerm, LogIndex myLastIndex);

struct RaftConfig {
    // Election timeout is redrawn uniformly from [min, max] every time the
    // timer is (re)armed. Heartbeat interval must be < electionTimeoutMin.
    Duration electionTimeoutMin{150};
    Duration electionTimeoutMax{300};
    Duration heartbeatInterval{50};
};

// The Raft consensus state machine for one node: leader election (Phase 2)
// plus log replication, commit, and apply (Phase 3). No disk yet (Phase 4).
//
// Threading contract: NOT thread-safe by design. Every call (start, handle,
// tick) must come from a single thread — the Raft event loop — so all Raft
// state is touched by exactly one thread and needs no locks. Outbound
// messages go through the SendFn callback, which is invoked synchronously
// from that same thread; the core never touches sockets.
//
// Time comes only from the injected Clock; randomness only from the seeded
// PRNG. With a ManualClock the core is fully deterministic.
class RaftCore {
public:
    using SendFn = std::function<void(NodeId to, const rsm::rpc::Message& m)>;
    // Observability hook: invoked on every role/term transition with
    // (term, newRole, event). Tests use it to assert Election Safety.
    using TransitionFn =
        std::function<void(Term term, Role role, const char* event)>;
    // Phase 5 seams, both invoked on the event-loop thread (so handlers may
    // call propose() and read core state without locks). The core itself
    // stays client-agnostic: it neither parses commands nor builds replies.
    using ClientRequestFn =
        std::function<void(NodeId from, const rsm::rpc::ClientRequest&)>;
    // After each apply, in index order: (index, entry, apply() result). The
    // entry reference is only valid for the duration of the callback and is
    // invalidated by anything that mutates the log (e.g. propose()).
    using ApplyFn = std::function<void(
        LogIndex index, const rsm::rpc::LogEntry&, const std::string& result)>;
    // Phase 7 runtime seam: when set, committed entries are HANDED OFF in
    // index order instead of being applied inline — the sink owner applies
    // them (on its own thread) and owns the state machine from then on. The
    // entry reference is only valid for the duration of the callback (copy
    // it; the log mutates on this thread). The sink returns false to REFUSE
    // the entry (e.g. its queue is full): the core then stops handing off —
    // lastApplied does not advance past a refusal, so the entry is offered
    // again on the next applyCommitted()/pumpApply(). This keeps the Raft
    // thread NON-BLOCKING under apply backpressure: a slow state machine
    // must never stall heartbeats or election timers. The sim and the
    // deterministic tests never set this, so their apply path is
    // byte-for-byte unchanged.
    using ApplySinkFn =
        std::function<bool(LogIndex index, const rsm::rpc::LogEntry&)>;

    RaftCore(NodeId self, std::vector<NodeId> peers, PersistentState& persist,
             rsm::storage::RaftLog& log, rsm::statemachine::StateMachine& sm,
             Clock& clock, std::uint64_t rngSeed, RaftConfig cfg, SendFn send);

    void setTransitionObserver(TransitionFn fn) { onTransition_ = std::move(fn); }
    void setClientRequestHandler(ClientRequestFn fn) {
        onClientRequest_ = std::move(fn);
    }
    void setApplyObserver(ApplyFn fn) { onApply_ = std::move(fn); }
    void setApplySink(ApplySinkFn fn) { applySink_ = std::move(fn); }

    // Arms the first election timeout. Call once, from the event-loop thread.
    void start();

    // Feed one inbound RPC or RPC reply. env.from identifies the sender
    // (used to route replies and de-duplicate votes).
    void handle(const rsm::rpc::Envelope& env, const rsm::rpc::Message& m);

    // Fires any timer whose deadline has passed per clock.now(): election
    // timeout (follower/candidate) or heartbeat interval (leader).
    void tick();

    // Internal propose seam (the client layer sits on this in Phase 5).
    // Leader: appends {currentTerm, command} to the log, triggers
    // replication, and returns the assigned index. Not leader: nullopt.
    std::optional<LogIndex> propose(std::vector<std::uint8_t> command);

    // Phase 7 group commit: semantically identical to calling propose() for
    // each command in order, except the entries reach the log in ONE append
    // (one fsync — the storage layer already makes each append call a
    // single durability point) and replication is triggered once. Returns
    // the index of the FIRST command (they are contiguous), or nullopt if
    // not leader (no command is appended in that case).
    // Takes the commands by reference and MOVES them out, leaving reusable
    // shells (the caller's batch container keeps its capacity).
    std::optional<LogIndex> proposeBatch(
        std::vector<std::vector<std::uint8_t>>& commands);

    // Re-offers committed-but-not-yet-handed-off entries to the apply sink
    // (no-op when none are pending). The runtime calls this every loop
    // iteration so a sink refusal (apply backpressure) is retried as soon
    // as the apply thread frees space, without ever blocking this thread.
    void pumpApply() { applyCommitted(); }

    // Earliest pending timer deadline; the event loop sleeps until this.
    TimePoint nextDeadline() const;

    Role role() const { return role_; }
    Term term() const { return persist_.currentTerm(); }
    std::optional<NodeId> votedFor() const { return persist_.votedFor(); }
    // Leader of the current term, once known (self if leader).
    std::optional<NodeId> leaderId() const { return leaderId_; }
    NodeId selfId() const { return self_; }
    // Exposed so timer-reset rules are directly assertable in unit tests.
    TimePoint electionDeadline() const { return electionDeadline_; }
    LogIndex commitIndex() const { return commitIndex_; }
    LogIndex lastApplied() const { return lastApplied_; }

private:
    void becomeFollower(Term newTerm, const char* event);
    void startElection();
    void becomeLeader();
    void sendHeartbeats();
    // One full AppendEntries to one peer: prev = nextIndex[peer]-1, entries
    // from nextIndex[peer] (empty entries == heartbeat), leaderCommit.
    void sendAppendEntries(NodeId peer);
    void advanceCommit();   // leader: Figure 8 rule (current-term majority)
    void applyCommitted();  // all roles: apply (lastApplied, commitIndex]
    void resetElectionTimer();
    void persistTermAndVote(Term term, std::optional<NodeId> votedFor);
    std::size_t majority() const { return (peers_.size() + 1) / 2 + 1; }

    void onRequestVote(NodeId from, const rsm::rpc::RequestVote& rv);
    void onRequestVoteReply(NodeId from, const rsm::rpc::RequestVoteReply& r);
    void onAppendEntries(NodeId from, const rsm::rpc::AppendEntries& ae);
    void onAppendEntriesReply(NodeId from,
                              const rsm::rpc::AppendEntriesReply& r);

    const NodeId self_;
    const std::vector<NodeId> peers_;  // excludes self
    PersistentState& persist_;
    rsm::storage::RaftLog& log_;
    rsm::statemachine::StateMachine& sm_;
    Clock& clock_;
    RaftConfig cfg_;
    SendFn send_;
    TransitionFn onTransition_;
    ClientRequestFn onClientRequest_;
    ApplyFn onApply_;
    ApplySinkFn applySink_;

    std::mt19937_64 rng_;
    Role role_ = Role::Follower;
    std::optional<NodeId> leaderId_;
    std::set<NodeId> votesFrom_;  // granted votes this candidacy, incl. self
    TimePoint electionDeadline_ = TimePoint::max();
    TimePoint heartbeatDeadline_ = TimePoint::max();

    // Hot-path scratch state (Phase 7 allocation-free steady state). These
    // change nothing semantically — every outbound message and log append
    // carries the same bytes — they only let buffers be REUSED so a steady
    // propose/replicate/heartbeat cycle performs zero plumbing allocations
    // (asserted by the allocation test; measured impact in DESIGN.md).
    rsm::rpc::Message aeMsg_{rsm::rpc::AppendEntries{}};  // reused AE + buffers
    std::vector<std::vector<std::uint8_t>> cmdPool_;  // recycled cmd buffers
    std::vector<rsm::rpc::LogEntry> entryScratch_;    // propose/append staging

    // Volatile on all servers.
    LogIndex commitIndex_ = 0;
    LogIndex lastApplied_ = 0;
    // Leader-only volatile state, reinitialized on every election win.
    std::map<NodeId, LogIndex> nextIndex_;
    std::map<NodeId, LogIndex> matchIndex_;
};

}  // namespace rsm::raft
