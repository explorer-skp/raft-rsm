#pragma once

// Phase 6 invariant checkers. Deliberately decoupled from the simulation
// harness: every checker consumes plain data (terms, node ids, log images),
// accumulates human-readable violation strings, and never aborts — so the
// checker self-tests can feed fabricated violations and assert they are
// FLAGGED, which is what makes a green chaos run meaningful (a checker that
// cannot fail proves nothing).

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "rpc/messages.h"
#include "storage/log.h"

namespace rsm::sim {

using rsm::rpc::LogIndex;
using rsm::rpc::NodeId;
using rsm::rpc::Term;

// 64-bit FNV-1a, the project-internal fingerprint for log entries and apply
// results. Collisions are astronomically unlikely at test scale and a
// collision could only HIDE a violation, never fabricate one.
std::uint64_t fnv1a(const void* data, std::size_t len,
                    std::uint64_t h = 14695981039346656037ULL);

// A log entry reduced to (term, fingerprint-of-term-and-command). The term is
// kept separate because Log Matching keys on "same index AND same term".
struct EntryImage {
    Term term = 0;
    std::uint64_t fp = 0;
    bool operator==(const EntryImage&) const = default;
};
using LogImage = std::vector<EntryImage>;  // [i] describes log index i+1

EntryImage entryImage(const rsm::rpc::LogEntry& e);
LogImage logImage(const rsm::storage::RaftLog& log);

// The (clientId, seqNo) session identity embedded as a command's leading 16
// bytes (Phase 5 format); nullopt if the command is too short.
std::optional<std::pair<std::uint64_t, std::uint64_t>> commandIdentity(
    const std::vector<std::uint8_t>& command);

// Invariant 1 — Election Safety: at most one leader per term. Fed every
// `won-election` transition over the whole run (continuous monitoring).
class ElectionSafetyChecker {
public:
    void observeLeader(Term term, NodeId node);
    const std::vector<std::string>& violations() const { return violations_; }

private:
    std::map<Term, std::set<NodeId>> leadersByTerm_;
    std::vector<std::string> violations_;
};

// Invariant 2 — Leader Append-Only: while a node remains leader of one term,
// its log may only grow; no entry it holds is ever overwritten or deleted.
// Fed (role, term, log image) for every node on every simulated step. The
// baseline resets whenever the node is not leader or changes term — a
// FOLLOWER truncating its log on divergence repair is legal.
class LeaderAppendOnlyChecker {
public:
    void observe(NodeId node, bool isLeader, Term term, const LogImage& log);
    const std::vector<std::string>& violations() const { return violations_; }

private:
    struct Baseline {
        Term term = 0;
        LogImage log;
    };
    std::map<NodeId, Baseline> baseline_;  // present only while leader
    std::vector<std::string> violations_;
};

// Invariant 3 — Log Matching: if two logs hold the same term at the same
// index, the logs are identical at all indices up to and including it.
// Stateless sweep over a snapshot of all logs (run periodically and at
// quiescence; the per-step monitors cover the continuous invariants).
std::vector<std::string> checkLogMatching(
    const std::map<NodeId, LogImage>& logs);

// Invariant 5 — State Machine Safety: no two nodes ever apply a different
// command at the same log index — and, stricter, applying it must produce
// the same result on every replica (determinism check). Fed every apply via
// the apply-observer seam, including re-applies after a restart, which are
// thereby checked against what the cluster applied before the crash.
class AppliedConsistencyChecker {
public:
    void observeApply(NodeId node, LogIndex index, EntryImage entry,
                      std::uint64_t resultFp);
    const std::vector<std::string>& violations() const { return violations_; }

private:
    struct Applied {
        EntryImage entry;
        std::uint64_t resultFp = 0;
        NodeId firstNode = 0;
    };
    std::map<LogIndex, Applied> record_;
    std::vector<std::string> violations_;
};

// Invariant 4 — Leader Completeness — plus the no-lost-commit property.
// Maintains the global record of every entry ever observed committed (any
// node's log prefix up to that node's commitIndex):
//  - a committed index later observed with a DIFFERENT entry, on any node,
//    is flagged (a committed entry may never change or disappear);
//  - every newly elected leader must hold the entire committed record
//    (Leader Completeness, checked at the moment of every election win).
class CommitChecker {
public:
    // Call for every live node on every step.
    void observeCommit(NodeId node, LogIndex commitIndex, const LogImage& log);
    // Call from the transition observer on every `won-election`.
    void observeElectionWin(NodeId node, Term term, const LogImage& log);

    const std::vector<EntryImage>& committed() const { return committed_; }
    const std::vector<std::string>& violations() const { return violations_; }

private:
    std::vector<EntryImage> committed_;  // [i] = committed entry at index i+1
    std::vector<std::string> violations_;
};

}  // namespace rsm::sim
