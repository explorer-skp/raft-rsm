#include "faults/checkers.h"

#include <cstdarg>
#include <cstdio>

namespace rsm::sim {

namespace {

#if defined(__GNUC__)
__attribute__((format(printf, 1, 2)))
#endif
std::string
format(const char* fmt, ...) {
    char buf[256];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    return buf;
}

}  // namespace

std::uint64_t fnv1a(const void* data, std::size_t len, std::uint64_t h) {
    const auto* p = static_cast<const unsigned char*>(data);
    for (std::size_t i = 0; i < len; ++i) {
        h ^= p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

EntryImage entryImage(const rsm::rpc::LogEntry& e) {
    std::uint64_t h = fnv1a(&e.term, sizeof(e.term));
    h = fnv1a(e.command.data(), e.command.size(), h);
    return EntryImage{e.term, h};
}

LogImage logImage(const rsm::storage::RaftLog& log) {
    LogImage out;
    const LogIndex last = log.lastIndex();
    out.reserve(last);
    for (LogIndex i = 1; i <= last; ++i) out.push_back(entryImage(log.entryAt(i)));
    return out;
}

std::optional<std::pair<std::uint64_t, std::uint64_t>> commandIdentity(
    const std::vector<std::uint8_t>& command) {
    if (command.size() < 16) return std::nullopt;
    std::uint64_t clientId = 0;
    std::uint64_t seqNo = 0;
    for (int i = 0; i < 8; ++i) {
        clientId |= static_cast<std::uint64_t>(command[i]) << (8 * i);
        seqNo |= static_cast<std::uint64_t>(command[8 + i]) << (8 * i);
    }
    return std::make_pair(clientId, seqNo);
}

void ElectionSafetyChecker::observeLeader(Term term, NodeId node) {
    auto& winners = leadersByTerm_[term];
    if (!winners.empty() && !winners.contains(node)) {
        violations_.push_back(format(
            "ELECTION SAFETY: term %llu has two leaders: node %u and node %u",
            static_cast<unsigned long long>(term),
            static_cast<unsigned>(*winners.begin()),
            static_cast<unsigned>(node)));
    }
    winners.insert(node);
}

void LeaderAppendOnlyChecker::observe(NodeId node, bool isLeader, Term term,
                                      const LogImage& log) {
    if (!isLeader) {
        baseline_.erase(node);
        return;
    }
    const auto it = baseline_.find(node);
    if (it != baseline_.end() && it->second.term == term) {
        const LogImage& base = it->second.log;
        if (log.size() < base.size()) {
            violations_.push_back(format(
                "LEADER APPEND-ONLY: leader %u (term %llu) shrank its log "
                "from %zu to %zu entries",
                static_cast<unsigned>(node),
                static_cast<unsigned long long>(term), base.size(),
                log.size()));
        } else {
            for (std::size_t i = 0; i < base.size(); ++i) {
                if (log[i] != base[i]) {
                    violations_.push_back(format(
                        "LEADER APPEND-ONLY: leader %u (term %llu) changed "
                        "its entry at index %zu",
                        static_cast<unsigned>(node),
                        static_cast<unsigned long long>(term), i + 1));
                    break;
                }
            }
        }
    }
    baseline_[node] = Baseline{term, log};
}

std::vector<std::string> checkLogMatching(
    const std::map<NodeId, LogImage>& logs) {
    std::vector<std::string> violations;
    for (auto a = logs.begin(); a != logs.end(); ++a) {
        for (auto b = std::next(a); b != logs.end(); ++b) {
            const LogImage& la = a->second;
            const LogImage& lb = b->second;
            const std::size_t top = std::min(la.size(), lb.size());
            // Highest index where both hold the same term; the invariant
            // then requires identical entries at every index up to it.
            std::size_t match = 0;
            for (std::size_t i = top; i >= 1; --i) {
                if (la[i - 1].term == lb[i - 1].term) {
                    match = i;
                    break;
                }
            }
            for (std::size_t i = 1; i <= match; ++i) {
                if (la[i - 1] != lb[i - 1]) {
                    violations.push_back(format(
                        "LOG MATCHING: nodes %u and %u agree on term at "
                        "index %zu but diverge at index %zu",
                        static_cast<unsigned>(a->first),
                        static_cast<unsigned>(b->first), match, i));
                    break;
                }
            }
        }
    }
    return violations;
}

void AppliedConsistencyChecker::observeApply(NodeId node, LogIndex index,
                                             EntryImage entry,
                                             std::uint64_t resultFp) {
    const auto it = record_.find(index);
    if (it == record_.end()) {
        record_[index] = Applied{entry, resultFp, node};
        return;
    }
    const Applied& first = it->second;
    if (first.entry != entry) {
        violations_.push_back(format(
            "STATE MACHINE SAFETY: node %u applied a different command at "
            "index %llu than node %u did",
            static_cast<unsigned>(node),
            static_cast<unsigned long long>(index),
            static_cast<unsigned>(first.firstNode)));
    } else if (first.resultFp != resultFp) {
        violations_.push_back(format(
            "APPLY DETERMINISM: node %u got a different result applying "
            "index %llu than node %u did",
            static_cast<unsigned>(node),
            static_cast<unsigned long long>(index),
            static_cast<unsigned>(first.firstNode)));
    }
}

void CommitChecker::observeCommit(NodeId node, LogIndex commitIndex,
                                  const LogImage& log) {
    if (commitIndex > log.size()) {
        violations_.push_back(format(
            "COMMIT: node %u reports commitIndex %llu beyond its log (%zu)",
            static_cast<unsigned>(node),
            static_cast<unsigned long long>(commitIndex), log.size()));
        return;
    }
    // Extend the global committed record with anything this node newly
    // vouches for.
    while (committed_.size() < commitIndex) {
        committed_.push_back(log[committed_.size()]);
    }
    // A committed entry must never change or disappear: this node's log up
    // to ITS OWN commitIndex must match the record exactly.
    for (std::size_t i = 0; i < commitIndex; ++i) {
        if (log[i] != committed_[i]) {
            violations_.push_back(format(
                "NO LOST COMMIT: node %u holds a different entry at "
                "committed index %zu (committed term %llu, node has %llu)",
                static_cast<unsigned>(node), i + 1,
                static_cast<unsigned long long>(committed_[i].term),
                static_cast<unsigned long long>(log[i].term)));
            break;
        }
    }
}

void CommitChecker::observeElectionWin(NodeId node, Term term,
                                       const LogImage& log) {
    if (log.size() < committed_.size()) {
        violations_.push_back(format(
            "LEADER COMPLETENESS: node %u won term %llu with only %zu "
            "entries but %zu are committed",
            static_cast<unsigned>(node), static_cast<unsigned long long>(term),
            log.size(), committed_.size()));
        return;
    }
    for (std::size_t i = 0; i < committed_.size(); ++i) {
        if (log[i] != committed_[i]) {
            violations_.push_back(format(
                "LEADER COMPLETENESS: node %u won term %llu missing the "
                "committed entry at index %zu",
                static_cast<unsigned>(node),
                static_cast<unsigned long long>(term), i + 1));
            return;
        }
    }
}

}  // namespace rsm::sim
