#pragma once

#include <cassert>
#include <cstddef>
#include <utility>
#include <vector>

#include "rpc/messages.h"

namespace rsm::storage {

using rsm::rpc::LogEntry;
using rsm::rpc::LogIndex;
using rsm::rpc::Term;

// The replicated log behind the seam Phase 4 will make durable.
//
// Indexing convention (used everywhere in this project): indices are
// 1-based. Index 0 is the empty sentinel — an empty log has lastIndex() == 0
// and lastTerm() == 0, and termAt(0) == 0 so a prevLogIndex of 0 passes the
// AppendEntries consistency check trivially.
struct RaftLog {
    // Appends at lastIndex()+1 onward.
    virtual void append(std::vector<LogEntry> entries) = 0;
    // Term of the entry at i; 0 for i == 0. Precondition: i <= lastIndex().
    virtual Term termAt(LogIndex i) const = 0;
    virtual const LogEntry& entryAt(LogIndex i) const = 0;  // 1 <= i <= last
    // All entries in [i, lastIndex()]; empty if i > lastIndex().
    virtual std::vector<LogEntry> entriesFrom(LogIndex i) const = 0;
    // Deletes [i, lastIndex()]. No-op if i > lastIndex().
    virtual void truncateSuffixFrom(LogIndex i) = 0;
    virtual LogIndex lastIndex() const = 0;
    virtual Term lastTerm() const = 0;
    virtual ~RaftLog() = default;
};

class InMemoryLog final : public RaftLog {
public:
    void append(std::vector<LogEntry> entries) override {
        entries_.insert(entries_.end(),
                        std::make_move_iterator(entries.begin()),
                        std::make_move_iterator(entries.end()));
    }

    Term termAt(LogIndex i) const override {
        if (i == 0) return 0;
        assert(i <= lastIndex());
        if (i > lastIndex()) return 0;  // defensive in release builds
        return entries_[i - 1].term;
    }

    const LogEntry& entryAt(LogIndex i) const override {
        assert(i >= 1 && i <= lastIndex());
        return entries_[i - 1];
    }

    std::vector<LogEntry> entriesFrom(LogIndex i) const override {
        if (i < 1) i = 1;
        if (i > lastIndex()) return {};
        return {entries_.begin() + static_cast<std::ptrdiff_t>(i - 1),
                entries_.end()};
    }

    void truncateSuffixFrom(LogIndex i) override {
        if (i < 1) i = 1;
        if (i > lastIndex()) return;
        entries_.resize(static_cast<std::size_t>(i - 1));
    }

    LogIndex lastIndex() const override { return entries_.size(); }

    Term lastTerm() const override {
        return entries_.empty() ? 0 : entries_.back().term;
    }

private:
    std::vector<LogEntry> entries_;
};

}  // namespace rsm::storage
