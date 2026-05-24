#pragma once

#include <optional>

#include "rpc/messages.h"

namespace rsm::raft {

using rsm::rpc::NodeId;
using rsm::rpc::Term;

// Raft's persistent state (currentTerm, votedFor) behind a seam: memory-backed
// in Phase 2, swapped for a durable fsync-ing implementation in Phase 4 with
// no logic change. save() takes both fields in one call so the durable
// implementation can write them atomically before any RPC reply goes out.
struct PersistentState {
    virtual Term currentTerm() const = 0;
    virtual std::optional<NodeId> votedFor() const = 0;
    virtual void save(Term term, std::optional<NodeId> votedFor) = 0;
    virtual ~PersistentState() = default;
};

class InMemoryPersistentState final : public PersistentState {
public:
    Term currentTerm() const override { return term_; }
    std::optional<NodeId> votedFor() const override { return votedFor_; }
    void save(Term term, std::optional<NodeId> votedFor) override {
        term_ = term;
        votedFor_ = votedFor;
    }

private:
    Term term_ = 0;
    std::optional<NodeId> votedFor_;
};

}  // namespace rsm::raft
