#pragma once

#include <optional>
#include <string>

#include "raft/persistent_state.h"

namespace rsm::storage {

using rsm::rpc::NodeId;
using rsm::rpc::Term;

// Durable PersistentState (Phase 4): currentTerm + votedFor in one
// fixed-size metadata file, replaced atomically on every save() via
// write-temp → fsync(temp) → rename → fsync(dir), so a crash at any instant
// leaves either the previous or the new metadata on disk, never a torn mix.
// save() returns only once the new state is durable, which is what lets the
// Raft core send messages that depend on it immediately afterwards.
//
// File layout (`meta`, 20 bytes, little-endian; mirrored in DESIGN.md):
//   [0]  u32 magic 0x4D4D5352 ("RSMM")
//   [4]  u8  version = 1
//   [5]  u64 currentTerm
//   [13] u8  hasVote (0 or 1; anything else is corruption)
//   [14] u16 votedFor (0 when hasVote == 0)
//   [16] u32 CRC32C over bytes [0, 16)
class DurablePersistentState final : public rsm::raft::PersistentState {
public:
    // Opens `dir`/meta, restoring term/vote; absent file (first ever start)
    // initializes to term 0 / no vote. A leftover meta.tmp from a crash
    // between temp-write and rename is never valid state: it is removed and
    // the previous metadata wins. Throws on a corrupt metadata file.
    explicit DurablePersistentState(const std::string& dir);

    Term currentTerm() const override { return term_; }
    std::optional<NodeId> votedFor() const override { return votedFor_; }
    void save(Term term, std::optional<NodeId> votedFor) override;

private:
    std::string dir_;
    std::string path_;
    std::string tmpPath_;
    Term term_ = 0;
    std::optional<NodeId> votedFor_;
};

}  // namespace rsm::storage
