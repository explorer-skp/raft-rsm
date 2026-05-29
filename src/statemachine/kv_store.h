#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "statemachine/state_machine.h"

namespace rsm::statemachine {

// ---------------------------------------------------------------------------
// KV command and result wire-within-the-log formats (Phase 5; documented in
// DESIGN.md). A command is the opaque byte vector Raft replicates:
//
//   [0]  u64 clientId   (0 = no session: no dedup, nothing recorded)
//   [8]  u64 seqNo      (monotonic per client, starting at 1)
//   [16] u8  op         (1=PUT 2=GET 3=DELETE 4=CAS 5=APPEND)
//   [17] u32 keyLen, key bytes
//        u32 argLen, arg bytes    (PUT/APPEND: value; CAS: expected)
//        u32 arg2Len, arg2 bytes  (CAS only: new value)
// All little-endian; fields beyond an op's needs are absent. Trailing bytes
// or truncation make the command malformed.
//
// The result (apply()'s return, relayed verbatim in ClientReply.result) is a
// status byte followed by an optional payload:
//   'O' + value   GET hit (payload = value), APPEND (payload = new value)
//   'O'           PUT, DELETE of an existing key, CAS success
//   'N'           GET miss, DELETE of a missing key
//   'F'           CAS mismatch (no side effect)
//   'E'           malformed command (no side effect, never cached)
// ---------------------------------------------------------------------------

enum class KvOp : std::uint8_t {
    Put = 1,
    Get = 2,
    Delete = 3,
    Cas = 4,
    Append = 5,  // value += arg; non-idempotent on purpose (dedup tests)
};

inline constexpr char kKvOk = 'O';
inline constexpr char kKvNotFound = 'N';
inline constexpr char kKvCasFailed = 'F';
inline constexpr char kKvMalformed = 'E';

// Builds the command bytes. `arg2` is only meaningful for CAS.
Command encodeKvCommand(std::uint64_t clientId, std::uint64_t seqNo, KvOp op,
                        const std::string& key, const std::string& arg = {},
                        const std::string& arg2 = {});

// The replicated key-value store with the exactly-once session table in its
// applied state. apply() is deterministic: ordered maps, no clock, no RNG.
//
// Dedup happens HERE, at apply time, identically on every replica: a command
// whose seqNo <= the session's last applied seqNo returns the cached result
// and has no side effect. That is what makes a client retry of the same
// (clientId, seqNo) safe across a leader failover — the new leader's applied
// state already carries the session entry. Only the latest result per client
// is cached (one outstanding request per client; DESIGN.md).
class KVStateMachine final : public StateMachine {
public:
    std::string apply(const Command& cmd) override;

    // Serializable state for Phase 8 snapshotting: the KV map AND the
    // session table (both must survive a snapshot or dedup breaks after
    // log truncation). Deterministic byte-for-byte (ordered maps).
    std::vector<std::uint8_t> serialize() const;
    // Replaces the entire state. Returns false (state unchanged) on
    // malformed input.
    bool deserialize(const std::vector<std::uint8_t>& bytes);

    // Test/diagnostic accessors.
    std::optional<std::string> get(const std::string& key) const;
    std::size_t keyCount() const { return kv_.size(); }
    std::size_t sessionCount() const { return sessions_.size(); }

private:
    struct Session {
        std::uint64_t lastSeq = 0;
        std::string lastResult;
    };

    std::string applyOp(KvOp op, std::string key, std::string arg,
                        std::string arg2);

    std::map<std::string, std::string> kv_;
    std::map<std::uint64_t, Session> sessions_;
};

}  // namespace rsm::statemachine
