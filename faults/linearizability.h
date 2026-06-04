#pragma once

// Linearizability checker for the KV client history (Phase 6 decision
// point 1). Algorithm: partition the history by key — keys are independent
// registers, so a history is linearizable iff each per-key subhistory is —
// then run a Wing-Gong/Lowe-style (WGL/Knossos) linearization search per
// key with memoization on (set-of-linearized-ops, register state).
//
// Soundness (no false accepts) is the property that matters: an order is
// accepted only if it (a) respects real-time precedence — an operation may
// be chosen next only if it was invoked no later than the earliest response
// among the remaining completed operations — and (b) every completed
// operation's recorded result exactly matches the sequential KV model
// (mirroring KVStateMachine::applyOp byte for byte, including CAS's
// absent-means-"" rule and APPEND returning the full new value).
// Incomplete operations (invoked, never acknowledged) may linearize at any
// point after their invocation or never — both must be explored, since the
// command may or may not have committed.

#include <cstdint>
#include <string>
#include <vector>

#include "statemachine/kv_store.h"

namespace rsm::sim {

struct ClientOp {
    std::uint64_t clientId = 0;
    std::uint64_t seqNo = 0;
    rsm::statemachine::KvOp op{};
    std::string key;
    std::string arg;   // PUT/APPEND value; CAS expected
    std::string arg2;  // CAS new value
    std::int64_t invokeMs = 0;
    std::int64_t returnMs = -1;  // -1: never completed (pending at run end)
    std::string result;          // KV result string; meaningful iff complete

    bool complete() const { return returnMs >= 0; }
};

struct LinearizabilityResult {
    bool ok = false;
    std::string explanation;  // which key failed, when ok=false
};

LinearizabilityResult checkLinearizable(const std::vector<ClientOp>& history);

// The sequential KV model for one key, exposed for the checker self-tests.
struct KvRegister {
    bool present = false;
    std::string value;
};
std::string kvModelApply(KvRegister& reg, const ClientOp& op);

}  // namespace rsm::sim
