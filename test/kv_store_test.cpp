// Phase 5 unit tests: KV semantics, apply-time exactly-once dedup, and the
// serializable state (KV map + session table) that Phase 8 snapshots need.

#include <string>

#include "doctest/doctest.h"
#include "statemachine/kv_store.h"

using rsm::statemachine::encodeKvCommand;
using rsm::statemachine::kKvCasFailed;
using rsm::statemachine::kKvMalformed;
using rsm::statemachine::kKvNotFound;
using rsm::statemachine::kKvOk;
using rsm::statemachine::KvOp;
using rsm::statemachine::KVStateMachine;

namespace {

// clientId 0 = sessionless (no dedup): used where dedup is not under test.
std::string raw(KVStateMachine& sm, KvOp op, const std::string& key,
                const std::string& arg = {}, const std::string& arg2 = {}) {
    return sm.apply(encodeKvCommand(0, 0, op, key, arg, arg2));
}

}  // namespace

TEST_CASE("kv: PUT/GET/DELETE round-trip and missing-key behavior") {
    KVStateMachine sm;
    CHECK(raw(sm, KvOp::Get, "k") == std::string{kKvNotFound});
    CHECK(raw(sm, KvOp::Put, "k", "v1") == std::string{kKvOk});
    CHECK(raw(sm, KvOp::Get, "k") == kKvOk + std::string("v1"));
    CHECK(raw(sm, KvOp::Put, "k", "v2") == std::string{kKvOk});  // overwrite
    CHECK(raw(sm, KvOp::Get, "k") == kKvOk + std::string("v2"));
    CHECK(raw(sm, KvOp::Delete, "k") == std::string{kKvOk});
    CHECK(raw(sm, KvOp::Get, "k") == std::string{kKvNotFound});
    CHECK(raw(sm, KvOp::Delete, "k") == std::string{kKvNotFound});
    // Empty value is a real value, distinct from absent.
    CHECK(raw(sm, KvOp::Put, "e", "") == std::string{kKvOk});
    CHECK(raw(sm, KvOp::Get, "e") == std::string{kKvOk});
}

TEST_CASE("kv: CAS succeeds only on exact match, absent reads as empty") {
    KVStateMachine sm;
    raw(sm, KvOp::Put, "k", "old");
    CHECK(raw(sm, KvOp::Cas, "k", "wrong", "new") ==
          std::string{kKvCasFailed});
    CHECK(raw(sm, KvOp::Get, "k") == kKvOk + std::string("old"));  // untouched
    CHECK(raw(sm, KvOp::Cas, "k", "old", "new") == std::string{kKvOk});
    CHECK(raw(sm, KvOp::Get, "k") == kKvOk + std::string("new"));
    // Absent key compares equal to "": create-if-absent.
    CHECK(raw(sm, KvOp::Cas, "fresh", "", "init") == std::string{kKvOk});
    CHECK(raw(sm, KvOp::Cas, "fresh2", "x", "init") ==
          std::string{kKvCasFailed});
    CHECK(raw(sm, KvOp::Get, "fresh2") == std::string{kKvNotFound});
}

TEST_CASE("kv: APPEND is non-idempotent and returns the new value") {
    KVStateMachine sm;
    CHECK(raw(sm, KvOp::Append, "k", "a") == kKvOk + std::string("a"));
    CHECK(raw(sm, KvOp::Append, "k", "b") == kKvOk + std::string("ab"));
}

TEST_CASE("kv: malformed commands are rejected without side effects") {
    KVStateMachine sm;
    CHECK(sm.apply({}) == std::string{kKvMalformed});  // too short
    CHECK(sm.apply({1, 2, 3}) == std::string{kKvMalformed});
    auto cmd = encodeKvCommand(0, 0, KvOp::Put, "k", "v");
    cmd[16] = 99;  // unknown op byte
    CHECK(sm.apply(cmd) == std::string{kKvMalformed});
    auto truncated = encodeKvCommand(0, 0, KvOp::Put, "k", "v");
    truncated.pop_back();  // value cut short
    CHECK(sm.apply(truncated) == std::string{kKvMalformed});
    auto trailing = encodeKvCommand(0, 0, KvOp::Get, "k");
    trailing.push_back(0);  // junk after a well-formed command
    CHECK(sm.apply(trailing) == std::string{kKvMalformed});
    CHECK(sm.keyCount() == 0);
    CHECK(sm.sessionCount() == 0);
}

TEST_CASE("dedup: a duplicate (clientId, seqNo) has one side effect and "
          "returns the cached result every time") {
    KVStateMachine sm;
    const auto cmd = encodeKvCommand(7, 1, KvOp::Append, "k", "x");
    const auto first = sm.apply(cmd);
    CHECK(first == kKvOk + std::string("x"));
    // Same identity re-applied (a client retry replicated again): no second
    // append, identical cached result.
    CHECK(sm.apply(cmd) == first);
    CHECK(sm.apply(cmd) == first);
    CHECK(sm.get("k") == std::string("x"));

    // A higher seqNo applies normally.
    CHECK(sm.apply(encodeKvCommand(7, 2, KvOp::Append, "k", "y")) ==
          kKvOk + std::string("xy"));
    // An older seqNo returns the (latest) cached result, no side effect.
    CHECK(sm.apply(cmd) == kKvOk + std::string("xy"));
    CHECK(sm.get("k") == std::string("xy"));

    // Sessions are per client: another client's seqNo 1 is not a duplicate.
    CHECK(sm.apply(encodeKvCommand(8, 1, KvOp::Append, "k", "z")) ==
          kKvOk + std::string("xyz"));
    CHECK(sm.sessionCount() == 2);
}

TEST_CASE("dedup: GET results are cached by the session like any command") {
    KVStateMachine sm;
    raw(sm, KvOp::Put, "k", "v1");
    const auto getCmd = encodeKvCommand(9, 1, KvOp::Get, "k");
    CHECK(sm.apply(getCmd) == kKvOk + std::string("v1"));
    raw(sm, KvOp::Put, "k", "v2");
    // The retry of seq 1 returns what THAT request observed (linearizable:
    // the read happened at its log position), not the newer value.
    CHECK(sm.apply(getCmd) == kKvOk + std::string("v1"));
}

TEST_CASE("serializable state includes both the KV map and the dedup table") {
    KVStateMachine sm;
    sm.apply(encodeKvCommand(7, 1, KvOp::Put, "a", "1"));
    sm.apply(encodeKvCommand(7, 2, KvOp::Append, "b", "2"));
    sm.apply(encodeKvCommand(8, 5, KvOp::Get, "a"));
    const auto bytes = sm.serialize();

    // Same KV contents but different session state must serialize
    // differently — the dedup table IS part of the state.
    KVStateMachine kvOnly;
    raw(kvOnly, KvOp::Put, "a", "1");
    raw(kvOnly, KvOp::Append, "b", "2");
    CHECK(kvOnly.get("a") == sm.get("a"));
    CHECK(kvOnly.serialize() != bytes);

    // Round-trip restores both maps; dedup keeps working from restored
    // state: the old seqNo is still a duplicate after deserialize.
    KVStateMachine restored;
    REQUIRE(restored.deserialize(bytes));
    CHECK(restored.serialize() == bytes);
    CHECK(restored.get("a") == std::string("1"));
    CHECK(restored.sessionCount() == 2);
    CHECK(restored.apply(encodeKvCommand(7, 2, KvOp::Append, "b", "2")) ==
          kKvOk + std::string("2"));  // cached result, not a second append
    CHECK(restored.get("b") == std::string("2"));

    // Malformed snapshot bytes are rejected without clobbering state.
    KVStateMachine victim;
    raw(victim, KvOp::Put, "x", "y");
    auto bad = bytes;
    bad.pop_back();
    CHECK_FALSE(victim.deserialize(bad));
    CHECK(victim.get("x") == std::string("y"));
}
