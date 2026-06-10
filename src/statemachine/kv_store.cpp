#include "statemachine/kv_store.h"

#include "metrics/alloc_gate.h"

#include "rpc/wire.h"

namespace rsm::statemachine {

namespace {

void putString(std::vector<std::uint8_t>& out, const std::string& s) {
    const auto len = static_cast<std::uint32_t>(s.size());
    out.push_back(static_cast<std::uint8_t>(len));
    out.push_back(static_cast<std::uint8_t>(len >> 8));
    out.push_back(static_cast<std::uint8_t>(len >> 16));
    out.push_back(static_cast<std::uint8_t>(len >> 24));
    out.insert(out.end(), s.begin(), s.end());
}

void putU64(std::vector<std::uint8_t>& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
    }
}

bool readString(rsm::rpc::Reader& r, std::string& out) {
    const std::uint32_t len = r.u32();
    if (!r.ok() || len > r.remaining()) return false;
    std::vector<std::uint8_t> bytes;
    if (!r.readBytes(bytes, len)) return false;
    out.assign(bytes.begin(), bytes.end());
    return true;
}

bool opNeedsArg(KvOp op) {
    return op == KvOp::Put || op == KvOp::Cas || op == KvOp::Append;
}

}  // namespace

Command encodeKvCommand(std::uint64_t clientId, std::uint64_t seqNo, KvOp op,
                        const std::string& key, const std::string& arg,
                        const std::string& arg2) {
    Command cmd;
    encodeKvCommandInto(cmd, clientId, seqNo, op, key, arg, arg2);
    return cmd;
}

void encodeKvCommandInto(Command& out, std::uint64_t clientId,
                         std::uint64_t seqNo, KvOp op, const std::string& key,
                         const std::string& arg, const std::string& arg2) {
    out.clear();
    putU64(out, clientId);
    putU64(out, seqNo);
    out.push_back(static_cast<std::uint8_t>(op));
    putString(out, key);
    if (opNeedsArg(op)) putString(out, arg);
    if (op == KvOp::Cas) putString(out, arg2);
}

std::string KVStateMachine::apply(const Command& cmd) {
    // All allocations in here are application state (the KV map, the
    // session table) or its result payload — data-proportional, exempted
    // by the Phase 7 allocation test (plumbing must be zero; this is not
    // plumbing).
    const rsm::metrics::AllocRetention allocTag;
    rsm::rpc::Reader r(cmd);
    const std::uint64_t clientId = r.u64();
    const std::uint64_t seqNo = r.u64();
    const std::uint8_t opByte = r.u8();
    if (!r.ok() || opByte < 1 || opByte > 5) return {kKvMalformed};
    const auto op = static_cast<KvOp>(opByte);
    std::string key, arg, arg2;
    if (!readString(r, key)) return {kKvMalformed};
    if (opNeedsArg(op) && !readString(r, arg)) return {kKvMalformed};
    if (op == KvOp::Cas && !readString(r, arg2)) return {kKvMalformed};
    if (!r.exhausted()) return {kKvMalformed};

    // Exactly-once: duplicates (same or older seqNo) return the cached
    // result with NO side effect, deterministically on every replica.
    // clientId 0 opts out of sessions (internal/test commands).
    if (clientId != 0) {
        const auto it = sessions_.find(clientId);
        if (it != sessions_.end() && seqNo <= it->second.lastSeq) {
            return it->second.lastResult;
        }
    }
    std::string result =
        applyOp(op, std::move(key), std::move(arg), std::move(arg2));
    if (clientId != 0) {
        sessions_[clientId] = Session{seqNo, result};
    }
    return result;
}

std::string KVStateMachine::applyOp(KvOp op, std::string key, std::string arg,
                                    std::string arg2) {
    switch (op) {
        case KvOp::Put:
            kv_[std::move(key)] = std::move(arg);
            return {kKvOk};
        case KvOp::Get: {
            const auto it = kv_.find(key);
            if (it == kv_.end()) return {kKvNotFound};
            return kKvOk + it->second;
        }
        case KvOp::Delete:
            return kv_.erase(key) ? std::string{kKvOk}
                                  : std::string{kKvNotFound};
        case KvOp::Cas: {
            const auto it = kv_.find(key);
            // CAS against a missing key succeeds only if expected is empty
            // (treating absent as ""): create-if-absent in one primitive.
            const std::string current = it == kv_.end() ? "" : it->second;
            if (current != arg) return {kKvCasFailed};
            kv_[std::move(key)] = std::move(arg2);
            return {kKvOk};
        }
        case KvOp::Append: {
            auto& value = kv_[std::move(key)];
            value += arg;
            return kKvOk + value;
        }
    }
    return {kKvMalformed};  // unreachable; op validated by caller
}

std::vector<std::uint8_t> KVStateMachine::serialize() const {
    std::vector<std::uint8_t> out;
    putU64(out, kv_.size());
    for (const auto& [key, value] : kv_) {  // std::map: deterministic order
        putString(out, key);
        putString(out, value);
    }
    putU64(out, sessions_.size());
    for (const auto& [clientId, session] : sessions_) {
        putU64(out, clientId);
        putU64(out, session.lastSeq);
        putString(out, session.lastResult);
    }
    return out;
}

bool KVStateMachine::deserialize(const std::vector<std::uint8_t>& bytes) {
    rsm::rpc::Reader r(bytes);
    std::map<std::string, std::string> kv;
    std::map<std::uint64_t, Session> sessions;
    const std::uint64_t kvCount = r.u64();
    for (std::uint64_t i = 0; i < kvCount && r.ok(); ++i) {
        std::string key, value;
        if (!readString(r, key) || !readString(r, value)) return false;
        kv[std::move(key)] = std::move(value);
    }
    const std::uint64_t sessCount = r.u64();
    for (std::uint64_t i = 0; i < sessCount && r.ok(); ++i) {
        const std::uint64_t clientId = r.u64();
        Session s;
        s.lastSeq = r.u64();
        if (!readString(r, s.lastResult)) return false;
        sessions[clientId] = std::move(s);
    }
    if (!r.exhausted()) return false;
    kv_ = std::move(kv);
    sessions_ = std::move(sessions);
    return true;
}

std::optional<std::string> KVStateMachine::get(const std::string& key) const {
    const auto it = kv_.find(key);
    if (it == kv_.end()) return std::nullopt;
    return it->second;
}

}  // namespace rsm::statemachine
