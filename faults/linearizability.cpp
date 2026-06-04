#include "faults/linearizability.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>

namespace rsm::sim {

using rsm::statemachine::kKvCasFailed;
using rsm::statemachine::kKvNotFound;
using rsm::statemachine::kKvOk;
using rsm::statemachine::KvOp;

std::string kvModelApply(KvRegister& reg, const ClientOp& op) {
    // Mirrors KVStateMachine::applyOp exactly (src/statemachine/kv_store.cpp).
    switch (op.op) {
        case KvOp::Put:
            reg = {true, op.arg};
            return {kKvOk};
        case KvOp::Get:
            if (!reg.present) return {kKvNotFound};
            return kKvOk + reg.value;
        case KvOp::Delete: {
            const bool was = reg.present;
            reg = {false, {}};
            return was ? std::string{kKvOk} : std::string{kKvNotFound};
        }
        case KvOp::Cas: {
            const std::string current = reg.present ? reg.value : "";
            if (current != op.arg) return {kKvCasFailed};
            reg = {true, op.arg2};
            return {kKvOk};
        }
        case KvOp::Append:
            if (!reg.present) reg = {true, {}};
            reg.value += op.arg;
            return kKvOk + reg.value;
    }
    return {rsm::statemachine::kKvMalformed};
}

namespace {

constexpr std::int64_t kInf = std::numeric_limits<std::int64_t>::max();

// WGL search over one key's operations. Returns true iff a linearization
// exists. n is small per key by workload design; memoization on
// (mask, register) keeps revisits out.
class KeySearch {
public:
    explicit KeySearch(std::vector<const ClientOp*> ops)
        : ops_(std::move(ops)), words_((ops_.size() + 63) / 64) {}

    bool linearizable() {
        return dfs(std::vector<std::uint64_t>(words_, 0), KvRegister{});
    }

private:
    bool taken(const std::vector<std::uint64_t>& mask, std::size_t i) const {
        return (mask[i / 64] >> (i % 64)) & 1;
    }

    bool dfs(std::vector<std::uint64_t> mask, KvRegister reg) {
        // Earliest response among completed, still-unlinearized ops: nothing
        // invoked after it may be linearized before it (real-time order).
        std::int64_t minReturn = kInf;
        bool allCompletedDone = true;
        for (std::size_t i = 0; i < ops_.size(); ++i) {
            if (taken(mask, i) || !ops_[i]->complete()) continue;
            allCompletedDone = false;
            minReturn = std::min(minReturn, ops_[i]->returnMs);
        }
        if (allCompletedDone) return true;  // pending ops may simply never run

        if (!visited_.emplace(memoKey(mask, reg)).second) return false;

        for (std::size_t i = 0; i < ops_.size(); ++i) {
            if (taken(mask, i)) continue;
            const ClientOp& op = *ops_[i];
            if (op.invokeMs > minReturn) continue;  // would violate real time
            KvRegister next = reg;
            const std::string result = kvModelApply(next, op);
            // A completed op's observed result must match the model; a
            // pending op's effect is unconstrained (nobody saw its result).
            if (op.complete() && result != op.result) continue;
            auto nextMask = mask;
            nextMask[i / 64] |= 1ULL << (i % 64);
            if (dfs(std::move(nextMask), std::move(next))) return true;
        }
        return false;
    }

    std::string memoKey(const std::vector<std::uint64_t>& mask,
                        const KvRegister& reg) const {
        std::string key;
        key.reserve(words_ * 8 + 2 + reg.value.size());
        for (const std::uint64_t w : mask) {
            for (int b = 0; b < 8; ++b) {
                key.push_back(static_cast<char>(w >> (8 * b)));
            }
        }
        key.push_back(reg.present ? '\1' : '\0');
        key.append(reg.value);
        return key;
    }

    std::vector<const ClientOp*> ops_;
    std::size_t words_;
    std::set<std::string> visited_;
};

}  // namespace

LinearizabilityResult checkLinearizable(const std::vector<ClientOp>& history) {
    std::map<std::string, std::vector<const ClientOp*>> byKey;
    for (const ClientOp& op : history) byKey[op.key].push_back(&op);
    for (auto& [key, ops] : byKey) {
        // Deterministic op order within a key (stable input order is fine,
        // but make it explicit): by invocation, then identity.
        std::sort(ops.begin(), ops.end(),
                  [](const ClientOp* a, const ClientOp* b) {
                      if (a->invokeMs != b->invokeMs) {
                          return a->invokeMs < b->invokeMs;
                      }
                      if (a->clientId != b->clientId) {
                          return a->clientId < b->clientId;
                      }
                      return a->seqNo < b->seqNo;
                  });
        if (!KeySearch(ops).linearizable()) {
            return {false, "history is not linearizable for key \"" + key +
                               "\" (" + std::to_string(ops.size()) + " ops)"};
        }
    }
    return {true, {}};
}

}  // namespace rsm::sim
