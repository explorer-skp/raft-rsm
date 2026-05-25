#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace rsm::statemachine {

using Command = std::vector<std::uint8_t>;

// Spec §4.5. apply() must be deterministic: no wall-clock reads, no RNG, no
// iteration-order dependence — replicas apply the same commands in the same
// order and must reach identical states.
struct StateMachine {
    virtual std::string apply(const Command& cmd) = 0;
    virtual ~StateMachine() = default;
};

// Phase 3's trivial deterministic state machine: records the ordered
// sequence of applied commands so tests can assert identical apply order
// across replicas. The real KV store is Phase 5.
class RecordingStateMachine final : public StateMachine {
public:
    std::string apply(const Command& cmd) override {
        applied_.push_back(cmd);
        return {};
    }
    const std::vector<Command>& applied() const { return applied_; }

private:
    std::vector<Command> applied_;
};

}  // namespace rsm::statemachine
