#pragma once

#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include "rpc/messages.h"

namespace rsm::transport {

inline constexpr std::size_t kLengthPrefixSize = 4;
// Largest legal frame body: envelope + max payload.
inline constexpr std::size_t kMaxFrameSize =
    rsm::rpc::kEnvelopeSize + rsm::rpc::kMaxPayloadSize;

// Writes the 4-byte big-endian length prefix into out[0..3].
void writeLengthPrefix(std::uint32_t bodyLen, std::uint8_t* out);

// Reassembles length-prefixed frames (4-byte big-endian body length, then
// body) from an arbitrarily fragmented/coalesced byte stream.
class FrameAssembler {
public:
    using FrameHandler = std::function<void(std::span<const std::uint8_t>)>;

    // Appends data and invokes onFrame once per completed frame body, in
    // order. Returns false on a protocol error (declared body length zero or
    // > kMaxFrameSize); the assembler is then poisoned and rejects all
    // further input — the caller must drop the connection.
    bool feed(std::span<const std::uint8_t> data, const FrameHandler& onFrame);

private:
    std::vector<std::uint8_t> buf_;
    bool poisoned_ = false;
};

}  // namespace rsm::transport
