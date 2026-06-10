#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "metrics/alloc_gate.h"
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
    // Appends data and invokes onFrame once per completed frame body, in
    // order. Returns false on a protocol error (declared body length zero or
    // > kMaxFrameSize); the assembler is then poisoned and rejects all
    // further input — the caller must drop the connection.
    //
    // Templated on the handler (Phase 7): a capturing lambda binds directly
    // with no std::function temporary — the per-read type-erasure heap
    // allocation was the rx thread's last per-message allocation.
    template <typename FrameHandler>
    bool feed(std::span<const std::uint8_t> data, const FrameHandler& onFrame) {
        if (poisoned_) return false;
        if (buf_.capacity() - buf_.size() < data.size()) {
            // Buffer growth tracks the connection's burst high-water mark
            // (bounded), not per-message churn — tagged accordingly for
            // the Phase 7 allocation discipline.
            const rsm::metrics::AllocRetention allocTag;
            buf_.reserve(buf_.size() + data.size());
        }
        buf_.insert(buf_.end(), data.begin(), data.end());

        std::size_t off = 0;
        while (buf_.size() - off >= kLengthPrefixSize) {
            const std::uint32_t len =
                (static_cast<std::uint32_t>(buf_[off]) << 24) |
                (static_cast<std::uint32_t>(buf_[off + 1]) << 16) |
                (static_cast<std::uint32_t>(buf_[off + 2]) << 8) |
                static_cast<std::uint32_t>(buf_[off + 3]);
            if (len == 0 || len > kMaxFrameSize) {
                poisoned_ = true;
                buf_.clear();
                return false;
            }
            if (buf_.size() - off < kLengthPrefixSize + len) break;
            onFrame(std::span<const std::uint8_t>(
                buf_.data() + off + kLengthPrefixSize, len));
            off += kLengthPrefixSize + len;
        }
        buf_.erase(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(off));
        return true;
    }

    // Discards buffered bytes and clears the poison flag, keeping the
    // buffer's capacity — for reusing one assembler across connections
    // (Phase 8 client hot path) without per-connection allocations.
    void reset() {
        buf_.clear();
        poisoned_ = false;
    }

private:
    std::vector<std::uint8_t> buf_;
    bool poisoned_ = false;
};

}  // namespace rsm::transport
