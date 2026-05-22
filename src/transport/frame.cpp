#include "transport/frame.h"

namespace rsm::transport {

void writeLengthPrefix(std::uint32_t bodyLen, std::uint8_t* out) {
    out[0] = static_cast<std::uint8_t>(bodyLen >> 24);
    out[1] = static_cast<std::uint8_t>(bodyLen >> 16);
    out[2] = static_cast<std::uint8_t>(bodyLen >> 8);
    out[3] = static_cast<std::uint8_t>(bodyLen);
}

bool FrameAssembler::feed(std::span<const std::uint8_t> data,
                          const FrameHandler& onFrame) {
    if (poisoned_) return false;
    buf_.insert(buf_.end(), data.begin(), data.end());

    std::size_t off = 0;
    while (buf_.size() - off >= kLengthPrefixSize) {
        const std::uint32_t len = (static_cast<std::uint32_t>(buf_[off]) << 24) |
                                  (static_cast<std::uint32_t>(buf_[off + 1]) << 16) |
                                  (static_cast<std::uint32_t>(buf_[off + 2]) << 8) |
                                  static_cast<std::uint32_t>(buf_[off + 3]);
        if (len == 0 || len > kMaxFrameSize) {
            poisoned_ = true;
            buf_.clear();
            return false;
        }
        if (buf_.size() - off < kLengthPrefixSize + len) break;
        onFrame(std::span<const std::uint8_t>(buf_.data() + off + kLengthPrefixSize, len));
        off += kLengthPrefixSize + len;
    }
    buf_.erase(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(off));
    return true;
}

}  // namespace rsm::transport
