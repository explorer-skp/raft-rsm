#include "transport/frame.h"

namespace rsm::transport {

void writeLengthPrefix(std::uint32_t bodyLen, std::uint8_t* out) {
    out[0] = static_cast<std::uint8_t>(bodyLen >> 24);
    out[1] = static_cast<std::uint8_t>(bodyLen >> 16);
    out[2] = static_cast<std::uint8_t>(bodyLen >> 8);
    out[3] = static_cast<std::uint8_t>(bodyLen);
}

}  // namespace rsm::transport
