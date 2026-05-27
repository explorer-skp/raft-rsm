#include "storage/crc32c.h"

#include <array>

namespace rsm::storage {

namespace {

constexpr std::uint32_t kPoly = 0x82F63B78u;  // CRC32C, reflected

constexpr std::array<std::uint32_t, 256> makeTable() {
    std::array<std::uint32_t, 256> t{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (kPoly ^ (c >> 1)) : (c >> 1);
        }
        t[i] = c;
    }
    return t;
}

constexpr std::array<std::uint32_t, 256> kTable = makeTable();

}  // namespace

std::uint32_t crc32c(const std::uint8_t* data, std::size_t len,
                     std::uint32_t crc) {
    crc = ~crc;
    for (std::size_t i = 0; i < len; ++i) {
        crc = kTable[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return ~crc;
}

}  // namespace rsm::storage
